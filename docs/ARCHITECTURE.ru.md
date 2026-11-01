# Текущая архитектура ColdStore и ответственность компонентов

Этот документ описывает текущую кодовую базу, а не полную целевую архитектуру. Целевой проект сохраняется в `docs/modules/` и `docs/DESIGN.ru.md`.

## 1. Позиционирование системы

ColdStore — это лента-ориентированная система холодного архива, совместимая с S3. Текущая реализация сфокусирована на том, чтобы сделать проверяемыми следующие замкнутые контуры:

- HTTP-доступ S3
- Бизнес-оркестрация в Scheduler
- Конечный автомат и персистентность в Metadata
- Data plane staging/restored в Cache
- Абстрактный интерфейс чтения/записи Tape
- Атомарная фиксация метаданных по завершении архивации
- Чтение из кэша после Restore

Сейчас это не полноценная многоузловая распределённая система. Многоузловой `persistent_raft` в Metadata сейчас выполняет fail-fast, чтобы несколько локальных состояний не выдавали себя за безопасный кластер Raft.

## 2. Обзор компонентов

```text
S3 Client
   |
   | HTTP/S3
   v
Gateway
   |
   | gRPC SchedulerService
   v
Scheduler
   |                 |                  |
   | MetadataService | CacheService     | TapeService
   v                 v                  v
Metadata          Cache              Tape
```

## 3. Ответственность компонентов

| Компонент | crate | Ответственность | Не отвечает |
|------|-------|------|--------|
| Gateway | `crates/gateway` | Маршрутизация S3 HTTP, разбор Range/условных запросов, разбор Restore XML, сопоставление кодов ошибок S3, пересылка в Scheduler | Не обращается напрямую к Metadata/Cache/Tape, не принимает решения об архивации |
| Scheduler | `crates/scheduler` | Бизнес-ядро; PUT/GET/HEAD/DELETE/List объектов; запросы Restore; сканирование архивации; планирование восстановления; оркестрация Metadata/Cache/Tape | Не сохраняет данные объектов, не управляет напрямую файлами на диске |
| Metadata | `crates/metadata` | Конечный автомат метаданных bucket/object/task/tape/worker; CAS завершения архивации; одноузловой путь персистентности; регистрация worker и heartbeat | Сейчас не обеспечивает настоящую многоузловую репликацию Raft |
| Cache | `crates/cache` | Запись в staging; кэш восстановленных данных (restored); бюджет ёмкости; вытеснение restored; backpressure для staging | Не хранит авторитетные метаданные, не вытесняет staging |
| Tape | `crates/tape` | Абстракция TapeService; запись/чтение bundle; интерфейс загрузки/выгрузки накопителя | Сейчас подключение реального оборудования — не полноценный производственный режим |
| Common | `crates/common` | Структуры конфигурации, значения по умолчанию, общие типы | Не содержит бизнес-конечных автоматов |
| Proto | `crates/proto` | Контракты gRPC/protobuf | Не содержит рантайм-логику |

## 4. Топология по умолчанию

Конфигурация по умолчанию — безопасная одноузловая топология metadata:

```text
Metadata:  127.0.0.1:21001
Scheduler: 127.0.0.1:22001
Cache:     127.0.0.1:23001
Tape:      127.0.0.1:24001
Gateway:   0.0.0.0:9000
```

`MetadataConfig` по умолчанию:

- `consensus_mode = persistent_raft`
- `cluster = 1:127.0.0.1:21001`
- `raft_state_path = data_path/node-{node_id}/raft`, если не задано явно

Многоузловой кластер metadata сейчас не является путём по умолчанию. Конфигурация многоузлового `persistent_raft` приводит к отказу при запуске, поскольку настоящий распределённый Raft runtime ещё не завершён.

## 5. Основные потоки данных

### 5.1 Запись PutObject

```text
Client PUT
  -> Gateway
  -> Scheduler.PutObject
  -> Cache.PutStaging
  -> Metadata.PutObject(ColdPending)
  -> Response(etag/version_id)
```

Ключевая семантика:

- После записи объект находится в состоянии `ColdPending`.
- Данные попадают в staging-зону Cache.
- staging — это durable buffer на пути записи; он не может быть автоматически вытеснен eviction.
- При превышении бюджета staging возвращается backpressure по ёмкости; upstream следует повторить попытку или дождаться дренажа архивации.

### 5.2 Архивация Archive

```text
Scheduler archive loop
  -> Cache.ListStagingKeys
  -> Metadata.HeadObject
  -> Cache.GetStaging
  -> проверка совпадения identity/size/checksum staging с metadata
  -> Tape.WriteBundle
  -> Metadata.CompleteArchiveObject(expected_*)
  -> Cache.DeleteStaging
```

`CompleteArchiveObject` — ключевая граница согласованности при завершении архивации. Она выполняется внутри одного apply в конечном автомате Metadata:

- проверка версии объекта
- проверка expected size
- проверка expected checksum
- проверка expected storage class
- проверка expected updated_at как generation token
- запись archive bundle
- обновление archive location объекта
- перевод storage class объекта в `Cold`

Если запись на tape выполнена успешно, а CAS завершился ошибкой, metadata неверно не зафиксирует результат архивации. На физическом tape может остаться невидимый (ни на что не ссылаемый) bundle — это остаточная проблема, которую должны решить будущие audit/GC.

### 5.3 Восстановление RestoreObject

```text
Client RestoreObject
  -> Gateway
  -> Scheduler.RestoreObject
  -> Metadata обновляет restore-статус / создаёт RecallTask
  -> Response 202/200/409

Scheduler recall loop
  -> Metadata.ListPendingRecallTasks
  -> Tape.ReadBundle
  -> проверка checksum/size
  -> Cache.PutRestored
  -> Metadata обновляет статусы task/object restore
```

Ключевая семантика:

- Если Expedited недоступен, возвращается S3-ошибка `GlacierExpeditedRetrievalNotAvailable`.
- Превышение бюджета restored в Cache — это повторяемый (retryable) backpressure.
- У данных restored есть TTL; по истечении они могут быть освобождены.

### 5.4 Чтение GetObject

```text
Client GET
  -> Gateway
  -> Scheduler.GetObject
  -> Metadata.Head/GetObject
  -> если объект не в состоянии Restore completed: InvalidObjectState
  -> Cache.Get
  -> Gateway HTTP response
```

Текущие ограничения:

- В потоковых интерфейсах Gateway и Scheduler всё ещё есть пути, полностью агрегирующие данные в `Vec`; усиление памяти для больших объектов пока требует рефакторинга.
- Чтение по Range сейчас не является полноценной производственной реализацией с полным проталкиванием (pushdown) в бэкенд.

### 5.5 ListObjects

```text
Client ListObjects
  -> Gateway parse prefix/marker/delimiter/max-keys
  -> Scheduler.ListObjects
  -> Metadata.ListObjects
  -> Gateway XML response
```

Текущая реализация поддерживает базовый list в стиле V1, delimiter/common prefixes и верхнюю границу `max-keys`. Полноценный ListObjectsV2 остаётся задачей на будущее.

## 6. Границы согласованности

| Сценарий | Текущая стратегия |
------|----------|
| Многоузловая Metadata | Настоящий распределённый Raft не завершён, многоузловой режим работает на fail-fast |
| Фиксация Archive | Используется одна команда конечного автомата `CompleteArchiveObject(expected_*)` |
| Давление на ёмкость Staging | staging не вытесняется, возвращается `ResourceExhausted` |
| Давление на ёмкость Restored | restored вытеснять можно; при отсутствии жертвы возвращается backpressure |
| Перезапись в Cache | Сначала запись в новое storage, затем атомарное переключение индекса после успеха, затем best-effort удаление старого storage |
| Дублирующийся key при перестроении Cache | Выбирается более свежая entry; проигравший дубликат удаляется best-effort и логируется как warning |
| Ограничение интенсивности в Gateway | Для data plane S3 возвращается `503 SlowDown + Retry-After` |

## 7. Интерфейсы между компонентами

### Gateway -> Scheduler

Интерфейс: `SchedulerService`

Назначение:

- PUT/GET/HEAD/DELETE
- RestoreObject
- ListObjects/ListBuckets
- CRUD над bucket

Gateway выполняет только адаптацию протокола и не обходит Scheduler.

### Scheduler -> Metadata

Интерфейс: `MetadataService`

Назначение:

- чтение и запись метаданных bucket/object
- управление archive bundle/task/recall task
- атомарная фиксация архивации `CompleteArchiveObject`
- чтение и запись состояний worker/tape

Scheduler — единственный оркестратор записи metadata на бизнес-пути.

### Scheduler -> Cache

Интерфейс: `CacheService`

Назначение:

- `PutStaging`: запись данных, ожидающих архивации
- `GetStaging`: чтение staging во время архивации
- `DeleteStaging`: очистка staging после успешной фиксации архивации
- `PutRestored`: запись restored после восстановления
- `Get`: GET размороженного объекта
- `Stats`: статистика кэша

### Scheduler -> Tape

Интерфейс: `TapeService`

Назначение:

- Acquire/Release drive
- Load/Unload tape
- WriteBundle
- ReadBundle

Tape не пишет в Metadata напрямую. После успешной записи на tape Scheduler вызывает атомарный интерфейс фиксации в Metadata.

## 8. Модель состояний

### Object storage class

```text
ColdPending -> Cold
```

- `ColdPending`: объект записан в staging и ожидает архивации.
- `Cold`: объект заархивирован, в metadata есть archive location.

### Restore status

```text
Pending -> WaitingForMedia -> InProgress -> Completed
                                  |
                                  v
                                Failed
Completed -> Expired
```

Текущая реализация уже содержит базовые переходы состояний, но атомарность состояний task и object restore, а также работу reconciler, нужно усиливать дальше.

### Cache category

```text
Staging:  write path buffer, no eviction
Restored: read cache, evictable
```

## 9. Что ещё нужно усилить

| Приоритет | Задача |
--------|------|
| P0 | Настоящий распределённый Raft runtime, leader redirect, quorum commit, log replay |
| P0 | Атомарность Restore task и restore-статуса объекта, чтобы исключить pending-without-task |
| P1 | Настоящее потоственное чтение/запись больших объектов в Gateway/Scheduler/Cache для снижения усиления памяти |
| P1 | Audit/GC сиротских bundle на Tape: обработка случая, когда запись на tape удалась, а CAS в metadata — нет |
| P1 | ListObjectsV2, идемпотентность DeleteObject и другие детали совместимости с S3 |
| P2 | Admin Console, полноценный OpenTelemetry, производственная валидация на реальном SPDK и реальной ленточной библиотеке |

## 10. Как пользоваться документацией

- При оценке текущей реализации следует опираться на этот документ и на код.
- `docs/modules/*` сохраняют более полный целевой проект и могут описывать возможности, которые ещё не реализованы.
- Если документ противоречит коду, необходимо обновить этот документ либо пометить в модульной документации как «целевое состояние».
