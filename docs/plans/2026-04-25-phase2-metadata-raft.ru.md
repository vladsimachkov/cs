# План реализации Phase 2: персистентность метаданных и Raft

> **Для Hermes:** использовать навык subagent-driven-development, чтобы реализовывать этот план задача за задачей.

**Цель:** развить подсистему метаданных ColdStore из текущей in-process модели состояний с локальным снимком в надёжный кластер метаданных на OpenRaft + RocksDB.

**Архитектура:** сохранить существующую семантику `MetadataState` как единственный источник истины. На Phase 2A сначала обеспечить персистентность модели состояний через включаемый локальный бинарный снимок — это уже реализовано в `MetadataServiceImpl::new_with_snapshot(config, path)`. На Phase 2B те же мутации оборачиваются в команды Raft, применяются через модель состояний OpenRaft, а данные журнала Raft и модели состояний хранятся в RocksDB/openraft-rocksstore.

**Технологический стек:** Rust, tonic, prost, tokio, OpenRaft, RocksDB/openraft-rocksstore, существующие сообщения `coldstore-proto`.

---

## Текущая база

Текущий код содержит:

- `crates/metadata/src/service.rs`
  - `MetadataState` с объектами, bucket'ами, archive bundle'ами, задачами архивации, задачами восстановления, картриджами, а также реестрами воркеров планировщика, кэша и лент.
  - `MetadataServiceImpl::new(config)` — поведение Phase 1 полностью в памяти.
  - `MetadataServiceImpl::new_with_snapshot(config, snapshot_path)` — включаемая персистентность снимка одного узла для Phase 2A.
  - Кодирование и декодирование бинарного снимка на существующих кодированиях сообщений prost.
- Модульный тест:
  - `persistent_snapshot_survives_service_restart`

Безопасная команда проверки:

```bash
cargo test -p coldstore-metadata --lib
```

Не запускать интеграционные тесты и не поднимать многопроцессные сервисы, если об этом явно не просят.

---

## Задача 1: вынести семантику мутаций в явные команды метаданных

**Цель:** определить дружелюбный к Raft enum команд, пока не меняя поведение рантайма.

**Файлы:**
- Изменить: `crates/metadata/src/service.rs`
- Тест: `crates/metadata/src/service.rs`

**Шаги:**
1. Добавить внутренний enum `MetadataCommand`, покрывающий существующие мутирующие операции: создание/удаление bucket, запись/удаление объекта, изменение storage class, обновление расположения архива, обновление статуса восстановления, запись/обновление archive bundle, задачи архивации, задачи восстановления и картриджа, регистрация/снятие с регистрации/обновление воркера, heartbeat.
2. Добавить чистую вспомогательную функцию `apply_command(state: &mut MetadataState, command: MetadataCommand) -> Result<(), Status>`.
3. Перевести под `apply_command` сначала одну низкорисковую операцию, например создание bucket.
4. Добавить модульный тест, доказывающий, что дубликат bucket по-прежнему возвращает `AlreadyExists`.
5. Так же поступить с остальными мутациями, небольшими коммитами.

**Проверка:**

```bash
cargo test -p coldstore-metadata --lib
```

---

## Задача 2: сделать сохранение снимков ориентированным на команды

**Цель:** добиться, чтобы каждая мутация состояния проходила через единый путь применения, а после успешного применения сохранялся снимок.

**Файлы:**
- Изменить: `crates/metadata/src/service.rs`

**Шаги:**
1. Добавить `MetadataServiceImpl::apply_and_persist(command)`.
2. При `snapshot_path = None` применять изменение только в памяти.
3. При `snapshot_path = Some` применять изменение, затем атомарно записывать снимок.
4. Заменить блоки мутаций под write-lock на `apply_and_persist`.
5. Методы только для чтения оставить без изменений.

**Проверка:**

```bash
cargo test -p coldstore-metadata persistent_snapshot_survives_service_restart --lib
cargo test -p coldstore-metadata --lib
```

---

## Задача 3: ввести фасад модели состояний Raft за feature flag

**Цель:** добавить зависимость OpenRaft и собрать минимальный тип модели состояний так, чтобы сервис по умолчанию не использовал Raft.

**Файлы:**
- Изменить: `Cargo.toml`
- Изменить: `crates/metadata/Cargo.toml`
- Создать: `crates/metadata/src/raft.rs`
- Изменить: `crates/metadata/src/lib.rs`

**Шаги:**
1. Добавить опциональные workspace-зависимости для `openraft` и RocksDB/openraft-rocksstore.
2. Добавить feature `metadata-raft` в `coldstore-metadata`.
3. Создать `raft.rs` с псевдонимами Raft-типов для ColdStore и каркасом модели состояний.
4. Сначала собрать с выключенными feature по умолчанию; затем собрать с `--features metadata-raft`.

**Проверка:**

```bash
cargo check -p coldstore-metadata
cargo check -p coldstore-metadata --features metadata-raft
```

---

## Задача 4: сохранять снимки модели состояний Raft в RocksDB

**Цель:** заменить файловый снимок Phase 2A на снимок модели состояний в RocksDB для сборок с включённым Raft.

**Файлы:**
- Изменить: `crates/metadata/src/raft.rs`
- Добавить тесты в `crates/metadata/src/raft.rs` под `#[cfg(test)]` и feature flags.

**Шаги:**
1. Определить детерминированный формат снимка, повторно использующий `encode_snapshot`/`decode_snapshot`.
2. Хранить снимки в отдельной column family RocksDB или под отдельным префиксом ключа.
3. Добавить модульный тест с временным каталогом RocksDB.
4. Сетевые узлы Raft пока не запускать.

**Проверка:**

```bash
cargo test -p coldstore-metadata --features metadata-raft --lib
```

---

## Задача 5: направлять записи MetadataService через Raft, когда он включён

**Цель:** сделать так, чтобы gRPC API записи предлагали команды Raft в режиме `metadata-raft`, а существующий локальный режим сохранился для безопасных тестов.

**Файлы:**
- Изменить: `crates/metadata/src/service.rs`
- Изменить: `crates/metadata/src/raft.rs`

**Шаги:**
1. Добавить enum бэкенда: `Local` и `Raft`.
2. В локальном режиме использовать существующий `apply_and_persist`.
3. В режиме Raft отправлять `MetadataCommand` в OpenRaft.
4. API для чтения должны работать с уже применённой моделью состояний.
5. Добавить модульные тесты выбора бэкенда без открытия сетевых сокетов.

**Проверка:**

```bash
cargo test -p coldstore-metadata --lib
cargo test -p coldstore-metadata --features metadata-raft --lib
```

---

## Задача 6: добавить безопасные однопросцессные тесты Raft

**Цель:** проверить применение команд Raft без внешних сервисов и ресурсов, опасных для хоста.

**Файлы:**
- Изменить: `crates/metadata/src/raft.rs`

**Шаги:**
1. Построить тестовый стенд одноузловой in-process группы Raft.
2. Предложить команды создания bucket и записи объекта.
3. Проверить, что модель состояний содержит ожидаемые метаданные.
4. Проверить, что сохранённое состояние RocksDB можно открыть повторно.

**Проверка:**

```bash
cargo test -p coldstore-metadata --features metadata-raft --lib
```

---

## Вне рамок этого плана

- Без реального доступа к ленточным устройствам.
- Без доступа к SPDK.
- Без разрушительных интеграционных тестов на хосте.
- Без запуска многопроцессного кластера, если пользователь явно об этом не попросит.
- Без инструментов продуктивной миграции, пока форма команд и модели состояний не стабилизируется.
