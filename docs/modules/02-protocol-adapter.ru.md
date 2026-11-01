# Проектирование модуля слоя адаптации протокола

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md)

## 1. Обзор модуля

Слой адаптации протокола отвечает за преобразование семантики холодного архива S3 (Glacier) во внутреннюю модель ColdStore и формирование ответов, соответствующих спецификации S3.

### 1.1 Обязанности

- Отображение StorageClass (S3 ↔ внутреннее состояние)
- Разбор RestoreRequest (Days, Tier)
- Формирование заголовка ответа x-amz-restore
- Сопоставление кодов ошибок (InvalidObjectState, RestoreAlreadyInProgress и других)
- Семантика значения по умолчанию ColdPending для PutObject

### 1.2 Место в архитектуре

```
Слой доступа S3 ──► Слой адаптации протокола ──► Scheduler Worker (бизнес-ядро)
                │
                ├─ Отображение StorageClass
                ├─ Разбор RestoreRequest
                ├─ Ответ x-amz-restore
                └─ Сопоставление кодов ошибок
```

---

## 2. Отображение StorageClass

ColdStore — это система чистого холодного архива: любой записанный объект сразу получает `ColdPending`, а после архивации переходит в `Cold`. Модель совпадает с AWS S3 Glacier Deep Archive.

| S3 Storage Class | Внутреннее состояние ColdStore | Описание |
|------------------|----------------|------|
| `GLACIER` / `DEEP_ARCHIVE` / `STANDARD` / любой | `ColdPending` | запись сразу ставится в очередь архивации (без разделения hot/cold, единое холодное хранилище) |
| архивация завершена | `Cold` | данные заархивированы на ленту |

---

## 3. Адаптация протокола RestoreObject

### 3.1 Разбор запроса

**Вход**: `POST /{bucket}/{key}?restore` + XML Body

```xml
<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Days>7</Days>
  <GlacierJobParameters>
    <Tier>Standard</Tier>
  </GlacierJobParameters>
</RestoreRequest>
```

**Выход** (внутренняя структура):

```rust
pub struct RestoreRequest {
    pub days: u32,           // минимум 1
    pub tier: RestoreTier,   // Expedited | Standard | Bulk
    pub version_id: Option<String>,
}
```

### 3.2 Отображение уровней восстановления (Tier)

| Tier | Внутренний приоритет | Стратегия планирования |
|------|------------|----------|
| Expedited | высокий | очередь высокого приоритета, резервируемая ёмкость настраивается |
| Standard | средний | очередь по умолчанию |
| Bulk | низкий | пакетное объединённое восстановление |

### 3.3 Спецификация ответа

| Сценарий | HTTP-статус | Описание |
|------|-----------|------|
| первая разморозка | 202 Accepted | задача принята |
| уже разморожено и не просрочено | 200 OK | возможно продлить Days |
| разморозка выполняется | 409 Conflict | RestoreAlreadyInProgress |
| недостаточно ёмкости Expedited | 503 Service Unavailable | GlacierExpeditedRetrievalNotAvailable |
| объект ещё не заархивирован (ColdPending) | 409 Conflict | объект ещё в очереди архивации, Restore невозможен |

---

## 4. Заголовок ответа x-amz-restore

При HEAD для холодного объекта ответ формируется в зависимости от состояния разморозки:

| Состояние | Заголовок ответа |
|------|--------|
| разморозка выполняется | `x-amz-restore: ongoing-request="true"` |
| разморожено | `x-amz-restore: ongoing-request="false", expiry-date="Fri, 28 Feb 2025 12:00:00 GMT"` |
| не разморожено | заголовок не возвращается (может отсутствовать) |

---

## 5. Сопоставление кодов ошибок

| Код ошибки AWS | HTTP-статус | Условие возникновения |
|------------|-----------|----------|
| InvalidObjectState | 403 | GET холодного объекта, который ещё не разморожен |
| RestoreAlreadyInProgress | 409 | повторный запрос Restore |
| GlacierExpeditedRetrievalNotAvailable | 503 | недостаточно ёмкости Expedited |
| ObjectNotYetArchived | 409 | Restore для объекта в состоянии ColdPending (архивация ещё не завершена) |

---

## 6. Контроль доступа GET к холодному объекту

| Состояние объекта | Поведение GET |
|----------|----------|
| Cold + не разморожено | 403 InvalidObjectState |
| Cold + разморозка выполняется | 403 InvalidObjectState |
| Cold + разморожено | чтение из кэша и возврат данных |
| Cold + срок разморозки истёк | 403 InvalidObjectState |

---

## 7. Семантика PutObject

ColdStore как система чистого холодного архива помечает все записанные через PutObject объекты как `ColdPending`,
после чего планировщик сканирует их и автоматически архивирует на ленту. Внешняя система горячего хранения, которой нужна архивация, напрямую вызывает PutObject ColdStore.

---

## 8. Поддержка S3 Multipart Upload

### 8.1 Цели проектирования

Поддержка API S3 Multipart Upload, позволяющего загружать крупные объекты по частям. Для системы холодного архива
Multipart Upload нужен в основном для переноса крупных файлов (уровень ГБ/ТБ) из внешней системы горячего хранения в ColdStore.

### 8.2 Совместимость API

| API | Описание | Реализация в ColdStore |
|-----|------|----------------|
| `CreateMultipartUpload` | инициализация загрузки по частям, возвращает upload_id | Scheduler создаёт запись MultipartUpload в Metadata |
| `UploadPart` | загрузка одной части | Scheduler помещает часть в staging на Cache Worker (StagingWriteApi) |
| `CompleteMultipartUpload` | завершение загрузки, сборка частей | Scheduler объединяет метаданные частей и помечает объект как ColdPending |
| `AbortMultipartUpload` | отмена загрузки, очистка частей | Scheduler удаляет все staging-части |
| `ListParts` | перечень загруженных частей | Scheduler читает из Metadata |
| `ListMultipartUploads` | перечень незавершённых загрузок по частям | Scheduler читает из Metadata |

### 8.3 Стратегия staging для частей

Каждая Part хранится в staging на Cache Worker независимо и идентифицируется через `staging_id`.
При `CompleteMultipartUpload` физическое слияние не выполняется (чтобы избежать большого I/O),
вместо этого в ObjectMetadata записывается список `staging_id` всех Part,
а планировщик архивации при записи на ленту последовательно читает каждую Part и пишет их на ленту подряд.

```
Процесс загрузки по частям:

1. CreateMultipartUpload → upload_id
2. UploadPart(upload_id, part_number, data) × N
   └─ каждая Part независимо попадает в staging на Cache Worker
3. CompleteMultipartUpload(upload_id, parts[])
   └─ проверка, что все Part загружены
   └─ создание ObjectMetadata (ColdPending, staging_ids=[...])
   └─ итоговая контрольная сумма объекта считается по правилу S3 ETag

При архивации:
  планировщик по порядку part_number читает каждую Part с Cache Worker
  и пишет их на ленту подряд; на ленте это один объект
```

### 8.4 Расширение метаданных

```rust
pub struct MultipartUploadInfo {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub parts: Vec<PartInfo>,
    pub created_at: DateTime<Utc>,
    pub status: MultipartUploadStatus,
}

pub struct PartInfo {
    pub part_number: u32,
    pub staging_id: String,
    pub size: u64,
    pub etag: String,
    pub uploaded_at: DateTime<Utc>,
}

pub enum MultipartUploadStatus {
    InProgress,
    Completed,
    Aborted,
}
```

### 8.5 Ограничения

| Ограничение | Значение | Описание |
|------|------|------|
| максимальное число частей | 10,000 | как в S3 |
| минимальный размер части | 5 MB | как в S3, кроме последней части |
| максимальный размер части | 5 GB | как в S3 |
| максимальный размер объекта | 5 TB | теоретический предел 10,000 × 5GB |
| таймаут загрузки | 7 дней | после таймаута staging-части автоматически очищаются |

### 8.6 Механизм очистки

Фоновая периодическая задача сканирует незавершённые Multipart Upload с истёкшим сроком и автоматически выполняет Abort с очисткой staging-частей.

---

## 9. Структура модуля

```
src/
├── s3/
│   ├── protocol/           # адаптация протокола (возможен отдельный подмодуль)
│   │   ├── mod.rs
│   │   ├── storage_class.rs  # отображение StorageClass
│   │   ├── restore.rs       # разбор RestoreRequest, формирование ответа
│   │   ├── errors.rs        # сопоставление кодов ошибок
│   │   └── put_object.rs    # семантика PutObject → ColdPending
```

---

## 10. Зависимости

| Зависимость | Назначение |
|------|------|
| Scheduler Worker (gRPC) | все бизнес-запросы: запросы метаданных, чтение и запись данных, постановка задач Restore |

> Слой адаптации протокола работает в процессе Gateway, в одном процессе со слоем доступа.
> Всё внешнее взаимодействие идёт через Scheduler Worker, прямых подключений к Metadata/Cache/Tape нет.

---

## 11. Справочные материалы

- [AWS S3 RestoreObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html)
- [S3 Glacier Retrieval Options](https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects-retrieval-options.html)
