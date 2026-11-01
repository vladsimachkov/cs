# Проектирование модуля слоя кэширования данных

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md)

## 1. Обзор модуля

Слой кэширования данных выполняет две ключевые задачи:

1. **Временное хранение данных PutObject**: приём исходных данных, записанных через PutObject, как временной зоны staging перед архивацией на ленту
2. **Кэш размороженных данных**: хранение размороженных данных, извлечённых с ленты, для быстрого ответа на GET-запросы без повторного чтения ленты

> **Модель развёртывания**: слой кэша работает на узлах **Cache Worker**, размещаемых на той же машине, что и **Scheduler Worker**.
> Scheduler общается с Cache Worker по gRPC (несмотря на одну машину, gRPC сохраняется для единообразия архитектуры).
> Gateway не подключается к Cache Worker напрямую; всё чтение и запись данных проксируются через Scheduler Worker.

### 1.1 Обязанности

- **Staging для PutObject**: приём и временное хранение данных PutObject до их архивации планировщиком на ленту
- **Кэш размороженных данных**: хранение данных объектов, размороженных чтением с ленты
- Ответы на GET-запросы (когда холодный объект уже разморожен)
- Вытеснение из кэша: LRU, LFU, TTL, ограничение ёмкости
- Интеграция с SPDK для высокопроизводительного I/O в пользовательском пространстве

### 1.2 Место в архитектуре

```
Процесс записи PutObject:
  Слой доступа ──► Scheduler ──► Cache Worker (staging) ──► ожидание планирования архивации
                                                       │
Процесс архивации:                                     ▼
  Планировщик архивации ──► Cache Worker (чтение staging) ──► запись на ленту ──► удаление staging

Процесс восстановления:
  Планировщик восстановления ──► чтение ленты ──► Cache Worker (кэш) ──► ответ GET

Важно: слой кэша не хранит собственный client метаданных.
Обновление метаданных целиком относится на слой планирования (вариант B).
```

---

## 2. Выбор технологий

| Компонент | Выбор | Описание |
|------|------|------|
| Привязка SPDK | **async-spdk** | [madsys-dev/async-spdk](https://github.com/madsys-dev/async-spdk) |
| Способ хранения SPDK | **Blobstore (Blob)** | Blob-ориентированный персистентный распределитель блоков, не raw bdev |
| Низкоуровневое устройство | NVMe bdev / Malloc (тесты) | Blobstore построен поверх bdev |

---

## 3. Размещение данных: способ SPDK Blob

### 3.1 Иерархия Blobstore

Blobstore SPDK определяет следующие абстракции ([документация SPDK Blobstore](https://spdk.io/doc/blob.html)):

| Уровень | Типичный размер | Описание |
|------|----------|------|
| **Logical Block** | 512B / 4KB | базовая единица, которую открывает диск |
| **Page** | 4KB | фиксированная страница, минимальная единица чтения/записи |
| **Cluster** | 1MB (256 pages) | единица выделения; Blob состоит из cluster |
| **Blob** | переменный | упорядоченный список cluster, персистентный, идентифицируется через blob_id |
| **Blobstore** | всё устройство | владеет нижележащим bdev, содержит область метаданных + набор blob |

### 3.2 Соответствие слоя кэша и Blob

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  Размещение Blobstore (весь bdev)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  [ Super Block ]  [ Metadata Region ]  [ область данных Blob ]          │
│       │                    │                    │                        │
│  только Cluster 0     список метаданных    Cluster 1..N                  │
│  подпись/версия/      Blob                 cluster каждого Blob          │
│  конфигурация                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Каждый объект кэша = 1 Blob
  - blob_id: уникальный ID, выданный Blobstore
  - данные: чтение и запись по page, поддержка thin provisioning
    (cluster выделяется при первой записи)
  - xattrs: bucket, key, size, checksum, expire_at, cached_at
```

### 3.3 Отображение объектов и Blob

| Объект кэша | Представление в Blob |
|----------|-----------|
| 1 объект S3 (bucket, key) | 1 Blob |
| данные объекта | последовательность cluster Blob, чтение и запись по page |
| метаданные | xattrs Blob (см. 3.3.1) |

**Индекс cache_key → blob_id**:

- Blobstore не предоставляет поиск по имени, поэтому сопоставление `cache_key → blob_id` поддерживается самостоятельно
- Способ хранения: HashMap в памяти + опциональная персистентность (например, запись в Super Blob или отдельный индексный Blob)
- `cache_key = format!("{}:{}", bucket, key)` либо с version_id

### 3.3.1 Поля Blob xattrs

xattrs — расширенные атрибуты Blob в формате «ключ-значение», используемые для хранения метаданных уровня объекта. Назначение и применение полей:

| xattr key | Тип | Обязателен | Значение | Применение |
|-----------|------|------|------|------|
| `bucket` | string | да | имя корзины S3 | вместе с key однозначно идентифицирует объект; проверка при GET, фильтрация по корзине при вытеснении |
| `key` | string | да | ключ объекта S3 | вместе с bucket однозначно идентифицирует объект; перестроение cache_key |
| `size` | u64 (сериализация в string) | да | размер объекта в байтах | выделение буфера при чтении; проверка длины чтения; расчёт ёмкости при вытеснении |
| `expire_at` | i64 Unix timestamp (string) | да | срок истечения разморозки | совпадает с restore_expire_at в метаданных; основа вытеснения по TTL |
| `cached_at` | i64 Unix timestamp (string) | да | время помещения в кэш | основа вытеснения LRU; наблюдаемость (время в кэше) |
| `checksum` | string (hex) | нет | контрольная сумма объекта SHA256 | проверка целостности после чтения; опционально, записывается если передана слоем планирования |
| `version_id` | string | нет | ID версии объекта S3 | поддержка версионирования; нужна, если cache_key включает version |
| `content_type` | string | нет | Content-Type объекта | заголовок ответа GET на протокольном уровне; опциональная передача как есть |
| `etag` | string | нет | ETag объекта | заголовок ответа GET на протокольном уровне; опциональная передача как есть |

**Сводка применения xattrs**:

- **Поиск и проверка**: bucket + key используются для перестроения cache_key и подтверждения принадлежности Blob при совпадении с индексом
- **Вытеснение**: expire_at задаёт вытеснение по TTL, cached_at — вытеснение LRU, size участвует в расчёте ёмкости
- **Целостность**: checksum проверяется после чтения; при обнаружении повреждения можно запустить повторный Restore
- **Передача на протокольный уровень**: content_type, etag используются в заголовках ответа GET, чтобы не обращаться к метаданным повторно

### 3.3.2 Запись индекса cache_key → blob_id

Поля каждой записи индекса в памяти (и опционально персистентного):

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `cache_key` | string | `{bucket}:{key}` или с version | первичный ключ, однозначно идентифицирует объект кэша |
| `blob_id` | u64 | Blob ID, выданный Blobstore |поиск Blob, выполнение open/read/write/delete |
| `size` | u64 | размер объекта | учёт ёмкости при вытеснении; выделение буфера при чтении |
| `expire_at` | i64 | timestamp истечения | быстрая проверка истечения без открытия Blob и чтения xattr |
| `cached_at` | i64 | timestamp помещения в кэш | сортировка LRU; без открытия Blob и чтения xattr |

**Связь индекса и xattrs**: индекс нужен для быстрого поиска (cache_key → blob_id) и решений о вытеснении; xattrs — это персистентные метаданные внутри Blob, используемые для проверки, перестроения индекса и передачи на протокольный уровень. Оба должны согласовываться, обновляются синхронно при записи.

### 3.3.3 Прочие связанные поля

| Источник | Поле | Значение | Применение |
|------|------|------|------|
| Blobstore | `blob_id` | уникальный идентификатор Blob | дескриптор всех операций с Blob |
| Blobstore | `blob_size` (num_clusters × cluster_size) | логический размер Blob | задаётся при resize; выравнивается с размером объекта по cluster |
| передаёт слой планирования | `data` | исходные байты объекта | payload, записываемый в Blob |
| передаёт слой планирования | `checksum` | опциональный SHA256 | записывается в xattr, проверка после чтения |
| передаёт слой планирования | `expire_at` | срок хранения разморозки | записывается в xattr и индекс; берётся из RestoreRequest.Days |

### 3.4 Создание Blob и процесс чтения/записи

```
Создание/запись:
  1. создать Blob:   spdk_bs_create_blob() → blob_id
  2. открыть Blob:   spdk_bs_open_blob(blob_id) → blob handle
  3. задать xattrs:  spdk_blob_set_xattr(blob, "bucket", ...) и т.д.
  4. изменить размер: spdk_blob_resize(blob, num_clusters)
  5. синхронизировать метаданные:  spdk_blob_sync_md(blob)
  6. записать данные:    spdk_blob_io_write(blob, channel, offset, len, buf, cb)
  7. закрыть Blob:   spdk_blob_close(blob)

Чтение:
  1. поиск по индексу:     cache_key → blob_id
  2. открыть Blob:  spdk_bs_open_blob(blob_id) → blob handle
  3. чтение:       spdk_blob_io_read(blob, channel, offset, len, buf, cb)
  4. закрыть Blob:  spdk_blob_close(blob)

Важно: set_xattr / resize вызываются на blob handle после open.
Единица чтения/записи — io_unit (обычно 4KB, получается через spdk_bs_get_io_unit_size()).
```

### 3.5 Формат данных Blob

- **Единица чтения/записи**: io_unit (обычно 4KB); offset и length в `spdk_blob_io_read`/`spdk_blob_io_write` указываются в io_unit
- **Данные объекта**: пишутся в Blob подряд; остаток меньше page дополняется либо пишется с выравниванием по page
- **Thin Provisioning**: можно создавать thin Blob, у которого cluster выделяется при первой записи, что экономит место

### 3.6 Ключевые параметры

| Параметр | Рекомендуемое значение | Описание |
|------|--------|------|
| cluster_size | 1MB | задаётся при создании Blobstore; Blob выделяется по cluster |
| page_size | 4KB | гранулярность чтения/записи |
| max_object_size | 5GB | предел одного объекта; сверх предела не кэшируется либо разбивается |
| blobstore_type | "coldstore_cache" | используется для идентификации принадлежности Blobstore |

### 3.7 Интеграция с async-spdk

async-spdk содержит пример `hello_blob`, использующий API Blobstore. Слой кэша должен:

- инициализировать Blobstore: `spdk_bs_init()` / `spdk_bs_load()` на указанном bdev
- использовать Blob API: `create_blob`, `open_blob`, `resize`, `write`, `read`, `close`, `delete_blob`
- хранить bucket, key, expire_at и другие через xattrs
- самостоятельно вести индекс `cache_key → blob_id` (в памяти + опциональная персистентность)

---

## 4. Ключевые структуры данных

### 4.1 CacheManager

Верхняя точка входа слоя кэша, реализующая одновременно `CacheReadApi` + `CacheWriteApi`.

```rust
pub struct CacheManager {
    backend: Box<dyn CacheBackend>,
    index: CacheIndex,
    config: CacheConfig,
    stats: CacheStats,
}
```

| Поле | Тип | Значение | Описание |
|------|------|------|------|
| `backend` | `Box<dyn CacheBackend>` | экземпляр бэкенда хранения | полиморфизм: SpdkBlobCacheBackend или FileCacheBackend |
| `index` | `CacheIndex` | индекс в памяти cache_key → blob_id | точка входа всех поисков и решений о вытеснении |
| `config` | `CacheConfig` | конфигурация кэша | предел ёмкости, TTL, стратегия вытеснения и т.д. |
| `stats` | `CacheStats` | статистика выполнения | доля попаданий, использование ёмкости, число вытеснений и т.д. |

### 4.2 Трейт CacheBackend

```rust
#[async_trait]
pub trait CacheBackend: Send + Sync {
    async fn write_blob(
        &self, blob_id: u64, data: &[u8], xattrs: &BlobXattrs,
    ) -> Result<u64>;
    async fn read_blob(&self, blob_id: u64) -> Result<Vec<u8>>;
    async fn delete_blob(&self, blob_id: u64) -> Result<()>;
    async fn create_blob(&self, xattrs: &BlobXattrs, size: u64) -> Result<u64>;
    async fn read_xattrs(&self, blob_id: u64) -> Result<BlobXattrs>;
    async fn list_blobs(&self) -> Result<Vec<u64>>;
}
```

| Метод | Значение |
|------|------|
| `write_blob` | запись данных объекта в уже созданный Blob |
| `read_blob` | чтение всех данных объекта из Blob |
| `delete_blob` | удаление Blob (вызывается при вытеснении) |
| `create_blob` | создать новый Blob, задать xattrs, выполнить resize, вернуть blob_id |
| `read_xattrs` | чтение xattrs Blob (для перестроения индекса при запуске) |
| `list_blobs` | перечень всех Blob ID в Blobstore (для перестроения индекса при запуске) |

### 4.3 BlobXattrs

Структурное представление на стороне Rust для пары «ключ-значение» xattrs, персистентно хранимых на Blob.

```rust
pub struct BlobXattrs {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub expire_at: i64,
    pub cached_at: i64,
    pub checksum: Option<String>,
    pub version_id: Option<String>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
}
```

| Поле | Тип | Обязательное | Значение | Момент записи |
|------|------|------|------|----------|
| `bucket` | String | да | имя корзины S3 | передаётся слоем планирования через put_restored |
| `key` | String | да | ключ объекта S3 | передаётся слоем планирования через put_restored |
| `size` | u64 | да | исходный размер объекта в байтах (не число cluster, занятых Blob) | слой планирования передаёт data.len() |
| `expire_at` | i64 | да | Unix timestamp истечения разморозки (секунды) | слой планирования вычисляет из RestoreRequest.Days и передаёт |
| `cached_at` | i64 | да | Unix timestamp помещения в кэш (секунды) | CacheManager берёт Utc::now() при записи |
| `checksum` | Option\<String\> | нет | строка SHA256 в hex | передаётся слоем планирования (если при чтении с ленты есть проверка) |
| `version_id` | Option\<String\> | нет | ID версии объекта S3 | передаётся слоем планирования (сценарий версионирования) |
| `content_type` | Option\<String\> | нет | MIME-тип | передаётся слоем планирования (передача как есть в ответе GET) |
| `etag` | Option\<String\> | нет | ETag S3 | передаётся слоем планирования (передача как есть в ответе GET) |

### 4.4 CacheEntry (запись индекса в памяти)

```rust
pub struct CacheEntry {
    pub cache_key: String,
    pub blob_id: u64,
    pub size: u64,
    pub expire_at: i64,
    pub cached_at: i64,
    pub last_accessed_at: i64,
    pub access_count: u64,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `cache_key` | String | `{bucket}:{key}` или `{bucket}:{key}:{version_id}` | первичный ключ индекса, однозначно идентифицирует объект кэша |
| `blob_id` | u64 | уникальный Blob ID, выданный Blobstore | дескриптор всех операций с Blob |
| `size` | u64 | исходный размер объекта в байтах | расчёт общей ёмкости при вытеснении; предварительное выделение буфера при чтении |
| `expire_at` | i64 | Unix timestamp истечения разморозки | решение о вытеснении по TTL; по истечении можно удалять |
| `cached_at` | i64 | Unix timestamp помещения в кэш | резервная сортировка LRU (используется, если истории обращений нет) |
| `last_accessed_at` | i64 | время последнего чтения GET | сортировка при вытеснении LRU, обновляется на каждом get |
| `access_count` | u64 | суммарное число обращений GET | сортировка при вытеснении LFU (опциональная стратегия) |

### 4.5 CacheIndex

```rust
pub struct CacheIndex {
    entries: HashMap<String, CacheEntry>,
    total_size: u64,
}
```

| Поле | Тип | Значение | Описание |
|------|------|------|------|
| `entries` | HashMap\<String, CacheEntry\> | cache_key → CacheEntry | полный индекс в памяти, поиск O(1) |
| `total_size` | u64 | текущее суммарное занимаемое кэшем число байт | обновляется при каждом put/delete, при вытеснении сравнивается с max_size |

### 4.6 CachedObject (возвращается при чтении)

```rust
pub struct CachedObject {
    pub data: Vec<u8>,
    pub size: u64,
    pub expire_at: DateTime<Utc>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub checksum: Option<String>,
}
```

| Поле | Тип | Значение | Потребитель |
|------|------|------|--------|
| `data` | Vec\<u8\> | полные данные объекта | протокольный слой помещает в тело HTTP-ответа |
| `size` | u64 | число байт данных | протокольный слой, Content-Length |
| `expire_at` | DateTime\<Utc\> | срок истечения разморозки | протокольный слой формирует заголовок `x-amz-restore` |
| `content_type` | Option\<String\> | MIME-тип | заголовок ответа Content-Type на протокольном уровне |
| `etag` | Option\<String\> | ETag | заголовок ответа ETag на протокольном уровне |
| `checksum` | Option\<String\> | SHA256 в hex | опциональная проверка целостности данных на протокольном уровне |

### 4.7 CacheConfig

```rust
pub struct CacheConfig {
    pub backend: CacheBackendType,
    pub max_size_bytes: u64,
    pub default_ttl_secs: u64,
    pub eviction_policy: EvictionPolicy,
    pub eviction_batch_size: usize,
    pub eviction_low_watermark: f64,
    pub bdev_name: String,
    pub cluster_size_mb: u32,
}
```

| Поле | Тип | Значение | Описание |
|------|------|------|------|
| `backend` | CacheBackendType | тип бэкенда | `Spdk` / `File` (тесты) |
| `max_size_bytes` | u64 | предел общей ёмкости кэша | при превышении запускается вытеснение |
| `default_ttl_secs` | u64 | TTL по умолчанию | резервное значение, если expire_at не указан |
| `eviction_policy` | EvictionPolicy | стратегия вытеснения | `Lru` / `Lfu` / `TtlFirst` |
| `eviction_batch_size` | usize | число объектов за одно вытеснение | пакетное удаление, снижает частоту вытеснений |
| `eviction_low_watermark` | f64 | нижняя граница (watermark) вытеснения | 0.8 означает вытеснять до 80% ёмкости, затем остановиться |
| `bdev_name` | String | имя SPDK bdev | например `"Malloc0"` или NVMe bdev |
| `cluster_size_mb` | u32 | размер cluster Blobstore | например 1MB |

### 4.8 CacheStats

```rust
pub struct CacheStats {
    pub total_size: u64,
    pub object_count: u64,
    pub hit_count: AtomicU64,
    pub miss_count: AtomicU64,
    pub put_count: AtomicU64,
    pub evict_count: AtomicU64,
    pub evict_bytes: AtomicU64,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `total_size` | u64 | текущий суммарный объём данных кэша в байтах |
| `object_count` | u64 | текущее число объектов кэша |
| `hit_count` | AtomicU64 | суммарное число попаданий в кэш |
| `miss_count` | AtomicU64 | суммарное число промахов кэша |
| `put_count` | AtomicU64 | суммарное число записей |
| `evict_count` | AtomicU64 | суммарное число вытесненных объектов |
| `evict_bytes` | AtomicU64 | суммарное число вытесненных байт |

---

## 5. Временное хранение данных PutObject

ColdStore — система чистого холодного архива, поэтому данные, записанные через PutObject, не отправляются на ленту напрямую, а сначала помещаются в staging на Cache Worker,
где дожидаются, пока планировщик просканирует объекты `ColdPending` и агрегирует их для записи на ленту. Staging-данные и кэш размороженных данных используют один и тот же бэкенд хранения, но логически управляются раздельно.

### 5.1 Процесс помещения в staging

```
Полный поток данных PutObject:

1. Gateway принимает запрос S3 PutObject
2. Gateway → Scheduler Worker (gRPC: PutObject)
3. Scheduler → Cache Worker (gRPC: PutStaging)
   └─ Cache Worker записывает данные в staging и возвращает staging_id
4. Scheduler → Metadata (Raft: PutObject)
   └─ запись ObjectMetadata (storage_class=ColdPending, staging_id)
5. Scheduler → Gateway: возврат 200 OK

Поток данных при архивации:

1. Планировщик архивации сканирует объекты ColdPending
2. Scheduler → Cache Worker (gRPC: GetStaging)
   └─ чтение staging-данных по staging_id
3. Scheduler → Tape Worker (gRPC: Write)
   └─ запись данных на ленту
4. Scheduler → Metadata (Raft: обновление ObjectMetadata → Cold)
5. Scheduler → Cache Worker (gRPC: DeleteStaging)
   └─ удаление staging-данных после завершения архивации
```

### 5.2 Определение интерфейса staging

```rust
/// интерфейс записи в staging, который слой кэша предоставляет слою планирования
#[async_trait]
pub trait StagingWriteApi: Send + Sync {
    /// записать staging-данные, вернуть staging_id
    async fn put_staging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        data: &[u8],
        checksum: &str,
    ) -> Result<String>;

    /// удалить staging-данные (вызывается после завершения архивации)
    async fn delete_staging(&self, staging_id: &str) -> Result<()>;

    /// пакетное удаление staging-данных
    async fn delete_staging_batch(&self, staging_ids: &[String]) -> Result<()>;
}

/// интерфейс чтения staging, который слой кэша предоставляет слою планирования
#[async_trait]
pub trait StagingReadApi: Send + Sync {
    /// прочитать staging-данные по staging_id
    async fn get_staging(&self, staging_id: &str) -> Result<Option<StagedObject>>;
}

pub struct StagedObject {
    pub staging_id: String,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub data: Vec<u8>,
    pub size: u64,
    pub checksum: String,
    pub staged_at: DateTime<Utc>,
}
```

### 5.3 Размещение staging-данных

Staging-данные и кэш размороженных данных используют общий бэкенд хранения (SPDK Blobstore / HDD), но различаются полем `data_type` в xattr:

| xattr key | Значение | Описание |
|-----------|------|------|
| `data_type` | `"staging"` | staging-данные (записаны PutObject, ожидают архивации) |
| `data_type` | `"restored"` | размороженные данные (извлечены с ленты, доступны для GET) |
| `staging_id` | строка UUID | уникальный идентификатор staging-данных (только для типа staging) |

### 5.4 Жизненный цикл staging-данных

| Фаза | Событие | Описание |
|------|------|------|
| запись | запрос PutObject поступил | Scheduler вызывает `put_staging`, данные пишутся на Cache Worker |
| ожидание | объект находится в ColdPending | staging-данные сохраняются, ожидая сканирования планировщиком архивации |
| чтение | планировщик архивации агрегирует Bundle | Scheduler вызывает `get_staging` для чтения данных и записывает их на ленту |
| удаление | архивация завершена | Scheduler вызывает `delete_staging` и очищает staging-данные |
| очистка по таймауту | объект удалён, а staging остался | фоновая периодическая чистка «сиротских» staging-данных |

### 5.5 Управление ёмкостью

Staging-данные и кэш размороженных данных делят ёмкость; управление идёт такими стратегиями:

- **Резерв ёмкости**: настраивается `staging_reserved_percent` (например, 30%), гарантирующий место под staging
- **Backpressure**: когда заполнение staging превышает порог, PutObject возвращает `503 SlowDown`, и внешняя система снижает темп
- **Приоритет вытеснения**: при нехватке ёмкости сначала вытесняются просроченные данные кэша разморозки; staging-данные активно не вытесняются (кроме случая, когда объект уже удалён)

### 5.6 staging_id в ObjectMetadata

В `ObjectMetadata` добавляется поле `staging_id`, фиксирующее идентификатор staging-данных объекта на Cache Worker:

```rust
pub struct ObjectMetadata {
    // ... существующие поля ...
    pub staging_id: Option<String>,    // ID staging-данных на Cache Worker
    pub staging_worker_id: Option<u64>, // Cache Worker, на котором лежат staging-данные
}
```

После завершения архивации слой планирования очищает `staging_id` и `staging_worker_id`.

---

## 6. Стратегии кэша

| Стратегия | Описание |
|------|------|
| LRU | вытесняется объект, к которому дольше всего не обращались |
| LFU | вытесняется объект с наименьшей частотой обращений |
| TTL | вытеснение по expire_at (отдельный фоновый скан) |
| Ёмкость | общий предел ёмкости; при превышении запускается вытеснение |

```rust
pub enum EvictionPolicy {
    Lru,        // вытеснение с сортировкой по last_accessed_at
    Lfu,        // вытеснение с сортировкой по access_count
    TtlFirst,   // сначала вытесняются просроченные объекты, затем LRU
}
```

**Логика вытеснения**:

1. **Скан TTL** (отдельная фоновая задача): периодически сканирует CacheEntry с `expire_at < now` и удаляет соответствующие Blob напрямую
2. **Вытеснение по ёмкости**: когда `total_size > max_size_bytes`, объекты для вытеснения выбираются по `EvictionPolicy` и удаляются пачками по `eviction_batch_size`, пока `total_size ≤ max_size_bytes × eviction_low_watermark`

---

## 7. Интеграция со слоем планирования

### 7.1 Определение интерфейсов

```rust
/// интерфейс, который слой кэша предоставляет слою планирования
#[async_trait]
pub trait CacheWriteApi: Send + Sync {
    async fn put_restored(&self, item: RestoredItem) -> Result<()>;
    async fn put_restored_batch(&self, items: Vec<RestoredItem>) -> Result<()>;
    async fn delete(&self, bucket: &str, key: &str, version_id: Option<&str>) -> Result<()>;
}

pub struct RestoredItem {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub data: Vec<u8>,
    pub checksum: Option<String>,       // SHA256 в hex
    pub expire_at: DateTime<Utc>,
    pub content_type: Option<String>,   // слой планирования получает из метаданных и передаёт
    pub etag: Option<String>,           // слой планирования получает из метаданных и передаёт
}
```

| Поле | Тип | Значение | Источник |
|------|------|------|------|
| `bucket` | String | имя корзины S3 | передаёт слой планирования |
| `key` | String | ключ объекта S3 | передаёт слой планирования |
| `version_id` | Option\<String\> | версия объекта | слой планирования берёт из метаданных |
| `data` | Vec\<u8\> | исходные данные объекта | чтение с ленты |
| `checksum` | Option\<String\> | строка SHA256 в hex | передаётся слоем планирования после проверки |
| `expire_at` | DateTime\<Utc\> | срок истечения разморозки | слой планирования вычисляет из RestoreRequest.Days |
| `content_type` | Option\<String\> | MIME-тип | слой планирования берёт из метаданных для передачи в ответе GET |
| `etag` | Option\<String\> | ETag S3 | слой планирования берёт из метаданных для передачи в ответе GET |

> Метод `delete` служит для очистки кэша при DeleteObject на слое доступа (вызывается слоем доступа).

### 7.2 Моменты вызова и процесс

Слой кэша **сам** не запускает обновление метаданных. После завершения записи он возвращает `Result<()>` слою планирования, а все последующие изменения метаданных целиком относятся на слой планирования.

```
Планировщик восстановления выполняет TapeReadJob              (обязанность слоя планирования)
    │
    ├─ 1. последовательное чтение объектов A, B, C с ленты
    │
    ├─ 2. после чтения каждого объекта:
    │        cache.put_restored(bucket, key, data, checksum, expire_at).await
    │        при ошибке: повтор или пометка RecallTask как Failed
    │
    └─ 3. после записи всех данных в кэш:                 (обновление метаданных — обязанность слоя планирования)
           metadata.update_restore_status(Completed, restore_expire_at)
```

> **Ключевой принцип**: слой кэша — чистое хранилище данных, MetadataClient у него отсутствует. Точка записи метаданных сосредоточена в слое планирования.

### 7.3 Поток данных и проверка

| Шаг | Ответственная сторона | Описание |
|------|--------|------|
| чтение с ленты | планировщик | с проверкой checksum |
| запись в кэш | слой кэша | хранит data + checksum, чистая операция с данными |
| обновление метаданных | **планировщик** | restore_status=Completed, restore_expire_at |
| проверка при GET | протокольный слой/слой кэша | опциональная проверка checksum после чтения |

### 7.4 Отказы и повторные попытки

- Ошибка записи в кэш: планировщик повторяет 2 раза; при повторной ошибке RecallTask помечается Failed, **метаданные не обновляются**
- Нехватка места в кэше: сначала запускается вытеснение, затем запись; если места по-прежнему не хватает, возвращается ошибка, и планировщик не обновляет метаданные
- Запись в кэш успешна, но обновление метаданных не удалось: в кэше данные есть, а GET всё равно возвращает «не разморожено»; планировщик должен повторить обновление метаданных

---

## 8. Отношение со слоем метаданных (вариант B: слой кэша не зависит от метаданных напрямую)

### 8.1 Принцип проектирования

**Слой кэша не хранит MetadataClient и не читает/не пишет метаданные напрямую.**

Точек записи метаданных всего две:
- **слой доступа**: `PutObject`, `DeleteObject`
- **слой планирования**: переходы состояний архивации/восстановления (`UpdateStorageClass`, `UpdateRestoreStatus` и другие)

Слой кэша — чистый слой хранения данных, наружу публикуются только `CacheReadApi` и `CacheWriteApi`.

### 8.2 Косвенная связь слоя кэша с метаданными

| Сценарий | Зависимость | Описание |
|------|------|------|
| перед GET | прямой зависимости нет | протокольный слой сначала читает метаданные и только при Completed обращается к кэшу |
| при вытеснении | нет | решение по собственному expire_at в xattrs, обращение к метаданным не нужно |
| при записи | нет | слой планирования передаёт expire_at, слой кэша пишет его в xattr напрямую |
| при запуске | нет | индекс в памяти перестраивается из xattrs Blobstore, от метаданных не зависит |

### 8.3 Гарантии согласованности (относятся на слой планирования)

| Порядок | Операция | Ответственная сторона |
|------|------|--------|
| 1 | чтение с ленты завершено | планировщик |
| 2 | `cache.put_restored(...)` успешно | планировщик → слой кэша |
| 3 | `metadata.update_restore_status(Completed)` | **планировщик** → слой метаданных |
| 4 | GET: чтение метаданных → Completed → чтение из кэша | протокольный слой |

Если шаг 2 успешен, а шаг 3 нет: в кэше данные есть, но метаданные не обновлены, и GET по-прежнему возвращает «не разморожено». Планировщик должен повторить обновление метаданных (механизм компенсации).

---

## 9. Интеграция с протокольным слоем/слоем доступа

### 9.1 Определение интерфейсов

```rust
/// интерфейс, который слой кэша предоставляет протокольному слою/слою доступа
#[async_trait]
pub trait CacheReadApi: Send + Sync {
    async fn get(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Option<CachedObject>>;

    async fn contains(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<bool>;
}
```

> Определение `CachedObject` см. в §4.6; содержит data, size, expire_at, content_type, etag, checksum.

### 9.2 Процесс вызова на протокольном слое

```
GET /{bucket}/{key}
    │
    ├─ протокольный слой/Handler: metadata.get_object(bucket, key)
    │
    ├─ if storage_class != Cold:
    │     чтение из hot-хранилища, возврат
    │
    ├─ if storage_class == Cold:
    │     if restore_status != Completed:
    │         возврат 403 InvalidObjectState
    │     if restore_expire_at < now:
    │         возврат 403 InvalidObjectState (срок истёк)
    │
    └─ cache.get(bucket, key)
           if Some(obj): вернуть obj.data с заголовком x-amz-restore
           if None:      вернуть 404 или 503 (данные в кэше потеряны, нужен повторный Restore)
```

### 9.3 Обработка промаха кэша

| Ситуация | Поведение протокольного слоя |
|------|------------|
| в метаданных Completed, но в кэше данных нет | возвращается 503 с предложением выполнить Restore заново; либо запускается асинхронное восстановление |
| данные вытеснены из кэша (в пределах TTL) | то же самое: restore_expire_at в метаданных ещё не истёк, но в кэше уже удалено |
| данные кэша повреждены | при сбое проверки возвращается 500 с рекомендацией выполнить Restore заново |

### 9.4 Заголовки ответа

При попадании в кэш для GET протокольный слой добавляет:

```
x-amz-restore: ongoing-request="false", expiry-date="Fri, 28 Feb 2025 12:00:00 GMT"
```

---

## 10. Сводная схема интеграции (вариант B)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Протокольный слой / слой доступа                    │
│  GET: metadata.get_object → if Completed → cache.get → возврат data     │
│  HEAD: metadata.get_object → формирование заголовка x-amz-restore       │
│  PUT/DELETE: metadata.put_object / delete_object                         │
└─────────────────────────────────────────────────────────────────────────┘
        │ чтение                           │ чтение
        ▼                                  ▼
┌───────────────────┐              ┌───────────────────────────────┐
│  Слой метаданных  │              │  Слой кэша (чистое хранение)  │
│  restore_status   │              │  CacheReadApi: get/contains   │
│  restore_expire_at│              │  CacheWriteApi: put_restored  │
└───────────────────┘              │  ⚡ MetadataClient отсутствует │
        ▲                          └───────────────────────────────┘
        │ запись (единственный координатор записи)     ▲
        │                                              │ запись
        │              ┌───────────────────────────────┘
        │              │
┌───────┴──────────────┴───────────────────────────────┐
│  Слой планирования (архивация/восстановление)       │
│  1. чтение ленты → 2. cache.put_restored → 3. метаданные │
│  единственный координатор записи метаданных          │
│  (кроме PUT/DELETE на слое доступа)                  │
└──────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────┐
│  Слой лент       │
│  (чистое железо) │
│  MetadataClient  │
│  отсутствует     │
└──────────────────┘
```

---

## 11. Способы интеграции

- **Вариант A**: сервис кэша как отдельный бинарник, общение с основным сервисом по gRPC/HTTP
- **Вариант B**: основной процесс запускает Axum внутри block_on SPDK (потребуется изменить порядок запуска)
- **Вариант C**: слой кэша сначала использует файловый бэкенд, а async-spdk подключается после стабилизации архитектуры

---

## 12. Структура модуля

```
src/cache/
├── mod.rs
├── manager.rs              # CacheManager, реализует CacheReadApi + CacheWriteApi
├── index.rs                # индекс cache_key → blob_id (в памяти + опциональная персистентность)
├── spdk/
│   ├── mod.rs
│   ├── blob_backend.rs     # SpdkBlobCacheBackend, обёртки Blobstore/Blob API
│   └── config.rs
└── file/                   # файловый бэкенд (совместимость/тесты)
    └── backend.rs
```

---

## 13. Параметры конфигурации

```yaml
cache:
  backend: "spdk"
  spdk:
    enabled: true
    config_file: "/etc/coldstore/cache_spdk.json"
    bdev_name: "Malloc0"
    blobstore_type: "coldstore_cache"
    cluster_size_mb: 1
    max_size_gb: 100
  ttl_secs: 86400
  eviction_policy: "Lru"
```

---

## 14. Зависимости

| Интегрируемая сторона | Направление | Интерфейс | Описание |
|--------|------|------|------|
| слой планирования | планирование → кэш | `StagingWriteApi`: `put_staging` / `delete_staging` | staging для данных PutObject и очистка после архивации |
| слой планирования | планирование → кэш | `StagingReadApi`: `get_staging` | чтение staging-данных при архивации |
| слой планирования | планирование → кэш | `CacheWriteApi`: `put_restored` / `put_restored_batch` | запись размороженных данных после восстановления |
| слой доступа | доступ → кэш | `CacheWriteApi`: `delete` | очистка кэша при DeleteObject |
| протокольный слой/слой доступа | протокол → кэш | `CacheReadApi`: `get` / `contains` | чтение размороженных данных для GET |
| слой метаданных | **зависимости нет** | — | слой кэша не хранит MetadataClient |

> **Принцип варианта B**: слой кэша — чистое хранилище данных, о метаданных он ничего не знает. Запись метаданных координируется слоем планирования единообразно.

---

## 15. Справочные материалы

- [async-spdk](https://github.com/madsys-dev/async-spdk) (с примером hello_blob)
- [SPDK Blobstore Programmer's Guide](https://spdk.io/doc/blob.html)
- [SPDK blob.h API](https://spdk.io/doc/blob_8h.html)
