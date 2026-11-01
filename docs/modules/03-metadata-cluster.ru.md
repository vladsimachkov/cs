# Проектирование модуля кластера метаданных

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md)

## 1. Обзор модуля

Кластер метаданных предоставляет хранение метаданных плоскости управления ColdStore. Текущую реализацию следует рассматривать в двух слоях:

- **текущая кодовая база (Phase 1 + Phase 2A)**: MetadataService сначала реализует все метаданные как конечный автомат в процессе и предоставляет опциональный вход локальной бинарной персистентности snapshot; эта возможность служит для безопасной проверки модели состояний и восстановления после перезапуска, настоящий распределённый кластер не запускается.
- **целевая архитектура (Phase 2B)**: тот же конечный автомат метаданных подключается к OpenRaft + RocksDB/openraft-rocksstore, что даёт strongly consistent репликацию, персистентный журнал Raft и изменение состава кластера.

Метаданные делятся на две большие категории:

| Категория | Содержание | Описание |
|------|------|------|
| **метаданные кластера** | сведения об узлах, топология кластера, состояние Raft | управление самим кластером |
| **архивные метаданные** | объекты, корзины, архивные комплекты, ленты, задачи | управление бизнес-данными |

### 1.1 Обязанности

- метаданные объекта: bucket, key, storage_class, archive_id, tape_id, restore_status и другие
- управление корзинами: создание и удаление корзин
- индексы архивных комплектов, лент, задач восстановления и задач архивации
- управление кластером: регистрация узлов, выборы лидера Raft, изменение состава
- strongly consistent чтение и запись, высокая доступность

### 1.2 Место в архитектуре

```
                            Console ──управляющие чтение/запись──▶ Metadata
                                                    ▲
Gateway ──все запросы──▶ Scheduler Worker ──чтение+запись──────┘
                          │
                          ├──gRPC──▶ Cache Worker (чистые данные)
                          └──gRPC──▶ Tape Worker  (чистое железо)
```

> Gateway к Metadata напрямую не подключён. Scheduler Worker — единственная бизнес-точка чтения и записи метаданных.

---

## 2. Выбор технологий

| Компонент | Выбор | Описание |
|------|------|------|
| Текущий конечный автомат | `MetadataState` в процессе | реализовано в Phase 1, покрывает объекты, корзины, архивные комплекты, задачи архивации, задачи восстановления, ленты, реестр Worker |
| Текущая персистентность | файл бинарного snapshot | начато в Phase 2A, включается opt-in через `MetadataServiceImpl::new_with_snapshot(config, path)`; после записи snapshot атомарно заменяется, при перезапуске загружается и восстанавливает состояние |
| Консенсус Raft | **openraft** | цель Phase 2B, [databendlabs/openraft](https://github.com/databendlabs/openraft) |
| Хранилище Raft | **openraft-rocksstore** | цель Phase 2B, бэкенд на RocksDB |
| Движок хранения | RocksDB | цель Phase 2B, LSM-tree, высокая пропускная способность |

---

## 3. Ключевые структуры данных

### 3.1 ObjectMetadata (метаданные объекта)

Самая ключевая структура системы: фиксирует полное состояние каждого объекта S3.

```rust
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub checksum: String,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub storage_class: StorageClass,
    pub archive_id: Option<Uuid>,
    pub tape_id: Option<String>,
    pub tape_set: Option<Vec<String>>,
    pub tape_block_offset: Option<u64>,
    pub restore_status: Option<RestoreStatus>,
    pub restore_expire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

| Поле | Тип | Значение | Кто пишет | Кто потребляет |
|------|------|------|--------|--------|
| `bucket` | String | имя корзины S3 | слой доступа, PutObject | все |
| `key` | String | ключ объекта S3 | слой доступа, PutObject | все |
| `version_id` | Option\<String\> | ID версии объекта | слой доступа, PutObject | протокольный слой, слой планирования |
| `size` | u64 | размер объекта в байтах | слой доступа, PutObject | слой планирования (расчёт агрегации), протокольный слой (Content-Length) |
| `checksum` | String | SHA256 в hex | слой доступа, PutObject | слой планирования (запись на ленту/проверка), слой кэша (передача как есть) |
| `content_type` | Option\<String\> | MIME-тип | слой доступа, PutObject | заголовок ответа GET протокольного слоя, слой планирования передаёт в слой кэша |
| `etag` | Option\<String\> | ETag S3 | слой доступа, PutObject | заголовок ответа GET протокольного слоя, слой планирования передаёт в слой кэша |
| `storage_class` | StorageClass | категория хранения | слой доступа (PutObject) / слой планирования (завершение архивации) | протокольный слой (решение о возможности GET), слой планирования (сканирование ColdPending) |
| `archive_id` | Option\<Uuid\> | ID ArchiveBundle, к которому относится | слой планирования (завершение архивации) | слой планирования позиционирует Bundle при восстановлении |
| `tape_id` | Option\<String\> | ID ленты с основной репликой | слой планирования (завершение архивации) | слой планирования позиционирует ленту при восстановлении |
| `tape_set` | Option\<Vec\<String\>\> | список всех лент-реплик | слой планирования (завершение архивации) | слой планирования переключает реплики |
| `tape_block_offset` | Option\<u64\> | блочное смещение объекта на ленте | слой планирования (завершение архивации) | слой планирования выполняет seek при восстановлении |
| `restore_status` | Option\<RestoreStatus\> | состояние разморозки | слой планирования (процесс восстановления) | протокольный слой (решение об ответе GET/HEAD) |
| `restore_expire_at` | Option\<DateTime\<Utc\>\> | срок истечения разморозки | слой планирования (завершение восстановления) | протокольный слой (заголовок x-amz-restore), слой кэша (TTL) |
| `created_at` | DateTime\<Utc\> | время создания | слой доступа, PutObject | слой планирования (сортировка агрегации) |
| `updated_at` | DateTime\<Utc\> | время последнего обновления | при каждой записи | аудит |

### 3.2 Перечисляемые типы

```rust
pub enum StorageClass {
    ColdPending,   // ожидает архивации (ставится сразу при записи PutObject, ждёт сканирования планировщиком и записи на ленту)
    Cold,          // заархивировано на ленту
}

pub enum RestoreStatus {
    Pending,          // поставлено в очередь, ожидает планирования
    WaitingForMedia,  // лента офлайн, ожидание ручного вывода в онлайн
    InProgress,       // идёт чтение с ленты
    Completed,        // записано в кэш, доступно для GET
    Expired,          // срок разморозки истёк
    Failed,           // восстановление не удалось
}

pub enum RestoreTier {
    Expedited,   // срочный: 1-5 минут
    Standard,    // стандартный: 3-5 часов
    Bulk,        // пакетный: 5-12 часов
}
```

### 3.3 ArchiveBundle (архивный комплект)

Определение совпадает с 05-scheduler-layer §8.3.

```rust
pub struct ArchiveBundle {
    pub id: Uuid,
    pub tape_id: String,
    pub tape_set: Vec<String>,
    pub entries: Vec<BundleEntry>,
    pub total_size: u64,
    pub filemark_start: u32,
    pub filemark_end: u32,
    pub checksum: Option<String>,
    pub status: ArchiveBundleStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}
```

| Поле | Тип | Значение | Кто пишет |
|------|------|------|--------|
| `id` | Uuid | уникальный идентификатор архивного комплекта | слой планирования |
| `tape_id` | String | ID ленты основной реплики | слой планирования |
| `tape_set` | Vec\<String\> | все ленты-реплики | слой планирования |
| `entries` | Vec\<BundleEntry\> | список объектов внутри комплекта со смещениями | слой планирования |
| `total_size` | u64 | общий размер в байтах | слой планирования |
| `filemark_start` | u32 | начальный FileMark | слой планирования (берёт из WriteResult) |
| `filemark_end` | u32 | завершающий FileMark | слой планирования |
| `checksum` | Option\<String\> | SHA256 всего комплекта | слой планирования (опционально) |
| `status` | ArchiveBundleStatus | состояние | слой планирования |
| `created_at` | DateTime\<Utc\> | время создания | слой планирования |
| `completed_at` | Option\<DateTime\<Utc\>\> | время завершения | слой планирования |

```rust
pub struct BundleEntry {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub offset_in_bundle: u64,
    pub tape_block_offset: u64,
    pub checksum: String,
}

pub enum ArchiveBundleStatus {
    Pending, Writing, Completed, Failed,
}
```

### 3.4 TapeInfo (сведения о ленте)

Определение совпадает с 06-tape-layer §4.6. Записывается слоем планирования в метаданные.

```rust
pub struct TapeInfo {
    pub id: String,
    pub barcode: Option<String>,
    pub format: String,
    pub status: TapeStatus,
    pub location: Option<String>,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub remaining_bytes: u64,
    pub archive_bundles: Vec<Uuid>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub error_count: u32,
    pub registered_at: DateTime<Utc>,
}
```

| Поле | Тип | Значение | Кто пишет |
|------|------|------|--------|
| `id` | String | уникальный идентификатор ленты | слой планирования (при регистрации) |
| `barcode` | Option\<String\> | штрихкод | слой планирования |
| `format` | String | "LTO-9", "LTO-10" | слой планирования |
| `status` | TapeStatus | Online/Offline/Error/Retired | слой планирования |
| `location` | Option\<String\> | описание расположения | слой планирования |
| `capacity_bytes` | u64 | полная ёмкость | слой планирования |
| `used_bytes` | u64 | использовано | слой планирования (обновляет после каждой записи) |
| `remaining_bytes` | u64 | остаток | слой планирования |
| `archive_bundles` | Vec\<Uuid\> | список записанных архивных комплектов | слой планирования |
| `last_verified_at` | Option\<DateTime\<Utc\>\> | время последней проверки | слой планирования |
| `error_count` | u32 | суммарное число ошибок | слой планирования |
| `registered_at` | DateTime\<Utc\> | время регистрации | слой планирования |

```rust
pub enum TapeStatus {
    Online, Offline, Error, Retired, Unknown,
}
```

### 3.5 RecallTask (задача восстановления)

Определение совпадает с 05-scheduler-layer §8.7.

```rust
pub struct RecallTask {
    pub id: Uuid,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub archive_id: Uuid,
    pub tape_id: String,
    pub tape_set: Vec<String>,
    pub tape_block_offset: u64,
    pub object_size: u64,
    pub checksum: String,
    pub tier: RestoreTier,
    pub days: u32,
    pub expire_at: DateTime<Utc>,
    pub status: RestoreStatus,
    pub drive_id: Option<String>,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}
```

### 3.6 ArchiveTask (задача архивации)

Определение совпадает с 05-scheduler-layer §8.6.

```rust
pub struct ArchiveTask {
    pub id: Uuid,
    pub bundle_id: Uuid,
    pub tape_id: String,
    pub drive_id: Option<String>,
    pub object_count: u32,
    pub total_size: u64,
    pub bytes_written: u64,
    pub status: ArchiveTaskStatus,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

pub enum ArchiveTaskStatus {
    Pending, InProgress, Completed, Failed,
}
```

### 3.7 BucketInfo (сведения о корзине)

```rust
pub struct BucketInfo {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub owner: Option<String>,
    pub versioning_enabled: bool,
    pub object_count: u64,
    pub total_size: u64,
}
```

| Поле | Тип | Значение | Кто пишет |
|------|------|------|--------|
| `name` | String | имя корзины | слой доступа, CreateBucket |
| `created_at` | DateTime\<Utc\> | время создания | слой доступа |
| `owner` | Option\<String\> | владелец | слой доступа |
| `versioning_enabled` | bool | включено ли версионирование | слой доступа |
| `object_count` | u64 | число объектов | изменяется при записи |
| `total_size` | u64 | общий размер | изменяется при записи |

### 3.8 ListObjectsResult

```rust
pub struct ListObjectsResult {
    pub objects: Vec<ObjectMetadata>,
    pub next_marker: Option<String>,
    pub is_truncated: bool,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `objects` | Vec\<ObjectMetadata\> | список объектов текущей страницы |
| `next_marker` | Option\<String\> | курсор пагинации (используется как marker следующего запроса) |
| `is_truncated` | bool | есть ли ещё результаты |

### 3.10 Метаданные кластера и модель развёртывания

#### 3.10.1 Модель физического развёртывания

ColdStore состоит из пяти типов узлов. **Scheduler Worker — единственный бизнес-хаб**: Gateway подключается только к Scheduler,
Console — только к Metadata, три типа Worker регистрируются в Metadata и передают heartbeat.

```
┌──────────────────────┐          ┌──────────────────────┐
│  Gateway (N узлов)   │          │  Console (1 узел)    │
│  S3 HTTP фронтенд    │          │  Web UI панели       │
│  без состояния       │          │  без состояния       │
└──────┬───────────────┘          └──────┬───────────────┘
       │ конфиг: scheduler_addrs         │ конфиг: metadata_addrs
       ▼                                 ▼
 ┌─────────────────────────────┐  ┌──────────────────────────┐
 │  Тот же физический узел     │  │  Узлы Metadata (3/5 шт)  │
 │  ┌─────────────────────────┐│  │  Консенсус Raft +RocksDB │
 │  │  Scheduler Worker       ││  │  Метаданные + реестр     │
 │  │  бизнес-ядро:           │┼─►│  Worker                  │
 │  │  оркестрация планирования││  └──────────────────────────┘
 │  ├─────────────────────────┤│             ▲
 │  │  Cache Worker           ││             │ heartbeat
 │  │  кэш SPDK Blobstore     ││  ┌──────────┴───────────────┐
 │  └─────────────────────────┘│  │  Tape Worker (отдельный  │
 └─────────────────────────────┘  │  физический узел)        │
                                  │  Накопители /dev/nst*    │
                                  │  Библиотека /dev/sg*     │
                                  └──────────────────────────┘
```

| Тип узла | Обязанности | Количество | Объекты подключения |
|----------|------|------|----------|
| **Metadata** | консенсус Raft, хранение метаданных, реестр Worker | 3/5 | Scheduler (чтение и запись), три типа Worker (heartbeat), Console (управление) |
| **Scheduler Worker** | **бизнес-хаб**: оркестрация планирования, работа со всеми слоями (на одной машине с Cache) | 1~N | Metadata, Cache Worker, Tape Worker, Gateway |
| **Cache Worker** | кэш данных SPDK NVMe (на той же машине, что и Scheduler) | 1~N | Scheduler Worker (gRPC), Metadata (heartbeat) |
| **Tape Worker** | управление ленточными накопителями, чтение и запись данных (отдельный физический узел) | 1~N | Scheduler Worker (gRPC), Metadata (heartbeat) |
| **Gateway** | доступ через S3 HTTP + адаптация протокола (без состояния) | N | **только Scheduler Worker** |
| **Console** | Web UI панели администрирования + Admin API (без состояния) | 1 | **только Metadata** |

**Ключевые проектные решения**:

- **Gateway не подключается к Metadata напрямую**: Gateway — чистый фронтенд протокола S3, все бизнес-запросы идут в Scheduler Worker.
  В конфигурации Gateway достаточно указать адреса Scheduler Worker.
- **Scheduler Worker — единственный бизнес-хаб**: хранит MetadataClient, оркестрирует Cache Worker и Tape Worker,
  принимает все запросы Gateway и является единственной бизнес-точкой чтения и записи метаданных.
- **Scheduler ↔ Cache общаются по gRPC**: несмотря на размещение на одной машине, связь идёт по gRPC, что сохраняет единообразие архитектуры
  и позволяет в дальнейшем масштабировать независимо.
- **Console подключается только к Metadata**: панель администрирования напрямую читает и пишет кластер метаданных, управляет добавлением и удалением Worker.
- **Tape Worker развёртывается отдельно**: ленточные накопители вынесены в отдельный физический узел и по gRPC принимают команды чтения и записи от Scheduler.

#### 3.10.2 Топология межузлового взаимодействия

```
Gateway                          Console
   │ конфиг: scheduler_addrs       │ конфиг: metadata_addrs
   │                               │
   └──(gRPC)──► Scheduler Worker   └──(gRPC)──► Metadata
                    │                               ▲
                    ├──(gRPC)──► Metadata ───────────┘ (чтение и запись метаданных)
                    ├──(gRPC)──► Cache Worker (та же машина, gRPC)
                    └──(gRPC)──► Tape Worker  (удалённо, gRPC)
                                    │
                 три типа Worker ──(gRPC)──► Metadata (регистрация heartbeat)
```

| Путь взаимодействия | Протокол | Сценарий |
|----------|------|------|
| Gateway → Scheduler Worker | gRPC | **все** запросы S3 (PUT/GET/HEAD/DELETE/Restore) |
| Scheduler → Metadata | gRPC | чтение и запись метаданных (PutObject, сканирование ColdPending, обновление состояний и т.д.) |
| Scheduler → Cache Worker | gRPC (та же машина) | запись и чтение кэша (staging для PutObject, размороженные данные для GET) |
| Scheduler → Tape Worker | gRPC (удалённо) | выдача команд записи и чтения ленты |
| Console → Metadata | gRPC | управление кластером, добавление и удаление Worker, запрос метаданных |
| три типа Worker → Metadata | gRPC | передача heartbeat (состояние ресурсов) |
| Metadata ↔ Metadata | Raft RPC | протокол консенсуса |

> Gateway **не подключается** к Metadata и **не подключается** к Cache Worker и Tape Worker.
> Весь бизнес-трафик идёт через Scheduler Worker.

#### 3.10.3 Общая информация о кластере

```rust
pub struct ClusterInfo {
    pub cluster_id: String,
    pub metadata_nodes: Vec<MetadataNodeInfo>,
    pub scheduler_workers: Vec<SchedulerWorkerInfo>,
    pub cache_workers: Vec<CacheWorkerInfo>,
    pub tape_workers: Vec<TapeWorkerInfo>,
    pub leader_id: Option<u64>,
    pub term: u64,
    pub committed_index: u64,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `cluster_id` | String | уникальный идентификатор кластера |
| `metadata_nodes` | Vec\<MetadataNodeInfo\> | список узлов метаданных Raft |
| `scheduler_workers` | Vec\<SchedulerWorkerInfo\> | список Worker планирования |
| `cache_workers` | Vec\<CacheWorkerInfo\> | список Worker кэша |
| `tape_workers` | Vec\<TapeWorkerInfo\> | список ленточных Worker |
| `leader_id` | Option\<u64\> | ID текущего узла Leader в Raft |
| `term` | u64 | текущая казнь (term) в Raft |
| `committed_index` | u64 | индекс зафиксированного журнала |

> Gateway / Console в `ClusterInfo` не присутствуют.
> Gateway по конфигу напрямую связан с Scheduler Worker; Console по конфигу напрямую связан с Metadata.

#### 3.10.4 Общие типы

```rust
pub enum NodeStatus {
    Online,       // штатно обслуживает
    Offline,      // потеря связи (таймаут heartbeat)
    Draining,     // осушение (новые задачи не выдаются, ожидание завершения текущих)
    Maintenance,  // режим обслуживания (устанавливается администратором вручную)
}

pub enum RaftRole {
    Leader,
    Follower,
    Learner,
}

pub enum WorkerType {
    Scheduler,
    Cache,
    Tape,
}
```

#### 3.10.5 MetadataNodeInfo (узел метаданных)

```rust
pub struct MetadataNodeInfo {
    pub node_id: u64,
    pub addr: String,              // адрес сервиса Raft RPC + gRPC
    pub raft_role: RaftRole,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub status: NodeStatus,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `node_id` | u64 | ID узла Raft |
| `addr` | String | адрес сервиса (`host:port`), по которому подключаются Console/Worker |
| `raft_role` | RaftRole | текущая роль в Raft |
| `last_heartbeat` | Option\<DateTime\<Utc\>\> | время внутреннего heartbeat Raft |
| `status` | NodeStatus | состояние узла |

#### 3.10.6 SchedulerWorkerInfo (Worker планирования)

Worker планирования — **бизнес-хаб** ColdStore: принимает все запросы S3 от Gateway и оркестрирует Cache Worker и Tape Worker.
Физически размещается **на той же машине**, что и Cache Worker, связь идёт по gRPC (для единообразия архитектуры).

```rust
pub struct SchedulerWorkerInfo {
    pub node_id: u64,
    pub addr: String,                  // адрес сервиса gRPC
    pub status: NodeStatus,
    pub last_heartbeat: Option<DateTime<Utc>>,

    // ── состояние планирования ──
    pub is_active: bool,               // активный ли это scheduler в режиме основной/резервный
    pub pending_archive_tasks: u64,    // число задач, ожидающих архивации
    pub pending_recall_tasks: u64,     // число задач, ожидающих восстановления
    pub active_jobs: u64,              // число выполняемых заданий

    // ── связанный Cache Worker ──
    pub paired_cache_worker_id: u64,   // ID Cache Worker на той же машине
}
```

| Поле | Тип | Значение | Обновляется heartbeat |
|------|------|------|:---:|
| `node_id` | u64 | глобально уникальный ID | нет |
| `addr` | String | адрес gRPC | нет |
| `status` | NodeStatus | состояние узла | да |
| `is_active` | bool | активный ли планировщик (при нескольких Scheduler активен только один) | да |
| `pending_archive_tasks` | u64 | число задач, ожидающих архивации | да |
| `pending_recall_tasks` | u64 | число задач, ожидающих восстановления | да |
| `active_jobs` | u64 | число выполняемых заданий | да |
| `paired_cache_worker_id` | u64 | ID Cache Worker на той же машине | нет (фиксируется при регистрации) |

#### 3.10.7 CacheWorkerInfo (Worker кэша)

Worker кэша запускает SPDK Blobstore и предоставляет высокопроизводительный кэш данных на NVMe.
Физически размещается **на той же машине**, что и Scheduler Worker.

```rust
pub struct CacheWorkerInfo {
    pub node_id: u64,
    pub addr: String,                  // адрес сервиса gRPC (для удалённого чтения размороженных данных при GET)
    pub status: NodeStatus,
    pub last_heartbeat: Option<DateTime<Utc>>,

    // ── ресурсы кэша ──
    pub bdev_name: String,             // имя SPDK bdev (например "NVMe0n1")
    pub total_capacity: u64,           // полная ёмкость (bytes)
    pub used_capacity: u64,            // использованная ёмкость (bytes)
    pub blob_count: u64,              // текущее число объектов кэша
    pub io_unit_size: u32,             // размер io_unit SPDK (bytes)
}
```

| Поле | Тип | Значение | Обновляется heartbeat |
|------|------|------|:---:|
| `node_id` | u64 | глобально уникальный ID | нет |
| `addr` | String | адрес gRPC (подключается при GET на Gateway) | нет |
| `status` | NodeStatus | состояние узла | да |
| `bdev_name` | String | имя SPDK bdev | нет (фиксируется при регистрации) |
| `total_capacity` | u64 | полная ёмкость NVMe | нет |
| `used_capacity` | u64 | использованная ёмкость | да |
| `blob_count` | u64 | число объектов кэша | да |
| `io_unit_size` | u32 | размер io_unit SPDK | нет |

#### 3.10.8 TapeWorkerInfo (ленточный Worker)

Ленточный Worker развёртывается отдельно — на физическом узле с подключёнными ленточными накопителями и библиотекой,
и по gRPC принимает команды чтения и записи от планировщика.

```rust
pub struct TapeWorkerInfo {
    pub node_id: u64,
    pub addr: String,                  // адрес сервиса gRPC
    pub status: NodeStatus,
    pub last_heartbeat: Option<DateTime<Utc>>,

    // ── ленточные накопители ──
    pub drives: Vec<DriveEndpoint>,

    // ── библиотека (опционально, None если библиотеки нет) ──
    pub library: Option<LibraryEndpoint>,
}

/// эндпоинт ленточного накопителя
pub struct DriveEndpoint {
    pub drive_id: String,              // уникальный идентификатор накопителя
    pub device_path: String,           // путь к устройству /dev/nst0
    pub drive_type: String,            // тип накопителя "LTO-9" / "LTO-10"
    pub status: DriveStatus,
    pub current_tape: Option<String>,  // ID текущей загруженной ленты
}

pub enum DriveStatus {
    Idle,       // свободен, может быть выдан
    InUse,      // выполняет задачу чтения или записи
    Loading,    // идёт загрузка ленты
    Unloading,  // идёт выгрузка ленты
    Error,      // неисправность
    Offline,    // офлайн, обслуживание
}

/// эндпоинт библиотеки
pub struct LibraryEndpoint {
    pub device_path: String,           // /dev/sg5
    pub slot_count: u32,               // число складских слотов
    pub import_export_count: u32,      // число слотов импорта/экспорта («почтовый ящик»)
    pub drive_count: u32,              // число накопителей в библиотеке
}
```

| Поле | Тип | Значение | Обновляется heartbeat |
|------|------|------|:---:|
| `node_id` | u64 | глобально уникальный ID | нет |
| `addr` | String | адрес gRPC (планировщик подключается по нему для выдачи команд) | нет |
| `status` | NodeStatus | состояние узла | да |
| `drives[].status` | DriveStatus | текущее состояние каждого накопителя | да |
| `drives[].current_tape` | Option\<String\> | ID ленты в накопителе | да |
| `drives[].drive_type` | String | модель накопителя | нет |
| `library.slot_count` | u32 | общее число слотов | нет |

#### 3.10.9 Регистрация узлов, heartbeat и управление

```
                      Console (операции добавления/вывода)
                          │
                          ▼
  ┌──────────────┐   RegisterWorker    ┌──────────────────────┐
  │ Scheduler    │ ─────────────────► │                      │
  │ Worker       │   Heartbeat (5s)   │   Кластер Metadata   │
  ├──────────────┤ ─────────────────► │   (Raft + RocksDB)   │
  │ Cache Worker │   Heartbeat (5s)   │                      │
  ├──────────────┤ ─────────────────► │  cf_scheduler_workers │
  │ Tape Worker  │   Heartbeat (5s)   │  cf_cache_workers     │
  └──────────────┘                    │  cf_tape_workers      │
                                      └──────────────────────┘
```

**Процесс регистрации**:

1. Администратор добавляет Worker через Console (указывает тип, адрес и основные сведения)
2. Console вызывает `RegisterXxxWorker` в Metadata и записывает сведения о кластере
3. После запуска процесс Worker подключается к Metadata, убеждается, что зарегистрирован, и начинает передавать heartbeat

**Механизм heartbeat**:

- **частота**: каждые 5s
- **содержание**: каждый Worker передаёт своё текущее состояние (глубина очереди, ёмкость кэша, состояние накопителей и т.д.)
- **хранение**: `last_heartbeat` обновляется локально в памяти Leader узла Metadata, в **журнал Raft не пишется**
- **изменения состояния идут через Raft**: только при существенном изменении состояния (Online ↔ Offline, смена состояния накопителя)
  выполняется персистентность через Propose в Raft

**Обнаружение потери связи**:

- Leader узла Metadata каждые 15s выполняет скан; Worker, от которого не получено heartbeat три раза подряд (>15s), помечается `Offline`
- Накопители на ленточном Worker в состоянии Offline планировщику для задач больше не выдаются

**Обработка last_heartbeat при смене Leader**:

`last_heartbeat` поддерживается только в локальной памяти Leader и в журнал Raft не пишется. После смены Leader таблица
`last_heartbeat` у нового Leader пуста, поэтому нужна специальная обработка, чтобы ошибочно не пометить все Worker как Offline:

1. **Льготный период (Grace Period)**: после вступления в должность новый Leader устанавливает льготный период, равный 2 интервалам heartbeat (то есть 10s).
   В течение льготного периода обнаружение потери связи не выполняется
2. **Восстановление первым heartbeat**: первый полученный в льготный период heartbeat восстанавливает запись `last_heartbeat` этого Worker
3. **Обработка после льготного периода**: Worker, от которых heartbeat так и не пришёл, помечаются `Offline`
   (такие Worker действительно потеряли связь, а не стали следствием ошибочного решения из-за смены Leader)

```rust
pub struct HeartbeatManager {
    last_heartbeat: HashMap<(WorkerType, u64), Instant>,
    leader_since: Option<Instant>,
    grace_period: Duration,  // по умолчанию 10s (2 × heartbeat_interval)
}

impl HeartbeatManager {
    fn on_become_leader(&mut self) {
        self.last_heartbeat.clear();
        self.leader_since = Some(Instant::now());
    }

    fn is_in_grace_period(&self) -> bool {
        self.leader_since
            .map(|since| since.elapsed() < self.grace_period)
            .unwrap_or(false)
    }

    fn check_offline(&self) -> Vec<(WorkerType, u64)> {
        if self.is_in_grace_period() {
            return vec![];
        }
        // обычная логика обнаружения потери связи...
    }
}
```

**Стратегия персистентности**: сам `last_heartbeat` в Raft не персистится (частая запись заполнила бы весь журнал).
После смены Leader состояние восстанавливается механизмом льготного периода; цена — лишь короткий интервал (10s),
в течение которого обнаружение потери связи не выполняется, что почти не влияет на доступность системы.

**Корректный вывод из эксплуатации**:

1. Администратор запускает Drain (осушение) через Console
2. Состояние Worker меняется на `Draining`, новые задачи не принимаются
3. Ожидание завершения выполняемых задач
4. После подтверждения администратор выполняет Deregister через Console

#### 3.10.10 Конфигурация и обнаружение сервисов

Конфигурация каждого типа узла своя, с соблюдением принципа «минимального знания»:

```yaml
# конфигурация Gateway — нужны только адреса Scheduler Worker
gateway:
  scheduler_addrs:
    - "10.0.2.1:22001"
    - "10.0.2.2:22001"

# конфигурация Console — нужны только адреса Metadata
console:
  metadata_addrs:
    - "10.0.1.1:21001"
    - "10.0.1.2:21001"
    - "10.0.1.3:21001"

# конфигурация Scheduler Worker — нужны адреса Metadata (Cache/Tape обнаруживаются через Metadata)
scheduler_worker:
  metadata_addrs:
    - "10.0.1.1:21001"
    - "10.0.1.2:21001"
    - "10.0.1.3:21001"

# конфигурация Cache Worker / Tape Worker — нужны только адреса Metadata (для регистрации heartbeat)
cache_worker:
  metadata_addrs:
    - "10.0.1.1:21001"
    - "10.0.1.2:21001"
    - "10.0.1.3:21001"
```

**Обнаружение сервисов для Scheduler Worker**: при запуске Scheduler запрашивает в Metadata находящиеся онлайн Cache Worker и Tape Worker:

```rust
async fn discover_workers(metadata: &MetadataClient) -> WorkerTopology {
    let caches = metadata.list_online_cache_workers().await?;
    let tapes = metadata.list_online_tape_workers().await?;
    WorkerTopology { caches, tapes }
}
```

| Сценарий | Путь обнаружения |
|------|----------|
| все запросы Gateway | адреса Scheduler из конфигурации → отправка запроса по gRPC |
| операции Scheduler с кэшем | запрос к Metadata → получение адресов Cache Worker → чтение и запись кэша по gRPC |
| операции Scheduler с лентами | запрос к Metadata → получение адресов Tape Worker → выдача ленточных команд по gRPC |
| управление кластером из Console | адреса Metadata из конфигурации → управляющие операции по gRPC |

---

## 4. Проектирование Column Family

| Имя CF | Формат Key | Тип Value | Описание |
|---------|----------|-----------|------|
| `cf_objects` | `obj:{bucket}:{key}` | ObjectMetadata | метаданные объекта (текущая версия) |
| `cf_object_versions` | `objv:{bucket}:{key}:{version_id}` | ObjectMetadata | многоверсионные объекты |
| `cf_buckets` | `bkt:{bucket}` | BucketInfo | сведения о корзинах |
| `cf_bundles` | `bundle:{uuid}` | ArchiveBundle | архивный комплект (с entries) |
| `cf_tapes` | `tape:{tape_id}` | TapeInfo | сведения о ленте |
| `cf_recall_tasks` | `recall:{uuid}` | RecallTask | задачи восстановления |
| `cf_archive_tasks` | `archive:{uuid}` | ArchiveTask | задачи архивации |
| `cf_idx_bundle_objects` | `ibo:{archive_id}:{bucket}:{key}` | пусто | обратный индекс «архивный комплект → объект» |
| `cf_idx_tape_bundles` | `itb:{tape_id}:{bundle_id}` | пусто | обратный индекс «лента → архивный комплект» |
| `cf_idx_pending` | `pend:{created_at}:{bucket}:{key}` | пусто | индекс сканирования объектов ColdPending (сортировка по времени) |
| `cf_idx_recall_by_tape` | `rbt:{tape_id}:{recall_id}` | пусто | поиск задач восстановления по ленте (для объединения) |
| `cf_scheduler_workers` | `sw:{node_id}` | SchedulerWorkerInfo | сведения о регистрации Worker планирования |
| `cf_cache_workers` | `cw:{node_id}` | CacheWorkerInfo | сведения о регистрации Worker кэша |
| `cf_tape_workers` | `tw:{node_id}` | TapeWorkerInfo | сведения о регистрации ленточного Worker |

---

## 5. Трейт MetadataService: API для остальных слоёв

### 5.1 Принцип проектирования

MetadataService разделён на несколько дочерних трейтов, сгруппированных по потребителям:

```
                     ┌──────────────────────────────┐
                     │     MetadataService          │
                     │  impl ObjectApi              │
                     │  impl BucketApi              │
                     │  impl ArchiveApi             │
                     │  impl RecallApi              │
                     │  impl TapeApi                │
                     │  impl ClusterApi             │
                     └──────────────────────────────┘
                          │           │
            ┌─────────────┘           └───────────────┐
            ▼                                         ▼
     слой доступа / протокольный слой              слой планирования
   ObjectApi (чтение+запись)                  ArchiveApi (чтение+запись)
   BucketApi (чтение+запись)                  RecallApi (чтение+запись)
   RecallApi (только запись: создание)         TapeApi (чтение+запись)
                                              ObjectApi (чтение+запись)
```

### 5.2 ObjectApi (метаданные объектов)

```rust
#[async_trait]
pub trait ObjectApi: Send + Sync {
    // ── используется слоем доступа ──
    async fn put_object(&self, meta: ObjectMetadata) -> Result<()>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMetadata>>;
    async fn get_object_version(
        &self, bucket: &str, key: &str, version_id: &str,
    ) -> Result<Option<ObjectMetadata>>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMetadata>>;
    async fn list_objects(
        &self, bucket: &str, prefix: Option<&str>, marker: Option<&str>, max_keys: u32,
    ) -> Result<ListObjectsResult>;

    // ── используется слоем планирования ──
    async fn update_storage_class(
        &self, bucket: &str, key: &str, class: StorageClass,
    ) -> Result<()>;
    async fn update_archive_location(
        &self,
        bucket: &str,
        key: &str,
        archive_id: Uuid,
        tape_id: &str,
        tape_set: Vec<String>,
        tape_block_offset: u64,
    ) -> Result<()>;
    async fn update_restore_status(
        &self,
        bucket: &str,
        key: &str,
        status: RestoreStatus,
        expire_at: Option<DateTime<Utc>>,
    ) -> Result<()>;
    async fn scan_cold_pending(
        &self, limit: u32,
    ) -> Result<Vec<ObjectMetadata>>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `put_object` | Scheduler (проксирует PutObject Gateway) | создание/перезапись метаданных объекта |
| `get_object` | Scheduler (проксирует GET/HEAD Gateway) | запрос текущей версии |
| `get_object_version` | Scheduler | запрос указанной версии |
| `delete_object` | Scheduler (проксирует DELETE Gateway) | удаление объекта |
| `head_object` | Scheduler (проксирует HEAD Gateway) | возвращает storage_class, restore_status |
| `list_objects` | Scheduler (проксирует ListObjects Gateway) | ListObjects |
| `update_storage_class` | слой планирования | после завершения архивации ColdPending → Cold |
| `update_archive_location` | слой планирования | после завершения архивации записывает archive_id, tape_id, tape_block_offset |
| `update_restore_status` | слой планирования | переходы состояния восстановления + expire_at |
| `scan_cold_pending` | слой планирования | сканирование объектов, ожидающих архивации (использует индекс `cf_idx_pending`) |

### 5.3 BucketApi (управление корзинами)

```rust
#[async_trait]
pub trait BucketApi: Send + Sync {
    async fn create_bucket(&self, info: BucketInfo) -> Result<()>;
    async fn get_bucket(&self, name: &str) -> Result<Option<BucketInfo>>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `create_bucket` | слой доступа | CreateBucket |
| `delete_bucket` | слой доступа | DeleteBucket (нужна проверка, что корзина пуста) |
| `list_buckets` | слой доступа | ListBuckets |

### 5.4 ArchiveApi (архивные метаданные)

```rust
#[async_trait]
pub trait ArchiveApi: Send + Sync {
    async fn put_archive_bundle(&self, bundle: ArchiveBundle) -> Result<()>;
    async fn get_archive_bundle(&self, id: Uuid) -> Result<Option<ArchiveBundle>>;
    async fn update_archive_bundle_status(
        &self, id: Uuid, status: ArchiveBundleStatus,
    ) -> Result<()>;
    async fn list_bundles_by_tape(&self, tape_id: &str) -> Result<Vec<Uuid>>;
    async fn put_archive_task(&self, task: ArchiveTask) -> Result<()>;
    async fn get_archive_task(&self, id: Uuid) -> Result<Option<ArchiveTask>>;
    async fn update_archive_task(&self, task: ArchiveTask) -> Result<()>;
    async fn list_pending_archive_tasks(&self) -> Result<Vec<ArchiveTask>>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `put_archive_bundle` | слой планирования | после завершения архивации записывает Bundle (с entries) |
| `get_archive_bundle` | слой планирования | при восстановлении получает Bundle (с entries, filemark_start) |
| `list_bundles_by_tape` | слой планирования | запрос всех Bundle на ленте (использует `cf_idx_tape_bundles`) |
| `put_archive_task` | слой планирования | создание задачи архивации |
| `update_archive_task` | слой планирования | обновление состояния/прогресса задачи |

### 5.5 RecallApi (метаданные восстановления)

```rust
#[async_trait]
pub trait RecallApi: Send + Sync {
    async fn put_recall_task(&self, task: RecallTask) -> Result<()>;
    async fn get_recall_task(&self, id: Uuid) -> Result<Option<RecallTask>>;
    async fn update_recall_task(&self, task: RecallTask) -> Result<()>;
    async fn list_pending_recall_tasks(&self) -> Result<Vec<RecallTask>>;
    async fn list_recall_tasks_by_tape(&self, tape_id: &str) -> Result<Vec<RecallTask>>;
    async fn find_active_recall(
        &self, bucket: &str, key: &str,
    ) -> Result<Option<RecallTask>>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `put_recall_task` | Scheduler (проксирует RestoreObject Gateway) | создание задачи восстановления |
| `get_recall_task` | Scheduler | запрос подробностей задачи |
| `update_recall_task` | Scheduler | обновление состояния Pending → InProgress → Completed |
| `list_pending_recall_tasks` | Scheduler | сканирование задач, ожидающих планирования |
| `list_recall_tasks_by_tape` | Scheduler | объединение задач на одну ленту (использует `cf_idx_recall_by_tape`) |
| `find_active_recall` | Scheduler (проверка дублей при проксировании RestoreObject) | RestoreAlreadyInProgress |

### 5.6 TapeApi (метаданные лент)

```rust
#[async_trait]
pub trait TapeApi: Send + Sync {
    async fn put_tape(&self, info: TapeInfo) -> Result<()>;
    async fn get_tape(&self, tape_id: &str) -> Result<Option<TapeInfo>>;
    async fn update_tape(&self, info: TapeInfo) -> Result<()>;
    async fn list_tapes(&self) -> Result<Vec<TapeInfo>>;
    async fn list_tapes_by_status(&self, status: TapeStatus) -> Result<Vec<TapeInfo>>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `put_tape` | слой планирования | регистрация новой ленты |
| `get_tape` | слой планирования | запрос сведений о ленте |
| `update_tape` | слой планирования | обновление used_bytes, status, error_count и т.д. |
| `list_tapes_by_status` | слой планирования | отбор доступных лент (Online + есть свободное место) |

### 5.7 ClusterApi (управление кластером)

```rust
#[async_trait]
pub trait ClusterApi: Send + Sync {
    // ── состояние Raft ──
    async fn cluster_info(&self) -> Result<ClusterInfo>;
    async fn is_leader(&self) -> bool;
    async fn ensure_linearizable(&self) -> Result<()>;

    // ── Scheduler Worker ──
    async fn register_scheduler_worker(&self, info: SchedulerWorkerInfo) -> Result<()>;
    async fn deregister_scheduler_worker(&self, node_id: u64) -> Result<()>;
    async fn list_online_scheduler_workers(&self) -> Result<Vec<SchedulerWorkerInfo>>;
    async fn get_scheduler_worker(&self, node_id: u64) -> Result<Option<SchedulerWorkerInfo>>;

    // ── Cache Worker ──
    async fn register_cache_worker(&self, info: CacheWorkerInfo) -> Result<()>;
    async fn deregister_cache_worker(&self, node_id: u64) -> Result<()>;
    async fn list_online_cache_workers(&self) -> Result<Vec<CacheWorkerInfo>>;
    async fn get_cache_worker(&self, node_id: u64) -> Result<Option<CacheWorkerInfo>>;

    // ── Tape Worker ──
    async fn register_tape_worker(&self, info: TapeWorkerInfo) -> Result<()>;
    async fn deregister_tape_worker(&self, node_id: u64) -> Result<()>;
    async fn list_online_tape_workers(&self) -> Result<Vec<TapeWorkerInfo>>;
    async fn get_tape_worker(&self, node_id: u64) -> Result<Option<TapeWorkerInfo>>;

    // ── общие операции с Worker ──
    async fn update_worker_status(&self, worker_type: WorkerType, node_id: u64, status: NodeStatus) -> Result<()>;
    async fn drain_worker(&self, worker_type: WorkerType, node_id: u64) -> Result<()>;
}
```

| Метод | Потребитель | Описание |
|------|--------|------|
| `cluster_info` | Console/Scheduler | получение общей информации о кластере |
| `register_xxx_worker` | Console (добавление администратором) | регистрация Worker в кластере |
| `deregister_xxx_worker` | Console (вывод из эксплуатации администратором) | удаление Worker из кластера |
| `list_online_xxx_workers` | Scheduler (обнаружение сервисов) | получение списка онлайн Cache/Tape Worker |
| `update_worker_status` | Leader Metadata | обновление состояния при таймауте heartbeat |
| `drain_worker` | Console | осушение Worker (новые задачи не выдаются) |

### 5.8 MetadataService (агрегирующий тип)

```rust
pub struct MetadataService {
    raft: Arc<Raft<ColdStoreTypeConfig>>,
    store: Arc<RocksStore>,
}

impl ObjectApi for MetadataService { /* ... */ }
impl BucketApi for MetadataService { /* ... */ }
impl ArchiveApi for MetadataService { /* ... */ }
impl RecallApi for MetadataService { /* ... */ }
impl TapeApi for MetadataService { /* ... */ }
impl ClusterApi for MetadataService { /* ... */ }
```

Для удобства внешних ссылок заданы псевдонимы типов:

```rust
pub type MetadataClient = MetadataService;
```

При внедрении в слой планирования и слой доступа используется `Arc<MetadataClient>` (то есть `Arc<MetadataService>`):

```rust
// слой доступа
pub struct S3Handler {
    metadata: Arc<MetadataClient>,   // ObjectApi + BucketApi + RecallApi
    cache: Arc<dyn CacheReadApi>,
}

// слой планирования
pub struct ArchiveScheduler {
    metadata: Arc<MetadataClient>,   // ObjectApi + ArchiveApi + TapeApi
    tape_manager: Arc<TapeManager>,
}

pub struct RecallScheduler {
    metadata: Arc<MetadataClient>,   // ObjectApi + RecallApi + ArchiveApi + TapeApi
    tape_manager: Arc<TapeManager>,
    cache: Arc<dyn CacheWriteApi>,
}
```

---

## 6. Команды конечного автомата Raft

```rust
pub enum ColdStoreRequest {
    // ── объекты ──
    PutObject(ObjectMetadata),
    DeleteObject { bucket: String, key: String },
    UpdateStorageClass { bucket: String, key: String, class: StorageClass },
    UpdateArchiveLocation {
        bucket: String, key: String,
        archive_id: Uuid, tape_id: String, tape_set: Vec<String>, tape_block_offset: u64,
    },
    UpdateRestoreStatus {
        bucket: String, key: String,
        status: RestoreStatus, expire_at: Option<DateTime<Utc>>,
    },

    // ── корзины ──
    CreateBucket(BucketInfo),
    DeleteBucket { name: String },

    // ── архивация ──
    PutArchiveBundle(ArchiveBundle),
    UpdateArchiveBundleStatus { id: Uuid, status: ArchiveBundleStatus },
    PutArchiveTask(ArchiveTask),
    UpdateArchiveTask(ArchiveTask),

    // ── восстановление ──
    PutRecallTask(RecallTask),
    UpdateRecallTask(RecallTask),

    // ── ленты ──
    PutTape(TapeInfo),
    UpdateTape(TapeInfo),

    // ── регистрация Worker (трёх типов) ──
    RegisterSchedulerWorker(SchedulerWorkerInfo),
    DeregisterSchedulerWorker { node_id: u64 },
    RegisterCacheWorker(CacheWorkerInfo),
    DeregisterCacheWorker { node_id: u64 },
    RegisterTapeWorker(TapeWorkerInfo),
    DeregisterTapeWorker { node_id: u64 },

    // ── изменение состояния Worker ──
    UpdateWorkerStatus { worker_type: WorkerType, node_id: u64, status: NodeStatus },
}
```

| Команда | Кто пишет | Описание |
|------|--------|------|
| `PutObject` | Scheduler Worker (проксирует запросы Gateway) | создание объекта (storage_class = ColdPending) |
| `DeleteObject` | Scheduler Worker (проксирует запросы Gateway) | удаление объекта |
| `UpdateStorageClass` | Scheduler Worker | ColdPending → Cold |
| `UpdateArchiveLocation` | Scheduler Worker | после завершения архивации записывает расположение на ленте |
| `UpdateRestoreStatus` | Scheduler Worker | переходы состояния восстановления (включая expire_at) |
| `PutArchiveBundle` | Scheduler Worker | создание архивного комплекта |
| `PutRecallTask` | Scheduler Worker (проксирует RestoreObject Gateway) | создание задачи восстановления |
| `UpdateRecallTask` | Scheduler Worker | обновление состояния задачи восстановления |
| `PutTape` / `UpdateTape` | Scheduler Worker | управление сведениями о лентах |
| `RegisterXxxWorker` | Console (добавление администратором) | регистрация Worker трёх типов |
| `DeregisterXxxWorker` | Console (вывод из эксплуатации администратором) |удаление Worker трёх типов из реестра |
| `UpdateWorkerStatus` | Leader Metadata | пишется только при изменении состояния (Online ↔ Offline и т.п.) |

> **Оптимизация heartbeat**: три типа Worker передают heartbeat каждые 5s, но `last_heartbeat` обновляется
> только в локальной памяти Leader и в журнал Raft не пишется. Только при **существенном изменении состояния**
> (узел выходит в онлайн/офлайн, состояние накопителя меняется) изменение персистится через
> `UpdateWorkerStatus` с Propose в Raft — так частые heartbeat не забивают журнал.

---

## 7. Пути чтения и записи

| Операция | Путь | Согласованность |
|------|------|--------|
| запись | Client → Leader → Propose в Raft → Append к большинству → Apply → RocksDB | строгая |
| чтение (по умолчанию) | любой узел → локальный RocksDB | окончательная |
| линейное чтение | `raft.ensure_linearizable().await` → локальный RocksDB | строгая |

**Рекомендации для пишущей стороны**:

| Операция | Требование к согласованности | Рекомендация |
|------|-----------|------|
| PutObject / DeleteObject | строгая | запись идёт через Raft |
| проверка дублей RestoreObject | строгая | `find_active_recall` использует линейное чтение |
| GET читает restore_status | допустима кратковременная задержка | достаточно чтения по умолчанию |
| scan_cold_pending | допустима кратковременная задержка | чтение по умолчанию |
| обновление по завершении архивации | строгая | запись идёт через Raft |

---

## 8. Схема архитектуры

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       MetadataService                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  ObjectApi | BucketApi | ArchiveApi | RecallApi | TapeApi | ClusterApi │  │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                         ┌──────────┴──────────┐                              │
│                         ▼                     ▼                              │
│                   ┌──────────┐          ┌──────────┐                        │
│                   │ путь     │          │ путь     │                        │
│                   │ записи   │          │ чтения   │                        │
│                   │ Propose  │          │ локальный│                        │
│                   │ в Raft   │          │ RocksDB  │                        │
│                   └────┬─────┘          └──────────┘                        │
│                        ▼                                                     │
│            ┌───────────────────────┐                                        │
│            │  Raft Core (openraft)  │                                        │
│            │  реплика к большинству │                                        │
│            │  → Apply               │                                        │
│            └───────────┬───────────┘                                        │
│                        ▼                                                     │
│            ┌───────────────────────┐                                        │
│            │  RocksDB              │                                        │
│            │  cf_objects           │                                        │
│            │  cf_bundles           │                                        │
│            │  cf_tapes             │                                        │
│            │  cf_recall_tasks      │                                        │
│            │  cf_archive_tasks     │                                        │
│            │  cf_buckets           │                                        │
│            │  cf_idx_*             │                                        │
│            └───────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Структура модуля

```
src/metadata/
├── mod.rs                   # pub use, публикация MetadataService
├── service.rs               # структура MetadataService, агрегирует реализации всех трейтов
├── traits/                  # ─── определения трейтов ───
│   ├── mod.rs
│   ├── object.rs            # ObjectApi
│   ├── bucket.rs            # BucketApi
│   ├── archive.rs           # ArchiveApi
│   ├── recall.rs            # RecallApi
│   ├── tape.rs              # TapeApi
│   └── cluster.rs           # ClusterApi
├── models/                  # ─── структуры данных ───
│   ├── mod.rs
│   ├── object.rs            # ObjectMetadata, StorageClass, RestoreStatus
│   ├── bundle.rs            # ArchiveBundle, BundleEntry, ArchiveBundleStatus
│   ├── tape.rs              # TapeInfo, TapeStatus
│   ├── task.rs              # RecallTask, ArchiveTask, RestoreTier
│   ├── bucket.rs            # BucketInfo
│   ├── cluster.rs           # ClusterInfo, MetadataNodeInfo, RaftRole, NodeStatus, WorkerType
│   └── worker.rs            # SchedulerWorkerInfo, CacheWorkerInfo, TapeWorkerInfo, DriveEndpoint, LibraryEndpoint
├── raft/                    # ─── реализация Raft ───
│   ├── mod.rs
│   ├── client.rs            # клиент Raft (Propose + линейное чтение)
│   ├── store.rs             # расширение RocksStore (инициализация CF)
│   ├── state_machine.rs     # логика Apply для ColdStoreRequest
│   └── network.rs           # межузловой RPC
└── index.rs                 # логика поддержки вторичных индексов
```

---

## 10. Параметры конфигурации

```yaml
metadata:
  backend: "RaftRocksDB"
  raft:
    node_id: 1
    cluster: "1:127.0.0.1:21001,2:127.0.0.1:21002,3:127.0.0.1:21003"
    data_path: "/var/lib/coldstore/metadata"
    snapshot_interval: 10000        # snapshot раз в 10000 записей журнала
    heartbeat_interval_ms: 200
    election_timeout_ms: 1000
  rocksdb:
    max_open_files: 1024
    write_buffer_size_mb: 64
    max_background_jobs: 4
```

---

## 11. Зависимости (вариант B)

### 11.1 Точки записи метаданных

| Кто пишет | Используемые трейты | Операции |
|--------|-------------|------|
| **слой доступа** | ObjectApi, BucketApi | PutObject, DeleteObject, CreateBucket |
| **протокольный слой** | RecallApi | PutRecallTask (создаётся при RestoreObject) |
| **слой планирования** | ObjectApi, ArchiveApi, RecallApi, TapeApi | переходы состояний всего процесса архивации/восстановления |

### 11.2 Кто читает метаданные

| Кто читает | Используемые трейты | Назначение |
|--------|-------------|------|
| слой доступа/протокольный слой | ObjectApi, BucketApi, RecallApi | запрос состояния объекта, списка корзин, проверка дублей Restore |
| слой планирования | ObjectApi, ArchiveApi, RecallApi, TapeApi | сканирование ColdPending, поиск расположения на ленте, объединение задач |

### 11.3 Слои без прямой зависимости

| Слой | Отношение |
|------|------|
| слой кэша | **MetadataService не хранит**, оркестрируется слоем планирования |
| слой лент | **MetadataService не хранит**, оркестрируется слоем планирования |

---

## 12. Справочные материалы

- [OpenRaft](https://github.com/databendlabs/openraft)
- [openraft-rocksstore](https://crates.io/crates/openraft-rocksstore)
- [RocksDB Column Families](https://github.com/facebook/rocksdb/wiki/Column-Families)
