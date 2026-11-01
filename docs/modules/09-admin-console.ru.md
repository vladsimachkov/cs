# Проектирование панели администрирования (Admin Console)

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md) | [03 — кластер метаданных](./03-metadata-cluster.ru.md) | [06 — слой управления лентами](./06-tape-layer.ru.md) | [08 — наблюдаемость](./08-observability.ru.md)

## 1. Цели проектирования

Панель администрирования предоставляет эксплуатационным инженерам и администраторам Web UI и REST API, покрывающее полное оперативное управление системой холодного архивного хранения ColdStore.

### 1.1 Ключевые функциональные области

| Функциональная область | Описание | Соответствующий бизнес-слой |
|--------|------|-----------|
| Управление кластером | добавление/вывод из эксплуатации/осушение трёх типов Worker, состояние Metadata, мониторинг Raft | слой метаданных (03) |
| Управление корзинами и объектами | CRUD корзин, запрос метаданных объектов, статистика по категориям хранения | слой метаданных (03) |
| Управление задачами архивации | список ArchiveTask, прогресс, повтор после сбоя | слой планирования (05) |
| Управление задачами восстановления | список RecallTask, отслеживание состояния, корректировка приоритета | слой планирования (05) |
| Управление ленточными носителями | регистрация лент, ёмкость, состояние, офлайн/вывод в онлайн, вывод из эксплуатации | слой лент (06) |
| Управление накопителями | состояние накопителей, загрузка, распределение | слой лент (06) |
| Мониторинг кэша | ёмкость, доля попаданий, статистика вытеснения | слой кэша (04) |
| Системный мониторинг | дашборды, метрики, оповещения, журнал аудита | наблюдаемость (08) |

### 1.2 Место в архитектуре

Console **подключается только к кластеру Metadata**, а не напрямую к Scheduler Worker, Cache Worker или Tape Worker.
Все данные выполнения (статистика кэша, состояние накопителей, глубина очередей и т.д.) каждый Worker передаёт в Metadata через heartbeat,
а Console читает их из Metadata.

```
┌─────────────────────────────────────────────────────────────────┐
│                Браузер (Admin Console)                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  React 18 + TypeScript + Ant Design 5                     │  │
│  └──────────────────────────┬────────────────────────────────┘  │
└─────────────────────────────┼───────────────────────────────────┘
                              │ HTTP/JSON
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│            ColdStore Admin API (Axum :8080)                     │
│  отдельный процесс, подключается только к кластеру Metadata     │
│  ┌──────────┬───────────┬──────────┬──────────┬───────────┐    │
│  │ /cluster │ /buckets  │ /objects │ /tapes   │ /tasks    │    │
│  │ /drives  │ /cache    │ /metrics │ /audit   │ /config   │    │
│  └──────────┴───────────┴──────────┴──────────┴───────────┘    │
│              │                                                   │
│              ▼                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  MetadataClient (gRPC)                                    │  │
│  │  все данные берутся из кластера Metadata                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────┘
                               │ gRPC (конфиг: metadata_addrs)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  Кластер Metadata (Raft + RocksDB)                              │
│  - метаданные объектов/корзин/задач                             │
│  - сведения о регистрации Worker + данные выполнения,           │
│    передаваемые через heartbeat                                  │
│  - сведения о лентах/накопителях (передаются heartbeat Worker)  │
│  - статистика кэша (передаётся heartbeat Cache Worker)          │
└─────────────────────────────────────────────────────────────────┘
```

**Ключевые проектные решения**:

- **Console не подключается ни к одному Worker напрямую**: в соответствии с DESIGN.ru.md §2.1 Console подключается только к Metadata
- **Источник данных выполнения**: каждый Worker каждые 5s передаёт своё состояние в Metadata heartbeat (см. 03-metadata-cluster §3.10.9),
  включая ёмкость/долю попаданий кэша (CacheWorkerInfo), состояние накопителей и текущую ленту (TapeWorkerInfo),
  глубину очередей и активные задания (SchedulerWorkerInfo)
- **Путь чтения Console**: Admin API панели через MetadataClient запрашивает сведения о Worker в Metadata
  и получает статистику кэша, состояние ленточных накопителей и другие данные выполнения

---

## 2. Выбор технологий

### 2.1 Бэкенд

| Компонент | Выбор | Описание |
|------|------|------|
| HTTP-фреймворк | **Axum** | тот же фреймворк, что в слое доступа S3, но отдельный Router и порт |
| Сериализация | **serde + serde_json** | JSON-запросы и ответы |
| Аутентификация | **JWT** (crate jsonwebtoken) | аутентификация по Bearer Token |
| Документация API | **utoipa + Swagger UI** | автоматическая генерация OpenAPI 3.0 |
| WebSocket | **axum::extract::ws** | публикация состояния задач и событий оповещений в реальном времени |

### 2.2 Фронтенд

| Компонент | Выбор | Описание |
|------|------|------|
| Фреймворк | **React 18 + TypeScript** | хуки и функциональные компоненты, типобезопасность |
| Библиотека UI | **Ant Design 5** | корпоративные компоненты панели управления (Table, Form, Chart) |
| Графики | **ECharts** (`echarts-for-react`) | диаграммы дашборда |
| HTTP | **Axios** | вызовы API |
| Управление состоянием | **Zustand** | лёгкое глобальное состояние |
| Маршрутизация | **React Router 6** | маршрутизация SPA |
| Сборка | **Vite** (`@vitejs/plugin-react`) | быстрая сборка |

---

## 3. Проектирование бэкенда Admin API

### 3.1 Общий обзор API

Все API имеют префикс `/api/v1/admin` и возвращают единый формат JSON.

```rust
pub struct ApiResponse<T: Serialize> {
    pub code: u32,         // 0 = успех, ненулевое = код ошибки
    pub message: String,
    pub data: Option<T>,
}

pub struct PageRequest {
    pub page: u32,         // начинается с 1
    pub page_size: u32,    // по умолчанию 20, максимум 100
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,  // "asc" / "desc"
}

pub struct PageResponse<T: Serialize> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u32,
    pub page_size: u32,
}
```

### 3.2 API управления кластером

#### 3.2.1 Обзор кластера

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/cluster/info` | обзор кластера (сведения обо всех узлах, состояние Raft) | read |
| GET | `/cluster/health` | проверка исправности (состояние Raft, доля онлайн Worker каждого типа) | read |
| GET | `/cluster/raft/status` | подробное состояние Raft (log index, commit index, snapshot) | read |

#### 3.2.2 Управление Scheduler Worker

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/cluster/scheduler-workers` | список Worker планирования | read |
| GET | `/cluster/scheduler-workers/{id}` | сведения о Worker планирования (глубина очереди, активные задания и т.д.) | read |
| POST | `/cluster/scheduler-workers` | **добавить** Worker планирования (регистрация в Metadata) | admin |
| DELETE | `/cluster/scheduler-workers/{id}` | **вывести из эксплуатации** Worker планирования (удаление из Metadata) | admin |
| POST | `/cluster/scheduler-workers/{id}/drain` | осушение (новые задачи не выдаются, ожидание завершения текущих) | admin |

#### 3.2.3 Управление Cache Worker

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/cluster/cache-workers` | список Worker кэша | read |
| GET | `/cluster/cache-workers/{id}` | сведения о Worker кэша (ёмкость, число объектов и т.д.) | read |
| POST | `/cluster/cache-workers` | **добавить** Worker кэша | admin |
| DELETE | `/cluster/cache-workers/{id}` | **вывести из эксплуатации** Worker кэша | admin |
| POST | `/cluster/cache-workers/{id}/drain` | осушение | admin |

#### 3.2.4 Управление Tape Worker

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/cluster/tape-workers` | список ленточных Worker | read |
| GET | `/cluster/tape-workers/{id}` | сведения о ленточном Worker (список накопителей, состояние библиотеки) | read |
| POST | `/cluster/tape-workers` | **добавить** ленточный Worker | admin |
| DELETE | `/cluster/tape-workers/{id}` | **вывести из эксплуатации** ленточный Worker | admin |
| POST | `/cluster/tape-workers/{id}/drain` | осушение | admin |

> Gateway / Console — узлы без состояния, в кластере не регистрируются и в панели администрирования не отображаются.
> Добавление и вывод из эксплуатации трёх типов Worker выполняются через Console, фактическая запись идёт в кластер Metadata.

**Пример ответа**:

```json
// GET /cluster/info
{
  "code": 0,
  "message": "ok",
  "data": {
    "cluster_id": "coldstore-prod-01",
    "metadata_nodes": [
      { "node_id": 1, "addr": "10.0.1.1:21001", "raft_role": "Leader", "status": "Online" },
      { "node_id": 2, "addr": "10.0.1.2:21001", "raft_role": "Follower", "status": "Online" },
      { "node_id": 3, "addr": "10.0.1.3:21001", "raft_role": "Follower", "status": "Online" }
    ],
    "scheduler_workers": [
      {
        "node_id": 10, "addr": "10.0.2.1:22001", "status": "Online",
        "is_active": true, "pending_archive": 42, "pending_recall": 5, "active_jobs": 3,
        "paired_cache_worker_id": 20
      }
    ],
    "cache_workers": [
      {
        "node_id": 20, "addr": "10.0.2.1:22002", "status": "Online",
        "bdev_name": "NVMe0n1", "total_capacity": "2TB", "used_capacity": "800GB", "blob_count": 15000
      }
    ],
    "tape_workers": [
      {
        "node_id": 30, "addr": "10.0.3.1:23001", "status": "Online",
        "drives": [
          { "drive_id": "drive0", "device": "/dev/nst0", "status": "InUse", "current_tape": "TAPE001" },
          { "drive_id": "drive1", "device": "/dev/nst1", "status": "Idle", "current_tape": null }
        ],
        "library": { "device": "/dev/sg5", "slots": 24, "drives": 2 }
      }
    ],
    "leader_id": 1,
    "term": 42,
    "committed_index": 1583920
  }
}
```

### 3.3 API управления корзинами

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/buckets` | список корзин (с числом объектов и ёмкостью) | read |
| GET | `/buckets/{name}` | сведения о корзине | read |
| POST | `/buckets` | создать корзину | admin |
| DELETE | `/buckets/{name}` | удалить корзину | admin |
| GET | `/buckets/{name}/stats` | статистика корзины (число и размер объектов по каждому StorageClass) | read |

**Ответ со статистикой корзины**:

```json
// GET /buckets/my-bucket/stats
{
  "code": 0,
  "data": {
    "name": "my-bucket",
    "total_objects": 125000,
    "total_size_bytes": 5368709120,
    "by_storage_class": {
      "ColdPending": { "count": 500, "size_bytes": 214748364 },
      "Cold": { "count": 114500, "size_bytes": 4080218932 }
    },
    "restore_in_progress": 12,
    "restored_available": 45
  }
}
```

### 3.4 API запроса метаданных объектов

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/objects?bucket=&prefix=&storage_class=&page=&page_size=` | список объектов (с фильтрацией) | read |
| GET | `/objects/{bucket}/{key}` | подробные метаданные объекта | read |
| GET | `/objects/{bucket}/{key}/versions` | список версий объекта | read |
| GET | `/objects/{bucket}/{key}/archive` | сведения об архивации объекта (Bundle, расположение на ленте) | read |
| GET | `/objects/{bucket}/{key}/restore` | состояние и история восстановления объекта | read |

**Параметры фильтрации**:

| Параметр | Тип | Описание |
|------|------|------|
| `bucket` | string | обязателен, имя корзины |
| `prefix` | string | необязателен, фильтр по префиксу key |
| `storage_class` | string | необязателен, `ColdPending`/`Cold` |
| `restore_status` | string | необязателен, `Pending`/`InProgress`/`Completed`/`Failed` |
| `page` | u32 | номер страницы |
| `page_size` | u32 | размер страницы |

**Ответ с подробными сведениями об объекте**:

```json
// GET /objects/my-bucket/data/file.bin
{
  "code": 0,
  "data": {
    "bucket": "my-bucket",
    "key": "data/file.bin",
    "version_id": null,
    "size": 104857600,
    "checksum": "a1b2c3d4...",
    "content_type": "application/octet-stream",
    "etag": "\"9e107d9d372bb6826bd81d3542a419d6\"",
    "storage_class": "Cold",
    "archive_info": {
      "archive_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
      "tape_id": "TAPE001",
      "tape_set": ["TAPE001", "TAPE002"],
      "tape_block_offset": 524288,
      "bundle_status": "Completed",
      "archived_at": "2026-02-20T08:30:00Z"
    },
    "restore_info": {
      "status": "Completed",
      "expire_at": "2026-03-06T08:30:00Z",
      "tier": "Standard",
      "recall_task_id": "c1d2e3f4-..."
    },
    "created_at": "2026-02-15T10:00:00Z",
    "updated_at": "2026-02-20T08:30:00Z"
  }
}
```

### 3.5 API задач архивации

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/tasks/archive?status=&page=&page_size=` | список задач архивации | read |
| GET | `/tasks/archive/{id}` | подробности задачи архивации (со списком объектов Bundle) | read |
| POST | `/tasks/archive/{id}/retry` | повторить завершившуюся сбоем задачу архивации | admin |
| POST | `/tasks/archive/{id}/cancel` | отменить задачу архивации в состоянии Pending | admin |
| GET | `/tasks/archive/stats` | статистика архивации (число завершённых, сбоев, пропускная способность, динамика) | read |

**Ответ со списком задач архивации**:

```json
{
  "code": 0,
  "data": {
    "items": [
      {
        "id": "a1b2c3d4-...",
        "bundle_id": "f47ac10b-...",
        "tape_id": "TAPE001",
        "drive_id": "drive-0",
        "object_count": 500,
        "total_size": 5368709120,
        "bytes_written": 3221225472,
        "progress_percent": 60.0,
        "status": "InProgress",
        "retry_count": 0,
        "created_at": "2026-02-27T09:00:00Z",
        "started_at": "2026-02-27T09:01:30Z"
      }
    ],
    "total": 42,
    "page": 1,
    "page_size": 20
  }
}
```

### 3.6 API задач восстановления

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/tasks/recall?status=&tier=&tape_id=&page=&page_size=` | список задач восстановления | read |
| GET | `/tasks/recall/{id}` | подробности задачи восстановления | read |
| POST | `/tasks/recall/{id}/retry` | повторить завершившуюся сбоем задачу восстановления | admin |
| POST | `/tasks/recall/{id}/cancel` | отменить задачу восстановления в состоянии Pending | admin |
| PUT | `/tasks/recall/{id}/priority` | скорректировать приоритет задачи | admin |
| GET | `/tasks/recall/stats` | статистика восстановления (доля выполнения SLA, глубина очереди, динамика) | read |
| GET | `/tasks/recall/queue` | текущее состояние очереди (глубина очередей трёх уровней, объединение) | read |

**Ответ с состоянием очереди восстановления**:

```json
// GET /tasks/recall/queue
{
  "code": 0,
  "data": {
    "expedited": { "depth": 2, "oldest_wait_secs": 45 },
    "standard": { "depth": 35, "oldest_wait_secs": 1800 },
    "bulk": { "depth": 150, "oldest_wait_secs": 7200 },
    "waiting_for_media": 3,
    "active_jobs": [
      { "job_id": "...", "tape_id": "TAPE003", "drive_id": "drive-1", "objects": 8, "progress_percent": 37.5 }
    ]
  }
}
```

### 3.7 API управления ленточными носителями

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/tapes?status=&format=&page=&page_size=` | список лент | read |
| GET | `/tapes/{tape_id}` | сведения о ленте (со списком Bundle, ёмкостью, ошибками) | read |
| POST | `/tapes` | зарегистрировать новую ленту | admin |
| PUT | `/tapes/{tape_id}/status` | обновить состояние ленты (Online/Offline/Retired) | admin |
| GET | `/tapes/{tape_id}/bundles` | список Bundle на ленте | read |
| POST | `/tapes/{tape_id}/verify` | запустить проверку ленты | admin |
| GET | `/tapes/{tape_id}/verify/history` | история проверок | read |
| POST | `/tapes/{tape_id}/retire` | вывести ленту из эксплуатации (запускает перенос данных) | admin |
| GET | `/tapes/offline-requests` | список текущих офлайн-запросов | read |
| POST | `/tapes/offline-requests/{id}/confirm` | подтвердить вывод ленты в онлайн | admin |
| GET | `/tapes/stats` | общая статистика лент (ёмкость, использование, исправность) | read |

**Ответ со сведениями о ленте**:

```json
// GET /tapes/TAPE001
{
  "code": 0,
  "data": {
    "id": "TAPE001",
    "barcode": "TAPE001L9",
    "format": "LTO-9",
    "status": "Online",
    "location": "Library-1 / Slot-5",
    "capacity": {
      "total_bytes": 18000000000000,
      "used_bytes": 12600000000000,
      "remaining_bytes": 5400000000000,
      "usage_percent": 70.0
    },
    "bundle_count": 245,
    "last_write_at": "2026-02-27T08:15:00Z",
    "last_read_at": "2026-02-27T09:30:00Z",
    "last_verified_at": "2026-02-25T02:00:00Z",
    "error_count": 0,
    "registered_at": "2025-06-01T00:00:00Z"
  }
}
```

### 3.8 API управления накопителями

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/drives` | список накопителей (состояние, текущая лента, загрузка) | read |
| GET | `/drives/{drive_id}` | подробности о накопителе | read |
| POST | `/drives/{drive_id}/eject` | извлечь ленту (аварийная операция) | admin |
| POST | `/drives/{drive_id}/clean` | запустить чистку | admin |
| GET | `/drives/stats` | общая статистика накопителей | read |

**Ответ со списком накопителей**:

```json
// GET /drives
{
  "code": 0,
  "data": [
    {
      "drive_id": "drive-0",
      "device": "/dev/nst0",
      "status": "InUse",
      "loaded_tape": "TAPE001",
      "current_task": { "type": "archive", "id": "a1b2c3d4-..." },
      "utilization_1h": 0.85,
      "total_bytes_written": 1073741824000,
      "total_bytes_read": 536870912000,
      "error_count": 0
    },
    {
      "drive_id": "drive-1",
      "device": "/dev/nst1",
      "status": "Available",
      "loaded_tape": null,
      "current_task": null,
      "utilization_1h": 0.12,
      "total_bytes_written": 536870912000,
      "total_bytes_read": 268435456000,
      "error_count": 1
    }
  ]
}
```

### 3.9 API мониторинга кэша

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/cache/stats` | статистика кэша (ёмкость, доля попаданий, вытеснение) | read |
| GET | `/cache/objects?bucket=&prefix=&page=&page_size=` | список объектов в кэше | read |
| DELETE | `/cache/objects/{bucket}/{key}` | вручную удалить объект из кэша | admin |
| POST | `/cache/evict` | вручную запустить вытеснение | admin |
| GET | `/cache/config` | конфигурация кэша | read |

**Ответ со статистикой кэша**:

```json
// GET /cache/stats
{
  "code": 0,
  "data": {
    "capacity": {
      "max_bytes": 1099511627776,
      "used_bytes": 824633720832,
      "usage_percent": 75.0,
      "object_count": 8432
    },
    "performance": {
      "hit_count": 1250000,
      "miss_count": 125000,
      "hit_rate": 0.909,
      "put_count": 45000,
      "evict_count": 12000,
      "avg_get_latency_us": 85,
      "avg_put_latency_us": 320
    },
    "eviction": {
      "policy": "Lru",
      "ttl_evictions": 8000,
      "capacity_evictions": 4000,
      "last_eviction_at": "2026-02-27T10:05:00Z"
    },
    "spdk": {
      "bdev": "NVMe0n1",
      "blobstore_clusters": 1048576,
      "used_clusters": 786432,
      "io_unit_size": 4096
    }
  }
}
```

### 3.10 API системного мониторинга

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/metrics/dashboard` | агрегированные данные дашборда | read |
| GET | `/metrics/prometheus` | метрики в формате Prometheus (эндпоинт сбора) | read |
| GET | `/alerts/active` | текущие активные оповещения | read |
| GET | `/alerts/history?page=&page_size=` | история оповещений | read |
| POST | `/alerts/{id}/acknowledge` | подтвердить оповещение | admin |
| GET | `/compensation/pending` | список задач компенсации | read |
| POST | `/compensation/{id}/retry` | вручную запустить повтор компенсации | admin |

### 3.11 API журнала аудита

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/audit?action=&user=&start=&end=&page=` | запрос журнала аудита | read |

**Структура записи аудита**:

```rust
pub struct AuditEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub detail: serde_json::Value,
    pub source_ip: String,
    pub result: AuditResult,
}

pub enum AuditResult { Success, Failed(String) }
```

### 3.12 API управления конфигурацией

| Метод | Путь | Описание | Право |
|------|------|------|------|
| GET | `/config` | получить текущую рабочую конфигурацию (без секретов) | read |
| GET | `/config/scheduler` | конфигурация планировщика | read |
| PUT | `/config/scheduler` | «горячее» обновление параметров планировщика (часть полей) | admin |
| GET | `/config/cache` | конфигурация кэша | read |
| PUT | `/config/cache` | «горячее» обновление параметров кэша (нижняя граница вытеснения и т.п.) | admin |

**Параметры, доступные для «горячего» обновления**:

| Параметр | Диапазон | Описание |
|------|------|------|
| `scheduler.recall.merge_window_secs` | 10–300 | окно объединения восстановлений |
| `scheduler.recall.max_concurrent_restores` | 1–100 | максимум параллельных восстановлений |
| `scheduler.archive.aggregation_window_secs` | 60–7200 | окно агрегации архивации |
| `cache.eviction_low_watermark` | 0.5–0.95 | нижняя граница вытеснения |
| `cache.max_size_bytes` | — | предел ёмкости кэша |

### 3.13 Публикация в реальном времени через WebSocket

```
WS /api/v1/admin/ws
```

**Типы публикуемых событий**:

```rust
pub enum WsEvent {
    TaskStatusChanged {
        task_type: String,
        task_id: Uuid,
        old_status: String,
        new_status: String,
    },
    AlertFired {
        alert_id: String,
        severity: String,
        message: String,
    },
    AlertResolved {
        alert_id: String,
    },
    TapeOfflineRequest {
        tape_id: String,
        barcode: Option<String>,
        reason: String,
    },
    DriveStatusChanged {
        drive_id: String,
        old_status: String,
        new_status: String,
    },
    ArchiveProgress {
        task_id: Uuid,
        bytes_written: u64,
        total_size: u64,
        progress_percent: f64,
    },
    RecallProgress {
        task_id: Uuid,
        objects_read: u32,
        total_objects: u32,
        progress_percent: f64,
    },
    ClusterLeaderChanged {
        old_leader: Option<u64>,
        new_leader: u64,
        term: u64,
    },
}
```

После подключения клиент может JSON-сообщением подписаться на определённые категории событий:

```json
{ "subscribe": ["task", "alert", "tape", "drive", "cluster"] }
```

---

## 4. Аутентификация и авторизация

### 4.1 Способы аутентификации

| Способ | Где применяется |
|------|----------|
| **JWT Bearer Token** | используется после входа в Web UI |
| **API Key** | вызовы автоматизированными скриптами / внешними системами |

```
Authorization: Bearer <jwt_token>
```

или

```
X-API-Key: <api_key>
```

### 4.2 Пользователи и роли

```rust
pub struct AdminUser {
    pub id: Uuid,
    pub username: String,
    pub password_hash: String,
    pub role: AdminRole,
    pub created_at: DateTime<Utc>,
    pub last_login_at: Option<DateTime<Utc>>,
}

pub enum AdminRole {
    SuperAdmin,    // полный набор прав
    Admin,         // управляющие операции + чтение
    ReadOnly,      // только чтение
}
```

### 4.3 Матрица прав

| Тип операции | SuperAdmin | Admin | ReadOnly |
|----------|-----------|-------|----------|
| просмотр кластера/состояния | ✓ | ✓ | ✓ |
| просмотр объектов/лент/задач | ✓ | ✓ | ✓ |
| просмотр мониторинга/аудита | ✓ | ✓ | ✓ |
| повтор/отмена задач | ✓ | ✓ | ✗ |
| создание/удаление корзин | ✓ | ✓ | ✗ |
| вывод лент в онлайн/офлайн/из эксплуатации | ✓ | ✓ | ✗ |
| «горячее» обновление конфигурации | ✓ | ✓ | ✗ |
| добавление и удаление узлов | ✓ | ✗ | ✗ |
| управление пользователями | ✓ | ✗ | ✗ |

### 4.4 API входа

| Метод | Путь | Описание |
|------|------|------|
| POST | `/auth/login` | вход, возвращает JWT |
| POST | `/auth/refresh` | обновить Token |
| POST | `/auth/logout` | выход (опционально чёрный список Token) |
| GET | `/auth/me` | сведения о текущем пользователе |

---

## 5. Проектирование страниц фронтенда

### 5.1 Структура навигации страниц

```
┌──────────────────────────────────────────────────────────────┐
│ ColdStore Admin                               [имя] [выход]  │
├────────────┬─────────────────────────────────────────────────┤
│            │                                                 │
│  📊 Дашборд │   ┌─────────────────────────────────────┐      │
│            │   │        основная область содержимого  │      │
│  🖥 Кластер │   │                                     │      │
│   ├ обзор   │   │                                     │      │
│   └ узлы    │   │                                     │      │
│            │   │                                     │      │
│  📦 Хранение│   │                                     │      │
│   ├ корзины │   │                                     │      │
│   └ объекты │   │                                     │      │
│            │   │                                     │      │
│  📼 Ленты   │   │                                     │      │
│   ├ носители│   └─────────────────────────────────────┘      │
│   ├ накопит.│                                                 │
│   └ офлайн  │                                                 │
│            │                                                 │
│  📋 Задачи  │                                                 │
│   ├ архив.  │                                                 │
│   └ восстан.│                                                 │
│            │                                                 │
│  💾 Кэш     │                                                 │
│            │                                                 │
│  📈 Монитор.│                                                 │
│   ├ метрики │                                                 │
│   └ алерты  │                                                 │
│            │                                                 │
│  ⚙ Настройки│                                                 │
│   ├ конфиг  │                                                 │
│   ├ пользователи│                                           │
│   └ аудит   │                                                 │
│            │                                                 │
└────────────┴─────────────────────────────────────────────────┘
```

### 5.2 Страница дашборда

**Маршрут**: `/dashboard`

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            ColdStore Dashboard                          │
├──────────────┬──────────────┬──────────────┬────────────────────────────┤
│  S3 QPS      │  доля ошибок  │  кластер     │  активные оповещения       │
│  ████ 975/s  │  0.3%        │  ✅ 3/3 узла │  ⚠ 2                     │
├──────────────┴──────────────┴──────────────┴────────────────────────────┤
│                                                                         │
│  ┌─ Динамика задержки запросов (P50/P95/P99) ────────────────────────┐│
│  │  [линейный график: 24 ч, группировка по method]                   ││
│  └──────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌─ Обзор хранилища ──────────────┐  ┌─ Ленты и накопители ──────────┐│
│  │  Всего объектов: 2.5M          │  │  Всего лент: 120              ││
│  │  Общая ёмкость: 2.1 PB         │  │  онлайн: 95 / офлайн: 25      ││
│  │  Restored: 100 GB              │  │  накопители: 4 (3 занято,      ││
│  │  ColdPending: 5 GB             │  │              1 свободен)       ││
│  │  Cold: 2.0 PB                  │  │  архивация: 320 MB/s          ││
│  │  [круговая диаграмма]          │  │  [цветовая карта накопителей] ││
│  └────────────────────────────────┘  └────────────────────────────────┘│
│                                                                         │
│  ┌─ Кэш ──────────────────────────┐  ┌─ Очереди задач ───────────────┐│
│  │  Ёмкость: 750GB / 1TB (75%)    │  │  архивация Pending: 3         ││
│  │  Попадания: 90.9%              │  │  восстановление Expedited: 2  ││
│  │  [шкала использования]         │  │  восстановление Standard: 35  ││
│  │                                │  │  восстановление Bulk: 150     ││
│  │  [динамика попаданий]          │  │  WaitingForMedia: 3           ││
│  └────────────────────────────────┘  └────────────────────────────────┘│
│                                                                         │
│  ┌─ Последние события ────────────────────────────────────────────────┐│
│  │  10:15  ArchiveTask a1b2... completed (500 objects, 5.2GB)        ││
│  │  10:12  RecallTask c3d4... started on drive-1 (TAPE003)           ││
│  │  10:10  ⚠ Tape TAPE042 offline request created                    ││
│  │  10:05  Cache eviction batch: 64 objects, 2.1GB freed             ││
│  │  09:58  Raft leader changed: node-2 → node-1 (term 42)            ││
│  └────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Страница обзора кластера

**Маршрут**: `/cluster`

| Область | Содержание |
|------|------|
| Карточка состояния кластера | cluster_id, term, committed_index, узел Leader |
| Таблица списка узлов | node_id, адрес, роль (цвет Tag), время heartbeat, действия (удалить) |
| Состояние Raft | log index, commit index, apply index, snapshot index |
| Таймлайн исправности узлов | таймлайн heartbeat и смены ролей за последние 24 ч |

### 5.4 Страница управления корзинами

**Маршрут**: `/buckets`

| Область | Содержание |
|------|------|
| Таблица списка корзин | имя корзины, число объектов, общий размер, версионирование, время создания |
| Сведения о корзине (разворот по клику) | круговая диаграмма статистики по каждому StorageClass (ColdPending/Cold) |
| Действия | создать корзину (всплывающая форма), удалить корзину (повторное подтверждение) |

### 5.5 Страница запроса объектов

**Маршрут**: `/objects`

```
┌─────────────────────────────────────────────────────────────────────┐
│  Корзина: [my-bucket ▼]  Префикс: [data/____]  Класс: [все ▼]      │
│                                          Состояние: [все ▼]        │
│                                                        [🔍 Поиск]   │
├─────────────────────────────────────────────────────────────────────┤
│  Key              │ Size    │ StorageClass │ Restore   │ Действия   │
│─────────────────────────────────────────────────────────────────────│
│  data/file1.bin   │ 100 MB  │ Cold 🏔      │ Completed │ [подробн.] │
│  data/file2.bin   │ 50 MB   │ Cold 🏔      │ —         │ [подробн.] │
│  data/file3.bin   │ 200 MB  │ ColdPending ⏳│ —         │ [подробн.] │
│  data/file4.bin   │ 1 GB    │ Cold 🏔      │ InProgress│ [подробн.] │
├─────────────────────────────────────────────────────────────────────┤
│  всего 4 записей  [< 1 >]                                           │
└─────────────────────────────────────────────────────────────────────┘
```

**Всплывающее окно сведений об объекте**:

| Вкладка | Содержание |
|-----|------|
| Основные сведения | bucket, key, size, checksum, content_type, etag, версия |
| Сведения об архивации | archive_id, tape_id, tape_set, tape_block_offset, состояние Bundle |
| Сведения о восстановлении | restore_status, expire_at, tier, recall_task_id, история |
| Таймлайн | время создания, время архивации, время восстановления, таймлайн переходов состояний |

### 5.6 Страница управления ленточными носителями

**Маршрут**: `/tapes`

```
┌─────────────────────────────────────────────────────────────────────┐
│  Состояние: [все ▼]  Формат: [все ▼]         [🔍 Фильтр] [+ лента]  │
├─────────────────────────────────────────────────────────────────────┤
│  ID ленты   │ Штрихкод  │ Формат│ Состояние│ Использование │ Bundle│ Действ│
│─────────────────────────────────────────────────────────────────────│
│  TAPE001    │ TAPE001L9 │ LTO-9 │ ✅ Online │ ████░░ 70%   │ 245   │ ...   │
│  TAPE002    │ TAPE002L9 │ LTO-9 │ ✅ Online │ ██████ 95%   │ 380   │ ...   │
│  TAPE042    │ TAPE042L9 │ LTO-9 │ ❌ Offline│ ███░░░ 45%   │ 120   │ ...   │
│  TAPE099    │ TAPE099L9 │ LTO-9 │ ⚠ Error   │ █████░ 82%   │ 290   │ ...   │
└─────────────────────────────────────────────────────────────────────┘
```

**Меню действий**: подробности, проверка, вывод в онлайн/офлайн, вывод из эксплуатации

**Страница сведений о ленте (по клику)**:

| Область | Содержание |
|------|------|
| Основные сведения | ID, штрихкод, формат, состояние, расположение |
| Визуализация ёмкости | диаграмма «занято/осталось», число Bundle |
| Список Bundle | все Bundle на этой ленте (ID, число объектов, размер, состояние, время) |
| История проверок | результат последней проверки, число неисправных блоков, время |
| Журнал ошибок | тип ошибки, время, состояние обработки |

### 5.7 Страница управления накопителями

**Маршрут**: `/drives`

Каждый накопитель отображается карточкой:

```
┌─ Drive 0 (/dev/nst0) ─────────────────┐  ┌─ Drive 1 (/dev/nst1) ──────────┐
│  Состояние: 🟢 InUse                   │  │  Состояние: 🟡 Available        │
│  Текущая лента: TAPE001                │  │  Текущая лента: —               │
│  Текущая задача: Archive a1b2... (60%) │  │  Очередь ожидания: 2 задачи    │
│  Загрузка (1ч): ████████░░ 85%         │  │  Загрузка (1ч): █░░░░░░░░░ 12%  │
│  Всего записано: 1.0 TB                │  │  Всего прочитано: 250 GB        │
│  [Извлечь ленту]  [Чистка]             │  │  [Извлечь ленту]  [Чистка]      │
└────────────────────────────────────────┘  └─────────────────────────────────┘
```

### 5.8 Страница задач восстановления

**Маршрут**: `/tasks/recall`

| Область | Содержание |
|------|------|
| Обзор очередей | столбчатая диаграмма глубины очередей трёх уровней, число WaitingForMedia |
| Таблица списка задач | ID, bucket/key, tier (цвет метки), состояние, лента, время ожидания, действия |
| Фильтры | состояние, приоритет, ID ленты |
| Действия | повтор, отмена, корректировка приоритета |

**Цвета меток состояний**:

| Состояние | Цвет |
|------|------|
| Pending | синий |
| WaitingForMedia | оранжевый |
| InProgress | зелёный с анимацией |
| Completed | серый |
| Failed | красный |

### 5.9 Страница офлайн-запросов

**Маршрут**: `/tapes/offline`

| Область | Содержание |
|------|------|
| Список необработанных запросов | tape_id, штрихкод, причина, время запроса, время ожидания, число связанных задач |
| Действия | подтвердить вывод в онлайн (запускает `confirm_media_online`) |
| История | обработанные офлайн-запросы, время обработки |

### 5.10 Страница оповещений

**Маршрут**: `/alerts`

| Вкладка | Содержание |
|-----|------|
| Активные оповещения | уровень серьёзности (P1 красный/P2 оранжевый/P3 жёлтый), имя оповещения, время срабатывания, длительность, кнопка подтверждения |
| История оповещений | фильтр по диапазону времени, имя оповещения, время «срабатывание → восстановление», обработавший |

### 5.11 Страница журнала аудита

**Маршрут**: `/audit`

Таблица с поиском и фильтрацией по диапазону времени/пользователю/типу операции:

| Время | Пользователь | Операция | Ресурс | Подробности | Результат |
|------|------|------|------|------|------|
| 10:15:30 | admin | retry_recall_task | recall/c3d4... | {"old_status":"Failed"} | ✅ |
| 10:12:00 | admin | confirm_online | tape/TAPE042 | {} | ✅ |
| 09:50:00 | system | evict_cache | cache/batch | {"count":64} | ✅ |

---

## 6. Структура модуля бэкенда

```
src/admin/
├── mod.rs                    # pub use, построение Router
├── router.rs                 # определение Axum Router (/api/v1/admin/*)
├── auth/
│   ├── mod.rs
│   ├── jwt.rs                # выпуск/проверка JWT
│   ├── middleware.rs         # middleware аутентификации
│   └── models.rs             # AdminUser, AdminRole
├── handlers/
│   ├── mod.rs
│   ├── cluster.rs            # handler управления кластером
│   ├── bucket.rs             # handler управления корзинами
│   ├── object.rs             # handler запроса объектов
│   ├── archive_task.rs       # handler задач архивации
│   ├── recall_task.rs        # handler задач восстановления
│   ├── tape.rs               # handler управления лентами
│   ├── drive.rs              # handler управления накопителями
│   ├── cache.rs              # handler мониторинга кэша
│   ├── metrics.rs            # handler метрик мониторинга
│   ├── alert.rs              # handler оповещений
│   ├── config.rs             # handler управления конфигурацией
│   └── audit.rs              # handler журнала аудита
├── websocket.rs              # публикация через WebSocket
├── dto/                      # DTO запросов/ответов
│   ├── mod.rs
│   ├── common.rs             # ApiResponse, PageRequest, PageResponse
│   ├── cluster.rs
│   ├── bucket.rs
│   ├── object.rs
│   ├── task.rs
│   ├── tape.rs
│   ├── drive.rs
│   ├── cache.rs
│   └── alert.rs
└── audit.rs                  # логика записи в журнал аудита
```

### 6.1 Структура проекта фронтенда

```
console/
├── package.json
├── vite.config.ts
├── tsconfig.json
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── routes.tsx             # определение маршрутов React Router
│   ├── api/                   # обёртки API
│   │   ├── client.ts          # экземпляр Axios (с перехватчиком JWT)
│   │   ├── cluster.ts
│   │   ├── bucket.ts
│   │   ├── object.ts
│   │   ├── task.ts
│   │   ├── tape.ts
│   │   ├── drive.ts
│   │   ├── cache.ts
│   │   ├── metrics.ts
│   │   ├── alert.ts
│   │   └── auth.ts
│   ├── stores/                # состояние Zustand
│   │   ├── useAuthStore.ts
│   │   ├── useWebSocketStore.ts   # соединение WS + рассылка событий
│   │   └── useNotificationStore.ts
│   ├── pages/                 # компоненты страниц
│   │   ├── Dashboard.tsx
│   │   ├── cluster/
│   │   │   ├── Overview.tsx
│   │   │   └── NodeDetail.tsx
│   │   ├── storage/
│   │   │   ├── BucketList.tsx
│   │   │   ├── BucketDetail.tsx
│   │   │   └── ObjectQuery.tsx
│   │   ├── tape/
│   │   │   ├── TapeList.tsx
│   │   │   ├── TapeDetail.tsx
│   │   │   ├── DriveList.tsx
│   │   │   └── OfflineRequests.tsx
│   │   ├── task/
│   │   │   ├── ArchiveTaskList.tsx
│   │   │   └── RecallTaskList.tsx
│   │   ├── cache/
│   │   │   └── CacheOverview.tsx
│   │   ├── monitor/
│   │   │   ├── Metrics.tsx
│   │   │   └── Alerts.tsx
│   │   └── settings/
│   │       ├── Config.tsx
│   │       ├── UserManage.tsx
│   │       └── AuditLog.tsx
│   ├── components/            # переиспользуемые компоненты
│   │   ├── StatusTag.tsx      # метка состояния (отображение цвета)
│   │   ├── CapacityBar.tsx    # шкала ёмкости
│   │   ├── DriveCard.tsx      # карточка накопителя
│   │   ├── EventTimeline.tsx  # таймлайн событий
│   │   └── ConfirmDialog.tsx  # диалог повторного подтверждения
│   ├── hooks/                 # собственные хуки
│   │   ├── usePolling.ts      # обновление данных опросом
│   │   └── useWebSocket.ts    # хук соединения WS
│   └── utils/
│       ├── format.ts          # форматирование байт/времени
│       └── constants.ts       # отображение перечислений
```

---

## 7. Параметры конфигурации

```yaml
admin:
  enabled: true
  listen: "0.0.0.0:8080"
  cors_origins: ["http://localhost:5173"]

  # Console достаточно указать только адреса Metadata
  metadata_addrs:
    - "10.0.1.1:21001"
    - "10.0.1.2:21001"
    - "10.0.1.3:21001"

  auth:
    jwt_secret: "${ADMIN_JWT_SECRET}"
    jwt_expire_hours: 24
    api_keys:
      - name: "monitoring"
        key: "${MONITORING_API_KEY}"
        role: "ReadOnly"

  initial_user:
    username: "admin"
    password: "${ADMIN_INITIAL_PASSWORD}"
    role: "SuperAdmin"

  websocket:
    heartbeat_interval_secs: 30
    max_connections: 100

  audit:
    enabled: true
    retention_days: 90

  static_files:
    enabled: true
    path: "./console/dist"     # артефакты сборки фронтенда, встраиваются в продакшене
```

---

## 8. Справочные материалы

- [Axum](https://docs.rs/axum/latest/axum/) — HTTP-фреймворк
- [utoipa](https://docs.rs/utoipa/latest/utoipa/) — генерация документации OpenAPI
- [jsonwebtoken](https://docs.rs/jsonwebtoken) — реализация JWT
- [React 18](https://react.dev/) — фронтенд-фреймворк
- [Ant Design 5](https://ant.design/) — библиотека UI-компонентов
- [Zustand](https://zustand-demo.pmnd.rs/) — управление состоянием
- [React Router 6](https://reactrouter.com/) — маршрутизация
- [ECharts](https://echarts.apache.org/) — библиотека диаграмм
