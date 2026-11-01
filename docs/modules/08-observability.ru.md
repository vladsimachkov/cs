# Проектирование наблюдаемости и трассировки

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md) | [07 — согласованность и производительность](./07-consistency-performance.ru.md)

## 1. Цели проектирования

Наблюдаемость ColdStore построена на едином стандарте **OpenTelemetry** и покрывает три опоры:

| Опора | Назначение | Технология |
|------|------|------|
| **Traces** | сквозная трассировка между слоями, поиск задержек | `tracing` + `tracing-opentelemetry` |
| **Metrics** | мониторинг метрик в реальном времени, планирование ёмкости | Meter API `opentelemetry` |
| **Logs** | структурированные логи, разбор отказов | `tracing` + JSON formatter |

### 1.1 Ключевые сложности

| Сложность | Причина | Решение |
|------|------|----------|
| синхронный запрос → асинхронная диспетчеризация | после запроса RestoreObject восстановление выполняется асинхронно в фоне | Span Link связывает trace запроса и trace диспетчеризации |
| пользовательские потоки SPDK | потоки reactor SPDK находятся вне рантайма Tokio | явная передача Context между потоками |
| сверхдлительные ленточные операции | одно восстановление может занимать 30 минут и больше | разбиение длинного Span, Event для ключевых точек |
| высоконагруженный путь GET | попадание в кэш должно укладываться в < 1 мс, большие издержки недопустимы | стратегия сэмплирования, минимальная instrumentation в горячем пути |

---

## 2. Выбор технологий

### 2.1 Зависимости Crate

```toml
[dependencies]
# ─── ядро OpenTelemetry ───
opentelemetry = "0.24"
opentelemetry_sdk = { version = "0.24", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.17", features = ["tonic", "metrics", "logs"] }
opentelemetry-semantic-conventions = "0.16"

# ─── экосистема tracing ───
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-opentelemetry = "0.25"

# ─── экспорт метрик ───
opentelemetry-prometheus = "0.17"      # режим pull для Prometheus (опционально)
prometheus = "0.13"                     # клиент Prometheus (опционально)
```

### 2.2 Архитектура экспорта

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           процесс ColdStore                              │
│                                                                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │ слой    │  │ метаданные│ │ слой    │  │ слой    │  │ слой    │     │
│  │ доступа │  │          │  │ кэша    │  │ планир. │  │ лент    │     │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘     │
│       │            │            │            │            │            │
│       └────────────┴────────────┴────────────┴────────────┘            │
│                              │                                         │
│               ┌──────────────┼──────────────┐                         │
│               ▼              ▼              ▼                         │
│         TracerProvider  MeterProvider  LoggerProvider                  │
│               │              │              │                         │
│               └──────────────┼──────────────┘                         │
│                              ▼                                         │
│                     OTLP gRPC Exporter                                │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
                 ┌─────────────────────────┐
                 │  OpenTelemetry Collector │
                 │  (otel-collector)        │
                 └─────────┬───────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Jaeger / │ │Prometheus│ │  Loki /  │
        │ Tempo    │ │          │ │  ES      │
        │ (Traces) │ │ (Metrics)│ │ (Logs)   │
        └──────────┘ └──────────┘ └──────────┘
              │            │            │
              └────────────┼────────────┘
                           ▼
                     ┌──────────┐
                     │ Grafana  │
                     │ Dashboard│
                     └──────────┘
```

### 2.3 Инициализация

```rust
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace, Resource};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub struct TelemetryConfig {
    pub service_name: String,
    pub otlp_endpoint: String,
    pub node_id: u64,
    pub environment: String,
    pub trace_sample_rate: f64,
    pub log_level: String,
    pub metrics_export_interval_secs: u64,
}

pub fn init_telemetry(config: &TelemetryConfig) -> Result<TelemetryGuard> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.instance.id", format!("node-{}", config.node_id)),
        KeyValue::new("deployment.environment", config.environment.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    // ── Traces ──
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.otlp_endpoint),
        )
        .with_trace_config(
            trace::Config::default()
                .with_resource(resource.clone())
                .with_sampler(trace::Sampler::TraceIdRatioBased(config.trace_sample_rate)),
        )
        .install_batch(runtime::Tokio)?;

    // ── Metrics ──
    let meter_provider = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.otlp_endpoint),
        )
        .with_resource(resource.clone())
        .with_period(Duration::from_secs(config.metrics_export_interval_secs))
        .build()?;

    opentelemetry::global::set_meter_provider(meter_provider);

    // ── сборка tracing subscriber ──
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_thread_ids(true)
        .with_span_list(true);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    Ok(TelemetryGuard { /* обработчики завершения */ })
}

pub struct TelemetryGuard {
    // при Drop вызывается opentelemetry::global::shutdown_tracer_provider()
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}
```

---

## 3. Проектирование распределённой трассировки

### 3.1 Общая топология Trace

Операции ColdStore делятся на **цепочки синхронных запросов** и **цепочки асинхронной диспетчеризации**, связанные через **Span Link**:

```
═══ Trace синхронного запроса ═══

[PUT Object]                                    Trace A
  └─ [parse_request]
  └─ [metadata.put_object]
       └─ [raft_propose]

[GET Object]                                    Trace B
  └─ [parse_request]
  └─ [metadata.get_object]
  └─ [check_restore_status]
  └─ [cache.get]
       └─ [spdk_blob_read]

[RestoreObject]                                 Trace C
  └─ [parse_restore_request]
  └─ [metadata.find_active_recall]              ← линейное чтение
  └─ [metadata.put_recall_task]
  └─ [response: 202 Accepted]


═══ Trace асинхронной диспетчеризации (Link → Trace C) ═══

[recall_scheduler.process]                      Trace D ──Link──▶ Trace C
  └─ [dequeue_task]
  └─ [merge_by_tape]
  └─ [execute_tape_read_job]
       └─ [tape.acquire_drive]
       └─ [tape.load]
       └─ [tape.seek]
       └─ [tape.read_objects]
            └─ [read_object: obj1]
            └─ [read_object: obj2]
       └─ [cache.put_restored_batch]
            └─ [spdk_create_blob]
            └─ [spdk_write_blob]
       └─ [metadata.update_restore_status]
  └─ [tape.unload]
```

### 3.2 Соглашение об именах Span

Используется плоское именование `{layer}.{operation}`, удобное для фильтрации по слоям в Jaeger/Tempo:

| Слой | Шаблон имени Span | Примеры |
|------|--------------|------|
| слой доступа | `s3.{method}` | `s3.put_object`, `s3.get_object`, `s3.restore_object` |
| протокольный слой | `protocol.{operation}` | `protocol.parse_restore`, `protocol.build_response` |
| метаданные | `metadata.{api}` | `metadata.put_object`, `metadata.scan_cold_pending` |
| Raft | `raft.{operation}` | `raft.propose`, `raft.apply`, `raft.ensure_linearizable` |
| кэш | `cache.{operation}` | `cache.get`, `cache.put_restored`, `cache.evict_batch` |
| SPDK | `spdk.{operation}` | `spdk.create_blob`, `spdk.write_blob`, `spdk.read_blob` |
| планирование | `scheduler.{operation}` | `scheduler.archive_scan`, `scheduler.recall_process` |
| ленты | `tape.{operation}` | `tape.acquire_drive`, `tape.load`, `tape.write`, `tape.read` |

### 3.3 Атрибуты Span (стандартизованные поля)

#### 3.3.1 Общие атрибуты

| Атрибут | Тип | Описание | Пример |
|-----------|------|------|------|
| `coldstore.bucket` | string | имя корзины S3 | `"my-bucket"` |
| `coldstore.key` | string | ключ объекта S3 | `"photos/2025/img.jpg"` |
| `coldstore.version_id` | string | версия объекта | `"v1"` |
| `coldstore.object_size` | i64 | размер объекта в байтах | `104857600` |
| `coldstore.storage_class` | string | категория хранения | `"Cold"` |
| `coldstore.node_id` | i64 | ID текущего узла | `1` |

#### 3.3.2 Атрибуты слоя метаданных

| Атрибут | Тип | Описание |
|-----------|------|------|
| `raft.is_leader` | bool | является ли текущий узел Leader |
| `raft.term` | i64 | текущая казнь (term) Raft |
| `raft.log_index` | i64 | индекс записанной записи журнала |
| `raft.commit_latency_ms` | f64 | задержка фиксации в Raft |
| `metadata.cf` | string | Column Family, с которой выполняется операция |
| `metadata.batch_size` | i64 | число операций в WriteBatch |

#### 3.3.3 Атрибуты слоя кэша

| Атрибут | Тип | Описание |
|-----------|------|------|
| `cache.hit` | bool | произошло ли попадание в кэш |
| `cache.blob_id` | i64 | SPDK Blob ID |
| `cache.blob_size` | i64 | занимаемый Blob объём |
| `cache.io_unit_count` | i64 | число io_unit при чтении/записи |
| `cache.usage_ratio` | f64 | текущая заполненность |
| `cache.eviction_reason` | string | причина вытеснения (`"ttl"` / `"capacity"` / `"lru"`) |

#### 3.3.4 Атрибуты слоя планирования

| Атрибут | Тип | Описание |
|-----------|------|------|
| `scheduler.task_id` | string | ID задачи (UUID) |
| `scheduler.task_type` | string | `"archive"` / `"recall"` |
| `scheduler.bundle_id` | string | ID ArchiveBundle |
| `scheduler.tier` | string | `"Expedited"` / `"Standard"` / `"Bulk"` |
| `scheduler.merge_count` | i64 | число объединённых задач |
| `scheduler.queue_depth` | i64 | текущая глубина очереди |
| `scheduler.retry_count` | i64 | число повторов |

#### 3.3.5 Атрибуты ленточного слоя

| Атрибут | Тип | Описание |
|-----------|------|------|
| `tape.tape_id` | string | ID ленты |
| `tape.drive_id` | string | ID накопителя |
| `tape.operation` | string | `"write"` / `"read"` / `"seek"` / `"load"` / `"unload"` |
| `tape.bytes_transferred` | i64 | передано байтов |
| `tape.blocks_transferred` | i64 | передано блоков |
| `tape.filemark` | i64 | позиция FileMark |
| `tape.seek_blocks` | i64 | число блоков, пропущенных при seek |
| `tape.swap_duration_ms` | f64 | длительность замены ленты |

### 3.4 Передача Context через асинхронные границы

#### 3.4.1 Проблема: RestoreObject → асинхронное восстановление

`RestoreObject` — синхронный HTTP-запрос (возвращает 202), а восстановление выполняется асинхронно в фоне. Два Trace нужно связать через **Span Link**:

```rust
use tracing::Span;
use opentelemetry::trace::TraceContextExt;

// ─── протокольный слой: обработчик RestoreObject ───
#[tracing::instrument(name = "s3.restore_object", skip(state, body))]
async fn restore_object(
    state: AppState,
    bucket: &str,
    key: &str,
    body: RestoreRequest,
) -> Result<Response> {
    let recall_task = RecallTask::new(bucket, key, body.tier, body.days);

    // захватываем context текущего span и сохраняем в RecallTask для слоя планирования
    let current_cx = Span::current().context();
    let span_context = current_cx.span().span_context().clone();
    let trace_context = serialize_span_context(&span_context);

    recall_task.trace_context = Some(trace_context);
    state.metadata.put_recall_task(recall_task).await?;

    Ok(Response::new(StatusCode::ACCEPTED))
}

// ─── слой планирования: обработка RecallTask ───
#[tracing::instrument(name = "scheduler.recall_process", skip(self))]
async fn process_recall_task(&self, task: RecallTask) {
    let span = Span::current();

    // устанавливаем Link на исходный запрос RestoreObject
    if let Some(ref ctx) = task.trace_context {
        if let Some(remote_cx) = deserialize_span_context(ctx) {
            span.add_link(remote_cx, vec![
                KeyValue::new("link.type", "restore_request"),
                KeyValue::new("link.recall_task_id", task.id.to_string()),
            ]);
        }
    }

    // дальнейшее выполнение...
    self.execute_tape_read_job(task).await;
}
```

#### 3.4.2 Проблема: потоки Tokio → reactor SPDK

reactor SPDK работает в отдельном потоке вне рантайма Tokio. Context нужно передавать явно:

```rust
use opentelemetry::Context;

// ─── CacheManager: захват Context на стороне Tokio ───
#[tracing::instrument(name = "cache.put_restored", skip(self, data))]
async fn put_restored(&self, item: RestoredItem) -> Result<()> {
    let parent_cx = Context::current();

    // отправляем в поток reactor SPDK через channel
    let (tx, rx) = oneshot::channel();
    self.spdk_sender.send(SpdkCommand::WriteBlob {
        xattrs: item.to_xattrs(),
        data: item.data,
        parent_cx,     // передача контекста
        reply: tx,
    })?;

    rx.await?
}

// ─── сторона потока reactor SPDK ───
fn handle_spdk_command(cmd: SpdkCommand) {
    match cmd {
        SpdkCommand::WriteBlob { xattrs, data, parent_cx, reply } => {
            let _guard = parent_cx.attach();
            let span = tracing::info_span!("spdk.write_blob",
                cache.blob_size = data.len() as i64,
            );
            let _enter = span.enter();

            // синхронная операция SPDK...
            let result = spdk_blob_write_sync(&xattrs, &data);
            let _ = reply.send(result);
        }
    }
}
```

#### 3.4.3 Проблема: сканирование архивации → пакетная запись на ленту

Планировщик архивации по таймеру создаёт несколько ArchiveTask; каждая задача — отдельный Trace, но они разделяют контекст одного сканирования:

```rust
#[tracing::instrument(name = "scheduler.archive_scan")]
async fn scan_and_archive(&self) {
    let objects = self.metadata.scan_cold_pending(self.config.batch_size).await?;
    let bundles = self.aggregate(objects);

    for bundle in bundles {
        let task_span = tracing::info_span!("scheduler.archive_task",
            scheduler.bundle_id = %bundle.id,
            scheduler.task_type = "archive",
            coldstore.object_count = bundle.entries.len() as i64,
        );

        // каждая ArchiveTask запускается отдельно, span сканирования становится Link
        let metadata = self.metadata.clone();
        let tape_mgr = self.tape_manager.clone();
        let scan_cx = Span::current().context();

        tokio::spawn(async move {
            task_span.add_link(
                scan_cx.span().span_context().clone(),
                vec![KeyValue::new("link.type", "archive_scan")],
            );
            execute_archive(metadata, tape_mgr, bundle)
                .instrument(task_span)
                .await;
        });
    }
}
```

### 3.5 Стратегия сэмплирования

| Путь | Доля сэмплирования | Обоснование |
|------|--------|------|
| `s3.get_object` (попадание в кэш) | 1% | высокочастотный горячий путь, сплошной сбор слишком дорог |
| `s3.put_object` | 10% | средняя частота операций |
| `s3.restore_object` | 100% | редкая и критичная операция, нужен сплошной сбор |
| `scheduler.archive_*` | 100% | редко, каждый Bundle должен быть прослеживаем |
| `scheduler.recall_*` | 100% | редко и влияет на пользовательский SLA |
| `tape.*` | 100% | редко, ленточные операции нужно фиксировать полностью |
| `raft.propose` | 10% | средняя частота, на Leader-узлах их может быть много |
| `cache.evict_batch` | 100% | редкая фоновая задача |

**Реализация**: собственный Sampler на основе имени Span:

```rust
pub struct ColdStoreSampler {
    default_rate: f64,
    overrides: HashMap<String, f64>,
}

impl trace::ShouldSample for ColdStoreSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        _trace_id: TraceId,
        name: &str,
        _span_kind: &SpanKind,
        _attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        // если родительский span уже сэмплирован, продолжаем сэмплировать (сохраняем целостность цепочки)
        if let Some(cx) = parent_context {
            if cx.span().span_context().is_sampled() {
                return SamplingResult {
                    decision: SamplingDecision::RecordAndSample,
                    ..Default::default()
                };
            }
        }

        let rate = self.overrides.get(name).copied().unwrap_or(self.default_rate);
        if rand::random::<f64>() < rate {
            SamplingResult { decision: SamplingDecision::RecordAndSample, ..Default::default() }
        } else {
            SamplingResult { decision: SamplingDecision::Drop, ..Default::default() }
        }
    }
}
```

### 3.6 Поле trace_context в RecallTask

Для поддержки Span Link в RecallTask и ArchiveTask добавлено поле trace context:

```rust
pub struct RecallTask {
    // ... уже существующие поля ...
    pub trace_context: Option<String>,  // сериализация W3C TraceContext
}

pub struct ArchiveTask {
    // ... уже существующие поля ...
    pub trace_context: Option<String>,
}
```

Формат сериализации — W3C Trace Context (формат заголовка `traceparent`):

```
00-{trace_id_hex}-{span_id_hex}-{trace_flags_hex}
```

---

## 4. Система метрик

### 4.1 Регистрация метрик

```rust
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

pub struct ColdStoreMetrics {
    meter: Meter,

    // ── слой доступа ──
    pub s3_requests_total: Counter<u64>,
    pub s3_request_duration: Histogram<f64>,
    pub s3_request_body_size: Histogram<f64>,
    pub s3_response_body_size: Histogram<f64>,
    pub s3_errors_total: Counter<u64>,

    // ── слой метаданных ──
    pub raft_propose_total: Counter<u64>,
    pub raft_propose_duration: Histogram<f64>,
    pub raft_apply_duration: Histogram<f64>,
    pub raft_leader_changes: Counter<u64>,
    pub raft_log_entries: Counter<u64>,
    pub metadata_read_duration: Histogram<f64>,
    pub metadata_write_batch_size: Histogram<f64>,

    // ── слой кэша ──
    pub cache_hits: Counter<u64>,
    pub cache_misses: Counter<u64>,
    pub cache_puts: Counter<u64>,
    pub cache_deletes: Counter<u64>,
    pub cache_evictions: Counter<u64>,
    pub cache_eviction_bytes: Counter<u64>,
    pub cache_get_duration: Histogram<f64>,
    pub cache_put_duration: Histogram<f64>,
    pub cache_usage_bytes: UpDownCounter<i64>,
    pub cache_object_count: UpDownCounter<i64>,
    pub spdk_io_duration: Histogram<f64>,
    pub spdk_io_bytes: Histogram<f64>,

    // ── слой планирования ──
    pub archive_bundles_created: Counter<u64>,
    pub archive_bundles_completed: Counter<u64>,
    pub archive_bundles_failed: Counter<u64>,
    pub archive_objects_total: Counter<u64>,
    pub archive_bytes_total: Counter<u64>,
    pub archive_throughput_bytes: Counter<u64>,
    pub archive_task_duration: Histogram<f64>,

    pub recall_tasks_created: Counter<u64>,
    pub recall_tasks_completed: Counter<u64>,
    pub recall_tasks_failed: Counter<u64>,
    pub recall_queue_depth: UpDownCounter<i64>,
    pub recall_wait_duration: Histogram<f64>,
    pub recall_read_duration: Histogram<f64>,
    pub recall_e2e_duration: Histogram<f64>,

    pub compensation_pending: UpDownCounter<i64>,
    pub compensation_retries: Counter<u64>,

    // ── ленточный слой ──
    pub tape_write_bytes: Counter<u64>,
    pub tape_read_bytes: Counter<u64>,
    pub tape_write_duration: Histogram<f64>,
    pub tape_read_duration: Histogram<f64>,
    pub tape_seek_duration: Histogram<f64>,
    pub tape_swap_total: Counter<u64>,
    pub tape_swap_duration: Histogram<f64>,
    pub tape_errors: Counter<u64>,
    pub drive_utilization: Histogram<f64>,
    pub drive_queue_wait: Histogram<f64>,
    pub offline_requests: Counter<u64>,
    pub offline_wait_duration: Histogram<f64>,
    pub verify_total: Counter<u64>,
    pub verify_failures: Counter<u64>,
}
```

### 4.2 Подробное описание метрик

#### 4.2.1 Метрики слоя доступа

| Метрика | Тип | Единица | Labels | Описание |
|--------|------|------|--------|------|
| `coldstore.s3.requests.total` | Counter | 1 | `method`, `status_code`, `bucket` | общее число запросов S3 |
| `coldstore.s3.request.duration` | Histogram | мс | `method`, `bucket` | распределение задержки запросов |
| `coldstore.s3.request.body.size` | Histogram | байты | `method` | распределение размера тела запроса |
| `coldstore.s3.response.body.size` | Histogram | байты | `method` | распределение размера тела ответа |
| `coldstore.s3.errors.total` | Counter | 1 | `method`, `error_code` | общее число ошибок (по кодам ошибок S3) |

**Histogram buckets (задержка)**: `[0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]` мс

**Значения Label**:

| Label | Значения |
|-------|------|
| `method` | `PUT`, `GET`, `HEAD`, `DELETE`, `RESTORE`, `LIST` |
| `status_code` | `200`, `202`, `403`, `404`, `409`, `500`, `503` |
| `error_code` | `InvalidObjectState`, `RestoreAlreadyInProgress`, ... |

#### 4.2.2 Метрики слоя метаданных

| Метрика | Тип | Единица | Labels | Описание |
|--------|------|------|--------|------|
| `coldstore.raft.propose.total` | Counter | 1 | `command_type` | число Propose в Raft |
| `coldstore.raft.propose.duration` | Histogram | мс | `command_type` | задержка от Propose до Commit |
| `coldstore.raft.apply.duration` | Histogram | мс | `command_type` | задержка Apply в RocksDB |
| `coldstore.raft.leader.changes` | Counter | 1 | — | число смен Leader |
| `coldstore.raft.log.entries` | Counter | 1 | — | накопленное число записей журнала |
| `coldstore.metadata.read.duration` | Histogram | мс | `cf`, `operation` | задержка операций чтения (по CF) |
| `coldstore.metadata.write_batch.size` | Histogram | 1 | `command_type` | число операций в WriteBatch |

**Label `command_type`**: `PutObject`, `DeleteObject`, `BatchArchiveComplete`, `BatchRecallComplete`, `PutRecallTask`, `UpdateTape`, ...

#### 4.2.3 Метрики слоя кэша

| Метрика | Тип | Единица | Labels | Описание |
|--------|------|------|--------|------|
| `coldstore.cache.hits` | Counter | 1 | — | число попаданий в кэш |
| `coldstore.cache.misses` | Counter | 1 | — | число промахов кэша |
| `coldstore.cache.puts` | Counter | 1 | — | число записей |
| `coldstore.cache.deletes` | Counter | 1 | — | число удалений (DELETE из слоя доступа / вытеснение) |
| `coldstore.cache.evictions` | Counter | 1 | `reason` | число вытеснений |
| `coldstore.cache.eviction.bytes` | Counter | байты | `reason` | число вытесненных байтов |
| `coldstore.cache.get.duration` | Histogram | мс | `hit` | задержка GET (раздельно попадание/промах) |
| `coldstore.cache.put.duration` | Histogram | мс | — | задержка PUT |
| `coldstore.cache.usage.bytes` | UpDownCounter | байты | — | текущий объём использования |
| `coldstore.cache.object.count` | UpDownCounter | 1 | — | текущее число объектов |
| `coldstore.spdk.io.duration` | Histogram | мкс | `operation` | задержка ввода-вывода SPDK |
| `coldstore.spdk.io.bytes` | Histogram | байты | `operation` | объём ввода-вывода SPDK |

**Label `reason`**: `ttl`, `capacity_lru`, `capacity_lfu`, `manual`

**Label `operation` (SPDK)**: `create_blob`, `write_blob`, `read_blob`, `delete_blob`

#### 4.2.4 Метрики слоя планирования

| Метрика | Тип | Единица | Labels | Описание |
|--------|------|------|--------|------|
| `coldstore.archive.bundles.created` | Counter | 1 | — | число созданных Bundle |
| `coldstore.archive.bundles.completed` | Counter | 1 | — | число завершённых Bundle |
| `coldstore.archive.bundles.failed` | Counter | 1 | — | число ошибочных Bundle |
| `coldstore.archive.objects.total` | Counter | 1 | — | всего заархивировано объектов |
| `coldstore.archive.bytes.total` | Counter | байты | — | всего заархивировано байтов |
| `coldstore.archive.throughput.bytes` | Counter | байты | — | число байтов, записанных на ленту (для расчёта пропускной способности) |
| `coldstore.archive.task.duration` | Histogram | с | — | длительность одной ArchiveTask |
| `coldstore.recall.tasks.created` | Counter | 1 | `tier` | число созданных задач восстановления |
| `coldstore.recall.tasks.completed` | Counter | 1 | `tier` | число завершённых задач восстановления |
| `coldstore.recall.tasks.failed` | Counter | 1 | `tier` | число ошибочных задач восстановления |
| `coldstore.recall.queue.depth` | UpDownCounter | 1 | `tier` | текущая глубина очереди |
| `coldstore.recall.wait.duration` | Histogram | с | `tier` | ожидание от постановки в очередь до начала выполнения |
| `coldstore.recall.read.duration` | Histogram | с | — | длительность чтения с ленты |
| `coldstore.recall.e2e.duration` | Histogram | с | `tier` | сквозная длительность (RestoreObject → Completed) |
| `coldstore.compensation.pending` | UpDownCounter | 1 | `saga_type` | число записей, ожидающих компенсации |
| `coldstore.compensation.retries` | Counter | 1 | `saga_type` | число повторов компенсации |

#### 4.2.5 Метрики ленточного слоя

| Метрика | Тип | Единица | Labels | Описание |
|--------|------|------|--------|------|
| `coldstore.tape.write.bytes` | Counter | байты | `tape_id` | число записанных байтов |
| `coldstore.tape.read.bytes` | Counter | байты | `tape_id` | число прочитанных байтов |
| `coldstore.tape.write.duration` | Histogram | с | — | длительность записи |
| `coldstore.tape.read.duration` | Histogram | с | — | длительность чтения |
| `coldstore.tape.seek.duration` | Histogram | мс | `seek_type` | длительность seek |
| `coldstore.tape.swap.total` | Counter | 1 | — | общее число замен лент |
| `coldstore.tape.swap.duration` | Histogram | с | — | распределение длительности замены лент |
| `coldstore.tape.errors` | Counter | 1 | `error_type`, `tape_id` | число ошибок |
| `coldstore.drive.utilization` | Histogram | доля | `drive_id` | загрузка накопителя |
| `coldstore.drive.queue.wait` | Histogram | с | `priority` | время ожидания накопителя |
| `coldstore.tape.offline.requests` | Counter | 1 | — | число запросов к офлайн-лентам |
| `coldstore.tape.offline.wait.duration` | Histogram | с | — | время ожидания офлайн-ленты |
| `coldstore.tape.verify.total` | Counter | 1 | `result` | число проверок |
| `coldstore.tape.verify.failures` | Counter | 1 | — | число неуспешных проверок |

**Label `seek_type`**: `fsf` (переход вперёд по FileMark), `fsr` (переход вперёд на блок), `rewind`

**Label `error_type`**: `hardware`, `media`, `write_protected`, `cleaning_required`

**Label `priority`**: `Expedited`, `Archive`, `Standard`, `Bulk`

### 4.3 Примеры кода instrumentation

#### 4.3.1 Слой доступа

```rust
use tracing::instrument;

#[instrument(
    name = "s3.get_object",
    skip(state),
    fields(
        coldstore.bucket = %bucket,
        coldstore.key = %key,
        coldstore.storage_class,
        cache.hit,
    )
)]
async fn handle_get_object(
    state: AppState,
    bucket: &str,
    key: &str,
) -> Result<Response> {
    let start = Instant::now();

    let meta = state.metadata.get_object(bucket, key).await?;
    let meta = meta.ok_or(Error::NoSuchKey)?;

    Span::current().record("coldstore.storage_class", &meta.storage_class.as_str());

    // ColdStore — чистая система холодного архива, все объекты находятся в Cold или ColdPending
    match meta.storage_class {
        StorageClass::ColdPending => {
            Err(Error::InvalidObjectState)
        }
        StorageClass::Cold => {
            match meta.restore_status {
                Some(RestoreStatus::Completed) if meta.restore_expire_at > Some(Utc::now()) => {
                    let cached = state.cache.get(bucket, key, None).await?;
                    match cached {
                        Some(obj) => {
                            Span::current().record("cache.hit", true);
                            state.metrics.cache_hits.add(1, &[]);
                            state.metrics.s3_request_duration.record(
                                start.elapsed().as_secs_f64() * 1000.0,
                                &[KeyValue::new("method", "GET"), KeyValue::new("bucket", bucket)],
                            );
                            Ok(build_response(obj, &meta))
                        }
                        None => {
                            Span::current().record("cache.hit", false);
                            state.metrics.cache_misses.add(1, &[]);
                            Err(Error::ServiceUnavailable("cache miss, please restore again"))
                        }
                    }
                }
                _ => Err(Error::InvalidObjectState),
            }
        }
    }
}
```

#### 4.3.2 Слой планирования

```rust
#[instrument(
    name = "scheduler.recall_process",
    skip(self),
    fields(
        scheduler.task_type = "recall",
        scheduler.merge_count,
    )
)]
async fn process_recall_batch(&self) {
    let tasks = self.metadata.list_pending_recall_tasks().await.unwrap_or_default();
    if tasks.is_empty() { return; }

    let jobs = self.merge_by_tape(tasks);
    Span::current().record("scheduler.merge_count", jobs.len() as i64);
    self.metrics.recall_queue_depth.add(-(jobs.len() as i64), &[]);

    for job in jobs {
        let span = tracing::info_span!("scheduler.execute_tape_read_job",
            tape.tape_id = %job.tape_id,
            scheduler.task_id = %job.job_id,
            coldstore.object_count = job.total_objects as i64,
        );

        self.execute_tape_read_job(job).instrument(span).await;
    }
}

#[instrument(
    name = "scheduler.execute_tape_read_job",
    skip(self, job),
    fields(
        tape.drive_id,
        tape.bytes_transferred,
    )
)]
async fn execute_tape_read_job(&self, job: TapeReadJob) {
    let acquire_start = Instant::now();

    // запрашиваем накопитель
    let guard = self.tape_manager.acquire_drive(
        job.priority.into()
    ).instrument(tracing::info_span!("tape.acquire_drive"))
    .await?;

    self.metrics.drive_queue_wait.record(
        acquire_start.elapsed().as_secs_f64(),
        &[KeyValue::new("priority", job.priority.as_str())],
    );

    Span::current().record("tape.drive_id", guard.drive_id());

    // загружаем ленту
    self.tape_manager.load_tape(&guard, &job.tape_id)
        .instrument(tracing::info_span!("tape.load", tape.tape_id = %job.tape_id))
        .await?;

    let mut total_bytes = 0u64;

    for segment in &job.segments {
        // позиционирование на FileMark
        guard.drive().seek_filemark(segment.filemark)
            .instrument(tracing::info_span!("tape.seek",
                tape.filemark = segment.filemark as i64,
            ))
            .await?;

        for obj in &segment.objects {
            let read_span = tracing::info_span!("tape.read_object",
                coldstore.bucket = %obj.bucket,
                coldstore.key = %obj.key,
                coldstore.object_size = obj.size as i64,
            );
            let _enter = read_span.enter();

            let data = guard.drive().read(obj.size).await?;
            total_bytes += data.len() as u64;

            // проверка контрольной суммы
            verify_checksum(&data, &obj.checksum)?;

            // запись в кэш
            self.cache.put_restored(RestoredItem {
                bucket: obj.bucket.clone(),
                key: obj.key.clone(),
                data,
                expire_at: obj.expire_at,
                // ...
            }).instrument(tracing::info_span!("cache.put_restored"))
            .await?;

            // обновление метаданных
            self.metadata.update_restore_status(
                &obj.bucket, &obj.key,
                RestoreStatus::Completed,
                Some(obj.expire_at),
            ).instrument(tracing::info_span!("metadata.update_restore_status"))
            .await?;

            self.metrics.recall_tasks_completed.add(1, &[
                KeyValue::new("tier", job.priority.as_str()),
            ]);
        }
    }

    Span::current().record("tape.bytes_transferred", total_bytes as i64);
    self.metrics.tape_read_bytes.add(total_bytes, &[
        KeyValue::new("tape_id", job.tape_id.clone()),
    ]);
}
```

#### 4.3.3 Слой метаданных

```rust
#[instrument(
    name = "metadata.put_object",
    skip(self, meta),
    fields(
        coldstore.bucket = %meta.bucket,
        coldstore.key = %meta.key,
        raft.log_index,
        raft.commit_latency_ms,
    )
)]
async fn put_object(&self, meta: ObjectMetadata) -> Result<()> {
    let start = Instant::now();

    let request = ColdStoreRequest::PutObject(meta);
    let response = self.raft.client_write(request).await?;

    let latency = start.elapsed().as_secs_f64() * 1000.0;
    Span::current().record("raft.log_index", response.log_id.index as i64);
    Span::current().record("raft.commit_latency_ms", latency);

    self.metrics.raft_propose_total.add(1, &[
        KeyValue::new("command_type", "PutObject"),
    ]);
    self.metrics.raft_propose_duration.record(latency, &[
        KeyValue::new("command_type", "PutObject"),
    ]);

    Ok(())
}
```

---

## 5. Проектирование структурированных логов

### 5.1 Связь логов с Trace

JSON formatter из `tracing` + `tracing-subscriber` автоматически добавляет в логи `trace_id` и `span_id`:

```json
{
  "timestamp": "2026-02-27T10:15:30.123Z",
  "level": "INFO",
  "target": "coldstore::scheduler::recall",
  "message": "recall task completed",
  "span": {
    "name": "scheduler.execute_tape_read_job",
    "scheduler.task_id": "a1b2c3d4-...",
    "tape.tape_id": "TAPE001"
  },
  "spans": [
    { "name": "scheduler.recall_process" },
    { "name": "scheduler.execute_tape_read_job" }
  ],
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "fields": {
    "recall_task_id": "a1b2c3d4-...",
    "bucket": "my-bucket",
    "key": "data/file.bin",
    "duration_ms": 45230,
    "objects_read": 15,
    "bytes_read": 1073741824
  }
}
```

### 5.2 Соглашение об уровнях логирования

| Уровень | Сценарий использования | Примеры |
|------|----------|------|
| **ERROR** | неустранимые ошибки, требуется ручное вмешательство | отказ от задачи компенсации, аппаратная неисправность ленты, потеря большинства Raft |
| **WARN** | устранимые аномалии, деградация производительности | выполнение повторов, промах кэша, таймаут выделения накопителя, офлайн-лента |
| **INFO** | ключевые бизнес-события | завершение архивации, завершение восстановления,пакетное вытеснение кэша, смена Leader |
| **DEBUG** | подробности операций | чтение и запись одного объекта, подробности журнала Raft, операции с Blob |
| **TRACE** | наимельчайшая детализация | детали ввода-вывода SPDK, байты чтения и записи RocksDB, выравнивание по блокам |

### 5.3 Ключевые события (Span Event)

Внутри длинных Span вехи фиксируются через `tracing::event!`:

```rust
#[instrument(name = "scheduler.execute_tape_read_job")]
async fn execute_tape_read_job(&self, job: TapeReadJob) {
    tracing::info!(tape_id = %job.tape_id, "acquiring drive");
    let guard = self.tape_manager.acquire_drive(priority).await?;

    tracing::info!(drive_id = %guard.drive_id(), "drive acquired, loading tape");
    self.tape_manager.load_tape(&guard, &job.tape_id).await?;

    tracing::info!("tape loaded, starting sequential read");

    for (i, obj) in objects.iter().enumerate() {
        tracing::debug!(
            object_index = i,
            bucket = %obj.bucket,
            key = %obj.key,
            size = obj.size,
            "reading object from tape"
        );
        // ...
    }

    tracing::info!(
        objects_read = objects.len(),
        bytes_read = total_bytes,
        duration_ms = start.elapsed().as_millis() as u64,
        "tape read job completed"
    );
}
```

---

## 6. Проектирование панелей мониторинга

### 6.1 Компоновка Dashboard

```
┌─────────────────────────────────────────────────────────────────┐
│                   ColdStore Overview Dashboard                    │
├─────────────┬─────────────┬─────────────┬───────────────────────┤
│ S3 QPS      │ Error Rate  │ P99 Latency │ Active Restores       │
│ [Counter]   │ [Gauge %]   │ [Gauge ms]  │ [Gauge]               │
├─────────────┴─────────────┴─────────────┴───────────────────────┤
│                                                                   │
│  ┌─ S3 Request Rate (by method) ──────────────────────────────┐ │
│  │  PUT ████████ 120/s                                        │ │
│  │  GET ████████████████████████ 850/s                        │ │
│  │  RESTORE ██ 5/s                                            │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌─ Request Latency (P50/P95/P99) ───────────────────────────┐ │
│  │  [Time series graph: GET, PUT, RESTORE]                    │ │
│  └────────────────────────────────────────────────────────────┘ │
├───────────────────────────────────────────────────────────────────┤
│                   Metadata Cluster                                │
├─────────────┬─────────────┬───────────────────────────────────────┤
│ Raft QPS    │ Leader Node │ Propose P99                          │
│ [Counter]   │ [Stat]      │ [Gauge ms]                           │
├─────────────┴─────────────┴───────────────────────────────────────┤
│                                                                   │
│  ┌─ Raft Propose Latency ────────────────────────────────────┐  │
│  │  [Histogram heatmap by command_type]                      │  │
│  └───────────────────────────────────────────────────────────┘  │
├───────────────────────────────────────────────────────────────────┤
│                     Cache Layer                                    │
├─────────────┬─────────────┬─────────────┬─────────────────────────┤
│ Hit Rate    │ Usage       │ Objects     │ Eviction Rate           │
│ [Gauge %]   │ [Gauge GB]  │ [Gauge]     │ [Counter /min]          │
├─────────────┴─────────────┴─────────────┴─────────────────────────┤
│                                                                   │
│  ┌─ Cache Hit/Miss Rate ─────┐  ┌─ SPDK I/O Latency ──────────┐│
│  │  [Stacked area chart]     │  │  [Histogram: read/write]     ││
│  └───────────────────────────┘  └──────────────────────────────┘│
├───────────────────────────────────────────────────────────────────┤
│                  Scheduler Layer                                   │
├─────────────┬─────────────┬─────────────┬─────────────────────────┤
│ Archive     │ Recall      │ Queue Depth │ Compensation            │
│ Throughput  │ Completed   │ [Gauge]     │ Pending                 │
│ [MB/s]      │ [/min]      │             │ [Gauge]                 │
├─────────────┴─────────────┴─────────────┴─────────────────────────┤
│                                                                   │
│  ┌─ Recall E2E Latency (by tier) ─────────────────────────────┐ │
│  │  Expedited  ██ P50=2min  P99=4.5min                        │ │
│  │  Standard   █████████████ P50=2hr  P99=4.8hr               │ │
│  │  Bulk       ██████████████████ P50=6hr  P99=11hr           │ │
│  └────────────────────────────────────────────────────────────┘ │
├───────────────────────────────────────────────────────────────────┤
│                    Tape Layer                                      │
├─────────────┬─────────────┬─────────────┬─────────────────────────┤
│ Drive       │ Swap Rate   │ Throughput  │ Errors                  │
│ Utilization │ [/hour]     │ [MB/s]      │ [Counter]               │
│ [Gauge %]   │             │             │                         │
├─────────────┴─────────────┴─────────────┴─────────────────────────┤
│                                                                   │
│  ┌─ Drive Utilization (per drive) ─┐  ┌─ Tape Errors ─────────┐│
│  │  [Multi-line gauge chart]       │  │  [by error_type]       ││
│  └─────────────────────────────────┘  └────────────────────────┘│
└───────────────────────────────────────────────────────────────────┘
```

### 6.2 Запросы основных Dashboard

#### 6.2.1 Скорость запросов S3

```promql
rate(coldstore_s3_requests_total[5m])
```

#### 6.2.2 Задержка S3 GET P99

```promql
histogram_quantile(0.99, rate(coldstore_s3_request_duration_bucket{method="GET"}[5m]))
```

#### 6.2.3 Доля попаданий в кэш

```promql
rate(coldstore_cache_hits[5m])
/ (rate(coldstore_cache_hits[5m]) + rate(coldstore_cache_misses[5m]))
```

#### 6.2.4 Пропускная способность архивации (MB/s)

```promql
rate(coldstore_archive_throughput_bytes[5m]) / 1048576
```

#### 6.2.5 Сквозная задержка Expedited P99

```promql
histogram_quantile(0.99, rate(coldstore_recall_e2e_duration_bucket{tier="Expedited"}[30m]))
```

#### 6.2.6 Загрузка накопителей

```promql
avg(coldstore_drive_utilization) by (drive_id)
```

#### 6.2.7 Задолженность задач компенсации

```promql
coldstore_compensation_pending
```

---

## 7. Правила оповещения

### 7.1 Критические оповещения (P1 — немедленное реагирование)

| Правило | Условие | Значение |
|------|------|------|
| в Raft нет Leader | `absent(raft_leader)` держится > 30 с | запись метаданных недоступна |
| потеря большинства | `raft_peers_connected < quorum` держится > 60 с | кластер недоступен |
| все накопители неисправны | `sum(drive_status == "available") == 0` держится > 5 мин | архивация и восстановление полностью заблокированы |
| неисправно устройство кэша | `spdk_bdev_status != "online"` | кэш полностью недоступен |
| отказ от задачи компенсации | `increase(compensation_abandoned[1h]) > 0` | риск для согласованности данных |

### 7.2 Оповещения высокого приоритета (P2 — реагирование в течение часа)

| Правило | Условие | Значение |
|------|------|------|
| резкий рост ошибок S3 | `rate(s3_errors_total[5m]) / rate(s3_requests_total[5m]) > 0.05` | более 5% запросов завершаются ошибкой |
| задержка Propose в Raft | `histogram_quantile(0.99, raft_propose_duration) > 100` мс | запись метаданных замедлилась |
| превышение SLA Expedited | `histogram_quantile(0.99, recall_e2e_duration{tier="Expedited"}) > 300` с | срочное восстановление дольше 5 минут |
| слишком высокая заполненность кэша | `cache_usage_bytes / cache_max_bytes > 0.95` | скоро начнутся частые вытеснения |
| задолженность задач компенсации | `compensation_pending > 10` | накопились неуспешные обновления метаданных |
| ожидание офлайн-ленты | `offline_wait_duration > 3600` с | лента не выведена в онлайн дольше часа |

### 7.3 Оповещения среднего приоритета (P3 — внимание в течение суток)

| Правило | Условие | Значение |
|------|------|------|
| низкая доля попаданий в кэш | `cache_hit_rate < 0.7` держится > 1 ч | возможно, нужно расширять кэш |
| падение пропускной способности архивации | `archive_throughput_mbps < 200` держится > 30 мин | производительность записи ниже нормы |
| частые замены лент | `rate(tape_swap_total[1h]) > 50` | возможно, нужно оптимизировать стратегию объединения |
| рост числа ошибок лент | `rate(tape_errors[1h]) > 5` | деградация качества носителей |
| задолженность очереди восстановления | `recall_queue_depth > 5000` | не хватает мощности восстановления |
| накопители загружены полностью | `avg(drive_utilization) > 0.95` держится > 1 ч | нужно добавить накопителей |
| частые смены Leader в Raft | `increase(raft_leader_changes[1h]) > 3` | нестабильная сеть или узлы |

---

## 8. Структура модуля

```
src/telemetry/
├── mod.rs                    # pub use, экспорт init_telemetry
├── config.rs                 # TelemetryConfig
├── init.rs                   # инициализация TracerProvider + MeterProvider + LoggerProvider
├── metrics.rs                # структура ColdStoreMetrics + регистрация всех метрик
├── sampler.rs                # собственный сэмплер ColdStoreSampler
├── context.rs                # сериализация/десериализация trace_context (W3C TraceContext)
├── middleware.rs             # промежуточный слой Axum (автоматическая запись span + метрик запросов S3)
└── shutdown.rs               # TelemetryGuard, корректное завершение
```

---

## 9. Параметры конфигурации

```yaml
telemetry:
  service_name: "coldstore"
  environment: "production"
  otlp_endpoint: "http://otel-collector:4317"
  log_level: "info"

  traces:
    enabled: true
    sample_rate: 0.1                         # по умолчанию 10%
    sample_overrides:
      "s3.get_object": 0.01                  # горячий путь GET — 1%
      "s3.restore_object": 1.0               # Restore — 100%
      "scheduler.archive_task": 1.0
      "scheduler.recall_process": 1.0
      "tape.*": 1.0
    export_batch_size: 512
    export_timeout_ms: 30000

  metrics:
    enabled: true
    export_interval_secs: 15
    histogram_buckets:
      latency_ms: [0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]
      latency_s: [0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600]
      size_bytes: [1024, 65536, 1048576, 10485760, 104857600, 1073741824]
    prometheus:
      enabled: true
      listen: "0.0.0.0:9090"

  logs:
    format: "json"
    include_trace_id: true
    include_span_list: true
```

---

## 10. Ключевые точки интеграции с другими слоями

### 10.1 Перечень instrumentation по слоям

| Слой | Способ интеграции | Требуемые возможности Crate |
|------|----------|------------------|
| слой доступа (01) | промежуточный слой Axum + `#[instrument]` | tracing layer из `tower-http` |
| протокольный слой (02) | `#[instrument]` на функциях разбора и формирования ответа | — |
| метаданные (03) | `#[instrument]` на реализациях трейтов + перехватчики Raft | колбэк метрик `openraft` |
| кэш (04) | `#[instrument]` + передача Context в потоки SPDK | явный `Context::current().attach()` |
| планирование (05) | `#[instrument]` + Span Link + Event | `tracing::Span::add_link` |
| ленты (06) | `#[instrument]` на реализациях трейтов | — |

### 10.2 Промежуточный слой Axum

```rust
use axum::middleware;
use tower_http::trace::TraceLayer;

let app = Router::new()
    .route("/*path", any(s3_handler))
    .layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<Body>| {
                let method = request.method().as_str();
                let path = request.uri().path();
                tracing::info_span!(
                    "s3.request",
                    http.method = %method,
                    http.url = %path,
                    http.status_code = tracing::field::Empty,
                    coldstore.bucket = tracing::field::Empty,
                    coldstore.key = tracing::field::Empty,
                )
            })
            .on_response(|response: &Response<Body>, latency: Duration, span: &Span| {
                span.record("http.status_code", response.status().as_u16());
            }),
    );
```

### 10.3 Оценка издержек

| Компонент | Издержки | Описание |
|------|------|------|
| `#[instrument]` | ~100 нс/span (вне сэмплирования) | Span коротко замыкается после решения о сэмплировании |
| создание сэмплируемого Span | ~1-2 мкс | выделение памяти + заполнение атрибутов |
| Counter/Gauge метрик | ~10 нс | атомарные операции |
| Histogram метрик | ~50 нс | поиск bucket + атомарные операции |
| пакетный экспорт OTLP | асинхронно в фоне | не блокирует бизнес-путь |
| форматирование JSON-логов | ~1-5 мкс/строка | только при совпадении уровня логирования |

**Суммарные издержки на горячем пути GET** (сэмплирование 1%): для ~99% запросов — всего ~200 нс накладных расходов, для 1% сэмплируемых запросов — ~5 мкс. При целевой задержке < 1 мс влиянием можно пренебречь.

---

## 11. Справочные материалы

- [OpenTelemetry Rust](https://opentelemetry.io/docs/languages/rust/) — официальная документация
- [opentelemetry crate](https://docs.rs/opentelemetry/latest/opentelemetry/) — документация API
- [tracing-opentelemetry](https://docs.rs/tracing-opentelemetry) — мостовой слой tracing
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/) — стандартизованные имена атрибутов
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) — конфигурация Collector
- [Grafana + Tempo + Prometheus](https://grafana.com/docs/) — бэкенды визуализации
