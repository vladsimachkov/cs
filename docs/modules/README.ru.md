# ColdStore: документация по проектированию модулей

В этом каталоге собраны документы по проектированию модулей, разделённые по слоям архитектуры. Обратите внимание: эти документы одновременно описывают и текущую реализацию, и целевое состояние; часть содержания опережает текущий код.

Авторитетное описание текущей реализации см. в [../ARCHITECTURE.ru.md](../ARCHITECTURE.ru.md). При принятии решений, касающихся реализации, в приоритете этот документ и `crates/*/src`.

## Указатель документов

| № | Модуль | Документ | Описание |
|------|------|------|------|
| 01 | Слой доступа | [01-access-layer.ru.md](./01-access-layer.ru.md) | S3 HTTP-сервис, маршрутизация, Axum |
| 02 | Слой адаптации протокола | [02-protocol-adapter.ru.md](./02-protocol-adapter.ru.md) | Отображение StorageClass, RestoreRequest, x-amz-restore, коды ошибок |
| 03 | Кластер метаданных | [03-metadata-cluster.ru.md](./03-metadata-cluster.ru.md) | Текущий путь персистентности на одном узле; OpenRaft на нескольких узлах — целевое состояние |
| 04 | Слой кэша данных | [04-cache-layer.ru.md](./04-cache-layer.ru.md) | Текущий кэш staging/restored; полноценный async-spdk — целевое состояние |
| 05 | Слой планировщика архивации и восстановления | [05-scheduler-layer.ru.md](./05-scheduler-layer.ru.md) | Archive Scheduler, Recall Scheduler |
| 06 | Слой управления лентами | [06-tape-layer.ru.md](./06-tape-layer.ru.md) | Собственный SDK, Linux SCSI |
| 07 | Межслойная согласованность и производительность | [07-consistency-performance.ru.md](./07-consistency-performance.ru.md) | Паттерн Saga, управление параллизмом, матрица отказов, оптимизация производительности |
| 08 | Наблюдаемость и трассировка | [08-observability.ru.md](./08-observability.ru.md) | OpenTelemetry, Traces, Metrics, Logs, оповещения |
| 09 | Панель администрирования (Admin Console) | [09-admin-console.ru.md](./09-admin-console.ru.md) | Web UI, Admin API, управление кластером/лентами/задачами |

## Взаимосвязи текущей реализации

```
Gateway(01/02)
    |
    v
Scheduler(05)
    |            |           |
    v            v           v
Metadata(03)  Cache(04)   Tape(06)
```

Gateway не подключается к Metadata/Cache/Tape напрямую. Scheduler — бизнес-ядро: он преобразует протокольные запросы в оркестрацию операций metadata/cache/tape.

## Известные устаревшие пункты

- Настоящий многоузловой Raft для Metadata остаётся целевым состоянием; текущий многоузловой `persistent_raft` завершается с ошибкой немедленно (fail-fast).
- Полноценный SPDK, реальная ленточная библиотека, Admin Console и полный контур OpenTelemetry остаются целевым состоянием.
- Multipart Upload, ListObjectsV2 и полная совместимость с S3 ещё требуют доработки.
