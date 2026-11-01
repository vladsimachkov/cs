# ColdStore — реализация на Rust

S3-совместимая ленточная система чистого холодного архива.

## Позиционирование проекта

ColdStore — это **система чистого холодного архива** (аналог AWS Glacier Deep Archive):
- Каждый записанный объект сразу помечается как ColdPending и автоматически ставится в очередь на архивацию на ленту
- Онлайн-слой hot-хранилища не предоставляется, управление жизненным циклом не выполняется
- Внешняя hot-система хранения вызывает PutObject API ColdStore, когда ей требуется холодный архив

## Текущее состояние реализации

Текущая кодовая база вышла на локальный замкнутый контур Phase 1 и начала Phase 2A — персистентность метаданных на одном узле:

| Этап | Статус | Реализованные возможности |
|------|------|------------|
| Phase 1 | реализовано / покрывается модульными тестами | Локальный замкнутый контур Gateway/Scheduler/Metadata/Cache; CRUD по bucket/object; Put/Head/Get/Delete/Restore/List; staging и restored на HDD Cache; на Phase-1 архивация помечает объект Cold и очищает staging; TapeService + безопасный эмуляционный накопитель/библиотека coldstore-vtl с замкнутым контуром load/unload и чтения/записи filemark; archive batch планировщика принимает внедряемые cache+tape writer и записывает ArchiveBundle/filemark-метаданные; CacheArchiveClient подключён к реальным CacheService staging list/get/delete в модульных тестах; TapeArchiveClient подключён к реальным TapeService WriteBundle/ReadBundle в gRPC-тестах |
| Phase 2A | начато / покрывается модульными тестами | Опциональный (opt-in) бинарный снимок Metadata: `MetadataServiceImpl::new_with_snapshot(config, path)` умеет сохранять состояние после записи и восстанавливать bucket/object/task/worker/tape и другие состояния после перезапуска |
| Phase 2B | следующий шаг | Подключить конечный автомат Metadata к OpenRaft + RocksDB/openraft-rocksstore, добавить безопасные модульные тесты конечного автомата Raft и тесты согласованности на нескольких узлах |

Пока что система не обращается к реальным ленточным устройствам и по умолчанию не запускает интеграционные тесты.

## Структура Workspace

| Crate | Тип | Назначение |
|-------|------|------------|
| coldstore-proto | lib | Определения протокола gRPC (генерация из protobuf) |
| coldstore-common | lib | Общие модели, ошибки, конфигурация |
| coldstore-metadata | bin | Узел кластера метаданных (Raft + RocksDB) |
| coldstore-gateway | bin | HTTP-шлюз S3 (без состояния) |
| coldstore-scheduler | bin | Worker планировщика (бизнес-ядро) |
| coldstore-cache | bin | Worker кэша (отдельный процесс, HDD/SPDK) |
| coldstore-tape | bin | Ленточный Worker (отдельный физический узел) |
| coldstore-vtl | lib | mhVTL/эмулятор ленточной библиотеки: обёртки над командами lsscsi/mtx/mt/sg + безопасный имитатор в памяти |

## Взаимодействие между компонентами

Все компоненты общаются по gRPC, определения протокола находятся в crates/proto/proto/:

- common.proto: общие перечисления и структуры данных
- metadata.proto: MetadataService (чтение и запись метаданных, управление кластером, heartbeat)
- scheduler.proto: SchedulerService (пересылка S3-запросов из Gateway)
- cache.proto: CacheService (staging данных и кэш разморозки)
- tape.proto: TapeService (чтение и запись лент, управление накопителями)

## Быстрый старт

Необходимые условия: Rust 1.75+, protobuf-compiler

    make build-debug

Запуск компонентов по отдельности:

    cargo run -p coldstore-metadata
    cargo run -p coldstore-cache
    cargo run -p coldstore-scheduler
    cargo run -p coldstore-gateway

## Лицензия

MIT OR Apache-2.0
