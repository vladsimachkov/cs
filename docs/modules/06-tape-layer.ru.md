# Проектирование модуля слоя управления лентами

> Архитектура: система холодного хранения ColdStore  
> См. также: [Проектирование общей архитектуры](../DESIGN.ru.md)

## 1. Обзор модуля

Слой управления лентами предоставляет полный функционал управления ленточными устройствами и ленточными библиотеками: собственный слой абстракции SDK развязывает его с нижележащим оборудованием, на первом этапе используется протокол Linux SCSI.

### 1.0 Текущее состояние реализации (безопасный базовый уровень Phase-1)

В текущем коде есть замкнутый ленточный цикл Phase-1, не воздействующий на устройства хоста:

- `crates/vtl` — отдельный харнес виртуальной ленточной библиотеки: разбор вывода `lsscsi -g`, построение спецификаций команд `mtx`/`mt`/`sg3_utils`/`dd`, внедрение runner, а также чистая `VirtualTapeLibrary` в памяти.
- `TapeServiceImpl` в `crates/tape` зависит от абстракции бэкенда через трейт `TapeBackend` и по умолчанию использует `SimulatorTapeBackend`, не обращаясь к `/dev/st*`, `/dev/nst*`, `/dev/sg*` или `/dev/sch*`.
- Через слой gRPC-сервиса опубликованы проверяемые юнит-тестами операции накопителя/библиотеки: `ListDrives`, `GetDriveStatus`, `AcquireDrive`, `ReleaseDrive`, `LoadTape`, `UnloadTape`, `Rewind`, `SeekToFilemark`, `GetTapeMediaStatus`, `Inventory`.
- Через service/helper и настоящий путь gRPC-клиента опубликованы запись bundle с разделением filemark и чтение по filemark; `TapeArchiveClient` на стороне Scheduler умеет писать в TapeService на симуляторе через `TapeServiceClient.WriteBundle`.
- Позиционирование по block offset в `ReadBundle` по-прежнему возвращает явный `UNIMPLEMENTED` — ждёт подключения реальных семантик позиционирования SCSI/ленты.
- Скрипт установки mhVTL находится в `scripts/setup-mhvtl-env.sh`, по умолчанию работает в режиме dry-run; текущая безопасная проверка выполняет только `bash -n`, системные пакеты не устанавливаются, модули ядра и сервисы не запускаются.

Реальный замкнутый цикл mhVTL/SCSI относится к цели последующего live-тестирования в выделенной ВМ: он обязан явно использовать live runner или `--execute` установочного скрипта и не может быть путём модульных тестов по умолчанию.

### 1.1 Полный перечень обязанностей

| Область функциональности | Описание |
|--------|------|
| Управление ленточными накопителями | обнаружение накопителей, мониторинг состояния, эксклюзивная выдача, освобождение |
| Управление ленточными носителями | регистрация носителей, учёт ёмкости, форматирование, проверка читаемости, жизненный цикл |
| Управление ленточной библиотекой | управление слотами, загрузка/выгрузка манипулятором, инвентаризация, сканирование штрихкодов |
| Чтение и запись данных | последовательная запись, последовательное/позиционное чтение, управление FileMark |
| Взаимодействие по офлайн-лентам | отслеживание состояния, запуск оповещений, процессы ручного подтверждения |
| Надёжность | две реплики, проверка, обработка ошибок, замена неисправной ленты |
| Наблюдаемость | загрузка накопителей, пропускная способность чтения и записи, доля ошибок, число перестановок ленты |

### 1.2 Место в архитектуре

```
Планировщики архивации/восстановления
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Слой управления лентами                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ TapeManager (API верхнего уровня, для планировщика)             │  │
│  │ - выдача накопителей, выбор ленты, учёт носителей, оповещения   │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                           │                                           │
│  ┌────────────────────────┼────────────────────────┐                 │
│  │                        │                        │                 │
│  ▼                        ▼                        ▼                 │
│  ┌──────────┐    ┌───────────────┐    ┌────────────────────┐       │
│  │ TapeDrive│    │ TapeMedia     │    │ TapeLibrary        │       │
│  │ trait    │    │ trait         │    │ trait              │       │
│  │(накопитель)│  │ (носитель)    │    │ (библиотека/       │       │
│  │          │    │               │    │  манипулятор)      │       │
│  └──────────┘    └───────────────┘    └────────────────────┘       │
│       │                │                       │                    │
│  ┌────┴────────────────┴───────────────────────┴────┐               │
│  │              Слой конкретных реализаций           │               │
│  │  ScsiTapeDrive | ScsiTapeMedia | ScsiTapeLibrary  │               │
│  │  (первый этап: Linux SCSI)                        │               │
│  └──────────────────────────────────────────────────┘               │
│       │                                                              │
│  ┌────┴─────────────────────────────────────────────┐               │
│  │              Нижележащие обёртки                  │               │
│  │  mtio.rs (ioctl MTIO) | sg.rs (SCSI Generic)     │               │
│  └──────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Слоистая структура кода

### 2.1 Принципы разбиения на слои

| Уровень | Обязанности | Стабильность |
|------|------|--------|
| **TapeManager** | бизнес-API для планировщика, оркестрация накопителей/носителей/библиотеки | максимально стабилен, не зависит от смены оборудования |
| **Слой абстракции SDK** (трейты) | определяет интерфейсы TapeDrive / TapeMedia / TapeLibrary | стабилен, интерфейсы меняются нечасто |
| **Слой реализации** | конкретная интеграция с оборудованием (SCSI, SDK вендоров) | заменяем без влияния на верхние слои |
| **Нижележащие обёртки** | обёртки ioctl / команд SCSI | самый нижний уровень, зависит от платформы |

### 2.2 Стратегия расширения

- Новый аппаратный бэкенд: достаточно реализовать трейты TapeDrive / TapeMedia / TapeLibrary
- Планировщик верхнего уровня: зависит только от API TapeManager и не знает о нижней реализации
- Переключение конфигурацией: реализация выбирается параметром `tape.sdk.backend`

---

## 3. Слой абстракции SDK: определение трейтов

### 3.1 Трейт TapeDrive (ленточный накопитель)

Накопитель — единица выполнения чтения и записи данных; за раз он привязан к одной ленте.

```rust
/// абстракция ленточного накопителя — единица выполнения I/O данных
#[async_trait]
pub trait TapeDrive: Send + Sync {
    // ── идентификаторы ──
    fn drive_id(&self) -> &str;
    fn device_path(&self) -> &str;

    // ── чтение и запись данных ──
    async fn write_blocks(&self, data: &[u8]) -> Result<u64>;
    async fn read_blocks(&self, buf: &mut [u8]) -> Result<u64>;

    // ── позиционирование ──
    async fn seek_to_filemark(&self, count: i32) -> Result<()>;
    async fn seek_to_record(&self, count: i32) -> Result<()>;
    async fn seek_to_end_of_data(&self) -> Result<()>;
    async fn rewind(&self) -> Result<()>;

    // ── FileMark ──
    async fn write_filemark(&self, count: u32) -> Result<()>;

    // ── состояние ──
    async fn status(&self) -> Result<DriveStatus>;
    async fn position(&self) -> Result<TapePosition>;
    async fn is_ready(&self) -> Result<bool>;

    // ── управление ──
    async fn eject(&self) -> Result<()>;
    async fn set_block_size(&self, size: u32) -> Result<()>;
    async fn set_compression(&self, enabled: bool) -> Result<()>;
}
```

### 3.2 Трейт TapeMedia (ленточный носитель)

Управление носителем охватывает жизненный цикл одной ленты.

```rust
/// абстракция ленточного носителя — метаданные и жизненный цикл одной ленты
#[async_trait]
pub trait TapeMedia: Send + Sync {
    // ── идентификаторы ──
    fn media_id(&self) -> &str;
    fn barcode(&self) -> Option<&str>;
    fn format(&self) -> &str;  // "LTO-9", "LTO-10"

    // ── ёмкость ──
    async fn capacity(&self) -> Result<MediaCapacity>;
    async fn remaining(&self) -> Result<u64>;
    async fn is_full(&self) -> Result<bool>;

    // ── состояние ──
    fn status(&self) -> MediaStatus;
    fn set_status(&mut self, status: MediaStatus);
    fn location(&self) -> MediaLocation;
    fn set_location(&mut self, location: MediaLocation);

    // ── жизненный цикл ──
    async fn verify_readable(&self, drive: &dyn TapeDrive) -> Result<VerifyResult>;
    async fn erase(&self, drive: &dyn TapeDrive) -> Result<()>;

    // ── метаданные ──
    fn label(&self) -> Option<&str>;
    async fn write_label(&self, drive: &dyn TapeDrive, label: &str) -> Result<()>;
    fn archive_bundles(&self) -> &[Uuid];
    fn add_archive_bundle(&mut self, bundle_id: Uuid);
}
```

### 3.3 Трейт TapeLibrary (ленточная библиотека)

Управление библиотекой охватывает автоматизированные операции: манипулятор, слоты, инвентаризация.

```rust
/// абстракция ленточной библиотеки — автоматическая библиотека с манипулятором
#[async_trait]
pub trait TapeLibrary: Send + Sync {
    fn library_id(&self) -> &str;

    // ── управление слотами ──
    async fn list_slots(&self) -> Result<Vec<SlotInfo>>;
    async fn list_drives(&self) -> Result<Vec<DriveSlotInfo>>;

    // ── операции манипулятора ──
    // drive_element — SCSI Data Transfer Element Address (u32)
    // TapeManager отвечает за отображение строкового drive_id в адрес SCSI element
    async fn load(&self, slot_id: u32, drive_element: u32) -> Result<()>;
    async fn unload(&self, drive_element: u32, slot_id: u32) -> Result<()>;
    async fn transfer(&self, from_slot: u32, to_slot: u32) -> Result<()>;

    // ── инвентаризация ──
    async fn inventory(&self) -> Result<LibraryInventory>;

    // ── импорт/экспорт ──
    async fn list_import_export_slots(&self) -> Result<Vec<SlotInfo>>;

    // ── состояние ──
    async fn status(&self) -> Result<LibraryStatus>;
}
```

### 3.4 Определения типов и описание полей

#### 3.4.1 DriveStatus (состояние накопителя)

```rust
pub struct DriveStatus {
    pub drive_id: String,
    pub is_ready: bool,
    pub has_media: bool,
    pub media_id: Option<String>,
    pub block_size: u32,
    pub compression: bool,
    pub write_protected: bool,
    pub error: Option<DriveError>,
}
```

| Поле | Тип | Значение | Источник |
|------|------|------|------|
| `drive_id` | String | уникальный идентификатор накопителя | задаётся конфигурацией |
| `is_ready` | bool | готов ли накопитель выполнять I/O | разбор MTIOCGET |
| `has_media` | bool | загружена ли лента | инверсия `GMT_DR_OPEN` из MTIOCGET |
| `media_id` | Option\<String\> | ID текущей загруженной ленты | сопоставление MediaRegistry |
| `block_size` | u32 | текущий размер блока (байт) | младшие 24 бита `mt_dsreg` из MTIOCGET |
| `compression` | bool | включено ли аппаратное сжатие | флаги накопителя из MTIOCGET |
| `write_protected` | bool | лента защищена от записи | `GMT_WR_PROT` из MTIOCGET |
| `error` | Option\<DriveError\> | текущая ошибка (None, если ошибок нет) | устанавливается после обнаружения нештатной ситуации |

#### 3.4.2 TapePosition (позиция на ленте)

```rust
pub struct TapePosition {
    pub filemark_number: u32,
    pub block_number: u64,
    pub at_bot: bool,
    pub at_eod: bool,
    pub at_filemark: bool,
}
```

| Поле | Тип | Значение | Источник |
|------|------|------|------|
| `filemark_number` | u32 | номер текущего FileMark (с нуля) | MTIOCPOS или программный счётчик |
| `block_number` | u64 | номер текущего блока | `mt_blkno` из MTIOCPOS |
| `at_bot` | bool | находимся ли в начале ленты (Beginning of Tape) | `GMT_BOT` из MTIOCGET |
| `at_eod` | bool | находимся ли в конце данных (End of Data) | `GMT_EOD` из MTIOCGET |
| `at_filemark` | bool | только что прошли FileMark | `GMT_EOF` из MTIOCGET |

#### 3.4.3 MediaCapacity (ёмкость носителя)

```rust
pub struct MediaCapacity {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub remaining_bytes: u64,
}
```

| Поле | Тип | Значение | Источник |
|------|------|------|------|
| `total_bytes` | u64 | полная ёмкость ленты в байтах | спецификация носителя (LTO-9: 18TB в сыром виде) |
| `used_bytes` | u64 | использованные байты | учёт суммарной записи в MediaRegistry |
| `remaining_bytes` | u64 | оставшиеся доступные байты | `total_bytes - used_bytes` |

#### 3.4.4 MediaStatus (состояние носителя)

```rust
pub enum MediaStatus {
    Online,
    Offline,
    InDrive(String),
    ImportExport,
    Error(String),
    Retired,
}
```

| Значение | Смысл | Условие перехода |
|------|------|----------|
| `Online` | в библиотеке, свободно доступно | инвентаризация подтвердила нахождение в слоте |
| `Offline` | хранится офлайн, в библиотеке отсутствует | не найдена при инвентаризации или помечена человеком |
| `InDrive(drive_id)` | загружена в указанный накопитель | устанавливается после успешного `load()` |
| `ImportExport` | в слоте импорта/экспорта | обнаружена при инвентаризации в слоте I/E |
| `Error(msg)` | нештатное состояние | ошибки чтения/записи, сбой проверки |
| `Retired` | выведена из эксплуатации | слишком много нечитаемых блоков или превышен срок службы |

#### 3.4.5 MediaLocation (расположение носителя)

```rust
pub enum MediaLocation {
    Slot(u32),
    Drive(String),
    Offsite(String),
    Unknown,
}
```

| Значение | Смысл | Применение |
|------|------|------|
| `Slot(slot_id)` | в слоте с номером N | позиционирование при `load(slot_id, drive_id)` |
| `Drive(drive_id)` | в накопителе | загружена, доступна для чтения и записи |
| `Offsite(location)` | описание офлайн-места хранения | ориентир при ручном поиске, например «серверная B-шкаф 3-полка 2» |
| `Unknown` | неизвестно | начальное состояние или сбой инвентаризации |

#### 3.4.6 DriveError (ошибки накопителя)

```rust
pub enum DriveError {
    MediaNotLoaded,
    WriteProtected,
    HardwareError(String),
    MediaError(String),
    CleaningRequired,
}
```

| Значение | Смысл | Стратегия обработки |
|------|------|----------|
| `MediaNotLoaded` | лента не загружена | сначала нужен load |
| `WriteProtected` | включён переключатель защиты ленты от записи | запись невозможна, нужна замена ленты или выключение физического переключателя |
| `HardwareError(msg)` | аппаратная неисправность накопителя | пометить накопитель недоступным, переключиться на резервный |
| `MediaError(msg)` | ошибка носителя (неисправные блоки и т.п.) | повтор → переключение на реплику → пометка ленты как неисправной |
| `CleaningRequired` | накопителю нужна чистка | отправить оповещение, вставить чистящую ленту |

#### 3.4.7 SlotInfo (сведения о слоте)

```rust
pub struct SlotInfo {
    pub slot_id: u32,
    pub element_type: SlotType,
    pub is_full: bool,
    pub media_id: Option<String>,
    pub barcode: Option<String>,
}
```

| Поле | Тип | Значение | Источник |
|------|------|------|------|
| `slot_id` | u32 | SCSI Element Address (номер слота) | ответ READ ELEMENT STATUS |
| `element_type` | SlotType | тип слота | код SCSI Element Type |
| `is_full` | bool | есть ли в нём лента | бит `Full` в Element Status |
| `media_id` | Option\<String\> | ID ленты (если есть) | сопоставление MediaRegistry |
| `barcode` | Option\<String\> | результат чтения штрихкода | Volume Tag (если библиотека поддерживает) |

#### 3.4.8 SlotType (тип слота)

```rust
pub enum SlotType {
    Storage,
    DataTransfer,
    ImportExport,
    MediumTransport,
}
```

| Значение | SCSI Element Type | Смысл |
|------|-------------------|------|
| `Storage` | 0x02 | складской слот, хранение лент |
| `DataTransfer` | 0x04 | позиция накопителя; загруженная сюда лента доступна для чтения и записи |
| `ImportExport` | 0x03 | слот импорта/экспорта («почтовый ящик»): человек кладёт или забирает ленту |
| `MediumTransport` | 0x01 | сам манипулятор |

#### 3.4.9 VerifyResult (результат проверки)

```rust
pub struct VerifyResult {
    pub readable: bool,
    pub blocks_verified: u64,
    pub blocks_failed: u64,
    pub errors: Vec<VerifyError>,
    pub duration_secs: u64,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `readable` | bool | читается ли лента в целом | быстрая оценка состояния ленты |
| `blocks_verified` | u64 | число успешно проверенных блоков | полнота проверки |
| `blocks_failed` | u64 | число непроверенных блоков | превышение порога запускает вывод из эксплуатации |
| `errors` | Vec\<VerifyError\> | список конкретных ошибок | позиционирование неисправных блоков |
| `duration_secs` | u64 | длительность проверки в секундах | наблюдаемость |

#### 3.4.10 WriteResult (результат записи)

```rust
pub struct WriteResult {
    pub bytes_written: u64,
    pub blocks_written: u64,
    pub filemark_position: u32,
    pub tape_block_offset: u64,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `bytes_written` | u64 | фактическое число записанных байт | обновление MediaCapacity.used_bytes |
| `blocks_written` | u64 | фактическое число записанных блоков | логи/мониторинг |
| `filemark_position` | u32 | номер FileMark после записи | фиксируется в BundleEntry для позиционирования при восстановлении |
| `tape_block_offset` | u64 | блочное смещение начала записи на ленте | фиксируется в BundleEntry.tape_block_offset |

#### 3.4.11 MediaRegistration (сведения о регистрации носителя)

```rust
pub struct MediaRegistration {
    pub media_id: String,
    pub barcode: Option<String>,
    pub format: String,
    pub capacity_bytes: u64,
    pub label: Option<String>,
    pub initial_location: MediaLocation,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `media_id` | String | уникальный идентификатор ленты | глобально уникален, например "TAPE001" |
| `barcode` | Option\<String\> | штрихкод (физическая наклейка) | автоматическое распознавание библиотекой |
| `format` | String | формат носителя | "LTO-9", "LTO-10" |
| `capacity_bytes` | u64 | номинальная ёмкость | LTO-9: 18TB = 18,000,000,000,000 |
| `label` | Option\<String\> | логическая метка | записывается в начало ленты, распознаётся человеком |
| `initial_location` | MediaLocation | начальное расположение | Slot(N) или Offsite("...") |

#### 3.4.12 LibraryInventory / DriveSlotInfo / LibraryStatus

```rust
pub struct LibraryInventory {
    pub slots: Vec<SlotInfo>,
    pub drives: Vec<DriveSlotInfo>,
    pub import_export: Vec<SlotInfo>,
    pub total_media: u32,
    pub empty_slots: u32,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `slots` | Vec\<SlotInfo\> | сведения обо всех складских слотах |
| `drives` | Vec\<DriveSlotInfo\> | сведения обо всех позициях накопителей |
| `import_export` | Vec\<SlotInfo\> | сведения о слотах импорта/экспорта |
| `total_media` | u32 | общее число лент в библиотеке |
| `empty_slots` | u32 | число пустых слотов |

```rust
pub struct DriveSlotInfo {
    pub drive_id: u32,
    pub device_path: String,
    pub is_loaded: bool,
    pub media_id: Option<String>,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `drive_id` | u32 | SCSI Data Transfer Element Address |
| `device_path` | String | путь к узлу устройства (например `/dev/nst0`) |
| `is_loaded` | bool | загружена ли лента |
| `media_id` | Option\<String\> | ID загруженной ленты |

```rust
pub struct LibraryStatus {
    pub library_id: String,
    pub is_online: bool,
    pub total_slots: u32,
    pub total_drives: u32,
    pub error: Option<String>,
}
```

| Поле | Тип | Значение |
|------|------|------|
| `library_id` | String | уникальный идентификатор библиотеки |
| `is_online` | bool | доступна ли библиотека для операций |
| `total_slots` | u32 | общее число складских слотов |
| `total_drives` | u32 | общее число накопителей |
| `error` | Option\<String\> | текущие сведения об ошибке |

#### 3.4.13 OfflineRequest / OfflineRequestStatus

```rust
pub struct OfflineRequest {
    pub request_id: Uuid,
    pub media_id: String,
    pub barcode: Option<String>,
    pub last_known_location: String,
    pub reason: String,
    pub requested_at: DateTime<Utc>,
    pub status: OfflineRequestStatus,
    pub notified_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub timeout_at: DateTime<Utc>,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `request_id` | Uuid | уникальный идентификатор запроса | отслеживание и дедупликация |
| `media_id` | String | ID ленты, которую нужно вывести в онлайн | содержание оповещения |
| `barcode` | Option\<String\> | штрихкод ленты | для ручного поиска |
| `last_known_location` | String | последнее известное расположение | например "Offsite: серверная B-шкаф 3" |
| `reason` | String | причина запроса | например "требуется чтение RecallTask xxx" |
| `requested_at` | DateTime\<Utc\> | время создания запроса | аудит |
| `status` | OfflineRequestStatus | состояние запроса | отслеживание процесса |
| `notified_at` | Option\<DateTime\<Utc\>\> | время отправки оповещения | аудит |
| `confirmed_at` | Option\<DateTime\<Utc\>\> | время подтверждения вывода в онлайн | расчёт времени ожидания |
| `timeout_at` | DateTime\<Utc\> | предельный срок ожидания | по истечении RecallTask автоматически помечается Failed |

```rust
pub enum OfflineRequestStatus {
    Pending,     // создан, оповещение не отправлено
    Notified,    // оповещение отправлено
    Confirmed,   // человек подтвердил вывод ленты в онлайн
    Cancelled,   // отменён (например, пользователь отменил задачу)
    TimedOut,    // подтверждение не получено до таймаута
}
```

---

## 4. TapeManager: бизнес-API для планировщика

### 4.1 Обязанности

TapeManager оркестрирует TapeDrive / TapeMedia / TapeLibrary и предоставляет планировщику API верхнего уровня.

### 4.2 Структура TapeManager

```rust
pub struct TapeManager {
    drives: Vec<Box<dyn TapeDrive>>,
    media_registry: MediaRegistry,
    library: Option<Box<dyn TapeLibrary>>,
    drive_allocator: DriveAllocator,
    notification: NotificationSender,
}
```

| Поле | Тип | Значение | Описание |
|------|------|------|------|
| `drives` | Vec\<Box\<dyn TapeDrive\>\> | все зарегистрированные экземпляры накопителей | создаются при запуске согласно конфигурации |
| `media_registry` | MediaRegistry | реестр носителей | отображение ID ленты → ёмкость/состояние/расположение/архивные комплекты |
| `library` | Option\<Box\<dyn TapeLibrary\>\> | экземпляр библиотеки (опционально) | на первом этапе без библиотеки — None |
| `drive_allocator` | DriveAllocator | распределитель накопителей | эксклюзивная выдача + приоритетная очередь |
| `notification` | NotificationSender | отправитель оповещений | запросы по офлайн-лентам, алерты об ошибках |

### 4.3 Ключевые интерфейсы

```rust

impl TapeManager {
    // ── выдача накопителей ──
    // priority: влияет на порядок в очереди и на выдачу зарезервированных накопителей
    pub async fn acquire_drive(&self, priority: DrivePriority) -> Result<DriveGuard>;

    // ── выбор и загрузка ленты ──
    pub async fn select_writable_tape(&self, min_remaining: u64) -> Result<String>;
    pub async fn load_tape(&self, media_id: &str, drive: &DriveGuard) -> Result<()>;
    pub async fn unload_tape(&self, drive: &DriveGuard) -> Result<()>;

    // ── запись данных (для планировщика архивации) ──
    pub async fn sequential_write(
        &self,
        drive: &DriveGuard,
        data: &[u8],
    ) -> Result<WriteResult>;
    pub async fn write_filemark(&self, drive: &DriveGuard) -> Result<()>;

    // ── чтение данных (для планировщика восстановления) ──
    pub async fn seek_and_read(
        &self,
        drive: &DriveGuard,
        filemark: u32,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>>;

    // ── управление носителями ──
    pub async fn register_media(&self, info: MediaRegistration) -> Result<()>;
    pub async fn retire_media(&self, media_id: &str) -> Result<()>;
    pub async fn get_media_info(&self, media_id: &str) -> Result<TapeInfo>;
    pub async fn update_media_status(&self, media_id: &str, status: MediaStatus) -> Result<()>;

    // ── офлайн-ленты ──
    pub async fn check_media_online(&self, media_id: &str) -> Result<bool>;
    pub async fn request_offline_media(
        &self,
        media_id: &str,
        reason: &str,
    ) -> Result<OfflineRequest>;
    pub async fn confirm_media_online(&self, media_id: &str) -> Result<()>;

    // ── инвентаризация библиотеки ──
    pub async fn run_inventory(&self) -> Result<LibraryInventory>;

    // ── исправность и проверка ──
    pub async fn verify_media(&self, media_id: &str) -> Result<VerifyResult>;
    pub async fn get_drive_status(&self, drive_id: &str) -> Result<DriveStatus>;
    pub async fn list_all_drives(&self) -> Result<Vec<DriveStatus>>;
}
```

### 4.4 DriveGuard (эксклюзивный токен накопителя)

```rust
pub struct DriveGuard {
    pub drive_id: String,
    drive: Arc<Box<dyn TapeDrive>>,
    release_tx: oneshot::Sender<String>,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `drive_id` | String | ID эксклюзивно занятого накопителя | трассировка в логах |
| `drive` | Arc\<Box\<dyn TapeDrive\>\> | ссылка на экземпляр накопителя | вызов операций read/write/seek |
| `release_tx` | Option\<oneshot::Sender\<String\>\> | канал освобождения | при Drop отправляет drive_id обратно в DriveAllocator |

> **Дизайн RAII**: явного метода `release_drive` нет. DriveGuard при Drop автоматически возвращает накопитель через `release_tx` (семантика `take()` гарантирует однократную отправку), что исключает забывание освобождения. Вызывающей стороне достаточно вывести DriveGuard из области видимости.

### 4.5 DriveAllocator (распределитель накопителей)

```rust
pub struct DriveAllocator {
    available: Mutex<VecDeque<String>>,
    waiters: Mutex<BinaryHeap<DriveWaiter>>,
    reserved_for_expedited: u32,
    total_drives: u32,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `available` | Mutex\<VecDeque\<String\>\> | очередь ID свободных накопителей | при выдаче извлекается через pop |
| `waiters` | Mutex\<BinaryHeap\<DriveWaiter\>\> | ожидающие выдачи запросы (с сортировкой по приоритету) | при отсутствии свободных накопителей помещаются в кучу |
| `reserved_for_expedited` | u32 | число накопителей, зарезервированных для Expedited | запросы не-Expedited не могут занять резерв |
| `total_drives` | u32 | общее число накопителей | расчёт загрузки |
| `drive_map` | HashMap\<String, Arc\<Box\<dyn TapeDrive\>\>\> | отображение drive_id → экземпляр накопителя | при acquire берётся экземпляр для построения DriveGuard |

```rust
pub enum DrivePriority {
    Expedited,   // наивысший, может использовать зарезервированные накопители
    Archive,     // запись при архивации
    Standard,    // стандартное восстановление
    Bulk,        // пакетное восстановление, наименьший
}
```

### 4.6 TapeInfo (DTO сведений о ленте)

Несущий только чтение вид, который возвращает `TapeManager.get_media_info()`; спроецирован из `MediaRecord`.

```rust
pub struct TapeInfo {
    pub id: String,
    pub barcode: Option<String>,
    pub format: String,
    pub status: MediaStatus,
    pub location: MediaLocation,
    pub capacity: MediaCapacity,
    pub archive_bundles: Vec<Uuid>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub error_count: u32,
}
```

> `TapeInfo` — несущая только чтение подмножество `MediaRecord`, без интерфейсов изменения. В слое метаданных этот же хранится и в CF `tapes` (записывается слоем планирования).

### 4.7 MediaRegistry (реестр носителей)

```rust
pub struct MediaRegistry {
    media: HashMap<String, MediaRecord>,
}

pub struct MediaRecord {
    pub media_id: String,
    pub barcode: Option<String>,
    pub format: String,
    pub capacity: MediaCapacity,
    pub status: MediaStatus,
    pub location: MediaLocation,
    pub label: Option<String>,
    pub archive_bundles: Vec<Uuid>,
    pub registered_at: DateTime<Utc>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub error_count: u32,
}
```

| Поле | Тип | Значение | Применение |
|------|------|------|------|
| `media_id` | String | уникальный идентификатор ленты | первичный ключ |
| `barcode` | Option\<String\> | штрихкод | автоматическое распознавание библиотекой |
| `format` | String | формат носителя | "LTO-9" и т.п. |
| `capacity` | MediaCapacity | сведения о ёмкости | total/used/remaining |
| `status` | MediaStatus | текущее состояние | Online/Offline/InDrive и т.д. |
| `location` | MediaLocation | текущее расположение | Slot/Drive/Offsite |
| `label` | Option\<String\> | логическая метка | распознаваемое человеком имя |
| `archive_bundles` | Vec\<Uuid\> | список ID записанных архивных комплектов | учёт содержимого ленты |
| `registered_at` | DateTime\<Utc\> | время регистрации | отслеживание срока службы носителя |
| `last_verified_at` | Option\<DateTime\<Utc\>\> | время последней проверки | планирование периодической проверки |
| `error_count` | u32 | суммарное число ошибок | превышение порога запускает вывод из эксплуатации |

**Связь трейта TapeMedia и MediaRecord**:
- трейт `TapeMedia` абстрагирует физические операции с носителем (те, где нужен накопитель: verify_readable, erase, write_label)
- `MediaRecord` — логическая управляющая запись в памяти, от физического накопителя не зависит
- `ScsiTapeMedia` реализует трейт `TapeMedia` и одновременно связан с одной `MediaRecord`
- `MediaRegistry` управляет только `MediaRecord`; когда нужны физические операции, TapeManager сначала находит record в реестре, затем выполняет их через соответствующий экземпляр `TapeMedia`

---

## 5. Реализация первого этапа: Linux SCSI

### 5.1 ScsiTapeDrive

| Интерфейс | Реализация |
|------|------|
| узел устройства | `/dev/nst0` (без автоматической перемотки, рекомендуется) |
| чтение и запись данных | стандартные системные вызовы `read()` / `write()` |
| позиционирование | ioctl MTIO: MTFSF, MTBSF, MTFSR, MTBSR, MTREW, MTEOM |
| состояние | MTIOCGET |
| позиция | MTIOCPOS |
| FileMark | MTIOCTOP + MTWEOF |
| блочный режим | MTSETBLK задаёт размер блока (рекомендуется 256KB) |
| сжатие | MTCOMPRESSION (MTIOCTOP); MTSETDRVBUFFER + MT_ST_DEF_COMPRESSION задаёт значение по умолчанию (нужен root) |
| извлечение ленты | MTOFFL |

### 5.2 Обёртка ioctl MTIO (mtio.rs)

```rust
pub struct MtioHandle {
    fd: RawFd,
}

impl MtioHandle {
    pub fn open(device: &str) -> Result<Self>;

    // операции MTIOCTOP
    pub fn op(&self, op: MtOp, count: i32) -> Result<()>;

    // MTIOCGET
    pub fn get_status(&self) -> Result<MtGet>;

    // MTIOCPOS
    pub fn get_position(&self) -> Result<MtPos>;
}

pub enum MtOp {
    Reset, Fsf, Bsf, Fsr, Bsr, Rew, Offl, Nop, Reten, Eom, Erase,
    Weof,            // запись FileMark
    SetBlk(u32),     // задание размера блока
    SetCompression(bool),
}
```

### 5.3 ScsiTapeLibrary (реализация среднего этапа)

Основан на протоколе SCSI Medium Changer (SMC); команды SCSI отправляются через `/dev/sg*`.

| Команда SCSI | Назначение |
|-----------|------|
| INITIALIZE ELEMENT STATUS | инвентаризация |
| READ ELEMENT STATUS | чтение состояния слотов/накопителей |
| MOVE MEDIUM | перенос манипулятором (load/unload/transfer) |
| REQUEST VOLUME ELEMENT ADDRESS | чтение штрихкода |

На период разработки возможна также обёртка над утилитой командной строки `mtx`.

### 5.4 sg.rs (обёртка SCSI Generic, опционально)

```rust
pub struct SgHandle {
    fd: RawFd,
}

impl SgHandle {
    pub fn open(device: &str) -> Result<Self>;
    pub fn send_command(&self, cdb: &[u8], data: &mut [u8]) -> Result<SgResult>;
}
```

---

## 6. Управление офлайн-лентами

### 6.1 Процесс работы с офлайн-лентой

```
Планировщик восстановления запрашивает загрузку tape_id
    │
    ▼
TapeManager.check_media_online(tape_id)
    │
    ├─ Online: load_tape → продолжаем
    │
    └─ Offline:
         1. TapeManager.request_offline_media(tape_id, reason)
         2. сервис оповещений: отправляет заявку/алерт (tape_id, barcode, расположение, заявитель)
         3. задача переходит в состояние wait_for_media
         4. человек: вставляет ленту в библиотеку → подтверждает сканированием
         5. TapeManager.confirm_media_online(tape_id)
         6. автоматическое продолжение: load_tape → чтение
```

### 6.2 Структура запроса по офлайн-ленте

Подробные определения `OfflineRequest` / `OfflineRequestStatus` см. в §3.4.13.

---

## 7. Надёжность

### 7.1 Запись в две реплики

| Стратегия | Описание |
|------|------|
| реплика на двух лентах | при архивации пишется на две разные ленты, фиксируется tape_set |
| размещение по разным библиотекам | реплики распределяются по разным библиотекам или офлайн-хранилищам, чтобы исключить отказ одной точки |
| взаимодействие с планировщиком | решение о двойной записи принимает планировщик, TapeManager его выполняет |

### 7.2 Механизмы проверки

| Механизм | Запуск | Описание |
|------|------|------|
| CRC после записи | при каждой записи | уровень накопителя фиксирует CRC, сравнение после записи |
| периодическая проверка чтением | периодическая задача | `TapeManager.verify_media` читает и проверяет |
| проверка Bundle | после завершения архивации | опциональное чтение обратно и проверка всего Bundle |

### 7.3 Обработка ошибок

| Тип ошибки | Обработка |
|----------|------|
| аппаратная ошибка накопителя | пометить накопитель недоступным, переключиться на резервный |
| ошибка чтения носителя | повтор → переключение на реплику → оповещение человека |
| ошибка записи на носитель | повтор → смена ленты и продолжение записи → пометка ленты как неисправной |
| необходимость чистки | DriveError::CleaningRequired → оповещение |

### 7.4 Вывод носителя из эксплуатации

```
периодическая проверка → рост числа нечитаемых блоков → пометка Retired → перенос данных на новую ленту → утилизация
```

---

## 8. Наблюдаемость

| Метрика | Описание |
|------|------|
| drive_utilization | загрузка накопителей |
| write_throughput_mbps | пропускная способность записи |
| read_throughput_mbps | пропускная способность чтения |
| tape_swap_count | число перестановок ленты |
| tape_swap_duration_ms | длительность перестановки ленты |
| media_error_count | число ошибок носителя |
| drive_error_count | число ошибок накопителя |
| offline_request_count | число запросов по офлайн-лентам |
| offline_wait_duration_s | время ожидания офлайн-ленты |
| verify_pass_rate | доля успешных проверок |

---

## 9. Структура модуля

```
src/tape/
├── mod.rs                      # точка входа модуля, pub use
│
├── manager.rs                  # TapeManager — бизнес-API для планировщика
├── drive_allocator.rs          # DriveAllocator — эксклюзивная выдача накопителей и очередь
├── media_registry.rs           # MediaRegistry — реестр носителей, ёмкость, состояние, расположение
├── offline.rs                  # запросы по офлайн-лентам и процесс подтверждения
│
├── sdk/                        # ─── слой абстракции SDK (трейты) ───
│   ├── mod.rs
│   ├── drive.rs                # трейт TapeDrive
│   ├── media.rs                # трейт TapeMedia
│   ├── library.rs              # трейт TapeLibrary
│   └── types.rs                # DriveStatus, SlotInfo, MediaStatus, ...
│
├── scsi/                       # ─── реализация Linux SCSI ───
│   ├── mod.rs
│   ├── drive.rs                # ScsiTapeDrive (impl TapeDrive)
│   ├── media.rs                # ScsiTapeMedia (impl TapeMedia)
│   ├── library.rs              # ScsiTapeLibrary (impl TapeLibrary, средний этап)
│   ├── mtio.rs                 # обёртка ioctl MTIO (MtioHandle)
│   └── sg.rs                   # обёртка SCSI Generic (SgHandle, опционально)
│
└── vendor/                     # ─── реализация на SDK вендора (поздний этап) ───
    └── mod.rs                  # VendorTapeDrive, VendorTapeLibrary
```

---

## 10. Параметры конфигурации

```yaml
tape:
  sdk:
    backend: "scsi"            # scsi | vendor

  drives:
    - id: "drive0"
      device: "/dev/nst0"
      block_size: 262144       # 256KB
      compression: true
    - id: "drive1"
      device: "/dev/nst1"
      block_size: 262144
      compression: true

  library:
    enabled: false             # на первом этапе при одном накопителе можно выключить
    device: "/dev/sg2"         # устройство SCSI Medium Changer
    auto_inventory: true       # автоматическая инвентаризация при запуске

  media:
    supported_formats:
      - "LTO-9"
      - "LTO-10"
    replication_factor: 2      # две реплики
    verify_interval_days: 90   # проверка раз в 90 дней

  offline:
    notification_enabled: true
    notification_timeout_secs: 86400  # таймаут 24 ч
```

---

## 11. Модель развёртывания и зависимости

Слой лент работает на узлах **Tape Worker** (отдельные физические узлы с подключёнными ленточными накопителями и библиотеками)
и по gRPC принимает удалённые вызовы Scheduler Worker.

Слой лент — **чистая аппаратная абстракция**, MetadataClient он не хранит. Все обновления метаданных целиком относятся на слой планирования.

| Интегрируемая сторона | Направление | Протокол | Интерфейсы | Описание |
|--------|------|------|------|------|
| Scheduler Worker | gRPC (удалённо) | планирование → TapeManager | acquire_drive, sequential_write, seek_and_read | оркестрация слоем планирования |
| слой метаданных | **прямой зависимости нет** | — | — | TapeInfo и прочее обновляет Scheduler Worker |
| кластер Metadata | gRPC (heartbeat) | Tape Worker → Metadata | сообщение о состоянии накопителей и доступности |
| сервис оповещений | локально | TapeManager → оповещения | запросы по офлайн-лентам, алерты об ошибках |

---

## 12. Среда разработки и тестирования: mhVTL

В среде разработки и CI реальное ленточное оборудование не нужно: [mhVTL (Linux Virtual Tape Library)](https://github.com/markh794/mhvtl) эмулирует ленточные накопители и библиотеку целиком.

### 12.1 Обзор mhVTL

mhVTL эмулирует устройства лент SCSI с помощью модуля ядра (псевдо-HBA) и пользовательских демонов (`vtltape`, `vtllibrary`), публикуя стандартные узлы устройств `/dev/nst*`, `/dev/sg*`. Для реализационного слоя SCSI в ColdStore виртуальная лента и настоящая имеют **полностью одинаковый интерфейс**.

| Компонент mhVTL | Роль | Точка интеграции ColdStore |
|-----------|------|-----------------|
| модуль ядра `mhvtl.ko` | псевдо-SCSI HBA | публикует `/dev/nst*`, `/dev/sg*` |
| демон `vtltape` | цель SSC, чтение и запись ленты | ioctl mtio в `ScsiTapeDrive` |
| демон `vtllibrary` | цель SMC, управление библиотекой | SCSI Medium Changer в `ScsiTapeLibrary` |
| утилита `mktape` | создание виртуальной ленты | подготовка носителей перед тестами |

### 12.2 Пример настройки среды разработки

```bash
# установка mhVTL (на примере Ubuntu/Debian)
sudo apt-get install mhvtl-utils lsscsi sg3-utils

# запуск виртуальной ленточной библиотеки
sudo systemctl start mhvtl

# просмотр эмулируемых устройств
lsscsi -g
# [3:0:0:0]  mediumx  SPECTRA  PYTHON           ...  /dev/sch0  /dev/sg5
# [3:0:1:0]  tape     QUANTUM  ULTRIUM-HH7      ...  /dev/st0   /dev/sg6
# [3:0:2:0]  tape     QUANTUM  ULTRIUM-HH7      ...  /dev/st1   /dev/sg7
```

Конфигурация ColdStore (режим разработки):

```yaml
tape:
  sdk:
    backend: "scsi"
  drives:
    - id: "drive0"
      device: "/dev/nst0"     # виртуальный накопитель mhVTL
      block_size: 262144
    - id: "drive1"
      device: "/dev/nst1"     # виртуальный накопитель mhVTL
      block_size: 262144
  library:
    enabled: true
    device: "/dev/sg5"        # виртуальная библиотека mhVTL
```

### 12.3 Назначение в тестах

| Назначение | Описание |
|------|------|
| модульные тесты | проверка обёрток ioctl mtio, позиционирования по FileMark, поблочного чтения и записи |
| интеграционные тесты | сквозной путь: S3 PUT → планирование архивации → запись на ленту → восстановление → кэш → GET |
| впрыск отказов | остановка процесса vtltape как имитация отказа накопителя; проверка обработки ошибок и повторов |
| CI/CD | установка mhVTL в GitHub Actions / GitLab CI, автоматизированные сквозные тесты |
| проверка планирования | имитация нескольких накопителей и лент, проверка алгоритмов планирования: объединение, агрегация, приоритеты |

---

## 13. Путь реализации

| Этап | Реализация | Область | Тестовая среда |
|------|------|------|----------|
| **Phase 1** | ScsiTapeDrive + MtioHandle | чтение и запись одного накопителя, позиционирование, FileMark, состояние | mhVTL, один накопитель |
| **Phase 2** | TapeManager + DriveAllocator | выдача нескольких накопителей, эксклюзивность, MediaRegistry | mhVTL, несколько накопителей |
| **Phase 3** | ScsiTapeLibrary + SgHandle | инвентаризация библиотеки, load/unload, штрихкоды | mhVTL с библиотекой |
| **Phase 4** | процесс офлайн + оповещения | request_offline_media, confirm | mhVTL + ручное извлечение ленты |
| **Phase 5** | надёжность | две реплики, verify_media, вывод из эксплуатации | mhVTL, несколько лент |
| **Phase 6** | VendorTapeDrive (опционально) | интеграция с SDK вендора | реальное оборудование |

---

## 14. Справочные материалы

- [Linux st(4) - SCSI tape device](https://man7.org/linux/man-pages/man4/st.4.html)
- [Linux SCSI tape driver (kernel.org)](https://kernel.org/doc/Documentation/scsi/st.txt)
- [mtx - control SCSI media changer devices](https://manpages.ubuntu.com/manpages/jammy/man1/mtx.1.html)
- [SCSI T-10 SSC (Streaming Commands)](https://www.t10.org/)
- [SCSI T-10 SMC (Medium Changer Commands)](https://www.t10.org/)
- [mhVTL - Linux Virtual Tape Library](https://sites.google.com/site/linuxvtl2/) — виртуальная ленточная библиотека для разработки и тестирования
- [mhVTL GitHub](https://github.com/markh794/mhvtl) — исходный код mhVTL
