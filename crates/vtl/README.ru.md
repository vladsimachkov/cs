# coldstore-vtl

Харнес виртуальной ленточной библиотеки для ColdStore. Задача этого crate — не автоматически устанавливать или запускать mhVTL на машине разработчика, а сначала зафиксировать границы, необходимые для дальнейшей разработки замкнутого цикла TapeService:

- Безопасные модульные тесты: без выполнения системных команд, без загрузки модулей ядра, без создания `/dev/st*` / `/dev/sg*` / `/dev/sch*`.
- Обёртки команд: стабильно публикуют построение команд `lsscsi`, `mtx`, `mt`, `sg3_utils`, `dd`, `vtlcmd`.
- Задел под реальный бэкенд: после установки mhVTL в изолированной ВМ или на выделенной тестовой машине те же команды выполняются через `SystemCommandRunner`.
- Имитатор в памяти: предоставляет поведение slot/barcode/load/unload/rewind/filemark/read/write для medium changer + tape drive, чтобы его использовали модульные тесты phase-1/phase-2.

## Слоистая структура модулей

| Модуль | Назначение | Воздействует на хост-систему |
|---|---|---|
| `model` | модели `TapeBarcode`, `ElementAddress`, `VirtualTape`, filemark/cursor | нет |
| `interface` | трейты `MediumChanger`, `TapeDrive`, `TapeInventory` | нет |
| `simulator` | чистый VTL в памяти: имитация slot, drive, barcode, load/unload, rewind, filemark, read/write | нет |
| `discover` | разбор вывода `lsscsi -g` в `ScsiInventory` | нет |
| `command` | `CommandSpec`, `CommandRunner`, `SystemCommandRunner`, `RecordedCommandRunner` | выполняет только `SystemCommandRunner` |
| `mhvtl` | построение команд инструмента mhVTL/live и опциональная точка выполнения | построение команд не воздействует; выполнение требует явного runner |

## Безопасная проверка

```bash
cargo test -p coldstore-vtl --lib --tests
```

Эта команда запускает только модульные тесты: она не устанавливает системные утилиты и не обращается к реальному оборудованию.

## Границы использования live mhVTL в дальнейшем

Live-окружение необходимо разворачивать в выделенной ВМ или на тестовой машине; выполнять его напрямую на текущей хост-машине разработчика не рекомендуется:

1. Перед выполнением вручную прочитайте `scripts/setup-mhvtl-env.sh`.
2. Запускайте только в выделенной ВМ:
   ```bash
   scripts/setup-mhvtl-env.sh --execute --start-services
   ```
3. Проверьте обнаружение устройств:
   ```bash
   lsscsi -g
   ```
4. Формируйте команды через `coldstore-vtl::mhvtl::MhvtlToolchain`, а решение об их выполнении принимает вышестоящий харнес тестов.

## Уже покрытые возможности

- Разбор образца вывода `lsscsi -g`: распознавание `mediumx` / `tape`, извлечение HCTL, vendor, product, revision, `/dev/schX`, `/dev/stX`, `/dev/nstX`, `/dev/sgX`.
- Обёртки команд `mtx`: status, transfer, load, unload.
- Обёртки команд `mt`: status, rewind, offline, weof, fsf.
- Обёртки команд `sg3_utils`: sg_inq, sg_turs, sg_logs, sg_modes.
- Обёртки команд `dd`: запись/чтение данных на ленточное устройство.
- VTL в памяти: установка картриджа в slot, load/unload, чтение и запись drive, rewind, переход по filemark.
