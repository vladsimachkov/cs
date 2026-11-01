# Правила сборки и тестирования

Авторитетные правила сборки, линтинга и тестирования ColdStore. Проверены на текущем дереве; если
вы меняете тулчейн, зависимость, влияющую на сборку, или цели `Makefile`, обновите этот файл и
`AGENTS.md` §3 в том же PR.

Область: только сборка/линтинг/тесты. Поведение рантайма и инварианты — в `AGENTS.md` §5–§6.

---

## 1. Два поддерживаемых окружения

| Окружение | Когда использовать | Точка входа |
|---|---|---|
| **Контейнер (Docker)** | По умолчанию. На хосте нет Rust-тулчейна либо хост менять нельзя. | `scripts/docker.sh` + `Dockerfile.dev` |
| **Локальный хост** | Rust-тулчейн и системные пакеты из §2.2 уже установлены. | `make …` / обычный `cargo …` |

Правило: **не ставьте системные пакеты и Rust-тулчейн на хост ради того, чтобы сборка прошла.**
Используйте контейнер. Если локальная сборка невозможна — так и напишите в PR вместо установки чего-либо.

---

## 2. Требования

### 2.1 Rust

- Тулчейн **зафиксирован в `rust-toolchain.toml`** (сейчас `1.91.1`), и этот файл — единственный
  источник истины. `scripts/docker.sh` читает из него `channel` и передаёт его как `--build-arg
  RUST_VERSION` при сборке образа; `.github/workflows/rust.yml` фиксирует ту же версию и содержит
  шаг, который падает, если они разойдутся. Компоненты `rustfmt` и `clippy` запрошены там же и
  проверяются при сборке образа.
- В `Cargo.toml` также объявлено `rust-version = "1.91"`. Это **MSRV** — самый старый тулчейн,
  которым всё ещё нужно уметь собирать, — а не тот, которым мы собираем. Поднимайте его только с
  обоснованием в PR.
- **`Cargo.lock` закоммичен.** Это workspace из бинарников, поэтому CI и каждый dev-контейнер
  должны разрешать один и тот же граф (openraft закреплён точно, `=0.10.0-alpha.37`). После
  изменения зависимостей обновляйте lock минимально — `scripts/docker.sh run cargo update -p <crate>
  [--precise <версия>]` либо просто сборкой — и кладите его в тот же PR. `cargo generate-lockfile`
  игнорирует существующий lock и переразрешает все пакеты заново; он для намеренного полного
  обновления. CI выполняет `cargo metadata --locked`, так что устаревший lock валит сборку.
- **`rust-toolchain.toml` перекрывает тег образа.** Контейнер, собранный с другим `RUST_VERSION`,
  молча доустановит и использует зафиксированный тулчейн (проверено: образ `1.99` скачал 1.91.1 и
  о нём же сообщил). Чтобы действительно проверить другую версию, задавайте `RUSTUP_TOOLCHAIN`;
  чтобы вместо этого получить громкую ошибку — `RUSTUP_AUTO_INSTALL=0`.

  ```bash
  docker build -f Dockerfile.dev --build-arg RUST_VERSION=1.99.1 -t coldstore-dev:1.99 .
  docker run --rm -e RUSTUP_TOOLCHAIN=1.99.1 -v "$PWD:/work" \
      -v coldstore-cargo-registry:/usr/local/cargo/registry \
      -v coldstore-cargo-git:/usr/local/cargo/git \
      -v coldstore-target-199:/work/target \
      coldstore-dev:1.99 cargo clippy --workspace --all-targets --all-features -- -D warnings
  ```

  Для каждой тулчейн-версии держите отдельный volume для `target`, иначе переключение сбрасывает
  артефакты другой версии. Обратите внимание: `bash -lc` внутри образа сбрасывает `PATH` и прячет
  `cargo`; используйте `bash -c`.

### 2.2 Системные пакеты (имена Debian/Ubuntu)

| Пакет | Кому нужен | Симптом отсутствия |
|---|---|---|
| `protobuf-compiler` | кодогенерация `tonic-build` в `crates/proto` | `protoc` не найден |
| `libprotobuf-dev` | разрешение `google/protobuf/timestamp.proto` в prost | `protoc failed: google/protobuf/timestamp.proto: File not found` → сборка `coldstore-proto` падает |
| `clang`, `libclang-dev` | `bindgen` для RocksDB | `bindgen` не находит `llvm-core`/`libclang.so` |
| `llvm-dev` | RocksDB (в CI тоже ставится) | ошибки на этапе линковки |

`libprotobuf-dev` **не** подтягивается зависимостью от `protobuf-compiler`. В CI всё проходило лишь
потому, что на раннерах GitHub `ubuntu-latest` этот пакет предустановлен; теперь он явно указан в
`.github/workflows/rust.yml` и в `Dockerfile.dev`.

### 2.3 Сеть и диск

- При первой загрузке cargo нужен доступ к crates.io. Сами тесты используют только loopback
  (`127.0.0.1:0`).
- Первая сборка компилирует RocksDB из C++: ≈1 мин на 32 ядрах, ≈10–30 мин на слабой машине.
  `target/` занимает ≈5 GB. Держите не менее 15 GB свободного места.
- `Cargo.lock` закоммичен (см. `AGENTS.md` §3.1), поэтому все окружения разрешают один и тот же
  граф. После изменения зависимостей обновите его минимально — `scripts/docker.sh run cargo update -p
  <crate>` (либо просто сборкой) — и положите в тот же PR; CI проверяет `cargo metadata --locked`.
  `cargo generate-lockfile` оставляйте для намеренного полного переразрешения, где просматривается
  каждая поднятая версия. `openraft` закреплён точно (`=0.10.0-alpha.37`), потому что
  пре-релизное требование без `=` «плывёт» между альфами.

---

## 3. Сборка в контейнере (по умолчанию)

### 3.1 Файлы

| Файл | Роль |
|---|---|
| `Dockerfile.dev` | `rust:` + канал из `rust-toolchain.toml` (сейчас `1.91.1`) + пакеты из §2.2 + `rustfmt`/`clippy`. Ничего не копирует из контекста. |
| `.dockerignore` | Держит контекст сборки пустым (`COPY` нет). |
| `scripts/docker.sh` | Обёртки: собирают образ при необходимости, затем запускают cargo внутри. |

### 3.2 Команды

```bash
scripts/docker.sh check-safe   # fmt-check → clippy -D warnings → lock → unit → vtl-unit → build → hook-test
scripts/docker.sh test         # cargo test --workspace
scripts/docker.sh build        # cargo build --workspace
scripts/docker.sh clippy       # cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/docker.sh fmt          # cargo fmt --all
scripts/docker.sh fmt-check    # cargo fmt --all -- --check
scripts/docker.sh vtl-unit     # cargo test -p coldstore-vtl --lib --tests
scripts/docker.sh lock-check   # cargo metadata --locked (lock против манифестов)
scripts/docker.sh hook-test    # поведение гейта ревью scripts/commit-msg (нужны только git + bash)
scripts/docker.sh shell        # bash в образе, /work = репозиторий
scripts/docker.sh run <cmd>    # что угодно, напр. run cargo tree -p coldstore-tape
scripts/docker.sh clean        # docker volume rm coldstore-target
```

Переопределяемые переменные: `IMAGE`, `DOCKERFILE`.

### 3.3 Монтирования и кэши

| Путь в контейнере | На хосте | Примечания |
|---|---|---|
| `/work` | репозиторий (bind mount) | чтение-запись |
| `/usr/local/cargo/registry` | volume `coldstore-cargo-registry` | кэш крейтов |
| `/usr/local/cargo/git` | volume `coldstore-cargo-git` | кэш git-зависимостей |
| `/work/target` | volume `coldstore-target` | артефакты сборки; перекрывает локальный `./target` |

Правила:

- **Не** монтируйте volume поверх всего `/usr/local/cargo` — шимы rustup для `cargo`/`rustc` лежат в
  `/usr/local/cargo/bin` и исчезнут.
- Кэши — именованные volumes, а не каталоги хоста: они переживают удаление контейнера и держат
  сборку инкрементальной. Никакого `cargo clean` внутри контейнера «чтобы починить сборку»; сначала
  пересоберите, а `scripts/docker.sh clean` используйте только при действительно повреждённом кэше.
- Контейнер работает от root, поэтому файлы, созданные через bind mount, принадлежат root. Docker
  создаёт пустой каталог `./target` с правами root как точку монтирования; он в git-ignored, и после
  последнего запуска его можно `rmdir`.
- Один контейнер одновременно на одно рабочее дерево: два параллельных cargo делят `coldstore-target`
  и блокируются на lock-файле package-кэша cargo.

### 3.4 Запуск ноды в контейнере

`coldstore-node` слушает `127.0.0.1` (gateway `:9000`, metadata `:21001`, scheduler `:22001`,
cache `:23001`, tape `:24001`), поэтому S3-клиент должен работать **в том же контейнере**. Запустите
бинарник в фоне, выполните curl из той же оболочки, затем остановите процесс и удалите
`COLDSTORE_DATA_DIR`. Smoke-поток из `AGENTS.md` §3.4 проверен именно так:
PUT bucket → PUT object → `COLD` → GET 403 → restore 202 → GET, байты совпадают.

Никогда не запускайте ноду от root против `/var/lib/coldstore`; всегда указывайте
`COLDSTORE_DATA_DIR` на одноразовый каталог в `/tmp`.

---

## 4. Локальная сборка

Те же команды, что и в CI, через `Makefile`:

```bash
make check-safe     # fmt-check, clippy, metadata-locked, unit, vtl-unit, build-debug, hook-test  ← гейт перед завершением
make check-all      # fmt-check, clippy, test
make vtl-unit
cargo test -p coldstore-scheduler archive_staging_batch   # один тест
```

`make install-tools` выполняет `rustup component add rustfmt clippy`; системные пакеты из §2.2 он
не ставит — установите их вручную или используйте контейнер.

---

## 5. Критерий готовности (часть про сборку)

Прогоняйте тот же гейт, что и CI (`.github/workflows/rust.yml`: пин тулчейна + проверка дрейфа →
`cargo metadata --locked` → fmt → clippy → build → test → `scripts/test-commit-msg.sh`):

```bash
scripts/docker.sh check-safe    # быстрый гейт
scripts/docker.sh test          # полный набор, 116 тестов
```

- [ ] `cargo fmt --all -- --check` чисто.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` чисто.
- [ ] `cargo build --workspace` успешно.
- [ ] `cargo test --workspace` проходит (на момент написания — 116 тестов); новое поведение и каждая
      исправленная ошибка покрыты тестами.
- [ ] `cargo metadata --locked` проходит; либо этот PR сам пересоздаёт `Cargo.lock`, и эта
      пересборка входит в просмотренный дифф.
- [ ] `scripts/test-commit-msg.sh` проходит (гейт ревью commit-msg; нужны только git + bash).
- [ ] Ни одной добавленной или поднятой зависимости без обоснования в PR (`AGENTS.md` §2.5).
- [ ] Хост не изменён, сервисы не оставлены запущенными, временные данные удалены.

---

## 6. Диагностика

| Симптом | Причина / решение |
|---|---|
| `google/protobuf/timestamp.proto: File not found` | Нет `libprotobuf-dev` (§2.2). |
| `protoc` не найден | Нет `protobuf-compiler`; в контейнере — пересобрать образ. |
| `bindgen: ... libclang.so: NotFound` | Нет `clang`/`libclang-dev`. |
| Чистая сборка падает без локальных изменений | Закоммиченный `Cargo.lock` устарел либо разрешён другим тулчейном. Обновите его минимально (`scripts/docker.sh run cargo update -p <crate>`) и посмотрите на diff; быстрее всех «плывёт» alpha `openraft`. |
| Cargo висит: `waiting for file lock on package cache` | Другой cargo (обычно второй контейнер) держит общий volume. Остановите его. |
| Сборка сильно замедлилась / `target` раздут | `docker volume rm coldstore-target` (`scripts/docker.sh clean`) и пересборка. |
| Clippy чист локально, но падает в CI | И CI, и контейнер используют канал из `rust-toolchain.toml`, так что расхождение означает: одно из мест в §2.1 уехало — сам файл либо две зафиксированные версии в workflow. Воспроизведите явным `docker build --build-arg RUST_VERSION=<версия>` плюс `-e RUSTUP_TOOLCHAIN=<версия>` (§2.1 — без переменной окружения файл тулчейна молча побеждает) и чините, а не отключайте линт оптом. Так исправлено для rustc 1.99: `result_large_err` на `tonic::Status` (allow на уровне crate, `AGENTS.md` §10) и `useless_borrows_in_formatting` в `scheduler/src/lib.rs`. |

---

## 7. Связанные документы

- `AGENTS.md` §3 — сжатые факты о тулчейне, команды, запуск, e2e smoke check.
- `AGENTS.md` §2, §6 — жёсткие правила и инварианты (не только про сборку).
- `Makefile` — цели для локальной сборки.
- `crates/vtl/README.md` — область VTL-стенда и границы безопасности live-mhVTL.
