# Build and test rules

Authoritative rules for building, linting and testing ColdStore. Verified against the working
tree; if you change the toolchain, a dependency that affects the build, or `Makefile` targets,
update this file and `AGENTS.md` §3 in the same PR.

Scope: build/test/lint only. Runtime behaviour and invariants live in `AGENTS.md` §5–§6.

---

## 1. Two supported environments

| Environment | Use when | Entry point |
|---|---|---|
| **Container (Docker)** | Default. The host has no Rust toolchain, or you must not modify the host at all. | `scripts/docker.sh` + `Dockerfile.dev` |
| **Native host** | You already have a working Rust toolchain and the system packages below. | `make …` / plain `cargo …` |

Rule: **never install a system package or a Rust toolchain on the host just to make a build pass.**
Use the container. If a native build is impossible, say so in the PR instead of installing things.

---

## 2. Requirements

### 2.1 Rust

- Stable toolchain. Verified with **1.91**; the "1.75+" in `README_RUST.md` is stale — current
  dependency versions need a newer compiler. Edition 2021, no `rust-toolchain` file.
- Components: `rustfmt`, `clippy`.
- The dev image pins the toolchain (`ARG RUST_VERSION`, default 1.91). To check the workspace
  against a newer stable before CI floats to it:

  ```bash
  docker build -f Dockerfile.dev --build-arg RUST_VERSION=1.99 -t coldstore-dev:1.99 .
  docker run --rm -v "$PWD:/work" -v coldstore-cargo-registry:/usr/local/cargo/registry \
      -v coldstore-cargo-git:/usr/local/cargo/git -v coldstore-target-199:/work/target \
      coldstore-dev:1.99 cargo clippy --workspace --all-targets -- -D warnings
  ```

  Use a separate `target` volume per toolchain, otherwise each switch invalidates the other's
  artifacts. Note `bash -lc` inside the image resets `PATH` and hides `cargo`; use `bash -c`.

### 2.2 System packages (Debian/Ubuntu names)

| Package | Needed by | Symptom when missing |
|---|---|---|
| `protobuf-compiler` | `tonic-build` codegen in `crates/proto` | `protoc` not found |
| `libprotobuf-dev` | prost resolving `google/protobuf/timestamp.proto` | `protoc failed: google/protobuf/timestamp.proto: File not found` → `coldstore-proto` build fails |
| `clang`, `libclang-dev` | RocksDB `bindgen` | `bindgen` cannot find `llvm-core`/`libclang.so` |
| `llvm-dev` | RocksDB (CI installs it too) | link-stage failures |

`libprotobuf-dev` is **not** pulled in by `protobuf-compiler`. CI passed without it only because
GitHub `ubuntu-latest` runners preinstall it; it is now listed explicitly in
`.github/workflows/rust.yml` and in `Dockerfile.dev`.

### 2.3 Network and disk

- Cargo needs crates.io on first fetch. Tests themselves only use loopback (`127.0.0.1:0`).
- First build compiles RocksDB from C++: ≈1 min on 32 cores, ≈10–30 min on a small machine.
  `target/` reaches ≈5 GB. Keep ≥15 GB free.
- `Cargo.lock` is git-ignored (§`AGENTS.md` 3.1): versions float on every fresh resolve. If a clean
  build breaks without your change, suspect upstream drift first — `openraft` is a pre-release pin
  (`0.10.0-alpha.18`). Do not start committing the lock file unless maintainers ask.

---

## 3. Container build (default)

### 3.1 Files

| File | Role |
|---|---|
| `Dockerfile.dev` | `rust:1.91` + §2.2 packages + `rustfmt`/`clippy`. Copies nothing from the context. |
| `.dockerignore` | Keeps the build context empty (nothing is `COPY`ed). |
| `scripts/docker.sh` | Wrappers that build the image if needed, then run cargo in it. |

### 3.2 Commands

```bash
scripts/docker.sh check-safe   # fmt-check → clippy -D warnings → unit → vtl-unit → build → hook-test
scripts/docker.sh test         # cargo test --workspace
scripts/docker.sh build        # cargo build --workspace
scripts/docker.sh clippy       # cargo clippy --workspace --all-targets -- -D warnings
scripts/docker.sh fmt          # cargo fmt --all
scripts/docker.sh fmt-check    # cargo fmt --all -- --check
scripts/docker.sh vtl-unit     # cargo test -p coldstore-vtl --lib --tests
scripts/docker.sh hook-test    # behavior of the scripts/commit-msg review gate (git + bash only)
scripts/docker.sh shell        # bash in the image, /work = repo
scripts/docker.sh run <cmd>    # anything else, e.g. run cargo tree -p coldstore-tape
scripts/docker.sh clean        # docker volume rm coldstore-target
```

Overridable env: `IMAGE`, `DOCKERFILE`.

### 3.3 Mounts and caches

| Path in container | Host | Notes |
|---|---|---|
| `/work` | repo (bind mount) | read-write |
| `/usr/local/cargo/registry` | volume `coldstore-cargo-registry` | crate cache |
| `/usr/local/cargo/git` | volume `coldstore-cargo-git` | git dependency cache |
| `/work/target` | volume `coldstore-target` | build output; shadows any host `./target` |

Rules:

- Do **not** mount a volume over all of `/usr/local/cargo` — the rustup shims for `cargo`/`rustc`
  live in `/usr/local/cargo/bin` and would disappear.
- Caches are named volumes, not host directories: they survive container removal and keep runs
  incremental. Never `cargo clean` inside the container to "fix" a build; rebuild first, and use
  `scripts/docker.sh clean` only for a genuinely corrupt cache.
- The container runs as root, so files created through the bind mount are root-owned. Docker creates
  an empty root-owned `./target` as the mount point; it is git-ignored and safe to `rmdir` after the
  last run.
- One container at a time per working tree: two concurrent cargo runs share `coldstore-target` and
  block on the cargo package cache lock.

### 3.4 Running the node in a container

`coldstore-node` binds `127.0.0.1` (gateway `:9000`, metadata `:21001`, scheduler `:22001`,
cache `:23001`, tape `:24001`), so an S3 client must run **inside the same container**. Start the
binary in the background, curl from the same shell, then kill it and delete `COLDSTORE_DATA_DIR`.
The §3.4 smoke flow in `AGENTS.md` was verified this way:
PUT bucket → PUT object → `COLD` → GET 403 → restore 202 → GET bytes identical.

Never run the node as root against `/var/lib/coldstore`; always point `COLDSTORE_DATA_DIR` at a
throwaway directory under `/tmp`.

---

## 4. Native build

Same commands as CI, via the `Makefile`:

```bash
make check-safe     # fmt-check, clippy, unit, vtl-unit, build-debug, hook-test  ← pre-finish gate
make check-all      # fmt-check, clippy, test
make vtl-unit
cargo test -p coldstore-scheduler archive_staging_batch   # one test
```

`make install-tools` runs `rustup component add rustfmt clippy`; it does not install the §2.2
packages — install those manually or use the container.

---

## 5. Definition of done (build side)

Run the same gate CI runs (`.github/workflows/rust.yml`: fmt check → clippy → build → test →
`scripts/test-commit-msg.sh`):

```bash
scripts/docker.sh check-safe    # fast gate
scripts/docker.sh test          # full suite, 116 tests
```

- [ ] `cargo fmt --all -- --check` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo build --workspace` succeeds.
- [ ] `cargo test --workspace` passes (116 tests at the time of writing); new behavior and every
      fixed defect have tests.
- [ ] `scripts/test-commit-msg.sh` passes (the commit-msg review gate; needs git + bash only).
- [ ] No dependency added or bumped without justification in the PR (`AGENTS.md` §2.5).
- [ ] No host modifications, no services left running, temp data removed.

---

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `google/protobuf/timestamp.proto: File not found` | Missing `libprotobuf-dev` (§2.2). |
| `protoc` not found / `PROTOCOL_BUFFERS_PYTHON...` | Missing `protobuf-compiler`; in the container rebuild the image. |
| `bindgen: ... libclang.so: NotFound` | Missing `clang`/`libclang-dev`. |
| Build fails on a clean checkout with no local change | `Cargo.lock` is ignored → fresh resolve picked up a new upstream version, `openraft` alpha especially. Pin locally, report it; do not commit the lock file silently. |
| Cargo hangs with `waiting for file lock on package cache` | Another cargo (usually a second container) holds the shared volume. Stop it. |
| Build got much slower / `target` huge | `docker volume rm coldstore-target` (`scripts/docker.sh clean`) and rebuild. |
| Clippy is clean locally but fails in CI | CI installs `stable`, which floats; the container pins 1.91. Reproduce with `--build-arg RUST_VERSION=<ci version>` (§2.1) and fix, do not disable the lint wholesale. Fixed this way for rustc 1.99: `result_large_err` on `tonic::Status` (crate-wide allow, `AGENTS.md` §10) and `useless_borrows_in_formatting` in `scheduler/src/lib.rs`. |

---

## 7. Related documents

- `AGENTS.md` §3 — condensed toolchain facts, commands, running, e2e smoke check.
- `AGENTS.md` §2, §6 — hard rules and invariants (not build-specific).
- `Makefile` — native targets.
- `crates/vtl/README.md` — VTL harness scope and live-mhVTL safety boundary.
