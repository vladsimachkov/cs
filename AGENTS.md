# AGENTS.md — ColdStore

Operating manual for AI coding agents (Codex, Claude Code, Cursor, Copilot, Hermes, …) and for humans
working in this repository. Read it fully before changing code.

Last verified against commit `cc1a5db` plus the PL-06a working state (committed `Cargo.lock`, pinned
toolchain): the workspace builds, `cargo test --workspace` passes (116 tests), and the e2e smoke flow in §3.4 was run by hand.
If you change anything this file states as fact, update this file in the same PR.

---

## 1. Read this first: vision vs. reality

`README.md` / `README_ZH.md` describe the **target product** (S3-compatible tape HSM with LTO-9/10,
Raft metadata, bundle aggregation, dual copy / EC, offline-tape workflows, ≥300 MB/s).
`README_RUST.md` describes the **current code** and is authoritative for status.
`docs/` (ARCHITECTURE, DESIGN, modules/01–09) is design intent, not implementation.

**Never assume a README feature exists. Grep for it.** What actually exists today:

| Capability | State | Where |
|---|---|---|
| S3 path-style: bucket CRUD, Put/Get/Head/Delete object, ListObjects **v1**, single `Range`, `If-Match`/`If-None-Match`/`If-(Un)Modified-Since`, `POST ?restore`, `x-amz-restore` | ✅ | `crates/gateway/src/router.rs`, `s3/*.rs`, `protocol.rs` |
| SigV4 auth, multipart upload, ListObjectsV2, virtual-host style, `x-amz-meta-*`, `aws-chunked` decoding, CopyObject, versioning | ❌ absent (no auth at all: every request is accepted) | — |
| PUT → cache staging → background archive to tape → `COLD` | ✅ but **1 object = 1 bundle = 1 filemark**, single tape and drive from config | `scheduler/src/archive.rs::archive_staging_batch` |
| Bundle aggregation, dual-tape copy, EC | ❌ (`tape_set` field and aggregation config exist, unused) | — |
| Restore queue: sort tier → tape → offset, SHA-256 verify, write to restored cache with TTL | ✅ | `recall_pending_batch`, `process_recall_task` |
| Offline-tape notification, operator confirmation API, merge by `archive_id` | ❌ (only `Unavailable` → `WaitingForMedia` → retry next tick) | — |
| Cache: HDD backend, LRU/LFU, TTL, staging/restored budgets, watermarks, index rebuild on restart | ✅ most mature part | `crates/cache/src/service.rs`, `hdd.rs` |
| SPDK cache backend | ⚠️ facade that delegates to the HDD backend | `crates/cache/src/spdk.rs` |
| Tape worker | ⚠️ **in-memory simulator only, not persistent** | `crates/tape/src/service.rs::SimulatorTapeBackend` |
| Real drives / LTFS / SCSI / LTO-9/10 | ❌; `coldstore-vtl` has mhVTL command builders (`mtx`, `mt`, `sg_*`, `dd`) not wired into `TapeService` | `crates/vtl/` |
| Metadata | ⚠️ single node: in-memory state machine + snapshot + local RocksDB log. OpenRaft is **not running** (only `BasicNode` type is used); multi-node configs are rejected at startup | `crates/metadata/src/` |
| Metrics, WORM/retention, admin console, notifications | ❌ (logs via `tracing` only) | — |

---

## 2. Hard rules

1. **Never touch real hardware.** Do not run `scripts/setup-mhvtl-env.sh --execute`, and do not run
   `mtx`, `mt`, `sg_*`, `dd`, or `vtlcmd` against `/dev/st*`, `/dev/nst*`, `/dev/sg*`, `/dev/sch*`.
   `coldstore_vtl::command::SystemCommandRunner` is for a dedicated VM and only on explicit human request.
   In tests use `SimulatorTapeBackend` or `RecordedCommandRunner`.
2. **Don't leave services running.** Start `coldstore-node` only when an e2e check is needed. Use a
   throwaway `COLDSTORE_DATA_DIR` under `/tmp`, then stop the process and delete the directory.
   The standalone binaries default to `/var/lib/coldstore/...`; don't run them as root to "make it work".
3. **Don't weaken safety checks to make tests pass.** This covers SHA-256 verification, size checks,
   `expected_*` CAS preconditions in `CompleteArchiveObject`, and `validate_restore_transition`.
   A failing test means the logic is wrong.
4. **Don't silently break string contracts** (§6.5). They are parsed across crate boundaries.
5. **Don't edit generated code.** Change `crates/proto/proto/*.proto`; `tonic-build` regenerates on build.
6. **Don't add or bump dependencies casually.** `openraft` is pinned **exactly** to
   `=0.10.0-alpha.37` (a bare `"0.10.0-alpha.18"` was a floating caret range that admitted newer
   alphas silently — see `crates/metadata/Cargo.toml`), and `rocksdb` compiles C++ for minutes.
   `Cargo.lock` is committed (§3.1), so any bump changes the lock too: regenerate it, stage both, and
   justify the change in the PR.
7. **Don't claim features.** When you implement a README capability, update the table in
   `README_RUST.md` (and §1 here) in the same PR. When you can't finish one, say so in the PR.
8. Tests must not use the network beyond loopback and must not need root, devices, or external services.
9. **Every code change is reviewed by an independent reviewer agent before it is committed or pushed.**
   - **Unit of review.** The whole change as it will land on `main`: the cumulative branch diff against
     the merge base, the working-tree diff, **and the full contents of every untracked path** — not just
     their names (`git status --porcelain` prints names only; use `git add -N .` first, §7.5). Reviewing
     one commit is not enough — PRs are squash-merged (§11), so no agent commit is the final one. A review
     is invalid if any untracked or modified path was hidden from the reviewer.
   - **What counts as a code change.** Anything under `crates/` (including `crates/proto/proto/*.proto`),
     `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `Makefile`, `Dockerfile*`, `scripts/`,
     `configs/`, `.github/`, `.gitignore`. A commit touching only `*.md` is exempt — unless it edits
     `AGENTS.md` or `docs/BUILD*.md`, which define how code is built and reviewed. Anything unlisted
     and not `*.md` counts as a code change unless a human classifies it otherwise.
   - **Who may review.** A different agent session that shares no conversation with the author. It is
     given the review artifact (§7.5), §2/§6 of this file, and the author's recorded command and test
     output — and none of the author's conversation history. The isolation is about the author's
     reasoning, not about withholding the repository: the reviewer may read any file in the tree, since
     it has to judge docs-vs-reality and test quality. The agent that wrote the change may not review
     it. Human-authored changes are reviewed on the same terms.
   - **When.** After the pre-finish gate is green (`scripts/docker.sh check-safe`, §3.5; `make check-safe`,
     §3.2) and before `git commit` or `git push`. Any edit after a review invalidates it — re-review.
   - **What the reviewer checks.** The §2 hard rules; the §6 invariants; that new behavior has tests,
     that they are meaningful, and that they match the author's recorded evidence that they fail without
     the change (the reviewer runs nothing and edits nothing — the author puts the command and its output
     in the PR); string contracts and the Status→S3 mapping; proto/snapshot compatibility (§6.7–6.8);
     whether the docs still match reality. Findings are `file:line` + why.
   - **Verdict.** Blocking findings are fixed and re-reviewed; commit only when the reviewer reports
     none. The PR carries the reviewer's report verbatim (§11) and the commit carries the
     `Reviewed-by:` / `Review-scope:` trailers (§7.5). `scripts/commit-msg` rejects code-path commits
     without them, but only where it is installed (`make install-hooks`). It is a reminder, not a
     substitute for the review. Git does not invoke `commit-msg` at all for `git cherry-pick`,
     `git revert` or `git am`; `--amend` of a message alone and `--allow-empty` reach it with an empty
     staged diff, `--no-verify` skips it and `core.hooksPath` redirects it elsewhere; and nothing is
     checked inside a merge commit — not a conflict resolution, nor any other file staged into it.
   - **Waiver.** Only on explicit human request, quoted verbatim in the PR and as a `Waived-by:` trailer
     with a non-empty value. Never available for changes touching §6.7–6.8 formats, §2.3 safety checks,
     `.github/`, or this rule. The hook enforces only part of the path-detectable floor (`.github/`,
     `crates/proto/proto/`, `AGENTS.md`, `docs/BUILD*.md`, `scripts/commit-msg`,
     `scripts/test-commit-msg.sh`); the rest is on the reviewer and the PR.

---

## 3. Environment and commands

### 3.1 Toolchain

- **Rust: pinned.** `rust-toolchain.toml` is the source of truth (`channel = "1.91.1"`, components
  `rustfmt`, `clippy`). `scripts/docker.sh` reads that channel and passes it to the image build, so
  the container follows the file automatically. **A bump is not one edit:** the version is also
  hard-coded twice in `.github/workflows/rust.yml` (`uses: dtolnay/rust-toolchain@<version>` and the
  `RUSTUP_TOOLCHAIN` of the drift step). The drift step fails when that `RUSTUP_TOOLCHAIN` and
  `channel` disagree; a `uses:`-only edit is inert, because the toolchain file governs every other
  step. `rust-version = "1.91"` in the workspace is the MSRV, a different thing. Edition 2021.
- **System packages:** `protobuf-compiler` (protoc), `libprotobuf-dev` (ships
  `/usr/include/google/protobuf/*.proto`; without it prost cannot resolve
  `google/protobuf/timestamp.proto` and `coldstore-proto` fails to build), `clang`,
  `libclang-dev`, `llvm-dev` (RocksDB via bindgen).
- **Build rules:** full build/test/lint policy in [`docs/BUILD.md`](docs/BUILD.md)
  (Russian: [`docs/BUILD.ru.md`](docs/BUILD.ru.md)). §3.5 is the short version.
- **Components:** `rustfmt`, `clippy`.
- **First build is slow.** RocksDB is compiled from C++ source (≈10–30 min on a small machine).
  Incremental builds are fast afterwards. Don't `cargo clean` without reason.
- **`Cargo.lock` is committed** (decision at PL-06a, 2026-11): a binaries workspace must resolve one
  graph everywhere, and openraft is pinned exactly (`=0.10.0-alpha.37`) for the same reason. After a
  dependency change, refresh the lock **minimally**: `scripts/docker.sh run cargo update -p <crate>
  [--precise <version>]`, or simply build — cargo patches only what the manifest change requires —
  then commit the lock in the same PR. `cargo generate-lockfile` ignores the existing lock and
  re-resolves every package to the newest compatible version; use it only for a deliberate full
  refresh, and review the whole diff. CI and `check-safe` run `cargo metadata --locked`, so a stale
  lock fails the build.

### 3.2 Everyday commands

| Purpose | Command |
|---|---|
| Format | `cargo fmt --all` |
| Lint (same as CI) | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| All tests | `cargo test --workspace` (116 tests, ~1 s runtime once built) |
| One crate / one test | `cargo test -p coldstore-scheduler archive_staging_batch` |
| VTL harness only | `make vtl-unit` |
| Default pre-finish gate | `make check-safe` (fmt-check, clippy, `metadata-locked`, unit, vtl-unit, build-debug, hook-test) |
| What CI runs | toolchain pin + drift check against `rust-toolchain.toml` → `cargo metadata --locked` → fmt check → clippy → `cargo build --workspace` → `cargo test --workspace` → `scripts/test-commit-msg.sh` (`.github/workflows/rust.yml`; the same lock and lint checks also run in `scripts/docker.sh check-safe`, which is the authoritative gate while Actions are off — disabled by the maintainer, 2026-11) |

Tests spin up in-process tonic servers on `127.0.0.1:0` and use temp dirs. They need nothing external.

### 3.3 Running

Use the all-in-one runtime. It is the only binary that reads a config file:

```bash
COLDSTORE_DATA_DIR=/tmp/cs cargo run -p coldstore-node                  # with simulated tape
cargo run -p coldstore-node -- --config configs/single-node-no-tape.yaml # no tape: archive/recall off
# env: COLDSTORE_CONFIG=<yaml>, COLDSTORE_DATA_DIR=<dir>, COLDSTORE_NO_TAPE=1 (or --no-tape)
```

Fixed ports in `crates/node/src/main.rs`:

| Component | Address |
|---|---|
| S3 gateway | `127.0.0.1:9000` |
| metadata | `:21001` |
| scheduler | `:22001` |
| cache | `:23001` |
| tape | `:24001` |

The node sets the archive and recall scan intervals to 2 s.

The standalone `coldstore-{metadata,gateway,scheduler,cache,tape}` binaries ignore config files
(`Config::default()`, see the `TODO` in `metadata/src/main.rs`).

### 3.4 E2E smoke check (verified)

```bash
GW=http://127.0.0.1:9000
curl -X PUT $GW/bkt                                                # 200
curl -X PUT --data-binary @small.bin $GW/bkt/obj                   # 200; must be < 2 MiB, see §8
sleep 5; curl $GW/bkt | grep -o '<StorageClass>[^<]*'              # COLD (was COLD_PENDING)
curl -i $GW/bkt/obj                                                # 403 InvalidObjectState
curl -X POST "$GW/bkt/obj?restore" -d '<RestoreRequest><Days>1</Days></RestoreRequest>'  # 202
sleep 5; curl -o out.bin $GW/bkt/obj && cmp small.bin out.bin      # 200, identical
```

The gateway does not verify signatures, so any S3 client works with dummy credentials and
path-style addressing.

### 3.5 Containerized build and test (Docker)

Full policy, mounts, cache rules and troubleshooting: **[`docs/BUILD.md`](docs/BUILD.md)**
(`docs/BUILD.ru.md` in Russian). Short version:

- `Dockerfile.dev` = `rust:` + the `rust-toolchain.toml` channel (currently `1.91.1`) + the §3.1
  system packages + `rustfmt`/`clippy`; `scripts/docker.sh`
  wraps it. Use this instead of installing anything on the host.
- `scripts/docker.sh check-safe | test | build | clippy | fmt | fmt-check | vtl-unit | lock-check | hook-test | shell | run <cmd> | clean`.
- Repo at `/work`; `cargo registry`/`git` and `target/` are named volumes, so runs stay incremental.
  Cold `cargo build --workspace` ≈1 min on 32 cores, `target` ≈5 GB.
- The node binds `127.0.0.1` inside the container, so the §3.4 smoke check runs *inside* one
  container. Verified: PUT bucket/object → `COLD` → GET 403 → restore 202 → GET bytes identical.

---

## 4. Repository map

| Crate | Kind | Role | Key files |
|---|---|---|---|
| `coldstore-proto` | lib | gRPC/protobuf contracts | `proto/{common,metadata,scheduler,cache,tape}.proto`, `build.rs` |
| `coldstore-common` | lib | config structs and defaults, shared models/errors | `src/config.rs`, `models.rs`, `error.rs` |
| `coldstore-gateway` | bin+lib | S3 HTTP (axum 0.7) → scheduler gRPC | `router.rs` (route table), `s3/` (one module per operation family: `bucket`, `object`, `list`, `restore`, `range`, `preconditions`, `headers`, `errors`, `xml`), `protocol.rs` (restore XML, error codes), `lib.rs` (`GatewayBackend`, gRPC client) |
| `coldstore-scheduler` | bin+lib | business core: S3 ops, archive loop, recall loop | `service.rs` (gRPC surface, the `Phase1SchedulerBackend` trait, background loops), `s3_ops.rs` (bucket/object CRUD, restore, restored-cache read), `archive.rs` (staging hand-off, archive task lifecycle, archive loop), `recall.rs` (recall task lifecycle, tape→cache restore, recall loop), `test_util.rs` (shared `#[cfg(test)]` fixtures), `clients/{cache,tape}.rs` (cache/tape traits + gRPC clients), `leases.rs` (item/task locks, tape lease, `capacity_reject` parsing), `responses.rs` (proto response builders), `util.rs` |
| `coldstore-metadata` | bin+lib | metadata state and persistence | `state_machine.rs` (**all mutations**), `command.rs`, `service.rs` (gRPC), `raft.rs`, `raft_storage.rs` (RocksDB) |
| `coldstore-cache` | bin+lib | staging + restored cache | `service.rs` (budgets, eviction, TTL), `hdd.rs`, `backend.rs`, `spdk.rs` |
| `coldstore-tape` | bin+lib | `TapeService` over the `TapeBackend` trait | `service.rs` (simulator backend), `tests/simulator_service.rs` |
| `coldstore-vtl` | lib | virtual library model, mhVTL/SCSI command builders, `lsscsi` parser | `simulator.rs`, `mhvtl.rs`, `command.rs`, `discover.rs`, `README.md` |
| `coldstore-node` | bin | single-process runtime running all components | `src/main.rs` |

Other paths:
- `configs/`: node YAMLs.
- `scripts/pre-commit`: lock/fmt/clippy/check/test gate; it skips when no host cargo is present (§3.5 is
  the sanctioned build). `scripts/commit-msg`: cargo-free review gate for §2.9, its behavior pinned by
  `scripts/test-commit-msg.sh` (`make hook-test`). Install the hooks with `make install-hooks`, and
  re-run it after any change to `scripts/commit-msg` — the hook is a copy and nothing detects a stale
  installed one.
- `scripts/setup-mhvtl-env.sh`: dry-run by default; VM only.

---

## 5. Data flow (where the code lives)

**PutObject**
1. `gateway::s3::object::put_object` buffers the body.
2. `scheduler` `MetadataBackedSchedulerBackend::put_object` computes SHA-256 (also used as the ETag).
3. Cache `put_staging` stores the bytes.
4. Metadata `PutObject` records the object with `COLD_PENDING`. If the metadata write fails, staging is
   deleted best-effort.

**Archive** (`run_archive_agent` → `archive_staging_once` → `archive_staging_batch`):
1. List staging keys and acquire the in-process item lock.
2. `head_object`, then claim the archive task.
3. `get_staging` and validate it against metadata.
4. `tape.write_bundle` (acquire drive, load tape).
5. Re-read metadata and require that it is unchanged.
6. `CompleteArchiveObject`, a CAS on `expected_size/checksum/storage_class/updated_at` that sets `COLD`
   and stores the bundle.
7. `delete_staging`, then finalize the task.

**Ordering is a safety invariant:** staging is deleted only after the metadata commit.

**RestoreObject** (`restore_object`):
- Only allowed for `COLD`.
- `Expedited` → `Unavailable`.
- Already `Completed` → 200; already in flight → 409.
- Otherwise: set restore status to `Pending` and put a `RecallTask` → 202.

**Recall** (`run_recall_agent` → `recall_pending_batch` → `process_recall_task`):
1. Sort tasks, take the tape lease, claim the task.
2. Read by filemark.
3. Verify size and SHA-256.
4. Cache `put_restored` with `expire_at`.
5. Task and object restore status → `Completed`.

Retriable errors (`Unavailable`, capacity rejects) move the task to `WaitingForMedia`. All other
errors → `Failed`.

**GetObject**
- Requires `COLD` and restore status `Completed`. Otherwise `FailedPrecondition`, which becomes
  S3 `InvalidObjectState` (403).
- Reads the full object from the restored cache. `Range` is sliced in the gateway after the full read.

**DeleteObject**
- Removes metadata and any cached copy, best-effort.
- Tape space is never reclaimed.

---

## 6. Invariants and conventions you must preserve

1. **Single mutation path.** Every metadata change is a `MetadataCommand` variant applied by
   `state_machine::apply_command`. `service.rs` must not mutate `MetadataState` directly.
2. **State transitions are validated centrally.**
   - Restore: `validate_restore_transition`.
   - Archive completion: `validate_complete_archive_preconditions`.
   - Extend these functions and their tests rather than bypassing them.
3. **Integrity.**
   - The checksum is the hex SHA-256 of the whole object, and it doubles as the ETag.
   - It is verified when staging is read for archive and after every tape read.
   - Byte counts are checked at every hop (`write.bytes_written`, cache size vs metadata size).
4. **Locks and leases** live in `SchedulerState`: archive item keys, recall task keys, the per-tape
   lease semaphore, and concurrency slots. Every early return must release what it acquired; follow the
   existing pattern, which releases explicitly on each error branch.
5. **String contracts across crates.** Change both sides plus tests, or don't change them.
   - `capacity_reject:<reason>:<detail>` is emitted by cache (`capacity_reject_error`). The scheduler
     (`capacity_reject_reason`, `is_retriable_recall_processing_error`) and the gateway
     (`cache_reject_status_hint`) match on the reason codes.
   - The gateway matches the message `glacier expedited retrieval is not available` to produce
     `GlacierExpeditedRetrievalNotAvailable`.
6. **gRPC `Status` codes are API.** The gateway maps them in `grpc_status_to_s3_response`:

   | `Status` | S3 error | HTTP |
   |---|---|---|
   | `NotFound` | NoSuchKey / NoSuchBucket | 404 |
   | `FailedPrecondition` | InvalidObjectState | 403 |
   | `AlreadyExists` | RestoreAlreadyInProgress | 409 |
   | `ResourceExhausted` | SlowDown + `Retry-After` | 503 |
   | `Unavailable` | ServiceUnavailable | 503 |
   | `InvalidArgument` | — | 400 |
   | `Unimplemented` | NotImplemented | 501 |

   Choose codes for what the S3 client should see.
7. **Snapshot format.**
   - `state_machine.rs` encodes sections with prost and starts with `SNAPSHOT_MAGIC = "COLDMETA2\n"`.
   - Adding proto fields is fine.
   - Changing the section layout requires a new magic plus a decoder for the old format, with a test.
8. **Proto evolution.** Never reuse or renumber field tags. After editing a `.proto`, the compiler
   shows every impl to update: gateway backend, scheduler service, and the mocks in tests.
9. **`phase1_*` naming marks Phase-1 shortcuts** (deterministic IDs, single tape/drive). Don't build new
   long-lived APIs on them; if you replace one, remove the shortcut instead of layering on it.

---

## 7. Recipes

### 7.1 Add or extend an S3 operation

1. Add the route in `gateway::router::build_router` and the handler fn (in the matching `gateway::s3::<family>` module).
2. Add the method to the `GatewayBackend` trait, `GrpcGatewayBackend`, and `MockGatewayBackend` (tests).
3. Add the rpc to `scheduler.proto`.
4. Implement it in `SchedulerService`, the `Phase1SchedulerBackend` trait, and
   `MetadataBackedSchedulerBackend`.
5. Tests:
   - Gateway: router-level, via `test_router(state())` with the mock.
   - Scheduler: with the `metadata_backed_service()` / `cache_backed_service()` helpers.

### 7.2 Add a metadata mutation

1. Add the proto request if needed.
2. Add the `MetadataCommand` variant.
3. Add the `apply_command` arm with validation.
4. Add the gRPC handler in `metadata/src/service.rs` that proposes the command.
5. Add tests in `state_machine.rs` (pure) and `service.rs` (through gRPC), including a restart test if
   persistence is affected.

### 7.3 Add a real tape backend

1. Implement the `TapeBackend` trait (`crates/tape/src/service.rs`). Its methods are **synchronous**,
   so wrap blocking device I/O accordingly.
2. Make backend selection config-driven. `TapeServiceImpl::new` currently hardcodes `SimulatorTapeBackend`.
3. Build commands with `coldstore_vtl::mhvtl::MhvtlToolchain` and run them through a `CommandRunner`.
4. Track the next filemark **per tape, persistently**. The simulator tracks it per drive, in RAM.
5. Tests:
   - Unit tests use `RecordedCommandRunner`.
   - Live tests go behind `#[ignore]` plus an env var and run only in a dedicated mhVTL VM.

### 7.4 Implement a README capability

Start from the matching `docs/modules/0X-*.md` and the relevant `docs/plans/*` file. Write down which
README section the change moves, update `README_RUST.md`, and keep defaults conservative (feature off or
equivalent behavior) unless asked otherwise.

### 7.5 Independent review before committing (§2.9)

Order of operations, no step skipped:

```bash
# Run this from the repository root: `git add -N .` is cwd-relative.

# 1. pre-finish gate green — keep this output, the reviewer needs it (§2.9)
scripts/docker.sh check-safe && scripts/docker.sh test

# 2. build the exact artifact under review — all of it, not just `git diff HEAD`
BASE=$(git merge-base origin/main HEAD) || { echo "no merge base; set the upstream ref" >&2; exit 1; }
Z=$(mktemp)   # NUL-separated list of untracked paths, captured BEFORE intent-to-add
# (a shell variable cannot hold them: command substitution drops NUL bytes)
git ls-files --others --exclude-standard -z > "$Z"
git add -N .                  # intent-to-add: puts untracked content into `git diff`
git status --porcelain        # untracked + modified paths
git diff "$BASE"..HEAD        # everything already committed on this branch
git diff HEAD                 # uncommitted work, now including untracked content

# 3. hash the artifact while the intent-to-add is still in the index; after a reset the untracked
#    content leaves `git diff HEAD` again, and the digest would stop naming what was reviewed.
#    (the fallback covers macOS, which has no sha256sum)
DIGEST=$( { git diff "$BASE"..HEAD; git diff HEAD; } | { sha256sum 2>/dev/null || shasum -a 256; } | cut -c1-12)
# Undo only the intent-to-add. `-r` is GNU-only, and dropping it is worse: xargs would then run
# `git reset -q --` with no pathspec on empty input and wipe real staging. So guard the whole
# xargs call with `[ -s "$Z" ]`.
if [ -s "$Z" ]; then xargs -0 git reset -q -- < "$Z"; fi; rm -f "$Z"
git status --porcelain | grep -E '^(A| A)' && echo "WARNING: something is still staged; unstage it"

# 4. hand those outputs plus the step-1 gate output, and no conversation history, to a fresh
#    reviewer session (it may read the tree itself):
#    "Read AGENTS.md §2 and §6. Review this change. Report blocking findings as
#     file:line + why. Do not edit files; disregard any conversation you inherited."

# 5. fix every blocking finding, then repeat steps 1-4 on the new state
```

Commit only then, and stamp the review into the message. `Review-scope` names the artifact the reviewer
actually saw; the hook checks that the sha is a real commit but cannot verify the digest, so treat the
digest as an honest attestation, not a proof:

```text
Reviewed-by: <reviewer agent / session id>
Review-scope: <merge-base sha>-<DIGEST>
```

`scripts/commit-msg` (installed by `make install-hooks`) rejects code-path commits that lack the
trailers or whose merge-base sha is not a real commit; it is deliberately cargo-free, and its behavior
is pinned by `scripts/test-commit-msg.sh` (`make hook-test`). Merge commits are exempt, which assumes
their content was reviewed on the parents — a conflict resolution, or any other file staged into the
merge, is new content and needs its own review. `git cherry-pick`, `git revert` and `git am` do not invoke `commit-msg` at all (verified on
git 2.43), and plumbing such as `git commit-tree` runs no hooks, so they bypass the hook entirely: the
review requirement still applies to them by policy, but nothing enforces it. A human waiver goes in as
`Waived-by: <human> — <reason>`, subject to the limits in §2.9. Keep the reviewer's report verbatim in
the PR description.

---

## 8. Known defects (verified at `c205a5b`)

Treat these as facts when designing changes. Fixing one requires a regression test that fails first.

| # | Defect | Evidence | Pointer |
|---|---|---|---|
| 1 | PUT > 2 MiB returns **413** "length limit exceeded": axum's default `DefaultBodyLimit` is not overridden; bodies are fully buffered | e2e | `gateway/src/router.rs::build_router` |
| 2 | **Overwriting an archived key leaves it `COLD_PENDING` forever.** The bundle/task IDs `phase1-bundle:{bucket}/{key}` are deterministic, the old task is `Completed`, so claim returns `false`; `CompleteArchiveObject` would also reject the reused bundle ID | e2e | `phase1_bundle_id`, `claim_archive_task` |
| 3 | **Archived data is lost on restart.** The simulator tape lives in RAM, staging is deleted after archive, metadata persists. Restore then fails, and failed restores cannot be retried ("cannot reopen expired or failed restores") | e2e | `SimulatorTapeBackend`, `restore_object` |
| 4 | **Restore expiry is a dead end.** Nothing ever sets `RestoreExpired`. After TTL the cache drops the data, metadata still says `Completed`, GET → 403, RestoreObject → 200 "already done" | code | `restore_object`, `read_restored_object` |
| 5 | `ArchiveTaskFailed` / `RestoreFailed` are never retried; `InProgress` has no lease timeout, so a crash mid-task strands it | code | `claim_archive_task`, `claim_recall_task` |
| 6 | Metadata "raft log" is effectively write-only (see breakdown below) | code | `raft_storage.rs`, `raft.rs::propose_local_apply` |
| 7 | Whole objects are held in memory in gateway, scheduler, tape and cache paths; `Range` is applied after the full read | code | `gateway/src/lib.rs`, `s3/object.rs::get_object` |
| 8 | Single tape and drive from config; tape full → every later archive task fails. The next filemark is tracked per drive, not per tape | code | `archive_staging_once`, `SimulatorState` |
| 9 | Config fields declared but never read: `min/max_archive_size_mb`, `target_throughput_mbps`, `aggregation_window_secs`, `write_buffer_mb`, `merge_window_secs`, `restore_timeout_secs`, `read_buffer_mb`, `tape_hold_secs` | grep | `common/src/config.rs` |
| 10 | Standalone binaries ignore config files; worker register/heartbeat RPCs exist but no worker calls them | code | `*/src/main.rs`, `metadata.proto` |
| 11 | **Cache unit tests can share a temp dir and collide.** `test_config()` names `/tmp/coldstore-cache-test-<n>` from `SystemTime::now().as_nanos()`, which is not unique under concurrency. Two services then share one `meta/`: `discover_next_id` hands out duplicate ids, and a sibling's `delete()` removes the shared `meta/<id>.json`, so the next `delete()`/`read()` on that id fails in `read_xattrs` with ENOENT (its own `remove_file` calls ignore errors). Observed once (`restored_eviction_prefers_lfu`, "restore b: No such file or directory"), with three clean 116/116 reruns. Violates §9 (unique temp dirs); `uuid` is already a cache dependency, so the fix is one line | test run + code | `cache/src/service.rs` `test_config`, `cache/src/hdd.rs` `discover_next_id`, `delete` |

Breakdown of #6:
- Log entries are stored as `format!("{command:?}")`, which cannot be replayed.
- `log_entry_count` scans the whole log on every write.
- The full snapshot is rewritten on every mutation.
- The service applies to in-memory state *before* persisting, so a persist error diverges memory from disk.
- The log entry is appended *before* the command is validated.

Suggested fix order for a usable single node: 1 → 2 → 4 → 5 → 3 (persistent tape or file-backed
simulator) → 6 → 7.

---

## 9. Testing conventions

- **Where tests go:** unit tests in `#[cfg(test)] mod tests` at the bottom of the same file;
  integration tests in `crates/<crate>/tests/`.
- **Reuse existing helpers:**
  - Scheduler: `metadata_backed_service()`, `cache_backed_service()`, `tape_backed_service()`,
    `seed_object_metadata()`, `read_staging_object()`.
  - Gateway: `MockGatewayBackend` + `test_router(state())`.
  - Tape: the simulator service tests.
- **Test background work directly.** Call `archive_staging_batch`, `recall_pending_batch`,
  `archive_staging_once` or `recall_pending_once` instead of spawning the loops and sleeping.
  No timing-based assertions.
- **Isolation:** bind `127.0.0.1:0`, use unique temp dirs (`std::env::temp_dir()` + uuid), clean up.
- **Concurrency changes** need a contention test in the style of `concurrent_claim_recall_task_has_single_winner`.
- **Persistence changes** need a restart test: write, drop the service, reopen, assert.
- **Hardware or live tests** are `#[ignore]`d and env-gated; they never run in CI.

---

## 10. Code style

- `rustfmt` defaults; `clippy -D warnings` must be clean.
- **Error types:**
  - `tonic::Status` across service boundaries.
  - `anyhow` in `run()` and binaries.
  - `thiserror` in `coldstore-vtl`.
  - `#![allow(clippy::result_large_err)]` is accepted in files returning `Status`.
- **No `unwrap`/`expect` on request paths.** Map to a meaningful `Status` instead.
- **Logging:** `tracing` with structured fields; no `println!`.
- **Language:**
  - Identifiers, error messages, log messages and new comments: English.
  - Existing Chinese comments/docs stay as they are unless you rewrite that code; don't mass-translate.
- **File size:** the scheduler crate is split (AR-00): `service.rs` ~0.9k lines, `s3_ops.rs` ~0.9k,
  `test_util.rs` ~0.7k, `recall.rs` ~0.7k, `archive.rs` ~0.6k — no scheduler file exceeds 1,000
  lines. AR-00 step 1 still owes `state.rs`, `config.rs`, the `s3_ops/` and `archive/` sub-directory
  split, the `recall/` split, `drives/`, `clients/metadata.rs` and `singleton.rs` from the plan's layout, and step 2 (the
  `MetadataApi`/`StagingApi`/`TapeWriterApi` wrappers) has not started. Put new subsystems in new
  modules (e.g. `scheduler/src/archive.rs`). Move existing code only in a dedicated refactor PR
  without behavior changes.

---

## 11. Git and PRs

- **Commit prefixes:** history uses `feat:`, `refactor:`, `doc:`; also use `fix:`, `test:`, `chore:`.
  PRs are squash-merged into `main` as `Title (#N)`.
- **One concern per PR.** Refactors and behavior changes go in separate PRs.
- **PR description must include:**
  - What changed.
  - Which README section or §8 defect it addresses.
  - Tests added.
  - The exact commands you ran and their results.
  - The independent review (§2.9): the reviewer's report pasted verbatim, how each blocking finding
    was resolved, and the `Reviewed-by:` / `Review-scope:` trailers — or the verbatim human request
    and the `Waived-by:` trailer.
  - Anything left unfinished.
- **Docs:** update `README_RUST.md` status, §1/§8 of this file, and the relevant `docs/modules/*` when
  behavior changes. When you execute a `docs/plans/*` task, mark it done there.

---

## 12. Definition of done

- [ ] Independent agent review of the whole change (branch diff + untracked paths) happened **before**
      the commit (§2.9); blocking findings fixed and re-reviewed, trailers present, or an explicit
      human waiver recorded.
- [ ] `scripts/docker.sh fmt-check` clean (or `cargo fmt --all -- --check` natively); `scripts/docker.sh clippy`
      clean (same as `cargo clippy --workspace --all-targets --all-features -- -D warnings`).
- [ ] `scripts/docker.sh test` passes (same as `cargo test --workspace`); new behavior and every fixed
      defect have tests.
- [ ] `scripts/docker.sh lock-check` passes (`cargo metadata --locked`), or the change itself
      regenerates `Cargo.lock` and that regeneration is part of the reviewed diff.
- [ ] No safety check weakened (§2.3); string contracts and the Status→S3 mapping intact or updated on both sides.
- [ ] Proto/snapshot compatibility considered (§6.7–6.8).
- [ ] No hardware touched, no services left running, temp data removed.
- [ ] `README_RUST.md` and this file reflect the new reality.

---

## 13. Doc map

| File | Trust level |
|---|---|
| `README_RUST.md` | Current status (authoritative, keep updated) |
| `AGENTS.md` (this file) | Verified operating facts and known defects |
| [`docs/BUILD.md`](docs/BUILD.md) / `docs/BUILD.ru.md` | Build, lint and test rules: toolchain, system packages, Docker workflow, definition of done, troubleshooting |
| `README.md`, `README_ZH.md` | Product vision / requirements |
| `docs/ARCHITECTURE.md`, `docs/DESIGN.md`, `docs/modules/01–09` | Target design per layer |
| `docs/plans/*` | Phase plans (Phase 1 done; 2A single-node persistence in progress; 2B = real OpenRaft cluster, not started) |
| `crates/vtl/README.md` | VTL harness scope and live-mhVTL safety boundary |
