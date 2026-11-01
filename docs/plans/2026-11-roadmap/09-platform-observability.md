# Plan 09: Platform: Configuration, Observability, Packaging, CI, Testing, Security

**Goal:** README 6 (HA, observability, compliance, extensibility) and everything the other plans
assume: real configuration, metrics and traces, health and graceful shutdown, deployable artifacts,
reproducible CI, deterministic distributed tests, benchmarks and security.
Design reference: `docs/modules/08-observability.md`.

## Current state (verified at `c205a5b`)

| Area | State |
|---|---|
| Configuration | Standalone binaries use `Config::default()` (`TODO` in `metadata/src/main.rs`). Only `coldstore-node` reads YAML, with hardcoded ports. Many config fields are never read (`AGENTS.md` §8 #9) |
| Telemetry | `tracing` logs only. No metrics, OTEL, health endpoints, or graceful shutdown |
| CI | GitHub Actions runs fmt, clippy, build and test on `ubuntu-latest`. No e2e, no mhVTL, no benchmarks |
| Reproducibility | `Cargo.lock` is git-ignored, so builds are not reproducible |
| README claims | "Rust 1.75+" is outdated (built and verified on 1.91) |
| Packaging and security | No packaging (Docker, systemd). No TLS anywhere. Gateway has no auth (GW-04) |

---

## Tasks

### PL-01: Unified configuration for every binary `[M0]` `P0` est. 3–4 d

**Design:**
- One `ColdStoreConfig` file with sections `metadata`, `gateway`, `scheduler`, `cache`, `tape`, `pools`,
  `notifications`, `admin`, `telemetry`. Each binary reads its section plus `telemetry`.
- `coldstore-node` reads the whole file and replaces its hardcoded ports with config values; the
  defaults stay the same.
- Loading order: defaults → file (`--config`, `COLDSTORE_CONFIG`) → env overrides
  (`COLDSTORE__SCHEDULER__ARCHIVE__BATCH_SIZE=…`, `config` crate with `__` separator) → CLI flags for a
  few common items (`--listen`, `--data-dir`).
- `#[serde(deny_unknown_fields)]` everywhere, so typos fail fast.
- A `validate()` step per section with human-readable errors (e.g. `min_archive_size_mb >
  max_archive_size_mb`).
- `coldstore-<bin> config print-default` and `config check <file>` subcommands.
- **Dead config fields** are either wired up by their owning task (AR, RC) or removed here. A test
  asserts that every field in `configs/reference.yaml` is read: a unit test per section using a
  "touched-fields" visitor, or simply code review plus `deny_unknown_fields` and the reference file.
- `configs/reference.yaml`: a fully commented reference generated from the structs (doc comments →
  `schemars` JSON schema → rendered). CI checks that it is up to date.

**Tests:** precedence (file vs env vs CLI); unknown field → error; validation messages; snapshot of the
default config.

### PL-02: Metrics `[M0 basic, M4 full]` est. 3 d + 4 d

**Design:**
- `metrics` facade + `metrics-exporter-prometheus` on `telemetry.metrics_listen` (default per component
  port + 1000, e.g. gateway 10000).
- Naming: `coldstore_<component>_<thing>_<unit>`.
- **M0 basic set:**
  - gateway: requests by op/status, latency histograms, in-flight, bytes in/out;
  - metadata: commands by type/result, apply latency, log size, snapshot age;
  - scheduler: tasks by kind/state, archive lag (`oldest COLD_PENDING age`), restore queue age;
  - cache: the CA-10 set;
  - tape: drive state, bytes read/written, mounts, errors.
- **M4 full set:** per `docs/modules/08 §4`, plus:
  - Raft: term, leader changes, commit index lag per follower, snapshot install;
  - per-drive throughput and starvation (AR-05);
  - the recall SLA histograms (RC-09).
- **Dashboards:** Grafana JSON committed in `deploy/grafana/`:
  - overview;
  - archive pipeline;
  - recall;
  - tapes and drives;
  - cache;
  - metadata cluster.
- **Alert rules:** Prometheus rules in `deploy/prometheus/alerts.yml`, mapped to the runbooks (OP-08):
  - archive lag > threshold;
  - staging > 80 / 95 %;
  - drive error;
  - media request open > 4 h;
  - under-protected bundles;
  - verify behind;
  - leader flapping;
  - `DataLossSuspected`.

**Tests:** a `/metrics` endpoint smoke test; a check that the key metrics exist after e2e (scrape and
grep in the e2e script).

### PL-03: Distributed tracing `[M4]` est. 4–5 d

**Design:**
- `tracing-opentelemetry` + an OTLP exporter (feature `otel`, off by default).
- Propagate W3C `traceparent` through:
  - HTTP (gateway, from the client if present);
  - gRPC metadata, with tonic interceptors on every client and server.
- **Long async flows:** a recall can take hours. Store the `trace_context` in `RecallTask` and
  `ArchiveTask` (`docs/modules/08 §3.6`). The executor starts a span linked to the original request.
  Restore latency then becomes visible end to end.
- Span names and attributes per `docs/modules/08 §3.2–3.3`.
- `x-amz-request-id` is the root attribute.
- Sampling: parent-based, 1 % by default, 100 % for errors (tail sampling is done in the collector).

**Tests:** an in-memory exporter test asserting the span hierarchy for PUT → archive → restore → GET.

### PL-04: Health, readiness, graceful shutdown, resource limits `[M0]` est. 2–3 d

- Every binary serves `/healthz` (liveness) and `/readyz` (dependencies reachable, storage writable,
  metadata leader known) on its telemetry port, plus the gRPC health service (`tonic-health`).
- **SIGTERM handling:**
  1. Stop accepting new requests and jobs.
  2. Finish or hand back leased tasks, renewing leases while finishing. The tape worker finishes the
     current bundle or aborts cleanly at a block boundary, then waits for the drive to report ready.
  3. Deregister from metadata.
  4. Exit within `shutdown_timeout_secs`.
- `coldstore-node` propagates shutdown to its components in reverse start order. Today it aborts tasks,
  which is unsafe for tape writes.

**Tests:** SIGTERM during an archive → the bundle is either committed or cleanly abandoned (AR-06
invariants hold).

### PL-05: Packaging, deployment and transport security `[M4]` est. 6–8 d

**Artifacts:**
- Static-ish release binaries (x86_64, aarch64) via `cargo dist`, or a custom workflow.
- `.deb`/`.rpm` with systemd units.
- Container images (distroless) for gateway, metadata, scheduler, cache and notifier.
- Tape workers run on bare metal or VMs with device passthrough. Container images are offered with
  documented `--device` flags, but systemd is the recommended way.

**Topologies** (`docs/ops/deployment.md`):

| Topology | Layout |
|---|---|
| Single node | `coldstore-node`, file or real tape. Small sites |
| Small cluster | 3 × metadata (+ scheduler + gateway colocated); N cache hosts (NVMe); tape hosts attached to the library |
| Large | Separate gateway tier behind an L4 load balancer; dedicated metadata NVMe nodes; schedulers scaled by drive count |

**Deployment assets:**
- `deploy/compose/`: a 3-node dev cluster with the file-backed tape simulator (used by the M4 exit test).
- `deploy/helm/` (optional) for the stateless parts.

**Transport security:**
- TLS for gRPC between components (rustls, `*.tls` config). mTLS optional with a cluster CA.
- `coldstore-admin certs init` generates a dev CA.
- TLS for the admin API.

**Upgrades:**
- Rolling-upgrade procedure with version-skew rules: N and N−1 interoperate.
- Proto changes are additive only.
- Metadata migrations run before serving (MD-02).

### PL-06: CI/CD hardening `[M0 a, M1+ b]` est. 2 d + 5 d

**(a) M0:**
- **Commit `Cargo.lock`** (binaries project; reproducible CI). Remove it from `.gitignore`. Update
  `AGENTS.md` §3.1 accordingly.
- Pin the toolchain with `rust-toolchain.toml` (current stable, e.g. 1.91) and set `rust-version` in
  the workspace. Fix the README claim.
- **E2E job:** build `coldstore-node`, then run `scripts/e2e/m0.sh` (curl + aws-cli + boto3 in a
  container) with the file-backed tape backend.
- Run unit tests with `cargo nextest` (faster, better isolation); cache RocksDB builds (`sccache`).
- The Makefile and the pre-commit hook mirror CI exactly. Today the Makefile uses `--all-features` in
  clippy while CI does not; pick one.

**(b) M1+:**
- Coverage (`cargo llvm-cov`) reported on PRs; no hard gate, but trend tracking.
- `cargo deny` (licenses, advisories, duplicate deps).
- Fuzzing job (nightly) for:
  - the tape format decoder (AR-04);
  - the aws-chunked decoder (GW-03);
  - the S3 XML parsers;
  - the SigV4 canonicalizer.
- mhVTL lab job (TP-08), nightly plus PR label `tape`.
- S3 compatibility matrix (GW-13): fast subset per PR, full nightly.
- Benchmark job (PL-08) nightly on a fixed runner, with regression alerts above 10 %.
- **Release workflow:**
  - tag → build artifacts → changelog (conventional commits) → images;
  - SBOM (`cargo cyclonedx`);
  - signed checksums.

### PL-07: Deterministic simulation and fault-injection testing `[M4]` est. 6–8 d

**Design:**
- Adopt a deterministic simulation framework for multi-node scenarios: `turmoil` (tokio-compatible
  network simulation), or `madsim` if a fuller environment is needed. Make the decision in a spike.
- Abstract the clock (`Clock` trait) and randomness throughout scheduler and metadata code. MD-06
  already requires passing `now` into commands.
- **Scenarios:**
  - leader kill during writes;
  - partition between scheduler and metadata during a bundle commit;
  - cache worker crash after ack;
  - tape worker crash mid-bundle;
  - clock skew;
  - message duplication (retries) on idempotent commands.
- **Invariant checkers**, run after each simulation:
  - every acknowledged PUT is eventually COLD with the required copies, or still COLD_PENDING with
    staging present;
  - no generation has two committed copies on the same tape;
  - tape append positions are monotonic;
  - no task is InProgress with an expired lease after quiescence;
  - restore states are consistent with the cache.
- Seeds are reproducible. A failing seed is printed and turned into a regression test.

### PL-08: Benchmark harness `coldstore-bench` `[M1]` est. 4–5 d

**Subcommands:**

| Subcommand | Measures |
|---|---|
| `s3 put/get` | Through the gateway, size distributions, concurrency |
| `archive` | AR-09: staging → tape pipeline with null, file or real sinks |
| `recall` | Random restores, merge efficiency |
| `metadata` | Command throughput and latency (MD-12) |
| `cache` | Streaming put/get, fsync cost |

**Output:** JSON results plus a markdown summary. Results for reference hardware go to
`docs/benchmarks/*.md`. The nightly CI comparison uses stored baselines.

### PL-09: Security and encryption at rest `[M6]` est. 6–10 d

**Threat model** (`docs/security/threat-model.md`):
- untrusted network clients;
- a compromised cache host;
- a stolen vaulted tape;
- an insider with admin access.

**At rest on tape** (choose per pool):
- **Drive hardware encryption** (LTO AES-256-GCM via SCSI SECURITY PROTOCOL IN/OUT). Keys come from a
  KMS (KMIP client, or file-based keys for small sites) and are referenced by key id in the bundle header.
  Qualify it per drive (TP-08).
- **Software encryption** (AES-256-GCM per record, `flags` bit in `ObjectHeader` per
  `docs/modules/05 §9.5`). It is drive-agnostic and costs CPU; benchmark it (AES-NI usually keeps up
  with 400 MB/s per core).

**Elsewhere:**
- Staging and restored cache: rely on disk-level encryption (LUKS), documented. Optional software
  encryption for staging later.
- **Secrets:**
  - never stored in config files in plain text: use env or file references;
  - S3 secret keys stored encrypted in metadata (GW-04 v2);
  - admin tokens hashed.
- **Hardening:**
  - per-key rate limits (gateway);
  - request size and header limits;
  - `cargo audit` in CI;
  - minimal container images;
  - systemd sandboxing directives in units (`ProtectSystem`, `NoNewPrivileges`; device allow-lists for
    tape workers).

### PL-10: Documentation and release discipline `[ongoing]`

- Keep `README_RUST.md` as the status page. Each milestone closes with an updated status table and a
  short "what changed" section.
- Format and compatibility versioning policy (`docs/compat.md`):
  - tape format (AR-04);
  - cache layout (CA-02);
  - metadata schema (MD-02);
  - proto (additive only).
  Each has an explicit version number and an upgrade test in CI (old fixture → new code).
- **Architecture decision records** in `docs/adr/`. Decisions already pending:
  - openraft version (MD-07);
  - tape format (D6);
  - ETag = MD5 (D7);
  - simulation framework (PL-07);
  - console stack (OP-07);
  - EC (DP-06).
- Translate the `docs/modules/*` Chinese design docs into English, or keep both, once each module is
  updated to match its implementation. Mark sections "implemented" or "planned" inline.

---

## Definition of done for the platform

- [ ] Every binary is configurable from one validated file; no dead fields; reference config generated.
- [ ] Metrics, dashboards and alerts in the repo, covering every runbook; traces span the async restore
  flow.
- [ ] Graceful shutdown proven safe for tape writes.
- [ ] Reproducible CI with lockfile, pinned toolchain, e2e, fuzzing, mhVTL, compatibility matrix and
  benchmarks.
- [ ] Deterministic simulation suite with invariant checkers in CI.
- [ ] Release pipeline with artifacts, SBOM and an upgrade test across N−1 → N.
