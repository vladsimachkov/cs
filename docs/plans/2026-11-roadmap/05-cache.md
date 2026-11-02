# Plan 05: Cache Worker (`coldstore-cache`)

**Goal:** README 3.4, 4.2 "Cache Layer", 7.6. The cache worker has two very different jobs:

| Area | What it holds | Durability |
|---|---|---|
| **Staging** | The only copy of every acknowledged object until it is archived | Must be durable: fsync, integrity-checked, never evicted, optionally replicated |
| **Restored** | Thawed copies with TTL | Disposable: capacity-limited, LRU/LFU, can always be re-read from tape |

Also: multipart parts, space reservations for recall, and streaming I/O fast enough to feed tape
drives (≥ 300 MB/s read for archive, concurrent with ingest).

## Current state (verified at `c205a5b`)

**Implemented (the most mature part of the codebase):**
- `CacheServiceImpl` with HDD backend, staging/restored categories, LRU/LFU eviction, TTL, staging vs
  restored budgets, low watermark, capacity reject codes (`capacity_reject:<reason>:`), index rebuild
  from `meta/*.json` on start, and stats.
- 11 tests.

**Gaps:**

| Gap | Detail |
|---|---|
| Staging is not durable | `HddBackend::write` = `fs::write(data)` then `fs::write(meta.json)`: no fsync of file or directory, no temp + rename; a crash can leave data without meta or partial files |
| No integrity check on read | Checksum stored but not verified in cache; the scheduler verifies later |
| Whole-object buffering | Streams are collected into `Vec` |
| Keys are `(bucket, key, version_id)` | Overwrite collisions; MD-04 requires `object_id` |
| SPDK backend is a facade | Delegates to HDD files |
| Single worker; no reservations | No `staging_ref` in metadata |
| Index rebuild cost | Reads every JSON file; slow at millions of entries |

## Target design

```
gRPC CacheService
  ├─ StagingStore     (durable, fsync, checksummed, never evicted; parts for multipart)
  ├─ RestoredStore    (TTL + LRU/LFU, reservations, evictable)
  ├─ SpaceManager     (budgets, watermarks, reservations, backpressure signal)
  ├─ Index            (RocksDB or sled-free: RocksDB local, keyed by object_id/category)
  └─ Backend trait    (FileBackend[buffered|O_DIRECT], later SpdkBackend behind feature)
```

**On-disk layout (FileBackend v2):**

```
<root>/
  VERSION                         # layout version "2"
  staging/<aa>/<object_id>.dat    # aa = first 2 hex chars → 256 dirs
  parts/<upload_id>/<part_no>.dat
  restored/<aa>/<object_id>.dat
  tmp/                            # in-flight writes, cleaned on start
  index/                          # local RocksDB: entries, reservations
```

**Write protocol (staging and parts):**
1. Write to `tmp/<uuid>`, hashing while writing.
2. `fsync(file)`.
3. Check the hash against the expected value from the trailer.
4. `rename` to the final path.
5. `fsync(parent dir)`.
6. Index put with sync.
7. Ack.

Restored entries use the same protocol without the fsyncs: losing them is harmless.

---

## Tasks

### CA-01: Streaming API v2, keyed by `object_id` `[M0]` `P0` est. 5–6 d

**Design:**
- Proto `cache.proto` v2 (new RPC names, keep the old ones for one release):

  | RPC | Notes |
  |---|---|
  | `PutStaging(stream {meta{object_id, size, expected_md5?, expected_sha256?} | data | trailer{md5, sha256}})` → `{staging_ref, md5, sha256}` | Commit only after the trailer, and only if the hashes match the trailer and any expected values |
  | `GetStaging(object_id, range?)` | Streams; verifies SHA-256 at the end of a full read and emits `DataLoss` instead of the final chunk on mismatch |
  | `PutRestored(stream …, reservation_token?)` / `GetRestored(object_id, range?)` | |
  | `DeleteStaging(object_id)` / `DeleteRestored(object_id)` | Idempotent |
  | `CopyStagingToRestored(object_id, expire_at)` | Local copy (reflink when the FS supports it), used by RC-10 |
  | `PutPart` / `GetPart` / `DeleteUpload` | Multipart (GW-08) |
  | `ListStaging(after, limit)` | Janitor and admin |

- **Streaming implementation:** `tokio::fs::File` with a `BufWriter` of 1 MiB, chunks forwarded as they
  arrive. Memory per stream ≤ 2 chunks.
- **Range reads** seek and read only the range. Integrity is not verified on partial reads; that is
  documented.

**Tests:**
- 5 GiB streaming put/get with process RSS < 64 MiB.
- Range reads.
- Trailer mismatch → nothing committed (no file, no index entry).

### CA-02: Durable staging and crash safety `[M0]` `P0` est. 4 d

**Design:** the write protocol above, plus a startup recovery step:
1. Delete everything in `tmp/`.
2. For index entries whose data file is missing → mark them lost and report them (`StagingLost` event via
   the scheduler).
3. For data files without index entries → move them to `orphans/` and report them. Never auto-delete
   before the scheduler janitor confirms with metadata (CA-03).
4. Verify the `VERSION` layout; migrate v1 (`staging/<id>.dat` + `meta/<id>.json`) → v2 with a one-shot
   migrator.

Also:
- Config `cache.fsync = always | never`. Default `always` for staging; `never` is allowed only in dev and
  tests and logs a loud warning.
- Optional background scrub of staging: re-hash entries older than 24 h at a low I/O priority
  (`cache.staging_scrub_mbps`) to catch silent disk corruption before archive.

**Tests:**
- Fault injection with a `CrashPoint` hook after each protocol step, then restart → state is either
  "not written" or "fully written", never partial.
- A real power-loss test is impossible in CI, so add a `--release` torture test (ignored) that uses
  `dm-flakey` in the lab VM, documented in `docs/ops/testing.md`.

### CA-03: Cache–metadata reconciliation (janitor) `[M0]` est. 2–3 d

Runs in the scheduler, since the cache stays metadata-agnostic per the design (scheme B).

Every `janitor_interval` (10 min), page through `ListStaging`:

| Staging entry | Action |
|---|---|
| Generation exists and is COLD_PENDING | Keep |
| Generation is COLD with required copies | Delete staging (finishes AR-06 step 5) |
| No generation, or superseded and not planned, and older than `grace` (1 h) | Delete and emit an event |
| Generation exists but staging is missing | `StagingLost` alert (P1) |

Restored cache: drop entries whose generation is not in Completed restore state (expired, deleted).

**Tests:** each row of the table.

### CA-04: Placement and multiple cache workers `[M1: a, M4: b]` est. 2 d + 6 d

**(a) M1:** every generation records `staging_ref {worker_id, path_id}` in metadata. All readers go to
`worker_id` from metadata, never to a "default cache" address. One worker is still OK.

**(b) M4:**
- **Placement on PUT:** the scheduler picks a cache worker by rendezvous hashing over online workers,
  weighted by free staging budget (heartbeats, MD-09). It skips workers in DRAINING or over their
  watermark.
- **Restored placement:** prefer the worker co-located with the tape worker or the gateway, to minimize
  hops (configurable affinity).
- **Drain:** `DrainWorker(cache)` → no new placements. The worker becomes removable when staging is empty
  (archive progresses) and its restored entries expire or are dropped.
- **Worker loss:** restored entries are recomputable (flip to Expired, MD-05). Staging on a lost worker
  means data loss unless CA-08 is enabled; that is the reason CA-08 exists.

**Tests:** placement distribution property test; drain; a worker offline → PUTs routed elsewhere within
one heartbeat.

### CA-05: Storage backends and tiering `[M4]` est. 5–8 d

**Decision:**
- Ship `FileBackend` with an optional O_DIRECT, aligned-buffer read path for staging reads during
  archive. This avoids page-cache thrash when streaming 10 GiB bundles.
- io_uring (`tokio-uring` or `glommio`) is a measured optimization, not a default.
- Real SPDK:
  - Only if benchmarks show the file backend cannot meet the target on NVMe.
  - Behind the `spdk` cargo feature, with its own CI job.
  - Until then, rename the current `SpdkBackend` to `HddBackendAlias` or remove it, so nobody believes SPDK
    exists.

**Tiering** (README 4.2 "multi-tier SSD/HDD"):
- `RestoredStore` may have two tiers:
  - NVMe "hot" (small);
  - HDD "warm" (large).
- New restores land on NVMe.
- When NVMe fills, entries are demoted to HDD instead of evicted (LRU), as long as the HDD budget allows.
- A GET on an HDD entry may promote it if `access_count ≥ promote_threshold`.
- Staging uses a single configured tier, NVMe recommended (AR-09 sizing).

**Tests:** demotion/promotion with a virtual clock; O_DIRECT alignment handling for unaligned tails.

### CA-06: SpaceManager: budgets, reservations, backpressure `[M1]` est. 4 d

**Design:**
- **Budgets:**
  - `staging_max_bytes`;
  - `restored_max_bytes`;
  - `reserve_free_bytes` (never fill the disk);
  - watermarks: `staging_high` (80 %) and `staging_critical` (95 %).
- **Reservations** (RC-07): `ReserveRestored{job_id, bytes, ttl}` → token. Reserved bytes count as
  used. Expired reservations are released.
- **Backpressure signals:**
  - `staging_high` → heartbeat flag. The archive arbiter raises archive priority (AR-08). The gateway
    begins `SlowDown` for a configurable fraction of PUTs.
  - `staging_critical` → reject all new PUTs with `ResourceExhausted{staging_budget_exceeded}` (→ S3
    `SlowDown` 503 with `Retry-After`).
- Staging is **never evicted**. Eviction only touches restored entries that are not pinned. "Pinned" =
  being read right now: reference counting prevents deleting a file mid-GET.
- Keep the existing reason codes and string contract (`AGENTS.md` §6.5); add `reservation_expired`.

**Tests:** watermark transitions; reservations under concurrent PUTs; an eviction never removes a pinned
or staging entry.

### CA-07: Persistent local index and fast startup `[M4]` est. 3–4 d

**Design:**
- Replace the scan of `meta/*.json` with a local RocksDB index:
  - `entries` (`object_id` → xattrs);
  - `lru` (`last_access` BE + `object_id`);
  - `expiry` (`expire_at` BE + `object_id`);
  - `reservations`.
- On startup, load counters only. Verify the index against the filesystem lazily, plus a background full
  check.
- Target: start in < 5 s with 10 M entries.

**Tests:** startup time benchmark; index/FS divergence repair.

### CA-08: Staging redundancy (optional HA) `[M4]` est. 6–8 d

**Problem:** between PUT and archive, staging is the only copy. Losing a cache worker's disk loses
acknowledged data. RAID helps against disk loss, not against host loss.

**Design:** `cache.staging_replicas = 1 | 2`. With 2:
- The scheduler writes the PUT stream to two workers in parallel (tee) and acks only after both commit.
  `staging_ref` lists both.
- Archive reads from either.
- Losing one worker triggers re-replication from the survivor (janitor task) and an alert.

This is cost vs safety and is documented in sizing. Default 1 for single-node, 2 recommended for clusters.

**Tests:** kill one worker mid-PUT → the PUT fails (not acked) or succeeds with 2 copies; kill one after
ack → data is still archived from the survivor and re-replicated.

### CA-09: Fault handling `[M1]` est. 2–3 d

| Fault | Handling |
|---|---|
| Disk full (ENOSPC despite accounting) | Fail the write cleanly, recompute usage, raise the `CacheDiskFull` alert |
| EIO on read | Mark the entry corrupt and quarantine it. Staging → `StagingCorrupt` P1 alert (data loss unless replica exists). Restored → drop and flip the restore to Expired |
| Directory permission or mount loss | Health turns NOT_READY; the worker deregisters (heartbeat status) |
| Clock jumps | TTL uses wall time, but eviction ordering uses a monotonic access counter |

**Tests:** fault injection through a `Backend` wrapper that returns errors on demand.

### CA-10: Observability `[M0 basic, M4 full]` est. 2 d

**Metrics:**
- `cache_bytes{category,tier}`, `cache_entries{category}`;
- `cache_reservations_bytes`;
- `cache_evictions_total{reason}`;
- `cache_hits_total` / `cache_misses_total`;
- `cache_write_bytes_total` / `cache_read_bytes_total{category}`;
- `cache_fsync_seconds` (histogram);
- `cache_staging_oldest_age_seconds` (archive lag indicator; key alert);
- `cache_rejects_total{reason}`.

**Health:** `/healthz` (process) and `/readyz` (disk writable, index open).

---

## Test hygiene follow-ups

Small, known, and deliberately left out of the defect-#11 fix (which was a one-line
naming change) so that PR stayed a single concern:

- `cache/src/service.rs::test_config` and `scheduler/src/test_util.rs::test_cache_config`
  are the same helper duplicated across two crates. Collapse them into one shared
  test helper; that also pins the uuid naming in one place instead of guarding each
  copy (only the cache copy is pinned today).
- Both helpers write under a literal `/tmp/` and never remove the directory they
  create. §9 asks for `std::env::temp_dir()` and cleanup; `metadata/src/raft_storage.rs`
  already shows the pattern.

---

## Definition of done for cache

- [ ] Crash-safety suite: no partial or orphan state after any crash point.
- [ ] Streaming 5 GiB put/get with bounded memory; range reads.
- [ ] Reservations and backpressure integrated with the scheduler and the gateway.
- [ ] Multi-worker placement and drain (M4); optional staging replication documented and tested.
- [ ] `docs/modules/04-cache-layer.md` updated: SPDK status, layout v2, protocols.
