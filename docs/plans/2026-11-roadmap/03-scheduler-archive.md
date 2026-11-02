# Plan 03: Archive Scheduler (`coldstore-scheduler`, archive side)

**Goal:** README 3.2, 3.5, 4.2 "Archive Scheduler", 5.1, 7.4:
- aggregate COLD_PENDING objects into bundles;
- write them sequentially to tape at ≥ 300 MB/s;
- commit metadata atomically;
- release staging only after a durable commit;
- survive crashes at any step without data loss or stuck objects.

## Current state (verified at `c205a5b`)

**How it works now:**
- `run_archive_agent` → `archive_staging_once` → `archive_staging_batch`.
- Work is discovered by **listing cache staging keys** (not from metadata).
- For each object:
  1. claim a deterministic task (`phase1-bundle:{bucket}/{key}`);
  2. read the whole object into memory;
  3. `write_bundle` = one tape file (filemark) per object;
  4. re-check metadata;
  5. `CompleteArchiveObject` (CAS on size, checksum, storage class, `updated_at`);
  6. delete staging;
  7. finalize the task.
- A single tape (`archive.tape_id`) and a single drive (`archive.drive_id`) come from config.

**Good parts to keep:**
- The re-validation after the write.
- The CAS commit.
- The ordering: commit before staging delete.

**Defects:**
- Overwrite → stuck COLD_PENDING (#2).
- Failed tasks are never retried, and InProgress tasks have no timeout (#5).
- No aggregation; full buffering.
- Config fields `min/max_archive_size_mb`, `aggregation_window_secs`, `write_buffer_mb` and
  `target_throughput_mbps` are unused.
- Tape full → every later task fails.
- `scheduler/src/service.rs` is 4.4 k lines (pre-AR-00; see the AR-00 status below) and mixes S3 ops, archive, recall and tests.

## Target architecture

```
           ┌──────────────── Scheduler process ──────────────────────────────┐
           │  BundlePlanner (singleton via lease)                             │
metadata ◀─┤    scans idx_pending → BundlePlan → CreateBundleTask (atomic)    │
           │  ArchiveExecutor × N (one per allocated drive)                   │
           │    ClaimTask(lease) → ReserveAppend(tape) → DriveScheduler       │
           │    → StagingPrefetcher ─▶ RingBuffer ─▶ TapeWriter stream        │
           │    → CommitArchiveBundle (atomic CAS) → ReleaseStaging           │
           │  LeaseKeeper, Janitor (orphan staging, stale reservations)       │
           └──────────────────────────────────────────────────────────────────┘
```

Module layout (AR-00):

```
crates/scheduler/src/
  lib.rs, state.rs, config.rs
  s3_ops/{mod,put,get,head,delete,list,restore,buckets}.rs   # what is in MetadataBackedSchedulerBackend today
  archive/{mod,planner,executor,pipeline,format,commit,tape_select,janitor}.rs
  recall/{...}                                                # Plan 04
  drives/{mod,arbiter}.rs                                     # shared with recall
  clients/{metadata,cache,tape}.rs
  leases.rs, singleton.rs
```

---

## Tasks

### AR-00: Modularize the scheduler crate (no behavior change) `[M0]` est. 2–3 d

**Status:** in progress. **Step 1, acceptance met for the file-size rule; layout not yet final.**
Two moves landed out of `service.rs`: the leaf modules (`clients/{mod,cache,tape}.rs`,
`leases.rs`, `responses.rs`, `util.rs`, 4408 → 3722 lines) and then `archive.rs`, `recall.rs`,
`s3_ops.rs` and `test_util.rs` (3722 → 891). No scheduler file now exceeds 1,000 lines, and the
28 scheduler tests are the same 28 by name and are unchanged in body. The 12 that moved went with the code
they cover (2 to `archive.rs`, 2 to `recall.rs`, 8 to `s3_ops.rs`). The other 16 stay in
`service.rs::tests`: the eleven tests of the gRPC surface that stayed there, and the five
archive-loop tests (`archive_staging_batch_*`, `scheduler_background_once_*`), which move with the
`archive/` directory split listed below. Moved `pub` items are re-exported from `service`, so consumer paths did not change.

Still ahead under step 1: `state.rs`, `config.rs`, `drives/`, `clients/metadata.rs`, `singleton.rs`,
and turning the flat `s3_ops.rs` / `archive.rs` / `recall.rs` files into the directories the layout
above names (`s3_ops/{put,get,head,delete,list,restore,buckets}.rs`, `archive/{planner,executor,…}.rs`)
— the plan's layout is a directory tree, what landed is one file per area. Step 2
(`MetadataApi`/`StagingApi`/`TapeWriterApi` wrappers) has not started; `Phase1ArchiveCache` and
`TapeArchiveWriter` already are the `StagingApi` and `TapeWriterApi` shapes.

One deviation to keep: `util.rs`, `responses.rs` and `test_util.rs` are **intentional additions** to
the layout above — true leaves with no back-references, and a shared `#[cfg(test)]` fixture module
that the per-area test modules all need.

**Steps:**
1. Move code into the layout above. Tests move with their code.
2. Introduce `clients/` wrappers so the archive and recall code depend on traits:
   - `MetadataApi`;
   - `StagingApi` (exists as `Phase1ArchiveCache`);
   - `TapeWriterApi` (exists as `TapeArchiveWriter`).

**Acceptance:** 28 scheduler tests are unchanged and green; no file > 1,500 lines.

### AR-01: Archive correctness fixes `[M0]` `P0` est. 4–5 d

**Depends on:** MD-04 (generations), MD-06 (leases), TP-01 (persistent simulator).

**Changes:**
- **IDs.** Task and bundle IDs become UUID v7. The staging key is `object_id`. This fixes the overwrite
  defect together with MD-04.
- **Claims.** Claim via `ClaimTask` with a lease. `LeaseKeeper` renews it. On renewal failure, cancel the
  job before commit.
- **Retries.**
  - `FailTask{retriable}` with exponential backoff (base 30 s, max 1 h).
  - `max_attempts` (default 10).
  - Classification:
    - `Unavailable`, `ResourceExhausted`, `DeadlineExceeded`, tape EW/full → retriable;
    - checksum mismatch in staging, or object deleted → not retriable for that object; drop it from the
      bundle and continue with the rest.
- **Startup reconciliation.** On start, the executor queries tasks it owned (`lease_owner = self.node_id`)
  and treats them as expired.
- **Delete-during-archive.** Covered by the commit CAS, but add an explicit test.
- **Tape full.** On EW/ENOSPC, mark the tape FULL (MD-10) and retry on another tape. Never fail the
  objects.

**Regression tests** (write them first):
- `overwrite_after_archive_archives_new_generation`.
- `failed_task_is_retried_after_backoff`.
- `expired_lease_is_reclaimed_and_stale_completion_rejected`.
- `tape_full_moves_to_next_tape`.
- `restart_mid_write_recovers` (file-backed simulator; kill between write and commit).

### AR-02: Work discovery from metadata, not staging `[M1]` est. 2–3 d

**Depends on:** MD-03.

**Design:**
- The planner pages through `ScanPendingGenerations(after=cursor, limit=page)`. The staging location is
  `generation.staging_ref {worker_id, cache_key}`.
- Staging entries with no metadata generation are orphans, handled by the janitor (CA-03): they are
  deleted after a grace period of 1 h, and every deletion is logged and evented.
- Remove `ListStagingKeys` from the archive path. Keep the RPC for the janitor and admin use.

**Tests:**
- An object whose metadata commit failed after staging (crash between steps) is cleaned by the janitor,
  not archived.
- A metadata entry whose staging data is missing → generation marked `STAGING_LOST`, event emitted. Such
  a PUT was acknowledged but its data was lost: this should be impossible after CA-02, so treat it as P1.

### AR-03: BundlePlanner (aggregation policy) `[M1]` `P0` est. 4–5 d

**Design (pure, deterministic function plus a thin driver):**

```rust
pub struct BundlePolicy {
    min_bytes: u64,           // archive.min_archive_size_mb
    max_bytes: u64,           // archive.max_archive_size_mb
    max_objects: usize,       // archive.batch_size
    max_wait: Duration,       // archive.aggregation_window_secs: flush even if < min_bytes when oldest item is older
    group_by: GroupBy,        // None | Bucket | PoolPolicy  (pool from bucket config, DP-01)
    large_object_threshold: u64, // objects > max_bytes get a dedicated bundle
}
pub fn plan(pending: &[PendingGen], now: Timestamp, policy: &BundlePolicy) -> Vec<BundlePlan>;
```

**Rules:**
- Iterate oldest first.
- Fill until the next object would exceed `max_bytes` or `max_objects`.
- Emit plans with `total ≥ min_bytes`, or where the oldest member is older than `max_wait`.
- An object larger than `max_bytes` becomes a single-object bundle (never split objects across bundles in
  format v1).
- Grouping keeps bundles homogeneous by target pool.

**Driver:**
- A singleton planner (lease name `archive-planner`, MD-06) runs every `scan_interval_secs`.
- It creates bundle tasks via one command `CreateBundleTask { bundle_id, entries[(object_id, version,
  size)], pool }`. The command marks the generations `planned_in = bundle_id` so they are not planned
  twice.
- If a planned generation is overwritten or deleted before the write, it is skipped at commit (AR-06).

**Tests:**
- Property tests: no generation in two plans; every plan ≤ max; age flush; a large object alone.
- Golden tests with fixtures.

**Acceptance:** with 10 k × 1 MiB objects and min = 1 GiB, max = 10 GiB, the plan is ≈ 1–10 bundles,
never 10 k.

### AR-04: On-tape format v1 (self-describing) `[M1]` `P0` est. 6–8 d

**Depends on:** TP-02.

**Decision (D6):**
- One **tape file per bundle** (filemark only after the bundle), with objects packed back-to-back inside
  the file.
- `docs/modules/05 §2.2.3` proposed a filemark per object. That is rejected because every synchronous
  filemark flushes the drive buffer and kills streaming on small objects.
- Even with buffered filemarks (`MTWEOFI`), thousands of files per tape complicate positioning.
- Objects are located by logical block address (LBA) plus intra-block offset.

**Layout:**

```
BOT
File 0: VolumeLabel  (1 block)                                   [FM]
File 1: Bundle A: BundleHeader | Rec(obj1) | Rec(obj2) | ... | BundleTrailer   [FM]
File 2: Bundle B: ...                                            [FM]
...                                                              EOD
```

**All structures** are little-endian. They carry a magic, a format version and a CRC32C of themselves:
- **VolumeLabel** `"CSVL"`: `format_version`, `tape_uuid`, `tape_id` (barcode), `pool_id`, `created_at`,
  `block_size`, `writer_version`, crc.
  - Written once when a new tape is onboarded (TP-05).
  - Verified before every append: a mismatch makes the tape SUSPECT and refuses the write.
- **BundleHeader** `"CSBH"`: `bundle_id`, `created_at`, `object_count`, `block_size`, `flags`,
  `prev_bundle_id` (chain), crc.
- **Record:** `ObjectHeader "CSOH"` v2. This extends the doc's v1 with `object_id`, `generation
  version`, `record_offset_in_bundle`, `user_metadata_len` + bytes (optional), `content_type`. It is
  followed by the raw object bytes.
  - Records are byte-packed.
  - A record starts at a block boundary only if the object is larger than `align_threshold` (default
    64 MiB), so large reads start cleanly.
- **BundleTrailer** `"CSBT"`: the entry index `[(object_id, bucket, key, offset, size, sha256)]`, plus
  the bundle-level SHA-256 over all record bytes, `bytes_total`, and crc.
  - With **catalog-on-tape**, any tape can be re-indexed without metadata (DP-05).
- Padding: the last block is zero-padded to `block_size` (fixed-block mode, TP-03).

**Addressing stored in metadata** (`BundleEntry`):
- `tape_file_number`;
- `bundle_start_lba` (from READ POSITION after the preceding filemark);
- `record_offset`, `header_len`, `data_len`.

To read an object: `LOCATE(bundle_start_lba + record_offset / block_size)`, then skip
`record_offset % block_size` bytes.

**Code:**
- `archive/format.rs`: encoder and decoder plus a streaming `BundleWriter` (accepts records, emits
  blocks) and `BundleReader` (parses from a block stream).
- `#![forbid(unsafe_code)]`.
- Fuzz the decoder with `cargo-fuzz` (corpus of valid bundles plus mutations).

**Tests:**
- Encode → decode round-trip (property test: random object sizes 0 B–20 MiB, random block sizes 64 KiB–
  1 MiB).
- Corrupt any single byte of header, trailer or label → detected.
- A truncated bundle (no trailer) → the reader recovers every complete record and reports truncation.
- Golden binary fixture committed as `tests/fixtures/bundle_v1.bin`, so format drift is caught.

**Docs:** `docs/format/tape-format-v1.md` with byte tables. It is normative; changing it requires
`format_version` + 1 and decoders for older versions.

### AR-05: Streaming, double-buffered write pipeline `[M1]` `P0` est. 6–8 d

**Depends on:** AR-04, CA-01, TP-02.

**Design (per executor, i.e. per drive):**

```
StagingPrefetcher (K=4 concurrent gets, ordered output)
   → RecordEncoder (headers + data, SHA-256 & MD5 verify on the fly)
   → RingBuffer (archive.write_buffer_mb, default 1024 MiB, block-aligned)
   → TapeWriter (gRPC client stream to tape worker; tape worker writes fixed blocks via spawn_blocking)
```

- **Integrity on the fly.** The object's SHA-256 is recomputed while streaming from staging. On a
  mismatch, abort the bundle before the trailer. The bundle is then incomplete garbage beyond the
  committed append position, which is safe (AR-06). Retry without the bad object and raise a
  `StagingCorrupt` event.
- **Backpressure.** If the buffer fill stays below a low watermark for more than 2 s while the drive
  waits, emit a `drive_starvation_seconds` metric. Stalls must be visible, because sustained underfeeding
  causes shoe-shining.
- **Byte accounting.** The tape writer returns `(start_lba, end_lba, bytes, file_number)`. The executor
  cross-checks the byte counts against the encoder.
- No full-object buffers anywhere. Memory per executor is `write_buffer_mb` plus K × chunk.

**Tests:**
- Pipeline with a mocked slow and a fast staging source and a mocked slow tape: verify ordering,
  backpressure and bounded memory.
- Injected checksum error → bundle aborted and the object excluded on retry.

**Acceptance:** archiving a 10 GiB bundle on the file-backed simulator keeps scheduler RSS
< write_buffer + 64 MiB.

### AR-06: Bundle commit protocol (saga) `[M1]` `P0` est. 4–5 d

**Depends on:** AR-03, AR-05, MD-06, MD-10.

**Protocol:**

```
1. ClaimTask(bundle_task, lease)
2. ReserveAppend(tape, owner=task, expected_position=P)        -- exclusive append lease on the tape
3. Tape worker: verify label + drive position == P; write bundle; write filemark; READ POSITION → P'
4. CommitArchiveBundle {                                         -- ONE metadata command, atomic
     task_id, lease_owner, task_version,
     tape_id, start=P, end=P', file_number,
     entries: [(object_id, expected_version, record addressing)],
   }
   state machine:
     for each entry: if generation is current-or-superseded-but-not-purged AND version matches AND
                       state == COLD_PENDING AND planned_in == bundle:
                         add ArchiveCopy{bundle, tape, addressing}; if copies >= pool.min_copies → COLD
                     else: mark entry ORPHAN (bytes count as dead on this tape)
     tape.append_position = P'; tape.used += bytes; bundle.status = COMMITTED; task.status = COMPLETED
5. ReleaseStaging for entries now COLD (idempotent; generation.staging_ref cleared by command after delete)
```

**Invariants:**
- **The committed append position is the truth.** Anything on tape beyond it is garbage by definition,
  and the next writer starts exactly at the committed position. On tape, writing at P truncates
  everything after P, so the space of crashed attempts is reclaimed automatically.
- **Staging is deleted only for generations that are COLD** with the required number of copies (DP-01).
- **Crash windows:**

  | Crash point | Effect |
  |---|---|
  | Before 4 | The task is reclaimed after lease expiry; the rewrite starts at P |
  | After 4, before 5 | Staging is released by the janitor (generation is COLD, `staging_ref` present → delete) |

**Tests:** crash injection at every step boundary (hooks in the executor and the file-backed
simulator), each followed by a restart. Assert:
- no lost objects;
- no double copies counted;
- staging eventually released;
- the tape position is consistent.

### AR-07: Tape pools, selection and append management `[M1]` est. 4 d

**Depends on:** MD-10.

**Design:**
- Pools are configured with:
  - `pools: [{id, tapes: selector(barcode prefix/regex), min_free_bytes_reserve (default 2 %), media:
    [LTO-9, LTO-10], purpose: archive|copy2|scratch}]`;
  - the archive pool for a bucket (default pool: `default`).
- **Selection order:**
  1. A tape of the target pool already loaded in an idle drive with
     remaining ≥ bundle + reserve (avoids a mount).
  2. The ONLINE tape in the library with the least remaining space that still fits (fill tapes
     sequentially; fewer partially filled tapes).
  3. A scratch tape: label it (TP-05) and assign it to the pool.
  4. Otherwise emit a `PoolExhausted` event and wait.
- **Early warning / ENOSPC during a write:**
  - Abort the bundle and mark the tape FULL at the committed position.
  - Re-plan on another tape. A bundle may never span tapes in v1.
  - If `bundle_size > largest_free_tape`, the planner splits the plan (AR-03 gets a `max_bytes` override).
- Capacity comes from the tape worker's report (native capacity minus a safety margin), not from config.

**Tests:** a selection table test; EW mid-bundle → tape FULL + retry; pool exhaustion event.

### AR-08: Drive arbitration shared with recall `[M1]` est. 4–5 d

**Design:** a `drives::Arbiter` in the scheduler (single-node) or backed by metadata leases (multi-
scheduler, AR-12).

**Requests** carry `(kind=archive|recall|verify|compaction, priority, tape_id?)`.

**Policy:**
- Recall has priority over archive.
- Keep at least `archive.min_drives` (default 1 if drives ≥ 2) reserved for archive, so that staging does
  not fill up.
- When staging fill > 80 %, archive priority is raised above Bulk recalls (`CA-06` emits the pressure
  signal).
- A drive with a tape mounted is preferentially given to requests for that tape.
- Hold the tape for `tape_hold_secs` after use if more work for it is queued (amortizes mount time,
  ~1–2 min on libraries).

**Grants:**
- A grant is a `DriveLease { drive_id, tape_id, until }`, renewed while in use.
- Preemption is never mid-bundle. Archive bundles are bounded (≤ 10 GiB ≈ 30–40 s at 300 MB/s), so
  recall latency stays bounded without preemption.

**Tests:** a scheduler simulation with a virtual clock: a mixed workload meets the recall SLA (RC-09);
archive is not starved when staging is high.

### AR-09: Throughput engineering and qualification `[M1 bench, M2 real]` est. 5–7 d

**Targets (README 3.5):**
- ≥ 300 MB/s aggregate archive writes on 1 LTO-9 drive (native max ~400 MB/s);
- linear scaling: 2 drives ≥ 600 MB/s, given staging bandwidth.

**Work:**
- `coldstore-bench archive` (PL-08) generates objects of a size distribution directly into staging and
  measures end-to-end bytes/s. It reports:
  - staging read throughput;
  - encoder throughput;
  - tape write throughput;
  - buffer fill histogram;
  - starvation seconds.
- Tune:
  - `block_size` (256 KiB vs 512 KiB vs 1 MiB; respect READ BLOCK LIMITS);
  - prefetch K;
  - buffer size;
  - gRPC chunk size;
  - `spawn_blocking` thread per drive;
  - `O_DIRECT` reads from staging (CA-05).
- Sizing guide: `docs/ops/sizing.md`. Staging must deliver ≥ 1.3 × the drive rate for concurrent
  archive + ingest. A single HDD cannot; spec NVMe or RAID.

**Acceptance:**
- M1: ≥ 300 MB/s per simulated drive on NVMe-backed staging, measured with the file-backed simulator in
  "null sink" and "file sink" modes.
- M2: ≥ 300 MB/s on a physical LTO-9 drive with 1 GiB bundles.

### AR-10: Optional write verification `[M1]` est. 2–3 d

`archive.verify_after_write = none | trailer | full`:
- `trailer`: read back the bundle trailer and compare it with the in-memory index.
- `full`: read the entire bundle and verify the SHA-256 of every record. This doubles drive time, so use
  it for compliance pools.

Record `bundle.verified_at`. A failure leaves the bundle uncommitted → retry on another tape, and the
current tape becomes SUSPECT.

### AR-11: Interaction with delete, overwrite and restore-from-staging `[M1]` est. 2 d

- Delete of a COLD_PENDING generation:
  - not yet planned → delete staging + metadata;
  - planned → mark it `deleted`; the commit makes it ORPHAN; its staging data is released by the
    janitor.
- `restore_from_staging` (GW-12) copies staging → restored cache **without** changing archive state.
- Tests for each interleaving: a table of {delete, overwrite, restore} × {before plan, after plan, during
  write, after commit}.

### AR-12: Multiple scheduler instances (HA) `[M4]` est. 4–5 d

**Depends on:** MD-07, MD-09.

**Design:**
- **Singletons** (planner, sweepers, janitor) use metadata leases:
  `AcquireSingletonLease{name, owner, ttl}`.
- **Executors** on any scheduler claim bundle tasks for drives they can reach (tape-worker affinity from
  the registry).
- The drive arbiter moves into metadata. `DriveLease` becomes a metadata entity with CAS, so two
  schedulers cannot use one drive.

**Tests:** two schedulers in a deterministic simulation; kill one mid-bundle → the other completes it
after lease expiry; no double write commits.

---

## Configuration (final shape)

```yaml
archive:
  enabled: true
  scan_interval_secs: 30
  min_archive_size_mb: 1024
  max_archive_size_mb: 10240
  batch_size: 2000
  aggregation_window_secs: 300
  align_threshold_mb: 64
  write_buffer_mb: 1024
  prefetch_concurrency: 4
  block_size: 262144
  verify_after_write: trailer
  max_attempts: 10
  retry_backoff: { base_secs: 30, max_secs: 3600 }
  min_drives: 1
  tape_hold_secs: 120
pools:
  - { id: default, selector: "^CS0", media: [LTO-9, LTO-10], reserve_pct: 2 }
```

Remove the dead fields (`target_throughput_mbps`, and `tape_id`/`drive_id`/`tape_set` once pools
exist), or wire them up. No config field may exist without a reader (add a test that deserializes the
sample config with `deny_unknown_fields`).

## Definition of done for archive

- [ ] 100 k-object e2e on the file-backed simulator: bundles ≥ min size, no stuck generations, all
  verified.
- [ ] Crash-injection suite green at every saga step.
- [ ] Bench ≥ 300 MB/s (simulator, M1) and physical LTO-9 (M2).
- [ ] `docs/format/tape-format-v1.md` published; fixture-based golden tests.
- [ ] `docs/modules/05-scheduler-layer.md` updated to the implemented design.
