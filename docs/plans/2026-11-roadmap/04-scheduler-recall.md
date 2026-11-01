# Plan 04: Recall Scheduler (`coldstore-scheduler`, restore side)

**Goal:** README 3.3, 3.6, 3.7 (recall half), 5.2, 7.5:
- a complete restore state machine with TTL;
- queueing that merges requests per tape and bundle and minimizes mounts;
- offline-tape waiting without holding drives;
- streaming reads into the restored cache;
- throughput of ≥ 1 restore task per 5 minutes per drive, scaling linearly with drives.

## Current state (verified at `c205a5b`)

**How it works now:**
- `run_recall_agent` → `recall_pending_once` → `recall_pending_batch`.
- Pending tasks are sorted by (tier, tape_id, offset, created_at).
- Tasks are processed **sequentially** within a batch. For each task:
  1. take the per-tape lease and claim the task;
  2. read the whole object by filemark;
  3. verify size and SHA-256;
  4. `put_restored` with `expire_at`;
  5. mark Completed.
- Retriable errors (`Unavailable`, capacity rejects) → `WaitingForMedia` and retry next tick.

**Defects:**
- `RestoreExpired` is never set, and Failed or Expired restores can't be reopened (#4).
- No leases, so an InProgress task stranded by a crash stays there forever (#5).
- One recall drive from config.
- No merge by bundle (meaningless with 1 object per bundle anyway).
- Whole-object buffering.
- No cache space reservation before reading.
- No alternate copy on read error.
- No notification on offline media.

## Target architecture

```
RestoreObject ─▶ metadata: RecallTask(Pending) + idx_recall_by_tape
                                   │
RecallPlanner (singleton) ─────────┘ every merge tick:
   group Pending by tape → TapeReadJob{tape, [bundle → sorted records]}, priority, age
   if tape OFFLINE → tasks WaitingForMedia + MediaRequest (OP-03)        (no drive held)
   else → enqueue job to DriveArbiter (AR-08)
RecallExecutor × drives:
   DriveLease → mount → for bundle in physical order: LOCATE → sequential read of needed records
   → stream each record: verify SHA-256 → CacheWriter (reserved space) → Complete(task) per object
```

---

## Tasks

### RC-01: Restore lifecycle fixes `[M0]` `P0` est. 3–4 d

**Depends on:** MD-05.

**Changes:**
- `restore_object` follows the GW-12 behavior matrix:
  - extend when Completed;
  - re-open when Expired or Failed (new task, `attempt = 0`);
  - fast path for COLD_PENDING (RC-10).
- **Expiry sweeper:** a singleton job (MD-06 lease `restore-expirer`) runs every 60 s. It calls
  `ExpireRestores{now}`, then asks the cache to drop the restored entries. The cache TTL also drops them
  independently, and that is fine.
- **Self-healing:** if GET finds `Completed` but the cache returns NotFound, the scheduler calls
  `ReportRestoredCopyMissing` → Expired, and the client gets `InvalidObjectState`. The client can
  re-restore, as in S3 after expiry.
- The task status mirrors the generation's restore status. Both are changed by the same command
  (`CompleteRecall`, `FailRecall`, …), never by two RPCs: today `update_recall_task` and
  `update_object_restore_status` are two separate writes.

**Regression tests:**
- `restore_after_expiry_creates_new_task`.
- `restore_extend_updates_expiry_and_cache_ttl`.
- `failed_restore_can_be_retried`.
- `cache_eviction_of_completed_restore_flips_to_expired`.
- `task_and_object_status_change_atomically`.

### RC-02: Leases and crash recovery for recall `[M0]` `P0` est. 2–3 d

**Depends on:** MD-06.

**Changes:**
- `ClaimTask` with a lease, renewed during long reads.
- A completion with a stale lease is rejected, so the cache write from a zombie worker is harmless:
  restored data is idempotent per `object_id`.
- Retries with backoff and `max_attempts` (default 5). Exhaustion → Failed + `RecallFailed` event.
- Startup reconciliation, as in AR-01.

**Tests:** kill during read → reclaimed after TTL → completes once.

### RC-03: Recall queue and merge planner `[M1]` `P0` est. 5–6 d

**Depends on:** MD-03 (recall-by-tape index), AR-06 (bundle addressing).

**Design:**
- **Dedup at request time.** A second restore for the same generation while one is in flight → 409 (S3
  semantics), so there is never more than one task per generation. Different generations of the same
  key are separate.
- **Planner tick** (`recall.merge_window_secs`, default 60 s; Expedited bypasses the window):
  1. Load Pending tasks grouped by `tape_id` via the index.
  2. For each tape, build a `TapeReadJob`:
     - bundles sorted by `bundle_start_lba`;
     - records within a bundle sorted by `record_offset`;
     - adjacent records merge into one read span when the gap is < `recall.coalesce_gap_mb`
       (default 256 MiB). Reading through a gap is faster than a locate at LTO speeds.
  3. **Job priority:**
     `score = tier_weight(max tier in job) + age_weight · age(oldest task) + size_bonus(#tasks)`.
     Aging guarantees that Bulk eventually wins (no starvation).
  4. Cap a job at `recall.max_job_bytes` (default 200 GiB) or `max_job_tasks` (5,000). Leftovers go to the
     next job for the same tape.
- Jobs are persisted as `RecallJob` entities, leased like tasks, so planning survives restarts and HA
  schedulers don't double-plan.
- **Tiers:**

  | Tier | Merge window | Weight |
  |---|---|---|
  | Expedited | 0 s (if enabled) | highest |
  | Standard | 60 s | default |
  | Bulk | up to `bulk_window_secs` (default 3,600 s) | lowest; maximizes merging |

**Tests:**
- Property test: the planner never produces overlapping jobs, preserves all tasks, and keeps order
  within a bundle monotonic.
- Simulation: 1,000 restores over 20 tapes → ≤ 20 mounts per window.

### RC-04: Bundle-aware streaming reads `[M1]` `P0` est. 5–6 d

**Depends on:** AR-04 (format), TP-02 (reader API), CA-01 (streaming put).

**Design:**
- For each read span:
  1. `TapeReader::locate(lba)`;
  2. stream blocks;
  3. a `BundleReader` slices records out;
  4. for each needed record:
     - verify `ObjectHeader` (magic, `object_id`, size);
     - stream the data into `CacheWriter::put_restored(object_id, size, expire_at)`, hashing
       SHA-256 on the fly;
     - finalize only if the hash matches;
     - complete the task.
  Records not requested inside a span are skipped (read and discarded).
- Per-record failure isolation: a checksum mismatch on one record fails that task (with an alternate
  copy attempt, RC-08) and continues with the others.
- Read-ahead buffer of `recall.read_buffer_mb` (default 256 MiB) between tape and cache writers. Cache
  writes may be slower than the tape (HDD cache), so if the cache is slower, the buffer fills and the drive
  stops streaming. Monitor `recall_drive_stall_seconds`; size the cache with the guide in
  `docs/ops/sizing.md`.

**Tests:**
- A file-backed simulator bundle with 1,000 records; restore 37 random ones → exactly those 37 written;
  reads stay sequential (assert the locate count ≤ number of spans).
- Corrupted record → only that task fails.

### RC-05: Offline media and WaitingForMedia `[M1 basic, M3 full]` est. 2 d + 3 d

**Basic (M1):**
- Before a job is queued for a drive, check `tape.status`.
- If it is OFFLINE, UNKNOWN or not in the library inventory, move all tasks of the job to
  WaitingForMedia (one command) **without taking a drive**.
- When the tape becomes ONLINE (inventory or operator event), move the tasks back to Pending and plan
  them immediately.

**Full (M3, with OP-03):**
- A `MediaRequest` is created once per tape (deduplicated). It carries:
  - tape_id, barcode, last known location;
  - archive_ids/bundle_ids;
  - number of tasks and bytes;
  - earliest requester time;
  - tier.
- Escalation: reminders after `media_request.remind_after` (e.g. 4 h); auto-fail tasks after
  `media_request.give_up_after` (default 14 d) with a `RestoreFailed{reason: media_unavailable}`
  event.
- If another copy of the data is ONLINE (DP-01), use it instead and don't raise a request.

**Tests:** 50 restores on an offline tape → one MediaRequest; bring the tape online → one job; give-up
path.

### RC-06: Mount amortization and fairness `[M1]` est. 2–3 d

**Depends on:** AR-08.

**Design:**
- After a job finishes, if more Pending tasks exist for the same tape (arrived during the job), the
  executor keeps the drive and runs a follow-up job. This is bounded by `recall.max_mount_secs`
  (default 1,800 s), so other tapes are not starved when drives are scarce.
- `tape_hold_secs` (shared with archive) lets a tape stay mounted while idle for a short time if the
  arrival rate predicts more requests.

**Tests:** a virtual-clock simulation of hot-tape vs cold-tape fairness.

### RC-07: Cache space reservation and admission `[M1]` est. 3 d

**Depends on:** CA-06.

**Design:**
- Before reading a job, the executor calls `cache.ReserveRestored{job_id, bytes, expire_at}` and gets a
  reservation token. The cache evicts according to policy to make room, or rejects with
  `ResourceExhausted{restored_budget_exceeded}`.
- If the job is larger than the free budget, split it so the reservation succeeds partially; the rest
  waits. Never read 200 GiB from tape and then fail on capacity.
- `PutRestored` consumes the reservation. Reservations expire with the job lease.

**Tests:** budget smaller than the job → the job is split; reservation leaks are reclaimed after a crash.

### RC-08: Read errors and alternate copies `[M2]` est. 3–4 d

**Depends on:** DP-01 (for alternates), TP-09 (error classification).

**Design:**

| Error class (from tape worker) | Action |
|---|---|
| Medium error / unrecovered read on a record | Retry once after re-locate; then try another copy (another tape in `tape_set`); then fail the task with `DataLossSuspected` |
| Tape errors across ≥ N records in one job | Mark the tape SUSPECT, emit an event, stop using it for reads if alternates exist, schedule DP-02 verification |
| Drive error (hardware, cleaning required) | Release the drive as ERROR (`OP` event), requeue the job on another drive without counting an attempt |
| Not ready / no medium | WaitingForMedia (RC-05) |

**Tests:** an error-injection matrix on the file-backed simulator (TP-01 supports faults).

### RC-09: SLA, metrics and recall simulation `[M1]` est. 3 d

**Metrics:**

| Metric | Labels |
|---|---|
| `recall_tasks{state}` | state |
| `recall_queue_age_seconds` | tier (histogram) |
| `recall_job_mounts_total` | — |
| `recall_bytes_read_total` | — |
| `recall_records_per_mount` | histogram |
| `recall_time_to_complete_seconds` | tier (histogram) |
| `recall_waiting_for_media_tasks` | — |
| `recall_drive_stall_seconds` | — |

**SLA definition** (README 3.6):
- with D online drives and all media online, the system completes ≥ D restore tasks per 5 minutes on a
  sustained queue;
- p95 time-to-complete for Standard ≤ `merge_window + mount_time + locate + read`.

**Simulation harness:**
- `crates/scheduler/tests/recall_sim.rs`, using the virtual clock.
- Models: drives with mount time ~90 s, locate time proportional to distance (≤ ~60 s average), and
  LTO-9 read rate.
- Generates workloads (uniform, hot-tape, burst) and asserts the SLA and merge efficiency.

**Acceptance:** the simulation shows linear scaling of completed tasks with drive count from 1 to 8 under
a uniform workload.

### RC-10: Restore fast path from staging `[M0]` est. 1–2 d

If the generation is COLD_PENDING and `recall.restore_from_staging = true`, stream staging → restored
cache (cache-local copy, CA-01 `CopyStagingToRestored` RPC to avoid a network round trip), then mark it
Completed. No tape involved.

**Tests:** PUT → immediate restore → GET returns the data while it is still COLD_PENDING.

### RC-11: Restore completion notifications `[M3]` est. 2 d

**Depends on:** OP-01, OP-02.

Emit `ObjectRestoreCompleted`, `ObjectRestoreFailed` and `ObjectRestoreExpired` events per bucket
notification config. Use S3-event-like JSON (`eventName: "s3:ObjectRestore:Completed"`), sent to a
webhook or MQ, so applications don't need to poll HEAD.

---

## Configuration (final shape)

```yaml
recall:
  enabled: true
  merge_window_secs: 60
  bulk_window_secs: 3600
  expedited_enabled: false
  coalesce_gap_mb: 256
  max_job_bytes_gb: 200
  max_job_tasks: 5000
  read_buffer_mb: 256
  max_mount_secs: 1800
  max_attempts: 5
  restore_from_staging: true
  default_days: 1
  max_days: 365
media_request:
  remind_after_secs: 14400
  give_up_after_secs: 1209600
```

## Definition of done for recall

- [ ] Every row of the GW-12 matrix is implemented and tested; defects #4 and #5 closed.
- [ ] 1,000-restore merge test: mounts ≤ tapes touched per window; all bytes verified.
- [ ] Offline-tape scenario (M3 exit test) green.
- [ ] Simulation SLA test in CI; metrics exported.
- [ ] `docs/modules/05-scheduler-layer.md §3` updated.
