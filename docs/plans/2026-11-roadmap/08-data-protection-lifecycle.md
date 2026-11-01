# Plan 08: Data Protection and Cold-Data Lifecycle

**Goal:** README 3.8, 3.9, 6 (compliance), 7.8:
- configurable reliability: dual-tape copies, with tape-level EC as an option;
- updates and deletes over the lifecycle, with tape space reclaimed;
- at least one full maintenance cycle per year;
- verifiable, traceable consistency between metadata and tape;
- WORM and retention.

## Current state (verified at `c205a5b`)

**None of this exists yet:**

| Area | State |
|---|---|
| Copies | `tape_set` and `ArchiveBundle.tape_set` exist in proto, but archive writes one copy |
| Delete / overwrite | Removes metadata only; tape bytes are never accounted or reclaimed |
| Maintenance | No verification, scrub, compaction or audit |
| Compliance | No WORM, retention or legal hold |

**Prerequisites from other plans:**
- generations (MD-04);
- bundle commit with copies (AR-06);
- the self-describing tape format with trailer catalog (AR-04);
- tape pools and positions (AR-07, MD-10);
- verify/scan primitives (TP-10);
- events (OP-01).

## Concepts

| Term | Definition |
|---|---|
| **Pool** | Set of tapes with a purpose (`primary`, `copy2`, `scratch`, `worm-primary`, …) and a media constraint |
| **Protection policy** | Per bucket (default global): `{copies: [pool_a, pool_b], min_copies_for_cold: 1\|2, ec: none\|{k,m,pool}}` |
| **ArchiveCopy** | `(object_id, bundle_id, tape_id, addressing, copy_index, written_at, verified_at)`. A generation has 1..n copies |
| **Live bytes** | Per bundle and tape, the bytes belonging to current (not deleted/superseded) generations. Dead bytes = written − live |

---

## Tasks

### DP-01: Dual-tape copies `[M5]` `P0` est. 8–10 d

**Design:**
- **Policy per bucket.** `PutBucketProtection` admin API; default from config.
- **Synchronous mode** (`min_copies_for_cold = 2`):
  - The planner (AR-03) creates *two* bundle tasks for the same bundle content, `copy_index 0` and `1`,
    in different pools. They are written by different drives in parallel, from the same staging data.
  - The generation becomes COLD only when both copies are committed (AR-06 `CommitArchiveBundle` adds
    one copy; the state machine flips to COLD when copies ≥ min).
  - Staging is released only then.
  - Costs one extra drive during archive.
- **Asynchronous mode** (`min_copies_for_cold = 1`, `copies = 2`):
  - Copy 0 is written as today.
  - A `CopyJob` for copy 1 is created for each committed bundle. It reads from staging if staging still
    exists (`staging_retain_until_copies: true`), otherwise tape-to-tape: the recall-style reader feeds
    the archive writer on a second drive, with a buffer.
  - Until copy 1 exists, the bundle is "under-protected" (metric + alert if older than `copy_sla`).
- **Placement constraints:**
  - The two copies must be on different tapes. Different libraries when `copies[].library_affinity`
    differs (README 7.8 "across libraries").
  - copy2 pools can be vaulted (OP-04 auto-export).
- **Recall** uses any ONLINE copy, preferring a mounted tape, then the closest library (RC-08 fallback).
- **Metadata:**
  - `ArchiveBundle` gains `copy_group_id`.
  - Generations store `copies[]`.
  - Migrate the single `tape_id` field into `copies[0]`.

**Tests:**
- Sync mode: kill a drive mid-copy-1 → the generation stays COLD_PENDING; staging is kept; retry
  succeeds.
- Async mode: tape-to-tape copy verified by SHA-256.
- A recall falls back when copy 0 is OFFLINE.
- The under-protection alert fires.

### DP-02: Verification (scrub) scheduling `[M5]` `P0` est. 5–6 d

**Design:**
- A `VerifyJob` per tape (or per bundle range) uses `TapeService.VerifyBundle` (TP-10) on the tape
  worker. Checksums are verified next to the drive, with no data shipped over the network.
- **Scheduler** (`maintenance.verify`):

  ```yaml
  maintenance:
    verify:
      cycle_days: 365          # README 3.9: at least one full cycle per year
      window: "Sat 00:00-Sun 23:59"   # optional maintenance windows
      max_drives: 1
      priority: below_bulk_recall
      after_write_days: 30     # first verification of new tapes sooner
  ```

  - Each day, the planner computes which tapes are due (`last_verified_at + cycle < now + slack`). It
    spreads the work so the cycle completes in time.
  - It **checks feasibility**: total bytes / (drive read rate × available drive-hours per cycle). For
    example, 1 PB at 300 MB/s ≈ 39 drive-days per year, which is feasible with 1 drive at ~11 %
    utilization.
  - If the cycle is infeasible, it emits a `MaintenanceBehind` warning with the numbers.
- **Results:**
  - Per bundle: `verified_at` and `verify_errors`.
  - On a record failure: mark that copy BAD. If another good copy exists, schedule **repair**: re-copy from
    the good copy to a new bundle (DP-01 machinery). Otherwise emit `DataLossSuspected` (critical).
- Tapes with rising soft-error rates (TapeAlert, TP-09) are moved forward in the schedule and
  potentially migrated (DP-08).

**Tests:** a virtual-clock simulation of a year with 200 tapes → every tape is verified within its cycle;
corruption injected into the file simulator → repair from copy 1.

### DP-03: Delete and overwrite accounting `[M5]` `P0` est. 3–4 d

**Depends on:** MD-04.

**Design:**
- When a COLD generation is deleted or superseded, the metadata command moves it to
  `deleted_generations {object_id, bundle_id(s), tape_id(s), bytes, deleted_at}`. It decrements
  `bundle.live_bytes` and `tape.live_bytes` (per copy), and increments the dead bytes.
- Restored-cache entries of deleted generations are dropped (CA-03 janitor).
- A **purge** of deleted-generation records happens only after compaction has rewritten or abandoned
  their tapes. This keeps "traceable consistency" (README 3.9): every byte on every tape is attributable.
- Admin API: `GET /v1/tapes/{id}/utilization` → live/dead/total bytes and bundles.

**Tests:** accounting invariants under random PUT/overwrite/delete sequences. Property: for each tape,
the sum of live bytes equals the sum over current generations' copies.

### DP-04: Compaction / repack `[M5]` est. 8–10 d

**Depends on:** DP-03, AR-06, RC-04, AR-08.

**Design** (`docs/modules/05 §8.3`, made concrete):
1. **Selection** (daily): tapes with `live/written < compaction_threshold` (default 0.3) **and** FULL,
   excluding tapes with WORM retention still active (DP-07). Order by reclaimable bytes per mount.
2. A `CompactionJob(tape)` on two drives:
   - **source:** sequential read of the live records only (RC-04 reader with a record filter);
   - **destination:** the archive writer into new bundles on a pool tape.
   - Prefer the source and destination in the same library; buffer between them.
3. **Commit:** a `RelocateCopies` command per new bundle. For each record it re-validates that the
   generation is still current and its old copy is still on the source tape (CAS), then atomically swaps
   the copy location. Generations deleted mid-compaction are simply not relocated.
4. When the source tape has no live copies, mark it `RECLAIMABLE`, purge its deleted-generation records,
   and either:
   - **reuse:** relabel as scratch. The next write starts at BOT after the label, which logically erases
     the old data. Only after a `reclaim_grace_days` hold, for safety.
   - **retire** it (media lifecycle, DP-08).
5. Compaction is preemptible between bundles. It has lower priority than recall and archive, except
   when the scratch pool is low (`ScratchLow` raises its priority).

**Tests:**
- A tape with 70 % deleted data → compacted.
- Concurrent overwrite during compaction is handled.
- A crash at each step → no lost or duplicated copies.
- Data is still readable from the new location.

### DP-05: Consistency audit and DR rebuild from tape `[M5]` `P0` est. 6–8 d

**Design:**
- **Audit job (metadata ⇔ tape):**
  1. For a sample (or all) of the tapes, `ScanTape` reads the label and the bundle headers and trailers
     (cheap: trailers only).
  2. Compare with metadata bundles and entries: missing bundles, extra bundles beyond the committed
     position, entry mismatches (size/sha), and position drift.
  3. Output an `AuditReport` (stored and exposed via the API) plus `AuditMismatch` events.
  4. Scheduled: full trailers-only audit every quarter, deep verify per DP-02.
- **DR rebuild tool:** `coldstore-admin dr rebuild-catalog --from-tapes <inventory>`.
  1. Scan all tapes.
  2. Rebuild buckets, generations, bundles, copies and tape positions into a fresh metadata store, from
     the labels, object headers and trailers (they contain bucket, key, object_id, size, sha256,
     user metadata).
  3. Conflict resolution for multiple generations of one key: the newest `object_id` (UUID v7 time
     ordering) wins; older ones become deleted records.
  4. The output is a metadata snapshot importable by MD-11.
- **Traceability document:** `docs/ops/consistency.md` explains which invariant guarantees what, and how
  to prove consistency to an auditor.

**Tests:** create a 10 k-object dataset with overwrites and deletes, wipe metadata, rebuild from the
file-simulator tapes → identical logical catalog (current generations, copies), checked with a
differential comparator.

### DP-06: Tape-level erasure coding (optional) `[M5+]` est. design 3 d; implementation 15+ d

**Do the design spike first**, then decide whether to implement.

**Model:** EC across tapes at bundle granularity:
- k data bundles of equal padded size plus m parity bundles (Reed-Solomon, `reed-solomon-simd`
  crate), written to k+m different tapes;
- a stripe record in metadata.

**Costs and risks to analyze:**
- Writes need k+m concurrent drives, or staging of all k bundles before parity.
- Degraded reads need k mounts. Recall latency explodes for offline media, so EC is only sensible within
  a single large library with many drives.
- Repair is heavy.
- Compare with dual copy on cost per TB for the expected scale.

**Deliverable of the spike:** `docs/adr/00xx-tape-ec.md` with a recommendation. The likely
recommendation is to support EC only for large single-library deployments and keep dual copy as the
default.

### DP-07: WORM, retention and legal hold `[M5]` est. 6–8 d

**Design:**
- S3 Object Lock subset:
  - bucket-level `ObjectLockEnabled` at create time;
  - default retention (GOVERNANCE/COMPLIANCE, days/years);
  - per-object `PutObjectRetention`, `PutObjectLegalHold`, and the corresponding Get APIs (gateway
    routes `?retention`, `?legal-hold`, `?object-lock`).
- Enforced in the metadata state machine:
  - DeleteObject and overwrite of a locked generation → `AccessDenied` (S3 semantics: an overwrite
    creates a new version only with versioning enabled; without MD-13, reject the overwrite);
  - GOVERNANCE can be bypassed by an admin principal with `x-amz-bypass-governance-retention`
    (audited, OP-06).
- Compaction (DP-04) must preserve retention: it may relocate copies, never drop them. A tape with
  locked data can only be retired after relocation.
- **WORM pools:** pools whose media are WORM cartridges (TP-05 detects them). Compliance buckets can be
  bound to WORM pools. Compaction never reuses WORM tapes; it only retires them after migration.

**Tests:** delete/overwrite denied while locked; governance bypass; retention expiry allows delete;
compaction keeps locked data.

### DP-08: Media lifecycle and generation migration `[M5]` est. 4–5 d

**Design:**
- **Retirement criteria** (configurable):
  - error counts (TP-09);
  - TapeAlert "media life";
  - load count (MAM attribute, if the drive exposes it);
  - age.
- Retirement = migrate (compaction with a 0 % threshold) → RETIRED → export with a "destroy/return"
  instruction.
- **Generation migration** (e.g. LTO-9 → LTO-10):
  1. Add an LTO-10 pool and set the bucket policy target.
  2. A migration job (same machinery as DP-04) moves bundles from LTO-9 tapes. It is rate-limited and
     scheduled in maintenance windows.
  3. LTO-9 tapes are retired when empty.
  4. Before planning, check the drive/media compatibility matrix as reported by the drives (TP-05). Do not
     assume backward-read compatibility rules from memory; qualify them.

**Tests:** a virtual-clock simulation of migrating 100 tapes; error-driven retirement.

---

## Definition of done for data protection and lifecycle

- [ ] Dual copy (sync and async) with fallback recall and under-protection alerts.
- [ ] A yearly verification cycle is planned, feasibility-checked and simulated; repair from a good copy
  works.
- [ ] Delete and overwrite accounting invariant holds (property tests); compaction reclaims space safely.
- [ ] Audit report plus DR rebuild from tapes reproduce the catalog.
- [ ] Object Lock subset enforced end to end (gateway → metadata → compaction).
- [ ] ADR for EC written.
