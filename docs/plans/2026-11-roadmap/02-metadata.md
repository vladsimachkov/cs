# Plan 02: Metadata Service (`coldstore-metadata`)

**Goal:** README 3.2 (metadata always online), 3.9 (traceable consistency), 4.2, 6 (HA), 7.3.
Build a strongly consistent, durable, replicated catalog of objects, generations, bundles, tapes and
tasks, which is also the work queue for every scheduler. Target scale for v1: 100 M object generations
and 10 k metadata writes/s on a 3-node cluster with NVMe.

## Current state (verified at `c205a5b`)

**State and service:**
- `MetadataState` = a set of `HashMap`s (objects, buckets, bundles, archive/recall tasks, tapes, worker
  registries).
- All mutations go through `MetadataCommand` and `state_machine::apply_command` (good; keep it).
- gRPC service with ~45 RPCs in `metadata.proto`.

**Persistence (`consensus_mode = persistent_raft`, the default):**
- The service applies the command to its in-memory state **first**, then calls
  `LocalSingleNodeMetadataRaft::propose`.
- `propose` appends the log entry **before** validating it, as `format!("{command:?}")` (a Debug string,
  not replayable).
- `log_entry_count` scans the whole log on every write: O(n) per write.
- `propose` re-applies the command to a *second* in-memory state and rewrites the **entire snapshot** to
  RocksDB on every write: O(state) per write.
- No fsync / `WriteOptions::set_sync`.
- The binary snapshot file uses tmp + rename without fsync.

**Other gaps:**
- Raft: none. Only `openraft::BasicNode` is used, and multi-node configs are rejected at startup.
- `ListObjects` scans every object in the HashMap and sorts on each call (O(N log N)).
- Deterministic phase-1 IDs; no object generation → overwrite defect.
- `RestoreExpired` is never applied; Failed and Expired cannot be reopened.
- No leases on tasks; claims use `updated_at` timestamps or status checks.
- Worker-registry RPCs exist, but nothing calls them.

## Target architecture

```
            ┌──────────── MetadataService (tonic) ─────────────┐
 gRPC ────▶ │ read path: linearizable (ReadIndex) or follower  │
            │ write path: Command → Consensus::propose ────────┼──▶ OpenRaft (M4) / LocalLog (M0–M3)
            └──────────────────────────────────────────────────┘          │
                                                        apply(committed)  ▼
                                    StateMachine over RocksDB column families (M4)
                                    (M0–M3: in-memory maps + durable command log + periodic snapshot)
```

A `Consensus` trait hides the backend:

```rust
#[async_trait]
pub trait Consensus: Send + Sync {
    async fn propose(&self, cmd: MetadataCommand, req_id: RequestId) -> Result<ApplyResult, Status>;
    async fn read_barrier(&self) -> Result<(), Status>;   // linearizable read fence
    fn is_leader(&self) -> bool;
    fn leader_hint(&self) -> Option<NodeAddr>;
}
```

Implementations:
- `LocalLogConsensus`: M0, single node, durable.
- `OpenRaftConsensus`: M4.

---

## Tasks

### MD-01: Durable, replayable command log; fix apply ordering `[M0]` `P0` est. 5–7 d

**Design:**
1. **Proto-encode commands.** Add `metadata_command.proto` with
   `message MetadataCommandEnvelope { uint64 request_id_hi, request_id_lo; oneof cmd {...} }`, one arm per
   `MetadataCommand` variant. Add `From`/`TryFrom` conversions and round-trip tests for every variant.
2. **Single writer.** A dedicated tokio task owns the state and RocksDB and receives `(command, oneshot)`
   over an mpsc. This removes the double state (service map vs storage map) and the read-modify-write
   races.
3. **Validate before persisting.** Apply order:
   1. validate against the current state with a dry-run: `apply_command` on a cheap overlay, or split
      `validate(&state, &cmd)` from `mutate(&mut state, cmd)`;
   2. append to the log with `WriteOptions::set_sync(true)` (group-commit: batch all commands queued in
      the channel into one `WriteBatch` with one sync);
   3. mutate the in-memory state;
   4. reply.
   A crash between 2 and 3 is fine (replay).
4. **Log keys:** `raft:log:{u64 BE index}`. Keep `last_index` in a dedicated key (no counting scans).
5. **Snapshots:** every `snapshot_every_commands` (default 10,000) or `snapshot_every_secs` (default
   300). Write the snapshot under `metadata:snapshot:{index}`, record `snapshot_index`, then
   `delete_range` the log entries ≤ the snapshot index.
6. **Startup:** load the latest snapshot, replay log entries > `snapshot_index` in order, verify the
   index continuity, start serving.
7. **Remove the stray snapshot-file path** (`node-N-snapshot.bin`). Keep a one-time import: if the file
   exists and RocksDB is empty, import it and rename the file to `.migrated`.
8. **Migrate the legacy log — this is not optional.** Today `raft_storage.rs:59` writes
   `format!("{command:?}")`, which cannot be decoded back into a `MetadataCommand`. Any node that
   upgrades with entries already in `raft:log:*` therefore hits undecodable records on its first
   replay, and there is no old encoding to write a parser for. Choose one behavior explicitly and test
   it:
   - **refuse** (default): on startup, if a log entry above `snapshot_index` fails to decode, do not
     serve; log the offending index and require an operator action (`coldstore-metadata migrate-log`,
     which snapshots the current in-memory state, records `snapshot_index`, and truncates the legacy
     range). Refusing is safe because the state in memory was already applied — the log is only a
     replay source.
   - **truncate**: decode forward from `snapshot_index`, stop at the first undecodable entry, truncate
     everything above it and log how many entries were dropped. Only acceptable if the dropped entries
     were provably applied to the snapshot (they were: the service applies before persisting).
   Whichever is chosen, the snapshot layout itself does **not** change, so `SNAPSHOT_MAGIC` stays
   `COLDMETA2` (§6.7): the log encoding and the snapshot encoding are independent.
9. **Idempotency table:** `request_id → result`, with LRU by count (1 M) persisted in snapshots. A retried
   command returns the stored result (needed by MD-08).

**Files:** `command.rs`, `raft_storage.rs` (rename to `local_log.rs`), `raft.rs` (→ `consensus/`),
`service.rs`, new `proto/metadata_command.proto`.

**Tests:**
- Every command round-trips proto ↔ enum.
- Restart after N writes reproduces the state (compare an encoded snapshot).
- Crash injection: a `fail_after_log_append` hook, then restart → command applied exactly once.
- An invalid command leaves no log entry.
- **Legacy log:** a fixture with `raft:log:*` entries written in the old `Debug` format makes startup
  take the chosen path (refuse → error naming the index; truncate → drops exactly the legacy range and
  serves), and a node with an empty log is unaffected.
- 100 k writes complete in < 30 s on CI (`--release`, ignored by default, run nightly).
- Snapshot + truncation keep the log bounded.

**Acceptance:** `AGENTS.md` defect #6 closed; write latency independent of state size.

### MD-02: RocksDB column-family data model `[M4]` `P1` est. 10–12 d

**Why:** in-memory HashMaps plus a whole-state snapshot don't reach 100 M objects. Raft snapshots must be
streamable checkpoints.

**Design:** column families per `docs/modules/03-metadata-cluster.md §4`, adjusted for generations
(MD-04):

| CF | Key | Value |
|---|---|---|
| `objects` | `{bucket}\0{key}` | `ObjectHead { current_generation, … }` |
| `generations` | `{object_id}` | `ObjectGeneration` (size, md5, sha256, state, archive copies, restore state, user metadata) |
| `buckets` | `{bucket}` | `BucketInfo` |
| `bundles` | `{bundle_id}` | `ArchiveBundle` (entries stored separately) |
| `bundle_entries` | `{bundle_id}{seq u32 BE}` | `BundleEntry` |
| `tapes` | `{tape_id}` | `TapeInfo` |
| `drives` | `{drive_id}` | `DriveInfo` |
| `archive_tasks` | `{task_id}` | `ArchiveTask` |
| `recall_tasks` | `{task_id}` | `RecallTask` |
| `idx_pending` | `{created_at BE}{object_id}` | ∅ |
| `idx_recall_by_tape` | `{tape_id}\0{tier}{created BE}{task_id}` | ∅ |
| `idx_bundles_by_tape` | `{tape_id}\0{position BE}{bundle_id}` | ∅ |
| `idx_gen_by_bundle` | `{bundle_id}{object_id}` | ∅ |
| `deleted_generations` | `{bundle_id}{object_id}` | `DeletedRecord` |
| `events_outbox` | `{seq BE}` | `Event` (OP-01) |
| `workers` | `{type}\0{node_id}` | `WorkerInfo` |
| `meta` | fixed keys | `applied_index`, `membership`, schema version |

**Rules:**
- The state machine applies each command as **one `WriteBatch`** including `applied_index`. Reads use
  RocksDB directly, through a small LRU cache for hot `objects`/`generations`.
- Snapshot = RocksDB checkpoint (hard links) streamed as a tar to followers (OpenRaft `InstallSnapshot`).
- Schema version key plus a migration framework: `migrations/0001_initial.rs`, …, applied at startup
  before serving.
- Key encoding helpers live in one module with property tests: order preservation, no prefix collisions
  (`\0` separators; bucket names cannot contain `\0`; keys validated).

**Tests:**
- Property test: random command sequences applied to the old in-memory model and to the CF model give an
  identical logical state (differential testing; keep the in-memory model as the test oracle).
- Migration from an MD-01 snapshot.
- 10 M object list/scan benchmark.

### MD-03: Ordered indexes and efficient listing `[M1]` est. 4–5 d

On the in-memory model first; MD-02 carries it over.

**Design:**
- `objects` becomes a `BTreeMap<(bucket, key), ObjectHead>`; `ListObjects` uses `range()` from the
  marker or continuation key.
- Delimiter handling skips over a common prefix by seeking to `prefix + delimiter + 0xFF…`, instead of
  iterating every key under it.
- Pending index (`BTreeSet<(created_at, object_id)>`) for AR-02.
- Recall-by-tape index for RC-03.
- Bundles-by-tape index for recall ordering and compaction.
- New RPCs:
  - `ScanPendingGenerations(after, limit)`: replaces `ScanColdPending` semantics; keep the old RPC
    deprecated.
  - `ListRecallTasksByTape` (exists, re-implement with the index).
  - `ListTapesWithPendingRecalls`.

**Tests:**
- Listing equivalence against a naive implementation (property test).
- Delimiter over 100 k keys with 10 prefixes returns in < 10 ms.

### MD-04: Object generations and unique IDs `[M0]` `P0` est. 5–6 d

**Problem:** IDs are derived from `bucket/key` (`phase1-bundle:{bucket}/{key}`), so overwrite and
re-PUT break (defect #2).

**Design:**
- `ObjectMetadata` gets `string object_id` (UUID v7, time-ordered) and `uint64 version` (entity CAS
  counter).
- Every PutObject creates a new generation.
- **Overwrite of an existing key:**
  - The old generation is moved to `deleted_generations` if it was COLD (it has tape copies to reclaim
    later, DP-03).
  - It is dropped if it was COLD_PENDING; the scheduler deletes its staging data, keyed by `object_id`.
  - All of this happens in one command (`PutObjectGeneration { new, expected_current: Option<object_id>
    }`) so concurrent PUTs to the same key are linearized: last writer wins, as in S3.
- Bundle and task IDs become UUID v7. Remove `phase1_bundle_id` / `phase1_recall_task_id`.
- Cache keys become `object_id` (CA-01). The `(bucket, key)` pair is kept only as descriptive xattrs.
- Restore state lives on the generation. A restore request targets the current generation.
- **Migration:** at startup, assign `object_id` to legacy objects (deterministic UUID v5 of
  bucket/key/created_at, so replay is stable). Legacy bundles keep their IDs.

**Tests:**
- PUT → archive → PUT same key → archive → both generations are traceable; the current one is COLD with
  a new bundle.
- Concurrent PUTs of the same key from 8 tasks → exactly one current generation; the others are recorded
  as superseded.
- Delete then re-PUT works.
- Migration test from a `c205a5b` snapshot fixture (commit the fixture file).

### MD-05: Restore lifecycle completeness `[M0]` `P0` est. 3 d

**Design:**

Transition table (enforced in `validate_restore_transition`):

```
None/Expired/Failed ──request──▶ Pending
Pending ──claim──▶ InProgress
Pending ──media offline──▶ WaitingForMedia
WaitingForMedia ──media online──▶ Pending
WaitingForMedia ──claim──▶ InProgress
InProgress ──retriable error──▶ Pending (attempt++)
InProgress ──offline──▶ WaitingForMedia
InProgress ──done──▶ Completed(expire_at)
InProgress ──fatal / attempts exhausted──▶ Failed
Completed ──extend──▶ Completed(expire_at')
Completed ──expiry──▶ Expired
Completed ──cache lost (reported)──▶ Expired
```

New commands:
- `ExpireRestores { now, limit }`: applied by a periodic sweeper, every 60 s. It is a command, not a
  timer inside the state machine, so it stays deterministic under Raft.
- `ExtendRestore { object_id, new_expire_at }`.
- `ReportRestoredCopyMissing { object_id }`: the scheduler calls it when the cache has no copy despite
  Completed.

The sweeper runs on the leader (or in a scheduler with leader-only election via a lease, MD-06) and emits
`RestoreExpired` events.

**Tests:** a table-driven transition test (every pair: allowed or rejected); sweeper expiry; GET after
expiry → InvalidObjectState; re-restore works (defect #4 closed).

### MD-06: Leases, versions and generic CAS for tasks `[M0]` `P0` est. 4–5 d

**Design:**
- `ArchiveTask` and `RecallTask` gain:
  - `uint64 version`;
  - `optional string lease_owner`;
  - `optional Timestamp lease_expires_at`;
  - `uint32 attempt`;
  - `uint32 max_attempts`;
  - `optional Timestamp not_before` (backoff).
- New commands:
  - `ClaimTask { kind, task_id, owner, lease_ttl, expected_version }`: succeeds if the task is claimable
    (Pending, or InProgress with an expired lease, or Failed with `attempt < max_attempts` and
    `not_before ≤ now`).
  - `RenewLease`.
  - `CompleteTask`.
  - `FailTask { retriable, backoff }`: computes `not_before = now + min(base·2^attempt, max)`.
- `now` is passed in the command by the proposer. The state machine never reads the clock (Raft
  determinism). To guard against skew, reject commands whose `now` differs from the leader's clock by more
  than 30 s.
- Scheduler-side `LeaseKeeper` renews every `ttl/3`. If renewal fails, the worker aborts the job
  (cancellation token) before writing results.
- **Fencing:** every completion command includes `(task_id, lease_owner, version)`. A stale owner gets
  `FailedPrecondition` and must not delete staging data (prevents a zombie worker from destroying data).
- The generic entity `version` replaces `expected_updated_at` timestamp comparisons in
  `CompleteArchiveObject`. Keep the field for one release.

**Tests:**
- A worker dies mid-task → after the TTL another worker claims it → the first worker's late completion is
  rejected.
- The backoff schedule.
- `max_attempts` exhausted → Failed and an event emitted.

### MD-07: OpenRaft cluster `[M4]` `P1` est. 12–15 d

**Spike first** (2 d, timeboxed):
- Compare openraft 0.9.x (stable) and 0.10 alpha APIs.
- Pin exactly (`=x.y.z`). Record the decision in `docs/adr/0001-openraft-version.md`.

**Design:**
- Implement `RaftLogStorage` on the RocksDB `raft_log` CF (from MD-01), with vote persisted synchronously.
- Implement `RaftStateMachine` on the MD-02 CF model; `applied_index` and membership are stored
  atomically with each batch.
- Network: `raft.proto` (`AppendEntries`, `Vote`, `InstallSnapshot` streaming chunks) over tonic. Use a
  separate port, `metadata.raft_listen`, so raft and client traffic can be isolated.
- **Bootstrap:** `coldstore-metadata init --node-id 1` on one node, then `add-learner` and
  `change-membership` via the admin CLI (OP-05), or an automatic bootstrap when all `cluster` peers are
  reachable and every store is empty (guard with an explicit `bootstrap: true` flag).
- **Writes on followers:** return `Unavailable` with a `leader-hint` metadata header; the client
  retries (MD-08). Optional forwarding later.
- **Reads:**
  - default linearizable via `ensure_linearizable()` (ReadIndex);
  - `consistency=stale` allowed for listing and admin dashboards.
- Snapshots: build from a RocksDB checkpoint, transfer in streamed chunks, install by swapping the CF
  directory atomically.
- Remove the startup rejection of multi-node configs once this is green.

**Tests:**
- In-process 3-node cluster on loopback: leader election, a write on the leader is visible on followers
  after a read barrier.
- Kill the leader → a new leader within 2× election timeout; no acknowledged write is lost.
- Snapshot install for a lagging follower.
- Membership change 3→5→3.
- A deterministic simulation (turmoil, PL-07) with partitions and random kills over 10 k operations,
  checked by a linearizability checker (porcupine-style history check on a register model).

### MD-08: Metadata client library `[M4]` est. 3–4 d

**Design:**
- `coldstore-metadata-client` crate used by the scheduler, cache, tape, gateway (when it needs metadata)
  and admin tools.
- Knows all peers; tracks the leader from hints; retries with jittered backoff on
  `Unavailable`/not-leader.
- Attaches a `request_id` to every mutating call (idempotent retry, MD-01 table).
- Typed wrappers instead of raw protos for the common flows (`claim_task`, `complete_archive_bundle`, …).
- Timeouts per call class.

**Tests:** a leader change in the middle of 1,000 writes → every write applied exactly once.

### MD-09: Worker registry, heartbeats and discovery `[M4]` est. 4 d

**Design:**
- Workers call `RegisterXWorker` at startup and `Heartbeat` every 5 s, reporting capacity:
  - cache: free bytes and budgets;
  - tape: drives and their states;
  - scheduler: running jobs.
- No heartbeat for 20 s → `NODE_OFFLINE` (sweeper command).
- The scheduler discovers cache and tape workers from metadata (`docs/modules/03 §3.10`); static
  addresses remain as a fallback for single-node mode.
- **Draining:** `DrainWorker` → no new work is assigned; the cache worker drains staging (CA-04) before
  it can be removed.

**Tests:** register/expire/drain lifecycle; the scheduler stops routing to an offline worker within one
heartbeat interval.

### MD-10: Tape, drive and append-position registry `[M1]` `P0` est. 4–5 d

**Design:**
- `TapeInfo` becomes authoritative for:
  - status (ONLINE / OFFLINE / ERROR / RETIRED / FULL / SUSPECT — add FULL and SUSPECT to the enum);
  - pool (`pool_id`);
  - generation/format (`LTO-9`, `LTO-10`, `SIM`);
  - capacity (from the drive);
  - `used_bytes`;
  - **append position** (`next_file_number`, `next_block`);
  - location (slot, drive, vault + shelf text);
  - error counters;
  - `last_verified_at`;
  - `write_protected` / WORM flags.
- New `DriveInfo { drive_id, worker_id, library_id, serial, device_path, status, loaded_tape,
  cleaning_required, error_count }`.
- **Commands:**
  - `ReserveAppend { tape_id, owner, bytes }`: exclusive append lease, CAS on the append position.
  - `CommitAppend { tape_id, new_position, used_bytes }`.
  - `MarkTapeFull`.
  - `SetTapeStatus`.
- The tape worker is never the source of truth for positions. It verifies the drive position equals the
  metadata position before writing (TP-02 / AR-07); a mismatch makes the tape SUSPECT.

**Tests:** two archivers reserving the same tape → one wins; a position mismatch is detected; FULL tapes
are excluded from selection.

### MD-11: Metadata backup and disaster recovery `[M4]` est. 4 d

**Design:**
- `coldstore-metadata backup --to <dir|s3-url>` streams a consistent checkpoint plus manifest
  (index, checksum).
- Periodic backups via config, with retention N.
- `restore --from` rebuilds a single-node cluster, then adds members.
- **Catalog-on-tape** (with AR-04): each bundle trailer contains its entries, so the catalog can be
  rebuilt from tapes alone (DP-05). Document the RPO for each mechanism.

**Tests:** backup → wipe → restore → identical logical state (differential check).

### MD-12: Performance and scale validation `[M4]` est. 3–4 d

**Targets** (3 × NVMe nodes, release build):
- ≥ 10 k single-object PUT commands/s with group commit;
- p99 write latency < 20 ms;
- `ListObjects` of 1,000 keys < 50 ms at 100 M generations;
- leader failover < 5 s;
- snapshot install of a 50 GB state machine < 15 min.

**Deliverables:** a `coldstore-bench metadata` subcommand (PL-08), results committed to
`docs/benchmarks/metadata.md`, and regression thresholds in the nightly job.

### MD-13: S3 object versioning (optional) `[M6]` est. 8–10 d

Generations already exist, so versioning means exposing them:
- `version_id = object_id`;
- delete markers;
- `ListObjectVersions`;
- `GetObject?versionId`, restore per version;
- `PutBucketVersioning`.
Only do this if a user asks; it interacts with compaction (DP-04) and WORM (DP-07).

---

## Definition of done for metadata

- [ ] No full-state work on the write path; writes are durable (fsync) before ack.
- [ ] Every mutation is a proto-encoded command; replay reproduces the state exactly.
- [ ] Generations, leases and restore lifecycle are covered by table and property tests.
- [ ] 3-node OpenRaft cluster passes the deterministic simulation and the failover soak.
- [ ] `docs/modules/03-metadata-cluster.md` updated (CF table, commands, consistency levels).
