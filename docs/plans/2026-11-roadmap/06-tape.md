# Plan 06: Tape Layer (`coldstore-tape`, `coldstore-vtl`)

**Goal:** README 3.10, 4.2 "Tape Management", 7.7, 7.8:
- vendor-neutral support for real tape libraries and drives, at minimum LTO-9 and LTO-10;
- a persistent, fault-injectable simulator for development and CI;
- mhVTL as the CI proxy for real hardware;
- media lifecycle (onboarding, labels, capacity, cleaning, errors);
- the primitives the schedulers need: positioned streaming writes and reads, inventory, import/export.

## Current state (verified at `c205a5b`)

**Tape service:**
- `TapeServiceImpl` always uses `SimulatorTapeBackend`. It is **in-memory only**, so restarting loses
  all "tape" data (defect #3).
- `next_filemark` is tracked **per drive**, not per tape.
- Capacity is hardcoded at 12 TiB.
- `TapeBackend` is a synchronous trait. Operations: list/acquire/release drives, load/unload, rewind,
  seek to filemark, write bundle (whole `&[u8]`), read bundle (filemark, length), inventory.
- `ReadBundle` by block offset → `Unimplemented`.

**VTL crate:**
- `coldstore-vtl` has a pure in-memory library model, `lsscsi -g` parsing, and command builders for
  `mtx`, `mt`, `sg_inq`, `sg_turs`, `sg_logs`, `sg_modes`, `dd` and `vtlcmd`.
- It has `SystemCommandRunner` and `RecordedCommandRunner`, but **none of it is wired into
  `TapeService`**.
- `scripts/setup-mhvtl-env.sh` is a dry-run-by-default installer.

**Missing:** no LTFS, no SG_IO, no MTIO ioctls, no TapeAlert, no labels, no density or capacity
detection, no import/export handling.

## Target architecture

```
tape worker (one per host with attached drives/changer)
  gRPC TapeService v2
    ├─ LibraryManager  ── Changer backend: Sim | File | Mtx(CommandRunner) | SgIo (later)
    ├─ DriveManager    ── Drive backend:   Sim | File | LinuxSt(/dev/nstX + sg side-channel) | SgIo (later)
    │     └─ per-drive actor thread (blocking I/O on a dedicated OS thread)
    ├─ MediaService    ── labels, capacity, density, WORM, cleaning, onboarding
    └─ Health/TapeAlert poller
```

**Traits** (`docs/modules/06-tape-layer.md §3` refined):

```rust
pub trait Changer: Send + Sync {
    fn inventory(&self) -> Result<Inventory>;                 // slots, drives, ie-slots, barcodes, drive serials
    fn move_medium(&self, from: Element, to: Element) -> Result<()>;
    fn init_element_status(&self) -> Result<()>;
}
pub trait Drive: Send {
    fn status(&mut self) -> Result<DriveStatus>;              // loaded, bot/eod/ew, write_protected, worm, density, cleaning
    fn load(&mut self) -> Result<()>; fn unload(&mut self) -> Result<()>;
    fn rewind(&mut self) -> Result<()>;
    fn position(&mut self) -> Result<TapePosition>;           // {file_number, lba}
    fn locate(&mut self, lba: u64) -> Result<()>;
    fn space_filemarks(&mut self, n: i64) -> Result<()>;
    fn seek_eod(&mut self) -> Result<()>;
    fn write_block(&mut self, buf: &[u8]) -> Result<WriteOutcome>;   // Ok | EarlyWarning
    fn write_filemarks(&mut self, n: u32, sync: bool) -> Result<()>;
    fn read_block(&mut self, buf: &mut [u8]) -> Result<ReadOutcome>; // Data(n) | Filemark | Eod
    fn capacity(&mut self) -> Result<Capacity>;              // remaining/maximum from LOG SENSE
    fn tape_alerts(&mut self) -> Result<TapeAlertFlags>;
}
```

---

## Tasks

### TP-01: Persistent, fault-injectable file-backed simulator `[M0]` `P0` est. 5–6 d

**Why:** it fixes defect #3 for dev and single-node demos, and gives CI real tape semantics:
- append-only;
- writing truncates everything after;
- filemarks, EOD, early warning;
- positioning.

**Design:**
- `FileTapeBackend` implements the new `Drive` and `Changer` traits (written ahead of TP-02; TP-02
  generalizes them).
- Library state lives in `<root>/library.json` (slots, drives, I/E slots, barcodes), written atomically
  with fsync.
- Each tape is a directory `<root>/tapes/<barcode>/` with:
  - `data.bin`: an append-only stream of length-prefixed blocks and filemark markers;
  - a sidecar `index.bin`: LBA → byte offset, rebuilt if missing;
  - `meta.json`: capacity, EW threshold, WORM flag.
- **Semantics:**
  - A write at position P truncates `data.bin` after P.
  - EW is reported when `used ≥ capacity − ew_margin`.
  - ENOSPC at capacity.
  - A filemark write with `sync = true` fsyncs.
  - A read past the last data returns EOD.
- **Speed modes:**
  - `realtime`: simulated mount 60–90 s, locate proportional to distance, rate cap (e.g. 400 MB/s); used
    for SLA simulation.
  - `fast`: no delays; used by tests.
  - `null-sink`: discards data, keeps positions; used to bench the pipeline without the disk.
- **Fault injection** (config or admin RPC):
  - `fail_write_at_lba`, `fail_read_lba`, `corrupt_block_lba`;
  - `drive_offline_after_ops`, `mount_failure`, `stuck_in_drive`;
  - `ew_at_bytes`.
- Default backend for `coldstore-node` becomes `file`; `sim` (in-memory) stays for unit tests.
- Config:

  ```yaml
  tape:
    backend: file
    file: { root, slots, drives, tapes: [{barcode, capacity_gb}], speed: fast|realtime }
  ```

**Tests:**
- Restart persistence: write, restart, read back.
- Truncation semantics.
- EW and ENOSPC.
- Each fault mode.
- The existing simulator service tests are ported.

**Acceptance:** the M0 exit test passes with `backend: file`.

### TP-02: Tape service API v2 (positioned streaming sessions) `[M1]` `P0` est. 6–8 d

**Design (proto `tape.proto` v2):**

| RPC | Purpose |
|---|---|
| `Mount(tape_id, drive_id?)` → `{drive_id, position}` | Changer move plus load; waits for ready with timeout `tape.load_timeout_secs` (default 7,200 s; covers LTO-9 first-load media optimization) |
| `Unmount(drive_id, to_slot?)` | |
| `OpenWrite(drive_id, tape_id, expected_position, expected_label)` → `session_id` | Verifies the label and that the position equals expected (seeking to the committed position = EOD of committed data). A mismatch → `FailedPrecondition{position_mismatch}` |
| `WriteStream(stream {session_id, data chunks…, finish{write_filemark: sync}})` → `{start_lba, end_lba, file_number, bytes, ew_reached}` | The tape worker re-blocks to `block_size` |
| `ReadStream(drive_id, tape_id, start_lba, length_hint, stop_at_filemark)` → stream of chunks | |
| `Locate`, `Position`, `Status`, `Inventory`, `Capacity`, `TapeAlerts` | |
| `Label(drive_id, tape_id, label)` | Only on scratch or blank media |
| `Import(ie_slot → slot)`, `Export(slot → ie_slot)` | Vault workflows (OP-04) |

**Rules:**
- **Session ownership:** sessions are bound to a `DriveLease` id from the scheduler. The tape worker
  rejects operations on a drive from a different lease id, which fences zombie schedulers.
- **Per-drive actor:** a dedicated OS thread owns the device fd. Commands flow over a channel, so
  blocking ioctls never block tokio.
- Remove the old `WriteBundle`/`ReadBundle`/filemark API after the scheduler migrates (one release of
  overlap).

**Tests:** contract tests run against `SimBackend` and `FileBackend` (the same test suite, generic over
the backend), and later against mhVTL (TP-08).

### TP-03: Linux SCSI tape drive backend (`st` driver) `[M2]` `P0` est. 8–10 d

**Design:**
- Open `/dev/nstN` (non-rewinding) with `O_RDWR`.
- `MTIOCTOP` operations, via `nix::ioctl_*` macros:

  | Operation | Use |
  |---|---|
  | `MTSETBLK` | Fixed block mode with `block_size` validated against READ BLOCK LIMITS (`sg_raw` or SG_IO side channel) |
  | `MTWEOF` | Synchronous filemark at bundle end (needed before the metadata commit) |
  | `MTWEOFI` | Buffered filemark, only for non-commit points |
  | `MTFSF`, `MTBSF`, `MTEOM`, `MTREW`, `MTOFFL` | Spacing, end of media, rewind, unload |
  | `MTSEEK` | LOCATE to a logical block |
  | `MTIOCPOS` | READ POSITION |
  | `MTIOCGET` | Status bits: `GMT_BOT`, `GMT_EOD`, `GMT_EOT`, `GMT_WR_PROT`, `GMT_ONLINE`, `GMT_DR_OPEN` |

- Drive buffering options via `MTSETDRVBUFFER` (`MT_ST_BUFFER_WRITES`, `MT_ST_ASYNC_WRITES`) for
  streaming. Durability comes from the synchronous filemark.
- Early warning: the `st` driver reports EOM early warning through a short write or `ENOSPC` and status
  bits. Map that to `WriteOutcome::EarlyWarning`. The scheduler closes and aborts the bundle (AR-07).
  Leave the remaining bytes as a margin; never write to physical EOM.
- **SG side channel** (`/dev/sgN` of the same drive, mapped by `lsscsi -g`) for things `st` doesn't
  expose:
  - REQUEST SENSE details after errors;
  - LOG SENSE TapeAlert page `0x2E`;
  - Tape Capacity page `0x31`;
  - MODE SENSE density code and WORM.
  Implement it with SG_IO ioctls in Rust (small `sgio.rs`) rather than shelling out per call. Keep the
  `sg_*` command builders for diagnostics only.
- Large I/O: buffers allocated aligned to `block_size`; read and write one block per syscall in fixed
  mode. HBA max transfer limits are checked at startup (`/sys/block/.../queue/max_sectors_kb` or READ BLOCK
  LIMITS).

**Tests:**
- Unit tests with a fake ioctl layer (trait `MtIo` with an in-memory implementation).
- The contract suite (TP-02) runs against mhVTL in the lab job.
- Manual qualification on hardware (TP-08).

### TP-04: Changer backend `[M2]` est. 5–6 d

**v1 via `mtx`:**
- Uses the existing `coldstore-vtl` command builders and `SystemCommandRunner`.
- Parse `mtx status` output (data transfer elements, storage elements, import/export elements,
  barcodes/volume tags).
- Operations: `load`, `unload`, `transfer`, and import/export via `transfer` to/from the IE element.
- Robust parsing: a fixture corpus of `mtx status` outputs from mhVTL and from real libraries of
  different vendors (collect them during qualification). Libraries format output differently.

**Drive ↔ device mapping** (critical; mistakes mean writing to the wrong drive):
- `mtx`/READ ELEMENT STATUS with drive identifiers (DVCID) gives drive serials, and INQUIRY VPD `0x80` on
  each `/dev/sgN` gives device serials. Match them.
- If DVCID is unsupported, require an explicit mapping in config (`drives: [{element: 0, device:
  /dev/nst0, serial: …}]`) and verify the serial at startup. Refuse to start on a mismatch.

**v2 (later):** a native SG_IO changer (READ ELEMENT STATUS `0xB8`, MOVE MEDIUM `0xA5`, INITIALIZE
ELEMENT STATUS `0x07`) to remove the `mtx` dependency and get proper sense handling.

**Inventory caching:** a full inventory can take minutes on large libraries. Cache it, and refresh after
moves and on operator request (OP-04). `init_element_status` only on demand.

**Tests:** parser fixtures; mapping logic; mhVTL contract tests.

### TP-05: Media management and onboarding `[M2]` est. 5–6 d

**Lifecycle:**

```
discovered (barcode in inventory, unknown) → [onboarding job] → labeled/scratch → assigned to pool
→ ONLINE/appending → FULL → (compaction) → scratch/erase → … → RETIRED
```

**Onboarding job** (runs in maintenance windows or on demand):
1. Mount.
2. Wait for ready. LTO-9 cartridges perform **media optimization on first load**, which can take from
   tens of minutes up to about two hours. Run it here, not during an archive.
3. Read the existing label. If it is a ColdStore label for this cluster, reconcile with metadata. If it
   is foreign data, quarantine: never overwrite unless an operator explicitly approves "wipe".
4. Read the density, generation, capacity and WORM status from the drive.
5. Write the VolumeLabel (AR-04) and register `TapeInfo` (MD-10).

**Generation and density:**
- Map the MODE SENSE density code to a generation in one table (`media.rs`).
- Populate it from REPORT DENSITY SUPPORT output captured during qualification. For example LTO-9
  reports 0x60. **Verify LTO-10 codes on real drives; do not hardcode from memory.**
- Reject media/drive combinations the drive reports as unsupported.

**Capacity:** take `maximum` and `remaining` from the LOG SENSE capacity page. Never use the
configured or hardcoded value (remove the simulator's 12 TiB constant).

**Cleaning:**
- TapeAlert flags "clean now" and "clean periodic" → the drive is marked `cleaning_required`.
- The library auto-cleans, or ColdStore loads a cleaning cartridge (barcode prefix `CLN` configurable),
  tracks cleaning cartridge uses, and alerts on an expired cleaner.

**WORM** media is detected and flagged; such tapes can only belong to WORM pools (DP-07).

### TP-06: Drive manager inside the tape worker `[M2]` est. 4 d

- Drive state machine: `EMPTY → LOADING → READY(tape) → BUSY(session) → UNLOADING → EMPTY`, plus
  `ERROR` and `CLEANING`.
- Timeouts:
  - load (including media optimization, see TP-05);
  - locate 600 s;
  - rewind 600 s;
  - I/O idle 120 s.
  A timeout → ERROR + event.
- Health poller every 60 s for idle drives: TEST UNIT READY and TapeAlert.
- Exposes drive states to metadata (heartbeat, MD-09) and to the arbiter (AR-08).
- The scheduler's `DriveLease` is honored here (fencing, TP-02).

### TP-07: Multi-host topology `[M4]` est. 3–4 d

- Several tape workers, each owning a set of drives (possibly in a shared library). Exactly one worker
  owns the changer (`changer_owner: true`). Other workers request moves through it via gRPC
  `RequestMove`, which serializes robot operations.
- Registration: drives and their library are registered in metadata with `worker_id`. The scheduler
  routes sessions to the owning worker.
- Failure: a tape worker down → its drives go OFFLINE in metadata, and mounted tapes are marked "in
  drive X (unavailable)". Recall falls back to other copies (RC-08).

### TP-08: mhVTL CI and hardware qualification `[M1 mhVTL, M2 hardware]` est. 4 d + lab time

**mhVTL lab job:**
- A self-hosted runner VM with mhVTL installed via `scripts/setup-mhvtl-env.sh --execute`.
- Library config: 2 LTO drives and 30 slots. Note that mhVTL's emulated drive models may lag behind LTO-10.
- Runs:
  - the TP-02 contract suite;
  - the AR/RC e2e on real `st` and `mtx` paths;
  - fault cases (eject during write via `vtlcmd`).
- Nightly plus on demand for PRs labeled `tape`.

**Hardware qualification checklist** (`docs/ops/hardware-qualification.md`), per drive and library
model:
- [ ] Discovery and mapping (serials).
- [ ] Load/unload timing; media optimization on fresh LTO-9.
- [ ] Block sizes 256 KiB / 512 KiB / 1 MiB: write and read 1 TB at ≥ 300 MB/s.
- [ ] Early-warning behavior; filling a cartridge to full (once per generation).
- [ ] Locate accuracy to an LBA; READ POSITION after filemarks.
- [ ] Power-cycle mid-write → recovery to the committed position.
- [ ] TapeAlert and cleaning.
- [ ] Import/export (mail slot) and barcode reading.
- [ ] WORM media.
- [ ] LTO-10 drive reading/writing LTO-10 media; generation compatibility rules as reported by the
  drive.
- [ ] Vendors covered for vendor neutrality: at least two drive vendors (e.g. IBM and HPE/Quantum OEM)
  and two library vendors.

Results are recorded in `docs/ops/hcl.md` (hardware compatibility list).

### TP-09: Error classification and TapeAlert `[M2]` est. 3–4 d

Map sense key / ASC / ASCQ to classes. The table lives in `errors.rs`, with tests built from captured
sense data.

| Sense / condition | Class | gRPC `Status` |
|---|---|---|
| NOT READY / medium not present | NoMedia | `Unavailable` (`no_medium`) |
| MEDIUM ERROR | Medium | `DataLoss` (`medium_error`) |
| HARDWARE ERROR | Drive | `Internal` (`drive_error`) |
| ILLEGAL REQUEST (bad block size, position beyond EOD) | Programming | `InvalidArgument` |
| DATA PROTECT / write protect | WriteProtected | `FailedPrecondition` (`write_protected`) |
| UNIT ATTENTION after media change/reset | Retry once | — |
| VOLUME OVERFLOW / EOM | Full | `ResourceExhausted` (`tape_full`) |
| BLANK CHECK (read past EOD) | Eod | `OutOfRange` |

- The error reason is carried in `Status` metadata `x-coldstore-tape-error`. Never rely on message text
  (`AGENTS.md` string-contract rule). Migrate existing message matching to codes.
- TapeAlert polling: flags are mapped to events (`OP-01`), e.g. read/write warnings, media life
  expired, cleaning.

### TP-10: Verification and scan primitives `[M5]` est. 3 d

Used by scrub, audit and DR (DP-02, DP-05):
- `VerifyBundle(tape, start_lba)`: stream-read and verify every record's SHA-256 inside the tape worker
  (no network transfer of data); returns per-record results.
- `ScanTape(tape)`: walk from BOT. It parses the label, then each bundle header and trailer (using
  `locate` to the trailer when the bundle length is known from the header, otherwise reading
  sequentially). It returns the catalog found on the tape.

### TP-11: Logical Block Protection (optional) `[M5]` est. 3 d

Enable LBP (CRC32C per block, verified by the drive on read and write) via MODE SELECT, the Control
Data Protection page, if the drive supports it. This adds end-to-end per-block integrity in addition to
per-object SHA-256. Qualify per drive model.

### TP-12: LTFS export (optional interoperability) `[M6+]` est. 10+ d

Out of the core path (D6). Provide an export job that copies selected objects to an LTFS-formatted
cartridge (using external `ltfs`/`mkltfs` tools) for data exchange with other systems. It is not used for
ColdStore's own pools.

---

## Configuration (final shape)

```yaml
tape:
  backend: linux            # sim | file | linux
  load_timeout_secs: 7200
  block_size: 262144
  changer: { kind: mtx, device: /dev/sg5, owner: true }
  drives:
    - { id: drive-0, device: /dev/nst0, sg: /dev/sg3, serial: "XYZ123", element: 0 }
  cleaning: { barcode_prefix: "CLN", auto: library }   # library | coldstore
  media:
    allow_generations: [LTO-9, LTO-10]
  file: { root: /var/lib/coldstore/vtl, speed: fast }   # backend=file only
```

## Definition of done for tape

- [ ] File-backed simulator is the default dev backend; data survives restarts.
- [ ] Contract suite passes on Sim, File and mhVTL.
- [ ] Qualification checklist passed on ≥ 1 physical LTO-9 drive + library; LTO-10 is on the HCL once
  tested.
- [ ] Error classification uses codes, not message strings.
- [ ] `docs/modules/06-tape-layer.md` updated; `docs/ops/hcl.md` exists.
