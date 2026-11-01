# Plan 07: Operations, Notifications and Human-in-the-Loop

**Goal:** README 3.7, 4.1 "Notification & Collaboration Layer", 4.2 "Tape Management & Notification
Module", 6 (observability, compliance), 7.7:
- support offline (vaulted) tapes;
- send notifications (messages, tickets, alerts) when a restore needs media;
- let operators confirm and scan tape identifiers;
- merge requests per tape;
- provide an admin API, CLI and later a web console (`docs/modules/09-admin-console.md`).

## Current state (verified at `c205a5b`)

There is nothing here except the `WaitingForMedia` restore state, which is entered on `Unavailable`
errors and retried every tick. There is no event model, no notification sink, no admin API, no CLI and
no audit log. Worker and tape registries exist in proto but are unused.

## Target architecture

```
 any component ──(metadata command with events)──▶ metadata.events_outbox (ordered, durable)
                                                        │
                       Notifier (singleton lease) ──────┘ reads outbox from cursor, fan-out:
                         ├─ webhook (HMAC-signed JSON, retries)    ├─ email (SMTP)
                         ├─ chat (Slack/Telegram/Teams-compatible) ├─ MQ (NATS/Kafka, feature-gated)
                         └─ ticket adapters (generic REST: Jira/ServiceNow/Redmine templates)
 Admin API (axum, separate port, authn/z) ◀── coldstore-admin CLI / Web console (M6)
```

---

## Tasks

### OP-01: Event model and transactional outbox `[M3]` `P0` est. 4–5 d

**Design:**
- `Event { seq, id (uuid v7), time, kind, severity (info|warning|critical), subject {tape_id?,
  drive_id?, bucket?, object_id?, task_id?}, dedupe_key, payload (proto Any or JSON) }`.
- Events are **appended by the same metadata command** that changes the state (e.g. `FailTask` appends
  `ArchiveTaskFailed`). This makes them atomic with the state change: no lost or phantom events.
- Outbox CF `events_outbox`, keyed by monotonically increasing `seq`. Retention by time and count
  (default 30 d / 10 M). The notifier keeps its cursor in metadata (`NotifierCursor{sink_id, seq}`).
- **Initial event kinds:**

  | Area | Events |
  |---|---|
  | Media | `MediaRequired`, `MediaRequestReminder`, `MediaRequestCancelled`, `TapeOnline`, `TapeOffline`, `TapeSuspect`, `TapeFull`, `PoolExhausted`, `ScratchLow` |
  | Drives | `DriveError`, `DriveCleaningRequired`, `TapeAlert{flags}` |
  | Tasks | `ArchiveTaskFailed`, `RecallFailed`, `RestoreCompleted` (S3-event style, RC-11) |
  | Data | `StagingLost`, `StagingCorrupt`, `DataLossSuspected`, `VerifyFailed`, `AuditMismatch` |
  | Capacity | `CacheWatermark{level}`, `CacheDiskFull` |
  | Cluster | `WorkerOffline`, `MetadataLeaderChanged` |

- `dedupe_key` lets the notifier collapse floods, e.g. one `DriveError` per drive per 15 min.

**Tests:** event emitted atomically with state (crash between → neither or both); ordering; retention
trimming.

### OP-02: Notifier with pluggable sinks `[M3]` est. 5–6 d

**Design:**
- Runs in the scheduler process as a singleton job (MD-06 lease `notifier`), or as a standalone
  `coldstore-notify` binary for isolation.
- Sinks are configured as `notifications.sinks[]`:

  ```yaml
  notifications:
    sinks:
      - id: ops-webhook
        kind: webhook
        url: https://ops.example/hook
        secret_env: COLDSTORE_WEBHOOK_SECRET   # HMAC-SHA256 header X-ColdStore-Signature
        filter: { kinds: [MediaRequired, DataLossSuspected], min_severity: warning }
        retry: { max_secs: 86400 }
      - id: mail
        kind: smtp
        to: [tape-ops@example]
        filter: { kinds: [MediaRequired, MediaRequestReminder] }
      - id: tickets
        kind: ticket
        template: jira          # generic REST + Handlebars/minijinja templates in config dir
        open_on: [MediaRequired]
        close_on: [MediaRequestFulfilled]
  ```

- **Delivery:** at-least-once with exponential backoff per sink. Each sink has its own cursor, so a slow
  sink doesn't block the others. Payloads include `event.id` for receiver-side dedupe.
- **Ticket adapters** keep a mapping `event subject → external ticket id` (metadata), so a later
  `MediaRequestFulfilled` closes the same ticket.
- **Templates:** messages are rendered from templates (minijinja). They contain tape_id, barcode,
  location, archive/bundle ids, the number of waiting restores and bytes, the earliest request time, and
  an operator instruction link.

**Tests:**
- A webhook sink against a local test server: signature verification, retries after 500s, ordering per
  sink.
- Templates render with every event kind (snapshot tests with `insta`).

### OP-03: Media request workflow (offline-tape restores) `[M3]` `P0` est. 5–6 d

**Depends on:** RC-05, OP-01, OP-02, TP-04.

**Entity:**

```
MediaRequest {
  id, tape_id, barcode, location,
  status: Open | Acknowledged | Fulfilled | Cancelled | Expired,
  bundles[], task_count, bytes, tiers, created_at, ack_by?, ack_at?, fulfilled_at?
}
```

There is at most one Open/Acknowledged request per tape. New tasks for the same tape are merged in,
and `task_count`/`bytes` are updated (README 3.7: batch restore merging, unified notification).

**Flow:**
1. RC-05 moves tasks to WaitingForMedia → `UpsertMediaRequest`. The first time, the request is
   created and a `MediaRequired` event is emitted. Later merges update it without a new notification.
   A digest reminder is sent after `remind_after`.
2. The operator acknowledges (CLI/API/console) → Acknowledged (stops reminders, records who).
3. The operator inserts the tape into the I/E slot. ColdStore detects the barcode during inventory
   polling (every 60 s while requests are open), **or** the operator confirms with `coldstore-admin
   media confirm <barcode>`. That covers the README's "scan or confirm tape identifiers"; a barcode
   scanner works as a keyboard into the CLI or console field.
4. Import: the I/E slot is moved to a free slot (TP-04). Then the label is verified (TP-05): the barcode
   must match the label `tape_id`, otherwise it is quarantined with an alert. Then `TapeOnline`.
5. The tasks are moved back to Pending in one command and planned immediately as one job (one mount).
   The request becomes Fulfilled when all its tasks are complete; this closes the ticket.
6. Optional: after fulfilment, the export policy (`return_to_vault: true`) schedules an export of the
   tape back to the I/E slot, with a notification "tape X ready to return to vault shelf Y".

**Cancellation:**
- The operator cancels with a reason ("tape destroyed") → tasks Failed with a reason, an event,
  and an optional `DataLossSuspected` if no other copy exists.
- `give_up_after` → Expired, same handling.

**Tests:** the M3 exit scenario (50 restores, 1 notification, 1 mount); a wrong tape inserted;
cancellation; a restart in the middle of the flow.

### OP-04: Vault management (export/import, location tracking) `[M3]` est. 3–4 d

**Commands:**
- `coldstore-admin tape export <barcode|--pool P --full-only> --location "Vault A / Shelf 12"`:
  1. checks that no tasks are pending for the tape;
  2. moves it to the I/E slot;
  3. marks it OFFLINE with the location;
  4. emits `TapeOffline`.
- `tape import`: the inverse operation; updates the location.
- **Policies:**
  - Auto-export FULL tapes of pool `copy2` (off-site copy, DP-01) in a daily batch, with a report listing
    which barcodes to take where.
  - Never export the only online copy if `keep_one_online: true`.
- Location history (audit, OP-06).

**Tests:** export → restore request → media request lists the correct location; policies.

### OP-05: Admin API and `coldstore-admin` CLI `[M3]` est. 6–8 d

**API:**
- REST over axum on `admin.listen` (default `127.0.0.1:9100`); JSON; OpenAPI spec generated with
  `utoipa`.
- Paths follow `docs/modules/09 §3`, trimmed to what exists:

  | Area | Endpoints |
  |---|---|
  | Cluster | `GET /v1/cluster`, `GET /v1/workers`, `POST /v1/workers/{id}/drain` |
  | Tapes | `GET /v1/tapes`, `GET /v1/tapes/{id}`, `POST /v1/tapes/{id}/export|import|suspend|retire|verify` |
  | Drives | `GET /v1/drives`, `POST /v1/drives/{id}/offline|online|clean` |
  | Media requests | `GET /v1/media-requests`, `POST /v1/media-requests/{id}/ack|cancel`, `POST /v1/media/confirm {barcode}` |
  | Tasks | `GET /v1/tasks?kind=&state=`, `POST /v1/tasks/{id}/retry|cancel` |
  | Objects | `GET /v1/objects/{bucket}/{key}` (generations, copies, restore state, staging ref; a debugging goldmine) |
  | Events | `GET /v1/events?after=` (+ SSE stream `/v1/events/stream`) |
  | Pools | `GET /v1/pools` |
  | Ops | `POST /v1/maintenance/inventory`, `POST /v1/maintenance/audit` |

- Auth v1:
  - static bearer tokens with roles `viewer` / `operator` / `admin` (permission matrix per
    `docs/modules/09 §4.3`, trimmed);
  - tokens stored hashed (argon2) in config.
- Auth v2 (M6): OIDC.

**CLI:**
- `coldstore-admin` (clap): a thin client of the admin API with table/JSON output and shell completions.
- `coldstore-admin media watch` shows open media requests live (SSE), for an operator terminal.

**Tests:** API integration tests against an in-process node; the CLI is tested with `assert_cmd`.

### OP-06: Audit log `[M3]` est. 2–3 d

- Every admin API mutation, and every security-relevant S3 action (bucket create/delete, object delete,
  failed auth bursts), produces an `AuditRecord {time, principal, action, target, result, request_id,
  source_ip}`.
- Stored in metadata (`audit` CF, append-only, retention configurable) and optionally shipped through a
  notifier sink (SIEM webhook).
- Supports the compliance requirements (README 6) and DP-07 (WORM: legal-hold changes are audited).

### OP-07: Web console `[M6]` est. 10–15 d

- Per `docs/modules/09 §5`, scoped:
  - dashboard (capacity, queues, drives);
  - tapes;
  - drives;
  - media requests (with the barcode-scan input field);
  - tasks;
  - object lookup;
  - events and alerts;
  - audit.
- Stack: a decision record is needed. Recommendation: a static SPA (React + Vite) served by the admin
  server, using only the admin API (no extra backend).
- Real-time via the SSE events stream.

### OP-08: Operator runbooks `[M3]` est. 2 d

`docs/ops/runbooks/`:
- media request handling;
- tape stuck in drive;
- drive error and cleaning;
- staging full;
- archive lag growing;
- `DataLossSuspected` response;
- metadata leader flapping;
- replacing a cache disk;
- onboarding new media (LTO-9 optimization time);
- exporting to vault.

Each runbook: symptoms → alerts → diagnosis commands (CLI) → actions → verification.

---

## Definition of done for operations

- [ ] M3 exit test green in CI (file-backed simulator with I/E slots) and on mhVTL.
- [ ] Events are atomic with state; at-least-once delivery per sink; tickets close automatically.
- [ ] Admin API with OpenAPI spec, role-based auth, CLI; audit trail for every mutation.
- [ ] Runbooks reviewed by someone who has operated a tape library.
