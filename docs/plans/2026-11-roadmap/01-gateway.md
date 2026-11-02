# Plan 01: S3 Gateway (`coldstore-gateway`)

**Goal:** README 3.1 / 7.2. Full S3 transparency for the operations a cold archive needs, so that
unmodified aws-cli, boto3, rclone, s3cmd and MinIO `mc` work. It must handle large streamed objects,
authenticate requests, and expose Glacier-style restore semantics.

## Current state (verified at `c205a5b`)

**What exists:**
- Axum 0.7 router with path-style routes only (`handler.rs::build_router`, pre-GW-00 path; now `router.rs::build_router`): `/`, `/:bucket`,
  `/:bucket/*key`.
- Bucket CRUD and ListObjects **v1** (marker, prefix, delimiter, max-keys ≤ 1000).
- Put/Get/Head/Delete object, single `Range`, conditional headers, and `POST ?restore`.
- `x-amz-restore` header and XML errors via `protocol.rs`.
- `GatewayBackend` trait with `GrpcGatewayBackend` and `MockGatewayBackend`; 35 tests.

**Gaps:**

| Gap | Effect |
|---|---|
| Body extracted as `Bytes` with axum's default 2 MiB limit | PUT > 2 MiB → 413 (confirmed e2e) |
| Whole objects buffered in memory on PUT and GET | `Range` is sliced after a full read |
| A new gRPC connection per request (`SchedulerServiceClient::connect` in every method) | Overhead and no connection reuse |
| No authentication (SigV4 absent) | Anyone with network access can read, overwrite or delete |
| No multipart, ListObjectsV2, virtual-host style, `x-amz-meta-*`, `aws-chunked` decoding, Content-MD5, CopyObject or DeleteObjects | Standard clients fail or misbehave |
| ETag is SHA-256 hex | Tools that validate ETag as MD5 fail |
| ListObjects reports `StorageClass` `COLD` / `COLD_PENDING` | Non-standard values |
| 409 for an in-progress restore has an empty body | No S3 error XML |
| Restore of an expired or failed restore → 403 forever | Defect #4 in `AGENTS.md` |

## Target architecture

```
client ──HTTP──▶ axum router
                  ├─ middleware: request-id, tracing span, auth (SigV4), limits, metrics
                  ├─ s3::ops::<operation> handlers (one module per op family)
                  │     └─ body: StreamReader<aws-chunked decoder?> → chunker (1 MiB) → hasher(MD5, SHA-256)
                  └─ GatewayBackend (gRPC client pool → scheduler fleet)
```

**New module layout** (split `handler.rs` before adding features; do the refactor in its own PR):

```
crates/gateway/src/
  lib.rs            # run(), GatewayState
  router.rs         # route table incl. query-param dispatch (?uploads, ?restore, ?delete, list-type=2)
  auth/{mod,sigv4,credentials,chunked}.rs
  s3/{bucket,object,list,multipart,restore,copy,delete_multi}.rs
  s3/xml.rs         # serializers (quick-xml)
  s3/errors.rs      # S3ErrorCode + Status mapping (moved from handler/protocol)
  s3/headers.rs     # metadata/system header parsing and emission
  backend/{mod,grpc,pool}.rs
  body.rs           # streaming body → chunk stream with hashing and length enforcement
```

---

## Tasks

### GW-00: Split `handler.rs` into modules (no behavior change) `[M0]` est. 1–2 d

**Status:** ✅ done, with the disclosed layout subset below. Landed as `router.rs` (route table) + `s3/{bucket,object,list,restore,range,preconditions,headers,errors,xml}.rs`, with one
mock double in `s3/testing.rs` now shared by six per-module test blocks (the old `handler.rs` had one `mod tests` with one mock; it was not duplicated, it was split).
The `auth/` and `backend/` directories from the layout above belong to GW-04 and GW-10 and are not created here. `handler.rs` remains as a temporary alias.
Note on the acceptance line "`git diff --stat` shows moves only": git cannot rename-detect a 1→12 file split (11 modules + `mod.rs`), so no `--stat` output could read that way; equivalence
was checked by other means (all 39 top-level `fn` items from the old file are present, plus the test-only `state()` which moved to top level of `s3/testing.rs` — 40 in the new
file set; byte-identical bodies; 35 tests before and after).

**Steps:**
1. Move code into the layout above.
2. Keep public items re-exported.
3. Move tests next to the code they test.
4. Replace hand-written XML string building with `quick-xml` serializers only where touched later;
   not in this PR.

**Acceptance:** identical test count (35) and identical behavior; `git diff --stat` shows moves only.

### GW-01: Streaming PutObject; remove the 2 MiB limit `[M0]` `P0` est. 3–4 d

**Depends on:** CA-01 (streaming staging), CA-02 (durable staging).

**Design:**
- Use `axum::body::Body` (`into_data_stream()`) instead of `Bytes`.
- Disable `DefaultBodyLimit` and enforce our own limits:
  - Require `Content-Length` (or `x-amz-decoded-content-length` for aws-chunked, GW-03).
  - Reject a single PUT larger than `gateway.max_put_object_size` (default 5 GiB) with
    `EntityTooLarge`.
- Pipe the stream through `body::ChunkedHasher`:
  - re-chunk to `gateway.stream_chunk_size` (default 1 MiB);
  - compute MD5 and SHA-256 incrementally;
  - count bytes.
- Send it as a gRPC client stream: `PutObjectMeta` first, then `Data` chunks. The proto already supports
  this.
- Verify at end of stream:
  - received bytes == declared length, else `IncompleteBody` (400);
  - `Content-MD5` matches if present, else `BadDigest`.
- The scheduler must not commit metadata until the final checksum is known. Add a `PutObjectTrailer`
  message: a third oneof variant, `trailer { md5, sha256, length }`. The scheduler validates and commits
  only on the trailer.
- Backpressure: the bounded mpsc between body reader and gRPC (capacity 4 chunks) keeps memory per PUT
  ≤ 5 MiB.

**Files:**
- `gateway/src/body.rs` (new) and `s3/object.rs`.
- `proto/scheduler.proto`: add `PutObjectTrailer`.
- `scheduler/src/service.rs::put_object` (consume the stream, pass through to cache staging streaming
  API).

**Tests:**
- Unit:
  - Chunker with 0 B, 1 B, exactly chunk size, chunk size + 1 and 3 MiB inputs.
  - Length mismatch → 400.
  - Content-MD5 mismatch → 400 `BadDigest`.
- Gateway router test with a mock backend that records the chunk sizes it receives.
- e2e: 3 MiB, 300 MiB and 5 GiB uploads with curl and aws-cli. RSS of the gateway process stays < 64 MiB
  during a 5 GiB upload (assert via `/proc/<pid>/status` in the script).

**Acceptance:** defect #1 closed; memory bounded; the existing 35 tests still pass.

### GW-02: Streaming GetObject with Range pushdown `[M0]` est. 2–3 d

**Depends on:** CA-01.

**Design:**
- Add `optional uint64 range_start, range_end` to `scheduler.GetObjectRequest` and `cache.GetRequest`.
- Parse `Range` *before* calling the backend.
- Return a `Body::from_stream` that maps gRPC data chunks to frames.
- Set `Content-Length` from metadata (or from the range) before streaming starts.
- If the stream fails mid-way, abort the connection. Never emit a truncated 200 that looks complete.
- Support `If-Range` (ETag form) in the same task.

**Tests:**
- Range at start, middle, suffix (`bytes=-500`), open-ended (`bytes=100-`), unsatisfiable → 416.
- A mid-stream backend error produces an aborted connection (hyper client sees an error).

**Acceptance:** a 5 GiB GET with gateway RSS < 64 MiB; a range GET reads only the requested bytes from
cache (assert via cache stats bytes_read).

### GW-03: `aws-chunked` decoding, flexible checksums, Content-MD5 `[M0]` `P0` est. 3–5 d

**Why:** modern AWS SDKs send `Content-Encoding: aws-chunked` with
`x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER` (CRC32 trailer) or
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`. Storing the raw body silently corrupts objects.

**Design:**
- `auth/chunked.rs` implements a streaming decoder (state machine): `hex-size[;chunk-signature=..]\r\n`
  + data + `\r\n` … `0\r\n` + trailers.
- Expose the decoded data stream and parsed trailers (`x-amz-checksum-crc32`, `crc32c`, `sha1`,
  `sha256`, `crc64nvme`).
- Validate:
  - `x-amz-decoded-content-length` equals the decoded byte count;
  - trailer checksums (add `crc32fast`, `crc32c` crates);
  - chunk signatures when SigV4 is enabled (GW-04 provides the signing key).
- Until GW-04 lands, accept unsigned chunked payloads. Log a warning when the body is signed but auth is
  disabled.
- Strip `aws-chunked` from the stored `Content-Encoding`.

**Tests:**
- Golden vectors from the AWS SigV4 streaming examples.
- A boto3 upload in e2e with the default checksum config, then GET back and compare SHA-256.

**Acceptance:** a boto3 ≥ 1.36 default upload round-trips byte-exact.

### GW-04: SigV4 authentication and access keys `[M1]` `P0` est. 6–8 d

**Design:**
- `auth::sigv4` verifies:
  - header-based auth (`Authorization: AWS4-HMAC-SHA256 …`);
  - presigned URLs (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
    `X-Amz-SignedHeaders`, `X-Amz-Signature`);
  - `UNSIGNED-PAYLOAD`;
  - streaming signatures (seed signature, then the chain of chunk signatures).
- Canonical request construction exactly per spec:
  - URI encoding per path segment (no double-encode for S3);
  - sorted query parameters;
  - lowercase trimmed headers.
- Constant-time compare (`subtle`).
- Clock skew ±15 min → `RequestTimeTooSkewed`.
- Region and service: `s3` with a configurable region (default `us-east-1`); accept the region from the
  credential scope only if it matches config.
- Credentials provider trait:
  - `StaticCredentials` from config (M1);
  - `MetadataCredentials` (M4): access keys stored in metadata (`MD-` follow-up), secret encrypted with a
    server key.
- Authorization model v1:
  - each access key → principal;
  - bucket owner = creating principal;
  - `admin` role sees everything;
  - other principals have full access to their own buckets only.
  Bucket policies and ACLs are out of scope.
- Anonymous access is disabled unless `gateway.allow_anonymous = true` (dev only; the node's dev config
  sets it).

**Error codes:** `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch`,
`AuthorizationHeaderMalformed`, `RequestTimeTooSkewed`, `ExpiredToken` (presigned).

**Tests:**
- The full AWS SigV4 test suite vectors (`get-vanilla`, `post-x-www-form-urlencoded`, unicode paths,
  …) converted to unit tests.
- Presigned GET/PUT generated by boto3 in e2e.
- Wrong secret → 403.
- Skew → 403.

**Acceptance:** all clients in the GW-13 matrix work with real credentials; the unauthenticated request
path is closed by default.

### GW-05: Virtual-hosted-style addressing `[M6]` est. 2 d

**Design:**
- `gateway.domains: ["s3.example.local"]`. If `Host` = `<bucket>.<domain>`, rewrite to path-style
  internally, before routing (tower layer).
- Bucket names with dots and TLS caveats are documented.

**Tests:** host parsing table test; aws-cli with `addressing_style = virtual`.

### GW-06: ListObjectsV2 and listing correctness `[M6]` (move to M1 if aws-cli `s3 ls` problems block users) est. 3 d

**Design:**
- Dispatch on `list-type=2`. Support `continuation-token` (opaque base64 of the last key + generation),
  `start-after`, `fetch-owner`, `KeyCount`, `encoding-type=url`.
- CommonPrefixes are computed on the metadata side (MD-03 provides an ordered prefix scan), not by
  scanning everything in the scheduler.
- Fix V1: `NextMarker` is only returned when a delimiter is set (S3 semantics).
- `StorageClass` uses the advertised name (D8).

**Tests:** paging over 2,500 keys with a delimiter; aws-cli `s3 ls --recursive` across 3 pages; rclone
`lsf`.

### GW-07: User metadata and system headers `[M6]` est. 2–3 d

**Design:**
- Add `map<string,string> user_metadata` and `optional string cache_control, content_disposition,
  content_encoding, content_language, expires` to `ObjectMetadata` and to the PutObject meta.
- Store `x-amz-meta-*` keys lowercased, with a total size ≤ 2 KiB (`MetadataTooLarge`).
- Return them on GET/HEAD. Allow overriding response headers via `response-content-type` etc. on GET.

**Tests:** a round-trip of 10 user metadata keys; the size limit; response-* overrides.

### GW-08: Multipart upload `[M1]` `P0` est. 8–10 d

**Depends on:** CA-01, MD-04, GW-09.

**API:**
- CreateMultipartUpload (`POST ?uploads`), UploadPart (`PUT ?partNumber&uploadId`).
- UploadPartCopy: only from restored objects; M6.
- CompleteMultipartUpload (`POST ?uploadId`), AbortMultipartUpload (`DELETE ?uploadId`).
- ListParts (`GET ?uploadId`), ListMultipartUploads (`GET /bucket?uploads`).

**Design (manifest, no concatenation copy):**
- Metadata entity `MultipartUpload { upload_id, bucket, key, initiated_at, owner, parts: map<part_no,
  PartInfo{size, md5, sha256, cache_ref}> }`. New CF/collection; commands `CreateUpload`, `PutPart`
  (CAS on upload state), `CompleteUpload`, `AbortUpload`.
- Parts are staged in the cache as category `part`, keyed by `(upload_id, part_no)`.
- Complete validates:
  - the part list is ascending and the ETags match;
  - every part except the last is ≥ 5 MiB;
  - there are ≤ 10,000 parts.
- Complete then creates a new object generation whose staging reference is a **manifest** (ordered part
  refs). The whole-object SHA-256 is computed by streaming the parts once, in the scheduler, before
  committing `PutObject`. The ETag is `hex(md5(concat(md5_i)))-N`.
- The archive (AR-05) reads manifest parts sequentially as one logical stream.
- Abort deletes parts.
- A janitor in the scheduler aborts uploads older than `multipart.max_age` (default 7 days) and emits an
  event.

**Tests:**
- 3 × 5 MiB + 1 KiB → correct ETag (compare with a value computed independently in the test).
- Wrong order → `InvalidPartOrder`.
- Small middle part → `EntityTooSmall`.
- Abort cleans the cache.
- aws-cli `cp` of a 2 GiB file (multipart default 8 MiB parts) round-trips.
- Restart in the middle of the upload; resume uploading remaining parts.

### GW-09: S3-compatible ETag and storage-class names `[M0]` est. 1–2 d

**Design:**
- Compute MD5 during PUT (GW-01) and store `etag = md5_hex`.
- Keep `checksum = sha256` (internal). Return `x-amz-checksum-sha256` (base64) when the request has
  `x-amz-checksum-mode: ENABLED`.
- Storage-class mapping: internal `COLD_PENDING` and `COLD` → `gateway.advertised_storage_class`
  (default `DEEP_ARCHIVE`) in List/HEAD/GET (`x-amz-storage-class`).
- **Migration:** objects created before this change have SHA-256 ETags. Keep them (ETags are opaque);
  document it.

**Tests:** the ETag of a known body equals its MD5; HEAD returns `x-amz-storage-class: DEEP_ARCHIVE`.

### GW-10: DeleteObjects, CopyObject, small bucket APIs `[M6]` est. 4–5 d

- **DeleteObjects** (`POST ?delete`): up to 1,000 keys, `Quiet` mode, per-key errors; requires
  Content-MD5 or a checksum header (S3 rule).
- **CopyObject:**
  - source must be restored (Glacier semantics: copying an unrestored archive object →
    `InvalidObjectState`);
  - the destination becomes a new generation in COLD_PENDING;
  - data is streamed from the restored cache into staging, scheduler-side;
  - supports the `x-amz-metadata-directive`.
- **Stubs with correct responses:**
  - GetBucketLocation (configured region);
  - GetBucketVersioning (`Suspended`/empty until MD-13);
  - GetBucketLifecycleConfiguration → `NoSuchLifecycleConfiguration`;
  - Put/GetBucketTagging (stored in BucketInfo).
- **Explicit `NotImplemented`** for unsupported sub-resources instead of falling through to GET object.

### GW-11: S3 response correctness `[M0]` est. 2 d

- `x-amz-request-id` and `x-amz-id-2` on every response, including errors (middleware); log the
  request-id in the span.
- 409 → XML body `RestoreAlreadyInProgress`.
- `x-amz-restore: ongoing-request="true"` while Pending, InProgress or WaitingForMedia.
- `Last-Modified`, `ETag` quoting (`"…"`), `Accept-Ranges`, `Content-Type` default
  `binary/octet-stream`.
- `NoSuchBucket` vs `NoSuchKey`: decide by route, not by counting `/` in the resource string. The current
  heuristic breaks on keys with no slash at bucket-level paths.
- HEAD errors must have no body (S3 behavior).

**Tests:** a table of operations × error kinds asserting status, code and headers.

### GW-12: Restore semantics parity with S3 Glacier `[M0]` est. 2–3 d

**Depends on:** MD-05, RC-01.

**Behavior matrix:**

| Object state | Restore request | Response | Effect |
|---|---|---|---|
| COLD, no restore | Days=N | 202 | Create recall task |
| COLD, Pending / InProgress / WaitingForMedia | any | 409 RestoreAlreadyInProgress | None (tier upgrade optional; out of scope) |
| COLD, Completed (not expired) | Days=N | 200 | **Extend** `restore_expire_at` to now+N (S3 semantics); update the cache TTL |
| COLD, Expired | Days=N | 202 | New recall (re-restore) |
| COLD, Failed | Days=N | 202 | New recall attempt |
| COLD_PENDING | Days=N | 202 (if `restore_from_staging=true`, default) | Fast path: copy staging → restored cache and mark Completed immediately; else 403 InvalidObjectState |

**Tier handling:**
- `Expedited` → 503 `GlacierExpeditedRetrievalNotAvailable` unless `recall.expedited_enabled`.
- `Standard` and `Bulk` map to priorities (RC-03).

**Tests:** one router test per row.

### GW-13: S3 compatibility matrix `[M1 start, M6 complete]` est. 3 d initial plus ongoing

**Design:**
- `scripts/s3-compat/` holds dockerized client tests:
  - aws-cli v2 (`s3`, `s3api` incl. `restore-object`, `head-object` restore field);
  - boto3 script;
  - rclone (`copy`, `check --download` after restore; `backend restore`);
  - s3cmd;
  - MinIO `mc`.
- Optional: a selected subset of `ceph/s3-tests` (only the operations we advertise).
- Output a markdown matrix committed to `docs/s3-compat.md` by CI (operation × client →
  pass/fail/n.a.).

**Acceptance:** a CI job on every PR to `main` (fast subset), with the full matrix nightly.

### GW-14: Backend pool, timeouts, limits, graceful shutdown, TLS `[M0: a, M4: b]` est. 2 d + 3 d

**(a) M0:**
- Create one `tonic::transport::Channel` per scheduler address at startup (lazy connect) and reuse it.
- `tower::balance::p2c` over channels, or round-robin with health.
- Per-RPC deadlines: HEAD/List 5 s; GET/PUT idle timeout 60 s between chunks.
- `gateway.max_concurrent_requests` (tower `ConcurrencyLimit`) → `SlowDown` when exceeded.
- Graceful shutdown on SIGTERM: stop accepting, drain in-flight requests up to 30 s.

**(b) M4:**
- Scheduler discovery from the metadata worker registry (MD-09) instead of a static list.
- Outlier ejection.
- TLS termination via `rustls` (`gateway.tls.cert/key`); HTTP/2 optional.

**Tests:** a test asserting that 100 sequential requests open 1 connection (count accepts on a fake
scheduler); a shutdown-drain test.

---

## Definition of done for the gateway component

- [ ] GW-13 matrix green for all advertised operations.
- [ ] No unbounded buffering (enforced by an RSS assertion in e2e).
- [ ] Every error path returns S3 XML with a correct code and request id.
- [ ] Auth enabled by default outside dev configs.
- [ ] `docs/modules/01-access-layer.md` and `02-protocol-adapter.md` updated to match the
  implementation.
