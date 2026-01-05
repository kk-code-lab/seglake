# SPEC: Seglake — current implementation state

Version: v0.2 (spec reflects current code)  
Scope: single-node, path-style + virtual-hosted-style S3, correctness > performance, minimal resource overhead.

---

## 1) Summary

Seglake is a simple, S3-compatible (minimum useful for SDK/tooling) object store built on:
- **append-only segments** with **4 MiB** chunks,
- **object manifests** as separate files (binary codec),
- **metadata in SQLite (WAL, synchronous=FULL)**,
- **hard durability contract**: fsync segments + WAL commit before an object is visible,
- **ops tooling**: status, fsck, scrub, rebuild-index, snapshot, support-bundle, GC plan/run, GC rewrite (gc-rewrite + plan/run),
- repl-validate (consistency comparison between nodes),
- **S3 API**: PUT/GET/HEAD (with `versionId`), LIST (V1/V2), range GET (single and multi-range), SigV4 + presigned, multipart upload.
- **ACL/IAM (MVP)**: per-action JSON policy v1 + bucket policies + conditions (sufficient for the current development stage).
- **Server ops**: configurable HTTP timeouts + graceful shutdown; replay protection cache has bounded size.

### 1.1 Key decisions
- **Replication**: multi-site P2P, multi-writer, LWW + tombstone, JSON/HTTP, HLC as event ordering.
- **Consistency**: no global transactions; local writes visible immediately, eventual consistency.
- **Consistency validation**: repl-validate compares manifests and version metadata (without comparing chunk contents).
- **ACL/IAM**: MVP with policy v1 + bucket policies + conditions; no full ACL/STS.

### 1.2 Glossary with examples
- **Bucket / key**: Bucket is a top-level namespace; key is the object path inside it.  
  Example: `s3://photos/2025/12/city.jpg` has bucket `photos` and key `2025/12/city.jpg`.
- **Chunk (4 MiB)**: Fixed-size piece of object data; the last chunk can be smaller.  
  Example: 10 MiB object -> 3 chunks (4 MiB, 4 MiB, 2 MiB).
- **Segment (append-only file)**: Storage file that holds many chunks; it grows until rotation.  
  Example: chunks from many objects are appended into one segment until ~1 GiB.
- **Manifest (object layout)**: Binary file listing which chunks make up an object and where they live.  
  Example: manifest for `photos/2025/12/city.jpg` lists 3 chunks with segment IDs and offsets.
- **Version ID**: Unique ID for a specific object version.  
  Example: `GET /photos/2025/12/city.jpg?versionId=...` fetches an older version.
- **ETag**: Content signature returned by S3 API.  
  Example: single PUT -> `MD5(object)`; multipart -> `MD5(concat(part MD5s)) + "-<partCount>"`.
- **Write barrier (durability)**: Sequence that guarantees data is durable before ACK.  
  Example: fsync segments -> write manifest + metadata in transaction -> WAL flush -> ACK.
- **LWW + tombstone**: Replication resolves conflicts by last-write-wins; deletes are recorded as tombstones.  
  Example: delete on node A wins over older write on node B and is replicated as a tombstone.
- **Range GET**: Read partial bytes, single or multi-range.  
  Example: `Range: bytes=0-1023` returns first 1 KiB.
- **Presigned URL**: Time-limited signed URL for GET/PUT without permanent credentials.  
  Example: client uploads via `PUT` using a URL valid for 15 minutes.

---

## 2) Implementation status (actually done)

### 2.1 Storage core
- 4 MiB chunking + BLAKE3 per chunk.
- Append-only segments with header and footer (footer with checksum + bloom/index).
- Segment rotation: **~1 GiB** or **~10 min idle** (whichever first).
- Reuse open segments; crash recovery (seal open segments on startup).
- Manifests: binary files, path usually `data/objects/manifests/<versionID>` or name `<bucket>__<key>__<version>`.

### 2.2 Metadata
- SQLite WAL + synchronous=FULL + wal_checkpoint(TRUNCATE) on flush.
- Tables: schema_migrations, buckets, versions, objects_current, manifests, segments, api_keys,
  api_key_bucket_allow, bucket_policies, multipart_uploads (content_type), multipart_parts,
  rebuild_state, ops_runs, oplog, repl_state, repl_state_remote, repl_metrics.

### 2.3 S3 API
- Path-style: `/<bucket>/<key>` + virtual-hosted-style (enabled by default).
- PUT/GET/HEAD object, ListObjectsV2, ListObjectsV1, ListBuckets, GetBucketLocation.
- Range GET: single and multi-range (multipart/byteranges).
- SigV4 (Authorization and presigned).
- SigV2 **not supported**.
- Presigned GET/PUT (TTL up to 7 days).
- Multipart: initiate, upload part, list parts, complete, abort, list multipart uploads.
- CORS/OPTIONS: preflight with Access-Control-Allow-* headers.
- SSE: not supported yet; planned SSE-S3 (server-managed keys).

### 2.4 Ops and observability
- Ops: status, fsck, scrub, rebuild-index, snapshot, support-bundle, gc-plan/gc-run,
  gc-rewrite/gc-rewrite-plan/gc-rewrite-run (throttle + pause file), mpu-gc-plan/mpu-gc-run (TTL), repl-validate.
- `/v1/meta/stats` with basic counters + traffic and latency.
- `/v1/meta/conflicts` lists conflicting versions (JSON).
- Request-id in logs and responses.

---

## 3) Architecture and data

### 3.1 On-disk layout
- Data root: `<data-dir>/objects/`
  - `segments/` — segment files
  - `manifests/` — manifest files
- Metadata: `<data-dir>/meta.db` (+ WAL/SHM)

### 3.2 Chunking
- Fixed size: **4 MiB** (final chunk may be smaller).
- Chunk hash: **BLAKE3**.

### 3.3 Segments
- Format:
  - Header: magic + version.
  - Records: `chunk_hash(32B) + len(u32) + data`.
- Footer: magic + version + bloom/index offsets + checksum (BLAKE3 over footer).
- State: OPEN → SEALED.
- Rotation: 1 GiB or 10 min idle.

### 3.4 Object manifest
- Manifest contains: bucket, key, versionID, size, list of chunks (hash, segment_id, offset, len).
- Storage:
  - manifest file on disk (binary codec),
  - manifest path in SQLite (table `manifests`).

### 3.5 Metadata (SQLite)
- `objects_current` points to the current object version.
- `versions` stores etag (MD5), size, content_type, last_modified_utc, state.
  - state can be `ACTIVE`, `DELETED`, `DAMAGED`, or `CONFLICT` (kept when replication loses LWW).
- `segments` stores state, size, footer checksum.
- Multipart: `multipart_uploads`, `multipart_parts`.

### 3.6 Durability / barrier
- **Write barrier**:
  - `sync_interval` ~100ms
  - `sync_bytes` ~128MiB
- Order: write segments → fsync segments → write manifest + metadata update in transaction → WAL flush.
- Client ACK after barrier completion.

### 3.7 Read path
- GET/HEAD: resolve `objects_current` → manifest → stream from segments.
- Range GET: single range or `multipart/byteranges` for multiple ranges.

### 3.8 Recovery
- On startup: open segments are sealed (footer appended) or marked SEALED
  if the footer was already valid.

---

## 4) S3 API — scope

### 4.1 Endpoints
- Bucket-level paths accept optional trailing slash (`/<bucket>/`).
- `GET /` — ListBuckets.
- `GET /<bucket>?list-type=2` — ListObjectsV2.
- `GET /<bucket>?prefix=...` — ListObjectsV1 (marker).
- `GET /<bucket>?location` — GetBucketLocation.
- `GET /<bucket>?policy` — GetBucketPolicy.
- `PUT /<bucket>?policy` — PutBucketPolicy.
- `DELETE /<bucket>?policy` — DeleteBucketPolicy.
- `GET /<bucket>?versioning` — GetBucketVersioning.
- `PUT /<bucket>?versioning` — PutBucketVersioning.
- `PUT /<bucket>` — CreateBucket (idempotent).
  - Nonstandard: `x-seglake-versioning: unversioned|enabled` sets the initial bucket versioning state (default: `enabled`).
- `PUT /<bucket>/<key>` — PUT object.
- `GET /<bucket>/<key>` — GET object.
- `HEAD /<bucket>/<key>` — HEAD object.
- `DELETE /<bucket>/<key>` — DELETE object (idempotent).
  - `?versionId=...` — GET/HEAD/DELETE a specific version (returns `x-amz-version-id`).
- `DELETE /<bucket>` — DELETE bucket (only if empty; delete markers do not count as objects).
  - Buckets with only delete markers can be deleted.
- `PUT /<bucket>/<key>` + `x-amz-copy-source` — CopyObject (full copy).
- Multipart:
  - `POST /<bucket>/<key>?uploads` — Initiate.
  - `PUT /<bucket>/<key>?partNumber=N&uploadId=...` — UploadPart.
  - `GET /<bucket>/<key>?uploadId=...` — ListParts.
  - `POST /<bucket>/<key>?uploadId=...` — Complete.
  - `DELETE /<bucket>/<key>?uploadId=...` — Abort.
- `GET /<bucket>?uploads` — ListMultipartUploads (key-marker/upload-id-marker, max-uploads, delimiter/prefix).

### 4.2 Auth
- SigV4: Authorization header or presigned query.
- Presigned TTL: 1..7 days.
- `X-Amz-Content-Sha256` supported; streaming modes accepted:
  - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (signed chunks),
  - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (signed chunks + signed trailers),
  - `STREAMING-UNSIGNED-PAYLOAD` and `STREAMING-UNSIGNED-PAYLOAD-TRAILER` (unsigned).
- `UNSIGNED-PAYLOAD` allowed by default; can be disabled via `-allow-unsigned-payload=false`.
- Authorization header requests require `X-Amz-Content-Sha256` and a matching signed header entry.
- Request time skew: default ±5 min (fixed; no flag).
- Region `us` normalized to `us-east-1`.
- Required signed headers: `host` and `x-amz-date`.
- Replay protection: signature cache within TTL window (default disabled; enable via `-replay-ttl`; logs by default, blocks only with `-replay-block`).
- Replay cache size limit: bounded in-memory cache (default cap; configurable via `-replay-cache-max`).
- Optional overwrite guard: `-require-if-match-buckets` enforces `If-Match` on overwrites (use `*` for all buckets).
- `If-Match: *` can be used as an overwrite guard (write only if the object exists); delete markers are treated as not found.
- DB keys (`api_keys`) support `rw`/`ro` policy plus bucket allow-list.
- Bucket allow-list: if an access key has one or more allowed buckets, `ListBuckets` returns only those buckets; if the allow-list is empty, `ListBuckets` returns all buckets (subject to policy).
- Policies are enforced for all operations, including `list_buckets` and `meta`.
- Policy format: JSON with `statements` (effect allow/deny, actions: ListBuckets, ListBucket, ListBucketVersions, GetBucketLocation, GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketVersioning, PutBucketVersioning, GetObject, HeadObject, PutObject, DeleteObject, DeleteBucket, CopyObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, ListMultipartParts, GetMetaStats, GetMetaConflicts, *, resources: bucket + prefix, conditions: source_ip CIDR, before/after RFC3339, headers exact match, prefix, delimiter, secure_transport). AWS-style policy JSON is accepted as input and mapped to this format (subset: Effect/Action/Resource, Condition: IpAddress aws:SourceIp, DateGreaterThan/DateLessThan aws:CurrentTime, StringEquals/StringLike s3:prefix, StringEquals s3:delimiter, Bool aws:SecureTransport; other elements are rejected). Note: `GET ?location` maps to `ListBucket` action (not `GetBucketLocation`).
- Enforcement: deny > allow; bucket policy and identity policy are combined (if neither allows, access denied).
- `X-Forwarded-For` is used only for trusted proxies (`-trusted-proxies`).
- Auth failure rate limiting per IP and per access key.
- Inflight limits per access key (default 32, per-key override).
- Logs redact secrets in query (e.g. X-Amz-Signature/Credential).
- Test references: `internal/s3/e2e_test.go`.

### 4.3 ETag
- Single PUT: `MD5` of the full payload.
- Multipart: `md5(concat(md5(part_i))) + "-<partCount>"`.
- Test references: `internal/s3/e2e_test.go`.

### 4.4 PUT / UploadPart — validation
- Requires `Content-Length` or `X-Amz-Decoded-Content-Length`.
- Supports `Content-Encoding: aws-chunked` (AWS SigV4 streaming); chunk framing is stripped before validation/storage.
- Streaming signatures are validated for signed modes; trailer checksums are validated when provided.
- Fuzzed aws-chunked parser: `FuzzAWSChunkedReader` in `internal/s3/streaming_fuzz_test.go`.
- Optional `Content-MD5` validation (when header present) → `BadDigest` on mismatch.
- Multipart: `Content-Type` from `InitiateMultipartUpload` is preserved and used on `Complete`.
- Enforce `Content-MD5` via `-require-content-md5`.

### 4.4 Range GET (behavior)
- `Range: bytes=a-b`, `bytes=a-`, `bytes=-n` supported.
- Multi-range → `multipart/byteranges` with boundary based on request-id.
- Unsupported/invalid ranges → `416 InvalidRange` + `Content-Range: bytes */<size>`.
- Test references: `internal/s3/range_test.go`, `internal/s3/e2e_test.go`.

### 4.5 Conditional GET/HEAD
- `If-Match` → 412 `PreconditionFailed` when ETag mismatches.
- `If-None-Match` → 304 `NotModified` when ETag matches.
- `If-Modified-Since` → 304 `NotModified` when unchanged since the given time.
- `If-Unmodified-Since` → 412 `PreconditionFailed` when modified after the given time.

### 4.6 Bucket versioning
- `GET /<bucket>?versioning` returns XML with `<Status>Enabled|Suspended</Status>`; unversioned buckets return an empty configuration.
- `PUT /<bucket>?versioning` accepts XML `<VersioningConfiguration><Status>Enabled|Suspended</Status></VersioningConfiguration>`.
- States: `enabled` (default), `suspended`, `disabled` (unversioned). Only `enabled`/`suspended` are settable via `PUT ?versioning`.
- `disabled` buckets are created via `x-seglake-versioning: unversioned` and cannot be reverted to unversioned once enabled/suspended.
- In `suspended`: new writes are tracked as the null version (`x-amz-version-id: null`), and `versionId=null` targets the null version.
- In `disabled`: version ids are not exposed; deletes remove the current object without creating a delete marker.

### 4.6.1 ListObjectVersions
- `GET /<bucket>?versions` returns XML `ListVersionsResult` with `Version`, `DeleteMarker`, and `CommonPrefixes` entries (AWS-compatible).
- Query params: `prefix`, `delimiter`, `key-marker`, `version-id-marker`, `max-keys` (default 1000, max 1000), `encoding-type=url`.
- Pagination: use `KeyMarker` + `VersionIdMarker` from the request; responses set `NextKeyMarker` + `NextVersionIdMarker` when truncated.
- For suspended buckets, null versions are listed with `VersionId` of `null` (and `version-id-marker=null` is accepted).
- For unversioned buckets (`disabled`), the response is empty (no `Version`/`DeleteMarker` entries).

### 4.7 Versioning delete markers
- `DELETE` without `versionId` creates a delete marker as the latest version.
- `GET`/`HEAD` without `versionId` returns 404 when the latest version is a delete marker.
- Responses include `x-amz-delete-marker: true` and `x-amz-version-id` for delete markers.

### 4.8 Conflict visibility (MVP)
- If current version state is `CONFLICT`, GET/HEAD include `x-seglake-conflict: true`.

### 4.9 Errors
- AWS-compatible XML (`Code`, `Message`, `RequestId`, `HostId`, `Resource`).
- Examples validated in tests (e.g. `SignatureDoesNotMatch`, `RequestTimeTooSkewed`,
  `XAmzContentSHA256Mismatch`): `internal/s3/e2e_test.go`.
- Additional codes: `AuthorizationHeaderMalformed`, `BadDigest`, `MissingContentLength`, `EntityTooLarge`.

---

## 5) Ops / maintenance

### 5.1 Modes
- `status` — count of manifests and segments.
- `fsck` — consistency of manifests and segment boundaries.
- `scrub` — verify chunk hashes; damaged → `DAMAGED`.
- `rebuild-index` — rebuild meta from manifests.
- `snapshot` — copy meta.db(+wal/shm) + report.
- `support-bundle` — snapshot + fsck + scrub.
- `buckets` — manage bucket entries (admin; bypasses S3 API).
- `repl-validate` — compare manifests and versions (live + all versions) between two data dirs.
- `gc-plan`/`gc-run` — removes segments that are 100% dead (gc-run requires `-gc-force`).
- `gc-rewrite` — rewrite partially-dead segments (throttle + pause file, requires `-gc-force`).
- `gc-rewrite-plan`/`gc-rewrite-run` — plan + execute rewrite (run requires `-gc-force`).
- `mpu-gc-plan`/`mpu-gc-run` — cleanup stale multipart uploads (TTL; run requires `-mpu-force`).
  - Segment GC treats multipart parts as live.

### 5.2 Stats API
`GET /v1/meta/stats` (JSON):
- objects, segments, bytes_live, live_manifests, manifests_total,
- last fsck/scrub/gc results (time + errors + reclaim/rewritten),
- requests_total{op,status_class}, inflight{op},
- bytes_in_total, bytes_out_total,
- replay_detected,
- latency_ms{op}: p50/p95/p99,
- requests_total_by_bucket / latency_ms_by_bucket,
- requests_total_by_key / latency_ms_by_key,
- gc_trends: GC history (mode, finished_at, errors, reclaimed/rewritten, reclaim_rate),
- replication: per-remote {last_pull_hlc, last_push_hlc, push_backlog, push_backlog_bytes, oplog_bytes_total, last_oplog_hlc, pull_lag_seconds, push_lag_seconds},
- replication_conflicts: conflict count from apply (LWW),
- replication_bytes_in_total: total bytes pulled by replication (manifests + chunk data).

### 5.3 Crash harness
- Integration test (optional): `go test -tags crashharness ./internal/ops -run TestCrashHarness`
  - Starts the binary and performs PUT/multipart + kill -9 + fsck/rebuild-index.
  - `CRASH_CORRUPT=1` enables controlled segment corruption (expected scrub/GET=500 errors).
  - `CRASH_ITER` controls iteration count (default 1).
- Crash durability test (optional): `go test -tags durability ./internal/ops -run TestDurabilityAfterCrash`

---

## 6) Limits and parameters

- Chunk: 4 MiB (fixed).
- Segment: ~1 GiB max, seal after ~10 min idle.
- Barrier: 100ms / 128MiB.
- ListObjects max-keys: 1000.
- ListMultipartUploads max-uploads: 1000.
- Multipart min part size: 5 MiB except the last.
- Multipart max part size: 5 GiB.
- Multipart max parts per upload: 10,000.
- Object size limit: `-max-object-size` (default 5 GiB, 0 = unlimited).

## 6.1) Ops / TLS / tooling
- TLS checklist and awscli/s3cmd examples: `docs/ops.md`.
- Optional in-app TLS: `-tls`, `-tls-cert`, `-tls-key` (hot reload certs).
- Policy management: `-mode keys` (per-key) and `-mode bucket-policy` (per-bucket).
- Public buckets (unsigned access): `-public-buckets` + bucket policy allowlist (see `docs/ops.md`).
- Deployment examples (systemd, Caddy, public policy) are in `examples/`.
- Limits and CORS: `-max-object-size`, `-cors-origins`, `-cors-methods`, `-cors-headers`, `-cors-max-age`.

---

## 7) Known gaps / limitations (current state)

 - No full ACL/IAM/policies (per-action JSON policy v1, bucket policies and conditions exist; no per-object ACL/STS/advanced conditions).
 - repl-validate does not compare chunk contents, only manifests and version metadata.

---

## 8) Next sensible steps (proposals)

- Out of scope for first iteration:
  - Strong global consistency.
  - Cross-region locking or transactional rename.
  - Advanced per-bucket replication policies (later).
