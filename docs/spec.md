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
- **ops tooling**: status, fsck, scrub, rebuild-index, snapshot, support-bundle, conflict listings, GC plan/run, GC rewrite (gc-rewrite + plan/run), manifest GC (plan/run), lifecycle plan/run, SSE-S3 KEK rewrap (plan/run),
- repl-validate (consistency comparison between nodes, with optional deep chunk-hash validation),
- **S3 API**: PUT/GET/HEAD (with `versionId`), object tagging, LIST (V1/V2), range GET (single and multi-range), SigV4 + presigned, multipart upload.
- **ACL/IAM (MVP)**: per-action JSON policy v1 + bucket policies + conditions (sufficient for the current development stage).
- **SSE-S3 / SSE-KMS-compatible API**: explicit `AES256` or `aws:kms` object writes plus bucket default encryption with local KEKs or Vault Transit behind an internal key-provider interface and envelope encryption. The `aws:kms` mode is an S3-compatible API surface over configured provider key IDs, not AWS KMS network integration.
- **Server ops**: configurable HTTP timeouts + graceful shutdown; replay protection cache has bounded size.

### 1.1 Key decisions
- **Replication**: multi-site P2P, multi-writer, LWW + tombstone, JSON/HTTP, HLC as event ordering.
- **Consistency**: no global transactions; local writes visible immediately, eventual consistency.
- **Consistency validation**: repl-validate compares manifests and version metadata; `-repl-validate-deep` also reads referenced chunks and verifies stored chunk hashes.
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
- Tables: schema_migrations, buckets, versions, objects_current, object_tags, manifests, segments, api_keys,
  api_key_bucket_allow, bucket_policies, bucket_encryption, bucket_lifecycle, multipart_uploads (content_type), multipart_parts,
  rebuild_state, ops_runs, oplog, repl_state, repl_state_remote, repl_metrics.

### 2.3 S3 API
- Path-style: `/<bucket>/<key>` + virtual-hosted-style (enabled by default).
- PUT/GET/HEAD object, ListObjectsV2, ListObjectsV1, ListBuckets, GetBucketLocation.
- Range GET: single and multi-range (multipart/byteranges).
- SigV4 (Authorization and presigned).
- SigV2 **not supported**.
- Presigned GET/PUT (TTL up to 7 days).
- Multipart: initiate, upload part, list parts, complete, abort, list multipart uploads.
- Object tagging: `GET/PUT/DELETE ?tagging` stores up to 10 key/value tags per object version. `x-amz-tagging` is supported on PutObject, and CopyObject supports `x-amz-tagging-directive` values `COPY` and `REPLACE`. GET/HEAD return `x-amz-tagging-count` only when the request is authorized for tag reads.
- Bucket lifecycle configuration: `GET/PUT/DELETE ?lifecycle` stores AWS-compatible bucket lifecycle XML in metadata and replicates config changes. MVP supports prefix/tag/And filters, current expiration, noncurrent expiration, and abort incomplete MPU configuration. `lifecycle-plan` evaluates stored configs and writes a read-only JSON plan; `lifecycle-run` executes saved plans under maintenance.
- CORS/OPTIONS: preflight with Access-Control-Allow-* headers.
- Server-side encryption: explicit `x-amz-server-side-encryption: AES256` stores SSE-S3 objects, and explicit `x-amz-server-side-encryption: aws:kms` stores SSE-KMS-labeled objects using the same envelope encryption path. `x-amz-server-side-encryption-aws-kms-key-id` maps directly to a configured provider key ID; if omitted, writes resolve the bucket default KMS key ID and then the active provider key. Bucket default encryption is supported through `GET/PUT/DELETE ?encryption` for `AES256` and `aws:kms` with optional `KMSMasterKeyID`. Explicit request headers override bucket defaults. GET/HEAD return `AES256` or `aws:kms` plus the resolved KMS key ID according to object metadata. SSE-C, DSSE-KMS, KMS encryption context, S3 Bucket Keys, and AWS KMS network/policy/grant semantics are unsupported.

### 2.4 Ops and observability
- Ops: status, fsck, scrub, rebuild-index, snapshot, support-bundle, gc-plan/gc-run,
  gc-rewrite/gc-rewrite-plan/gc-rewrite-run (throttle + pause file), manifest-gc-plan/manifest-gc-run,
  mpu-gc-plan/mpu-gc-run (TTL), lifecycle-plan/lifecycle-run,
  sse-rewrap-plan/sse-rewrap-run, repl-validate.
- `/v1/meta/stats` with basic counters + traffic and latency.
- `/v1/meta/conflicts` and `-mode conflicts` list conflicting versions with bucket/prefix filters and marker-based pagination.
- Request-id in logs and responses.
- Admin ops channel: local-only Unix socket (`.seglake-admin.sock`) with required token (`.seglake-admin.token`) for ops/maintenance/keys/buckets/bucket-policy/repl.

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
- Manifest v1/v2 objects are plaintext. Manifest v3 is used for SSE-S3 and SSE-KMS-compatible objects and includes encryption metadata: mode, API algorithm summary, AAD/nonce/wrap schemes, wrapped DEKs, per-key nonce prefixes, per-chunk plaintext length, and per-chunk key refs. `WrapAlgorithm=AES-256-GCM` identifies local KEK-wrapped DEKs; `WrapAlgorithm=vault-transit-v1` identifies Vault Transit ciphertext EDEKs stored in the same manifest v3 key-entry fields.
- For encrypted chunks, `len` is the stored ciphertext length and chunk hash is over ciphertext. Range reads use the explicit plaintext length.
- Storage:
  - manifest file on disk (binary codec),
  - manifest path in SQLite (table `manifests`).

### 3.5 Metadata (SQLite)
- `objects_current` points to the current object version.
- `versions` stores etag (MD5), size, content_type, last_modified_utc, state, and optional encryption summary fields.
  - state can be `ACTIVE`, `DELETED`, `DAMAGED`, or `CONFLICT` (kept when replication loses LWW).
- `segments` stores state, size, footer checksum.
- `object_tags` stores S3 object tags per version ID. Tags are metadata only and do not affect manifests, ETags, object bytes, GC, or scrub.
- Multipart: `multipart_uploads`, `multipart_parts`.

### 3.6 Durability / barrier
- **Write barrier**:
  - `sync_interval` ~100ms
  - `sync_bytes` ~128MiB
- Order: write segments → fsync segments → write manifest + metadata update in transaction → WAL flush.
- Replication chunk fetches use the same durable ordering for raw segment ranges:
  write bytes → fsync segment file → mark the segment `SEALED` in SQLite.
- Client ACK after barrier completion.

### 3.7 Read path
- GET/HEAD: resolve `objects_current` → manifest → stream from segments.
- Encrypted GET asks the configured key provider to decrypt the manifest EDEK before streaming bytes, decrypts full ciphertext chunks with AES-256-GCM, and returns plaintext. Key-provider failure fails closed before object bytes are streamed; authentication failure fails the read and must not return partial plaintext.
- Range GET: single range or `multipart/byteranges` for multiple ranges. Encrypted ranges map by plaintext length, read full ciphertext chunks, decrypt, then slice plaintext.

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
- `GET /<bucket>?lifecycle` — GetBucketLifecycleConfiguration.
- `PUT /<bucket>?lifecycle` — PutBucketLifecycleConfiguration.
- `DELETE /<bucket>?lifecycle` — DeleteBucketLifecycle.
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
- Policy format: JSON with `statements` (effect allow/deny, actions: ListBuckets, ListBucket, ListBucketVersions, GetBucketLocation, GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketVersioning, PutBucketVersioning, GetObject, HeadObject, GetObjectTagging, PutObjectTagging, DeleteObjectTagging, PutObject, DeleteObject, DeleteBucket, CopyObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, ListMultipartParts, GetMetaStats, GetMetaConflicts, *, resources: bucket + prefix, conditions: source_ip CIDR, before/after RFC3339, headers exact match, prefix, delimiter, secure_transport, require_sse_s3, require_encryption). AWS-style policy JSON is accepted as input and mapped to this format (subset: Effect/Action/Resource, Condition: IpAddress aws:SourceIp, DateGreaterThan/DateLessThan aws:CurrentTime, StringEquals/StringLike s3:prefix, StringEquals s3:delimiter, Bool aws:SecureTransport; other elements are rejected). Note: `GET ?location` maps to `ListBucket` action (not `GetBucketLocation`).
- Native `require_sse_s3: true` policy conditions require effective SSE-S3 on PutObject, CopyObject destination, and CreateMultipartUpload. Native `require_encryption: true` accepts either effective SSE-S3 or SSE-KMS. Effective encryption is satisfied by an explicit request header or bucket default encryption; plaintext writes fail authorization with `AccessDenied`.
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

### 4.7.1 Bucket lifecycle configuration
- `PUT /<bucket>?lifecycle` accepts XML `LifecycleConfiguration` with 1..1000 rules and stores the original XML plus normalized metadata/fingerprint for future lifecycle planning.
- Supported rule fields: `ID`, `Status`, `Filter` (`Prefix`, `Tag`, `And`), `Expiration` (`Days` or `Date`), `NoncurrentVersionExpiration` (`NoncurrentDays`), and `AbortIncompleteMultipartUpload` (`DaysAfterInitiation`).
- Unsupported lifecycle features such as transitions, noncurrent transitions, `ExpiredObjectDeleteMarker`, object-size filters, and storage class settings return `NotImplemented`.
- `AbortIncompleteMultipartUpload` with tag filters returns `InvalidArgument`; prefix-only and unfiltered MPU abort rules are accepted.
- `GET /<bucket>?lifecycle` returns the stored XML; missing config returns `NoSuchLifecycleConfiguration`.
- `DELETE /<bucket>?lifecycle` clears the stored config. Lifecycle config changes replicate through oplog; no object data transfer is required.
- `lifecycle-plan` scans stored lifecycle configs and metadata, then writes a JSON plan with `expire_current`, `expire_noncurrent`, and `abort_mpu` candidates. It stores bucket config fingerprints and candidate metadata for future stale-plan revalidation; it does not mutate object versions, delete markers, MPU state, segments, manifests, or tags.

### 4.8 Conflict visibility (MVP)
- If current version state is `CONFLICT`, GET/HEAD include `x-seglake-conflict: true`.
- ListObjects V1/V2 and ListObjectVersions include `x-seglake-conflicts: true` when the requested bucket/prefix has at least one conflicting version. The XML response remains S3-compatible; clients should use `/v1/meta/conflicts` or `-mode conflicts` to inspect details.
- Replication applies last-write-wins across put and delete operations using HLC and site ID. A losing put or delete marker is retained as `CONFLICT`. A losing non-marker delete whose target version is not present locally is retained as a conflict tombstone so delete-vs-put races remain visible. A winning non-marker delete suppresses older current object state without creating an S3 delete marker.

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
- `scrub` — verify stored chunk hashes; damaged -> `DAMAGED`. With `-scrub-deep-encrypted`, SSE encrypted chunks are also decrypted far enough to validate DEK unwrap and AEAD tags, requiring the referenced local KEKs or Vault provider access.
- `rebuild-index` — rebuild meta from manifests. Object tags are SQLite-only metadata and are not reconstructable from manifests in this MVP.
- `snapshot` — copy meta.db(+wal/shm) + report.
- `support-bundle` — snapshot + fsck + scrub + shallow redacted SSE diagnostics + aggregate object-tag counts.
- `buckets` — manage bucket entries (admin; bypasses S3 API).
- `repl-validate` — compare manifests and versions (live + all versions) between two data dirs. With `-repl-validate-deep`, also verify that referenced chunk bytes exist and match manifest hashes on both sides.
- `db-integrity-check` — run SQLite integrity_check on meta.db.
- `db-reindex` — rebuild SQLite indices in meta.db.
- `gc-plan`/`gc-run` — removes segments that are 100% dead (gc-run requires `-gc-force`).
- `gc-rewrite` — rewrite partially-dead segments (throttle + pause file, requires `-gc-force`).
- `gc-rewrite-plan`/`gc-rewrite-run` — plan + execute rewrite (run requires `-gc-force`).
- `manifest-gc-plan`/`manifest-gc-run` — plan + delete orphan manifest files only. Candidates are manifest files not referenced by current versions or active MPU parts and older than `-manifest-gc-ttl` (default 7 days). Run requires `-manifest-gc-force` and a saved plan.
- `mpu-gc-plan`/`mpu-gc-run` — cleanup stale multipart uploads (TTL; run requires `-mpu-force`).
  - Segment GC treats multipart parts as live.
- `lifecycle-plan` — read-only evaluation of stored bucket lifecycle configs. Flags: `-lifecycle-plan <path>` required, optional `-lifecycle-bucket`, `-lifecycle-as-of`, and `-lifecycle-limit` (default 10000).
- `lifecycle-run` — execute a saved lifecycle plan. Requires `-lifecycle-from-plan`, `-lifecycle-force`, and maintenance/quiesced mode. It creates delete markers, deletes eligible object versions, or aborts eligible MPUs through metadata paths; segments and manifests are reclaimed later by GC/manifest-GC.
- `sse-rewrap-plan`/`sse-rewrap-run` — rotate SSE-S3 EDEKs by rewriting only manifest EDEKs and SQLite encryption summaries. Local→local, Vault→Vault, and whole-manifest local→Vault rewrap are supported. The run writes new manifest paths, preserves object versions, ETags, sizes, Last-Modified values, chunk refs, and segment ciphertext, and records `sse_rewrap` oplog entries so peers fetch the new manifest bytes.

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
- replication_bytes_in_total: total bytes pulled by replication (manifests + chunk data),
- conflict_hotspots: top current conflict keys derived from `versions WHERE state='CONFLICT'`, ordered by conflict count, then bucket/key,
- sse_diagnostics: redacted metadata-only summary with plaintext/encrypted active version counts, damaged encrypted version count, counts by encryption mode, algorithm, key ID, and short EDEK fingerprint prefix. It is derived from SQLite version summaries and does not read manifests, decrypt objects, require KEKs/Vault, or expose KEKs, DEKs, raw EDEKs, Vault tokens, or nonce bytes.

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
 - repl-validate is shallow by default; deep chunk-hash validation must be requested with `-repl-validate-deep`.

---

## 8) Next sensible steps (proposals)

- Out of scope for first iteration:
  - Strong global consistency.
  - Cross-region locking or transactional rename.
  - Advanced per-bucket replication policies (later).
