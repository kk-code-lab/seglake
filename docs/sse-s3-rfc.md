# RFC: SSE-S3 support

Status: Draft  
Scope: Seglake S3 API, storage format, metadata, ops, and security model.  
Target: SSE-S3 MVP before considering SSE-C or external KMS integrations.

---

## 1) Summary

This RFC proposes adding server-side encryption with Seglake-managed keys, exposed through the S3-compatible `x-amz-server-side-encryption: AES256` API surface. The implementation should use envelope encryption: each object version gets a fresh data encryption key (DEK), object bytes are encrypted with that DEK, and the DEK is stored only as an encrypted data encryption key (EDEK) wrapped by a configured key encryption key (KEK).

The MVP should preserve Seglake's current durability model, range GET support, manifests, append-only segments, versioning, crash recovery, and ops workflows. It should not implement SSE-C or SSE-KMS yet.

---

## 2) Background

Amazon S3 SSE-S3 is server-side encryption with S3-managed keys. AWS documents that S3 encrypts each object with a unique key, then encrypts that key with a key that S3 rotates regularly, and uses AES-GCM for uploaded objects. AWS also notes that server-side encryption protects object data at rest, not object metadata.

For API compatibility, S3 accepts `x-amz-server-side-encryption: AES256` when creating an object through PUT, CopyObject, POST Object, and Initiate Multipart Upload. S3 confirms SSE-S3 storage by returning `x-amz-server-side-encryption` response headers on object creation and later read/metadata operations. AWS also documents that encryption request headers such as `x-amz-server-side-encryption` should not be sent on GET/HEAD for objects encrypted with SSE-S3; those requests fail with HTTP 400.

AWS KMS documentation describes the general envelope encryption pattern: data is encrypted with a data key, then the data key is encrypted under another key. KMS returns plaintext data keys for immediate use and encrypted copies that callers can store with encrypted data; KMS does not store or track those data keys for the caller.

Seglake should borrow the envelope encryption model, but start with a local KEK provider rather than an external KMS dependency.

Sources:
- [AWS S3: Using server-side encryption with Amazon S3 managed keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html)
- [AWS S3: Specifying server-side encryption with Amazon S3 managed keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-encryption.html)
- [AWS S3 API: PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [AWS S3 API: CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
- [AWS S3 API: GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
- [AWS KMS: Cryptography essentials, envelope encryption](https://docs.aws.amazon.com/kms/latest/developerguide/kms-cryptography.html)
- [AWS KMS: Generate data keys](https://docs.aws.amazon.com/kms/latest/developerguide/data-keys.html)

---

## 3) Goals

- Support explicit SSE-S3 object creation with `x-amz-server-side-encryption: AES256`.
- Encrypt object payload bytes at rest in segment files.
- Use a fresh DEK per object version.
- Store only EDEKs, never plaintext DEKs or KEKs, in manifests/SQLite.
- Preserve single-range and multi-range GET without decrypting unrelated object data.
- Preserve current durability ordering: segment sync before manifest and metadata visibility.
- Support plaintext legacy objects and encrypted objects side by side.
- Provide a KEK rotation path through EDEK rewrap without rewriting object data.
- Document ETag/checksum behavior clearly.
- Add focused tests and e2e coverage for API behavior, storage reads, ranges, copy, multipart, crash/recovery, and ops validation.

---

## 4) Non-goals

- SSE-C support.
- SSE-KMS or external KMS in the MVP.
- Bucket default encryption configuration in the MVP.
- Client-side encryption.
- Re-encrypting existing plaintext objects automatically.
- Encrypting SQLite metadata, object keys, bucket names, logs, or ops reports.
- Hiding object sizes, chunk counts, access patterns, or replication topology.
- Changing auth, policy, or public bucket semantics beyond encryption-specific header checks.

---

## 5) Proposed API behavior

### 5.1 PUT Object

Accepted:
- No SSE header: preserve current plaintext behavior for compatibility in MVP.
- `x-amz-server-side-encryption: AES256`: store the new object version encrypted with SSE-S3.

Decision: bucket default encryption is out of MVP. It should be implemented later as a separate bucket-level feature after explicit SSE-S3 is stable.

Rejected:
- Any other `x-amz-server-side-encryption` value: `400 InvalidArgument`.
- SSE-KMS headers: `501 NotImplemented`.
- SSE-C headers: already rejected with `501 NotImplemented`.
- Encryption request headers on GET/HEAD: `400 InvalidRequest`.

Successful encrypted PUT response:
- Include `ETag` as defined in section 8.
- Include `x-amz-server-side-encryption: AES256`.
- Include `x-amz-version-id` when versioning requires it.

### 5.2 GET Object and HEAD Object

For encrypted object versions:
- Decrypt transparently.
- Return plaintext object bytes for GET.
- Return existing `Content-Length`, `Content-Type`, `Last-Modified`, `ETag`, and version headers.
- Include `x-amz-server-side-encryption: AES256`.
- Support `Range` exactly as plaintext objects do.
- Reject `x-amz-server-side-encryption` request headers with `400 InvalidRequest`.
- Continue rejecting SSE-C request headers with `501 NotImplemented`.

For plaintext object versions:
- Preserve current behavior.
- Do not return `x-amz-server-side-encryption`.

### 5.3 CopyObject

MVP recommendation:
- Destination encryption is controlled by the destination request header.
- Source can be plaintext or SSE-S3 encrypted.
- If destination request includes `x-amz-server-side-encryption: AES256`, the destination version is encrypted with a new DEK.
- If destination request omits the header, the destination follows the MVP default: plaintext, not "copy source encryption".

This intentionally differs from some console-level workflows and should be documented. It matches the useful S3 API rule that destination encryption is explicit at object creation time.

Rejected:
- Unsupported destination encryption values: `400 InvalidArgument`.
- SSE-KMS headers: `501 NotImplemented`.
- SSE-C destination or copy-source headers: `501 NotImplemented`.

### 5.4 Multipart Upload

MVP recommendation:
- Accept `x-amz-server-side-encryption: AES256` only on Initiate Multipart Upload.
- Store the intended encryption mode with the multipart upload record.
- Reject encryption headers on UploadPart unless they are required by a future compatibility mode.
- Encrypt each uploaded part at UploadPart time and keep the part's encryption metadata until completion.
- CompleteMultipartUpload should continue using Seglake's existing virtual-manifest model and must not decrypt/re-encrypt all parts just to build the final object version.
- The completed object version may therefore contain chunk refs encrypted under multiple per-part DEKs in MVP. This is an accepted MVP tradeoff, and the manifest format must support per-chunk or per-run encryption metadata.

Successful encrypted MPU responses should include `x-amz-server-side-encryption: AES256` for Initiate, UploadPart, and Complete if this can be done compatibly with the stored upload state.

### 5.5 Presigned URLs and SigV4

If an encrypted PUT or Initiate Multipart Upload is presigned, the SSE-S3 header must be part of the signed request when present. Existing SigV4 canonicalization already handles signed headers; tests should ensure that removing or changing `x-amz-server-side-encryption` breaks signature verification when the header was signed.

### 5.6 CORS

Default CORS allowed headers should include `x-amz-server-side-encryption` once SSE-S3 is supported. SSE-C headers should remain absent from defaults.

---

## 6) Crypto model

### 6.1 Terms

- KEK: key encryption key, configured for the Seglake server.
- DEK: data encryption key, randomly generated per object version.
- EDEK: encrypted DEK, stored with object encryption metadata.
- Key ID: stable identifier for the KEK used to wrap a DEK.

### 6.2 Algorithm

MVP recommendation:
- Payload encryption: AES-256-GCM from Go `crypto/cipher`.
- DEK size: 32 bytes.
- KEK size: 32 bytes.
- DEK wrapping: AES-256-GCM using the KEK as the AEAD key.

This keeps the implementation in the Go standard library. If nonce management or performance constraints make AES-GCM awkward on the current storage path, XChaCha20-Poly1305 can be considered, but that would add a non-standard-library dependency.

### 6.3 Nonces

Nonce uniqueness is mandatory for AES-GCM. The design should avoid deriving nonces from mutable storage coordinates alone.

MVP recommendation:
- Generate a random 96-bit object nonce base for each object version.
- Encrypt each object chunk with a nonce derived from `(object_nonce_base, chunk_index)`.
- Use a derivation that cannot repeat within one object version. A simple option is to reserve 32 bits for the chunk index and 64 random bits for the object nonce prefix, limiting encrypted chunk count to `2^32 - 1` per object version.
- Store nonce base/prefix and nonce scheme in the manifest encryption metadata.

### 6.4 Associated authenticated data

Use AEAD AAD to bind ciphertext to object/version/chunk context. MVP AAD should include:
- format marker, for example `seglake:sse-s3:v1`;
- bucket;
- key;
- version ID;
- chunk index;
- plaintext chunk length.

Open question: whether to include segment ID and offset. Including physical location gives stronger tamper binding to current placement, but makes future segment rewrite/compaction more complex because ciphertext would need re-encryption or AAD-compatible relocation metadata. The MVP should not include segment ID/offset in AAD unless the GC rewrite design is updated accordingly.

### 6.5 Key providers

MVP provider:
- Local KEK loaded from a file or environment variable.
- Required config:
  - `sse_s3_enabled`;
  - active `sse_s3_key_id` for new encrypted writes;
  - one or more KEK sources, from files or env.
- Multiple KEKs are supported in MVP: one active writer key and zero or more read-only keys for older encrypted objects.

Operational recommendation:
- Prefer a file with restrictive permissions over an env var for long-running deployments.
- Do not log KEK, DEK, EDEK, or raw key source values.
- Fail startup if SSE-S3 is enabled and the active KEK cannot be loaded.
- Fail encrypted reads clearly when the object `key_id` is not configured.

Future provider:
- External KMS-style interface with `GenerateDataKey`, `DecryptDataKey`, and `RewrapDataKey`.

---

## 7) Storage format

### 7.1 Manifest encryption metadata

The manifest should be the authoritative source for data-path decryption because reads already resolve object layout through the manifest.

Add optional encryption metadata in a new manifest codec version, tentatively v3. The current binary codec rejects unsupported versions and trailing bytes, so encryption metadata cannot be appended to v2 without an explicit version bump.

```text
encryption:
  mode: "SSE-S3"
  algorithm: "AES-256-GCM"
  dek_wrap_algorithm: "AES-256-GCM"
  keys:
    - key_ref: 0
      key_id: "local:v1"
      encrypted_dek: bytes
      dek_wrap_nonce: bytes
      object_nonce_base: bytes
      nonce_scheme: "random64-counter32-v1"
  aad_scheme: "seglake-sse-s3-aad-v1"
```

Plaintext objects omit this block.

For single PUT and CopyObject, the manifest normally has one encrypted key entry. For MPU, the manifest may include one key entry per uploaded part or per contiguous run of chunks because CompleteMultipartUpload preserves existing encrypted part chunks without re-encryption.

### 7.2 SQLite metadata

SQLite should store enough encryption metadata for HEAD/list/debug and migration checks, but should not be the only source needed to decrypt object bytes. HEAD should not need to load and decode a manifest only to know whether to return `x-amz-server-side-encryption`.

Add optional columns or an auxiliary table keyed by `version_id`:
- `encryption_mode`;
- `encryption_algorithm`;
- `encryption_key_id`;
- `encrypted_dek_sha256` or short diagnostic fingerprint, not the EDEK itself unless needed for ops.

Recommendation: store full EDEK only in the manifest for MVP. Store a fingerprint and key ID in SQLite to support operational queries without duplicating sensitive wrapped-key material.

Decision: do not store full EDEK values in SQLite. SQLite stores only summary fields and redacted diagnostics such as key IDs and EDEK hash prefixes.

Multipart uploads need a separate metadata extension:
- `multipart_uploads.encryption_mode`;
- `multipart_uploads.encryption_algorithm`;
- possibly a small JSON/text summary of key IDs used by uploaded parts, or rely on part manifests until completion.

### 7.3 Segment records

Segment records continue to store chunk hash, length, and data. For encrypted chunks:
- record data is ciphertext plus authentication tag;
- record length is ciphertext length;
- manifest chunk length should clearly distinguish plaintext length from stored ciphertext length if they differ.

MVP recommendation:
- In manifest v3, keep chunk `Len` semantics as stored segment bytes for compatibility with segment scanning, replication chunk transfer, MissingChunks, and GC rewrite.
- Add explicit `PlainLen` to encrypted chunk refs.
- Object `Manifest.Size` remains plaintext object size.
- Range calculations must use `PlainLen`, not `Len`, for encrypted chunks.

Ambiguous length semantics will cause range-read, fsck, replication, and GC bugs.

### 7.4 Chunk hashes and integrity

Current chunks have BLAKE3 hashes. With encryption, decide whether chunk hashes represent plaintext or ciphertext.

MVP recommendation:
- Store ciphertext hash for segment-level corruption detection.
- Keep `ChunkRef.Hash` as ciphertext hash because `MissingChunks`, replication missing-range detection, and GC rewrite operate on raw segment bytes.
- Optionally add plaintext hash for fsck/scrub deep verification after decryption.
- Do not expose chunk hashes through S3 API.

AEAD authentication failures should mark the object read as failed and return `500 InternalError` with internal logs/request ID. Ops tools can classify this as damaged/corrupt encrypted data.

### 7.5 Multipart temporary parts

Current multipart upload stores parts as internal object manifests before CompleteMultipartUpload assembles the final object.

MVP options:
- Keep temporary part storage plaintext and encrypt only the final completed object. This is simpler but violates at-rest encryption expectations during MPU lifetime and forces CompleteMultipartUpload to rewrite all data.
- Encrypt temporary part data whenever the MPU was initiated with SSE-S3. Complete can preserve Seglake's current virtual-manifest behavior if manifest v3 can carry per-part/per-run encryption metadata.
- Encrypt each part with the final object DEK by creating the DEK at Initiate Multipart Upload. This preserves one DEK per completed object but requires storing an encrypted pending DEK for an incomplete upload and carefully cleaning it up on abort/GC.

Decision: encrypt temporary parts for SSE-S3 MPU and preserve virtual manifest completion using per-part DEKs in MVP. This keeps UploadPart encrypted at rest and keeps CompleteMultipartUpload metadata-only. It technically weakens the "one DEK per object version" goal for MPU-created objects, so the final implementation must document "one or more DEKs per object version, normally one for non-MPU writes". A future pending-upload-DEK mode can be added for new MPU objects without invalidating multi-key manifests.

Decision: manifest v3 stores `key_ref` per encrypted chunk initially. A future manifest version may add contiguous key runs if manifest size becomes a practical problem.

### 7.6 GC rewrite and physical location

Current GC rewrite copies raw chunk bytes into new segments and updates only `SegmentID` and `Offset` in manifests. Therefore encrypted chunk AAD must not include physical segment ID or offset in MVP.

GC rewrite can remain ciphertext-preserving if:
- `ChunkRef.Hash` is a ciphertext hash;
- `Len` is ciphertext length;
- AAD excludes physical location;
- manifest rewrite preserves encryption metadata and only changes physical chunk refs.

### 7.7 Range readers

The current range reader reads arbitrary byte slices directly from segment files. This is incompatible with AEAD encrypted chunks because decrypting a partial ciphertext chunk is not valid.

Encrypted range reads require a separate reader that:
- maps plaintext ranges using `PlainLen`;
- reads each full ciphertext chunk that intersects the requested plaintext range;
- decrypts the whole chunk;
- slices plaintext bytes to the requested range;
- never returns partial plaintext if authentication fails.

Plaintext objects can continue using the current direct range reader.

---

## 8) ETag and checksum behavior

AWS documents nuanced ETag behavior across plaintext, SSE-S3, SSE-KMS, multipart, and checksums. For Seglake, the priority is stable and documented behavior that does not leak implementation details or break current clients unnecessarily.

MVP recommendation:
- Preserve current ETag behavior as MD5 of plaintext object data for single PUT.
- Preserve current multipart ETag behavior as MD5 of concatenated part MD5s plus part count.
- Document that ETag is a compatibility identifier, not a confidentiality boundary.
- Continue validating `Content-MD5` and payload hashes against plaintext upload bytes before encryption.

Decision: keep current plaintext-compatible ETag behavior in MVP. Do not add opaque ETags until there is a specific compatibility or privacy requirement.

Rationale:
- Current clients may already rely on ETag compatibility.
- The server sees plaintext during PUT before encryption, so computing plaintext MD5 is straightforward.
- Changing ETag to ciphertext hash would make GET-side client validation surprising and would differ by key/nonce.

Security note: plaintext MD5 can reveal equality for identical object payloads. That is already true for plaintext objects and for many S3-compatible workflows. If this is unacceptable for a deployment, it needs a separate "opaque ETag" compatibility mode.

---

## 9) Read/write flows

### 9.1 Encrypted PUT

1. Validate request headers and payload integrity.
2. Generate object version ID.
3. Generate DEK and object nonce base.
4. Split plaintext stream into chunks.
5. For each chunk:
   - compute plaintext checksums needed for ETag;
   - encrypt chunk with DEK, derived nonce, and AAD;
   - append ciphertext chunk to a segment;
   - store manifest chunk ref with plaintext and ciphertext lengths.
6. Wrap DEK with active KEK into EDEK.
7. Write manifest with encryption metadata.
8. Record object metadata.
9. Fsync segments and commit metadata using the existing write barrier.
10. Return success with `x-amz-server-side-encryption: AES256`.

### 9.2 Encrypted GET

1. Resolve object metadata and manifest.
2. Load encryption metadata.
3. Select KEK by `key_id`.
4. Unwrap EDEK into DEK.
5. For full GET: read ciphertext chunks, decrypt each chunk, stream plaintext.
6. For range GET: read only chunks that intersect the requested range, decrypt those chunks, slice plaintext to the requested byte ranges.
7. Return `x-amz-server-side-encryption: AES256`.

### 9.3 HEAD

HEAD resolves metadata and returns encryption response headers, but does not need to unwrap the DEK.

### 9.4 CopyObject

Copy should read/decrypt the source through the same object reader used by GET, then write the destination through the same PUT pipeline. The destination encryption mode follows the destination request header.

---

## 10) Ops and configuration

### 10.1 Startup configuration

Possible flags:

```text
-sse-s3-enabled
-sse-s3-active-key local:v2
-sse-s3-kek local:v2=file:/etc/seglake/sse/local-v2.key
-sse-s3-kek local:v1=env:SEGLAKE_SSE_S3_KEK_V1_B64
```

Environment configuration:

```text
SEGLAKE_SSE_S3_ENABLED=true
SEGLAKE_SSE_S3_ACTIVE_KEY=local:v2
SEGLAKE_SSE_S3_KEKS=local:v2=env:SEGLAKE_SSE_S3_KEK_V2_B64,local:v1=file:/etc/seglake/sse/local-v1.key
SEGLAKE_SSE_S3_KEK_V2_B64=...
```

Single-key convenience configuration:

```text
SEGLAKE_SSE_S3_ENABLED=true
SEGLAKE_SSE_S3_ACTIVE_KEY=local:v1
SEGLAKE_SSE_S3_KEK_B64=...
```

When `SEGLAKE_SSE_S3_KEK_B64` is used, it is assigned to `SEGLAKE_SSE_S3_ACTIVE_KEY`.

Validation:
- Active key ID must be non-empty when SSE-S3 is enabled.
- The active key ID must resolve to a configured KEK.
- Each KEK must decode to 32 bytes.
- Key IDs must not contain `=`, `,`, or whitespace.
- Duplicate key IDs are rejected.
- KEK file should warn or fail if permissions are too broad on platforms where this can be checked.
- Multiple KEKs are supported in MVP so rotation can be staged: one active writer key and read-only keys for older objects.
- Logs may include key IDs and source types (`file`/`env`) but must never include env var values or decoded key bytes.

### 10.2 Rotation and rewrap

Rotation should not rewrite object payload data. It should:
1. Load old KEK and new KEK.
2. Iterate encrypted manifests by key ID.
3. Unwrap EDEK with old KEK.
4. Rewrap DEK with new KEK.
5. Atomically update manifest encryption metadata and SQLite key ID/fingerprint.

Open question: whether manifest rewrap should create a new manifest file path or update the existing manifest file in place. Given current append-only/correctness goals, prefer creating a new manifest revision or a durable temp-file-and-rename flow with metadata transaction ordering.

### 10.3 Backup and restore

Backups of encrypted data are useless without the KEK material. Ops docs must say:
- backup KEKs separately from data;
- test restore with KEKs;
- losing a KEK makes affected encrypted object versions unrecoverable;
- leaking a KEK compromises all DEKs wrapped by that KEK.

### 10.4 Scrub, fsck, rebuild-index

Ops tools need encryption awareness:
- shallow checks can validate segment and manifest structure without KEKs;
- deep checks require KEKs to decrypt and verify AEAD tags/plaintext hashes;
- support bundles must never include KEKs, DEKs, or raw EDEKs unless explicitly redacted/hashed.

### 10.5 Logging and redaction

Do not log:
- KEK source values;
- plaintext DEKs;
- decrypted object bytes;
- EDEK bytes;
- nonce values unless needed for debug and explicitly redacted.

Headers:
- `x-amz-server-side-encryption: AES256` is safe to log.
- SSE-C headers are already unsupported and should be redacted if request logging ever includes headers.

Support bundles may include redacted encryption diagnostics:
- encryption mode;
- algorithm;
- key ID;
- short EDEK SHA-256 prefix;
- key entry count;
- encrypted/plaintext object counts.

Support bundles must not include full EDEKs, DEKs, KEKs, or raw nonce bytes.

---

## 11) Compatibility and migration

Plaintext objects:
- Remain readable.
- Do not return SSE-S3 response headers.
- Can be copied into encrypted objects by CopyObject with `x-amz-server-side-encryption: AES256`.

Encrypted objects:
- Require the matching KEK by `key_id`.
- Return SSE-S3 response headers.
- Replicate encryption metadata and ciphertext exactly, or replicate through decrypted/re-encrypted writes only if the replication design explicitly supports independent KEKs.

Migration:
- No automatic bulk encryption in MVP.
- Future ops command can rewrite plaintext object versions as encrypted versions, likely using CopyObject semantics or a maintenance tool.

Format versioning:
- Manifest codec must reject unknown encryption metadata versions safely.
- Older binaries that do not understand encryption metadata must not silently serve ciphertext as plaintext.
- Manifest v3 should be introduced for encryption-capable manifests. Existing v1/v2 manifests remain plaintext.
- Any tool that rewrites manifests must preserve unknown future encryption metadata only if it understands the manifest version; otherwise it must fail closed.

---

## 12) Replication considerations

Preferred MVP:
- Replicate encrypted object versions as ciphertext plus manifest encryption metadata.
- Require peers to share or have access to the same KEK ID for reads. This is an MVP requirement, not an optional deployment hint.

Alternative:
- Decrypt at source and re-encrypt at destination under destination KEK. This enables per-node KEKs but changes replication semantics and requires plaintext exposure inside replication flow.

Recommendation:
- Start with shared KEK IDs across replication peers and document that encrypted replication requires coordinated key distribution.
- Add explicit repl-validate behavior for encrypted objects. Shallow validation can compare manifests/EDEK fingerprints; deep validation requires KEKs.

---

## 13) Security considerations

- SSE-S3 protects against offline disclosure of segment files and manifests without KEK access.
- SSE-S3 does not protect against a compromised running Seglake process, compromised API credentials, malicious authorized users, metadata disclosure, object size leakage, key name leakage, or access-pattern observation.
- KEK management is the main operational risk.
- Nonce reuse under the same DEK would be catastrophic for AES-GCM; tests and code structure must make nonce derivation simple and auditable.
- AEAD authentication failures must never return partial plaintext.
- If server-side encryption is enabled as a policy requirement in the future, unencrypted PUTs must fail before any object bytes are committed.

Threat model updates will be required before implementation lands.

---

## 14) Implementation plan

1. Add encryption metadata types to manifest package and codec tests.
2. Add KEK provider abstraction and local KEK provider.
3. Add DEK wrap/unwrap helpers with tests.
4. Add encrypted chunk writer/reader in storage engine.
5. Add manifest length semantics for plaintext/ciphertext lengths.
6. Add encrypted range reader using plaintext lengths and full-chunk decrypt.
7. Add SQLite migration for version encryption summary and multipart upload encryption state.
8. Add S3 header parsing and validation for SSE-S3.
9. Add PUT/GET/HEAD support for single-part encrypted objects.
10. Add CopyObject support.
11. Add MPU support with encrypted temporary parts and virtual manifest completion.
12. Add ops validation/scrub/fsck awareness.
13. Verify GC rewrite preserves encrypted objects without decrypting them.
14. Update docs: `docs/spec.md`, `docs/ops.md`, `docs/security/threat-model.md`, and roadmap.

---

## 15) Test plan

Unit tests:
- header parser accepts only `AES256`;
- unsupported encryption values fail;
- GET/HEAD encryption request headers fail;
- DEK wrap/unwrap round trips;
- wrong KEK fails unwrap;
- nonce derivation is unique over representative chunk indexes;
- manifest codec round trips encryption metadata;
- old plaintext manifests still decode.
- manifest v3 distinguishes ciphertext length and plaintext length.

Storage tests:
- encrypted PUT stores ciphertext not containing plaintext for simple inputs;
- encrypted GET returns original plaintext;
- AEAD tamper causes read failure;
- range GET decrypts only necessary chunks and returns exact bytes;
- crash/recovery preserves encrypted objects.
- MissingChunks detects ciphertext tamper through ciphertext hash.
- GC rewrite preserves encrypted object readability and does not require KEKs.

S3 handler/e2e tests:
- PUT with SSE-S3 returns `x-amz-server-side-encryption: AES256`;
- GET/HEAD encrypted object returns SSE-S3 response header;
- plaintext object does not return SSE-S3 response header;
- CopyObject encrypted destination works;
- CopyObject without destination SSE header follows documented MVP default;
- MPU initiated with SSE-S3 returns decrypted completed object;
- MPU encrypted parts are not stored as plaintext before completion;
- presigned PUT with signed SSE header fails if header is removed or changed;
- SSE-C remains rejected.

Ops tests:
- startup fails on invalid KEK config;
- fsck/scrub shallow works without KEK;
- deep validation fails clearly without KEK;
- rewrap plan/run changes key ID/EDEK but leaves ciphertext payload unchanged.

---

## 16) Open questions

- No open MVP-blocking questions remain.
- Future optimization: add contiguous key runs if per-chunk `key_ref` makes MPU manifests too large in practice.
- Future feature: bucket default encryption as a separate bucket-level configuration/API surface.
