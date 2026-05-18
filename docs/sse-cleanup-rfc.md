# RFC: SSE Cleanup and Stabilization

## Summary

Seglake now has SSE-S3, bucket default encryption, require-encryption policy controls, deep encrypted scrub, KEK rewrap, Vault Transit provider support, SSE-KMS-compatible API behavior, replication coverage, and redacted SSE diagnostics. This RFC defines a cleanup and stabilization pass for that work.

The goal is to reduce technical debt without changing public S3 behavior, manifest compatibility, replication protocol semantics, or existing encrypted object readability.

## Problem

The SSE work landed across several incremental phases. That was useful for de-risking behavior, but it can leave behind:

- duplicate effective-encryption decision logic;
- transitional fallback paths that are no longer needed;
- low-level crypto helpers still reachable from production code after the provider interface was introduced;
- inconsistent naming between SSE-S3, SSE-KMS-compatible API, and provider-level encryption;
- tests that overlap heavily or encode implementation details;
- documentation that still describes an earlier phase rather than the current model.

Cleanup is valuable, but unsafe cleanup could break reads of existing objects or remove compatibility that is still required by stored manifests.

## Goals

- Build an explicit compatibility ledger before removing behavior.
- Remove dead or transitional code that is not required for stored data, public API behavior, or documented operations.
- Centralize effective encryption decisions for PUT, CopyObject destination writes, and CreateMultipartUpload.
- Keep the `KeyProvider` interface as the production boundary for envelope key operations.
- Normalize SSE naming in code and docs.
- Consolidate tests where they duplicate the same contract, while preserving coverage for behavior, storage compatibility, and ops flows.
- Preserve all existing object readability guarantees.

## Non-goals

- No new S3 API features.
- No manifest migration.
- No manifest v4.
- No change to ciphertext chunk layout, ETag behavior, object sizes, version IDs, or replication wire semantics.
- No removal of plaintext manifest v1/v2 decode support.
- No removal of manifest v3 local or Vault-backed encrypted object readability.
- No real AWS KMS network integration.
- No SSE-C implementation.

## Compatibility Ledger

The cleanup pass must classify each compatibility behavior before changing it.

### Must Keep

- Plaintext manifest v1/v2 decoding.
- Manifest v3 decoding for existing SSE-S3 and SSE-KMS-compatible objects.
- Existing local-provider key-entry fields: key ID, wrapped DEK, wrap nonce, nonce prefix, nonce scheme, EDEK fingerprint.
- Vault Transit key entries and Vault ciphertext EDEKs.
- Absent provider metadata meaning the current local/default provider behavior.
- SSE-S3 `AES256` API behavior.
- SSE-KMS-compatible `aws:kms` API behavior over configured provider key IDs.
- Bucket default encryption behavior for `AES256` and `aws:kms`.
- Existing rewrap, deep scrub, manifest GC, replication, and redacted diagnostics behavior.
- Fail-closed behavior for unsupported SSE-C, DSSE-KMS, KMS encryption context, and S3 Bucket Keys.

### Candidates To Remove Or Narrow

- Production callers that still use low-level DEK wrap/unwrap helpers instead of `KeyProvider`.
- Duplicate SSE header parsing or effective-encryption computation.
- Compatibility branches that only protected intermediate development states and are not required by persisted data.
- Test-only helpers exported from production packages when they can move to tests.
- Repeated test cases that cover the same public contract at several layers without adding distinct risk coverage.

### Requires Decision Before Removal

- Any fallback that accepts incomplete or older manifest v3 encryption metadata.
- Any environment variable alias or CLI compatibility behavior not clearly documented.
- Any behavior that changes error type or status code for malformed SSE headers.
- Any behavior that changes support-bundle or stats JSON shape.

## Audit Areas

### `internal/sse`

Review provider constructors, normalized errors, local-provider helpers, Vault-provider helpers, test utilities, and any exported crypto functions. Production storage and ops code should depend on `KeyProvider` methods for data key generation, decrypt, and rewrap.

### `internal/storage/engine`

Review encrypted PUT, full GET, range GET, MPU, manifest creation, DEK caching, AAD construction, and error handling. Confirm that encryption mode and key resolution are carried as explicit write intent rather than recalculated.

### `internal/storage/manifest`

Review v1/v2/v3 decode paths and fail-closed behavior for unknown encrypted manifests. Do not remove decode compatibility required by existing objects.

### `internal/meta`

Review encryption summary fields, bucket encryption config, oplog payloads, replication apply behavior, diagnostics aggregation, and migration assumptions.

### `internal/s3`

Review SSE header parsing, bucket default resolution, policy context construction, CopyObject destination behavior, MPU initiation, GET/HEAD response headers, and unsupported SSE-KMS/SSE-C error paths.

### `internal/ops`

Review rewrap plan/run, deep encrypted scrub, manifest GC, support-bundle diagnostics, and redaction guarantees. Ops paths should remain shallow unless explicitly configured for deep encrypted verification.

### `internal/repl`

Review encrypted object transfer, rewrap oplog handling, bucket default replication, policy replication, and assumptions around shared provider key IDs.

### `cmd/seglake`

Review CLI/env parsing, provider construction, read-only provider construction, readiness validation, and whether deprecated aliases are intentionally supported.

### Docs And Tests

Review `docs/spec.md`, `docs/ops.md`, `docs/security/threat-model.md`, `docs/roadmap.md`, and SSE RFCs for stale phase-specific wording. Review test coverage for overlap, missing edge cases, and excessive coupling to implementation details.

## Proposed Cleanup Passes

### Pass 1: Inventory And Ledger

Produce a short audit note listing each discovered fallback, duplicate path, or questionable helper. Classify each as must keep, remove, narrow, or needs decision.

#### Pass 1 Audit Findings

This audit was performed against the current SSE-S3, Vault Transit, and SSE-KMS-compatible code paths.

##### Must Keep

- `sse.NormalizeWrapAlgorithm("")` currently treats an absent wrap algorithm as local `AES-256-GCM`. This is compatibility behavior for manifest entries that do not carry explicit provider metadata and should remain unless a future manifest version introduces a stronger provider discriminator.
- `manifest.ChunkRef.PlainLength()` falls back from `PlainLen` to stored `Len`. This is required for plaintext v1/v2 manifests and may also protect older or manually constructed manifests. Do not narrow this until encrypted v3 validation has explicit tests for missing `PlainLen`.
- Payload encryption still calls `sse.NewGCM`, `sse.ChunkNonce`, and `sse.ChunkAAD` from storage and ops code. This is expected: the provider owns envelope key operations, while Seglake still performs local AES-GCM payload encryption.
- `KeyProvider.DescribeKey` is used by rewrap planning/running and provider readiness. It should stay as a redacted diagnostics/readiness boundary.
- `KeyProvider` should expose only production envelope operations: generate, decrypt, rewrap, and describe. `WrapDataKey` remains a narrower provider capability for routing-provider cross-provider rewrap, not part of the base interface.
- Routing by manifest wrap algorithm is required for mixed local/Vault deployments and for local-to-Vault migration or rewrap flows.
- Existing unsupported SSE behavior must stay fail-closed: SSE-C returns `NotImplemented`, DSSE-KMS returns `NotImplemented`, KMS encryption context returns `NotImplemented`, and S3 Bucket Keys return `NotImplemented`.

##### Remove Or Narrow Candidates

- `internal/storage/engine/sse_key.go` and `internal/ops/sse_key.go` duplicate the same manifest-to-SSE and SSE-to-manifest key-entry adapters. This should be centralized in one internal helper package or moved closer to `manifest`/`sse` to avoid drift.
- Low-level local wrapping helpers `GenerateDEK`, `WrapDEK`, and `UnwrapDEK` are exported from `internal/sse`. Production callers mostly use `KeyProvider`; remaining non-provider uses appear to be tests or test helpers. These helpers can likely become unexported or be isolated as explicit test utilities after external test helpers stop depending on them.
- Server and ops CLI flag registration duplicate the SSE provider/local/Vault flag set. This is not behaviorally wrong, but it is easy for help text, env defaults, or future flags to diverge. A shared registration/build option helper would reduce maintenance risk.
- Handler response header setting for SSE modes is partly centralized through `setEncryptionResponseHeaders`, but MPU initiation/upload/complete and a KMS fallback in PUT/COPY still set headers manually. This can be narrowed with a single helper that accepts mode and key ID.
- Several tests cover near-identical S3 SSE write combinations at handler level. Keep behavior coverage, but consider table-driven consolidation for bucket default + explicit SSE-S3/SSE-KMS PUT/COPY/MPU cases.

##### Needs Decision Before Change

- `effectiveEncryptionForWrite` is the right central helper shape, but authorization computes it before request body consumption and handlers compute it again before writing. This duplicates metadata lookups and allows a narrow bucket-default race between authz and write if the bucket encryption config changes concurrently. Decide whether to attach the computed effective encryption result to request context during authorization and reuse it in PUT, CopyObject, and CreateMultipartUpload.
- `Engine.SSES3Enabled()` is used as the generic “encrypted writes are enabled” check for both SSE-S3 and SSE-KMS-compatible writes. The behavior is fine, but the name is now misleading. Rename to `EncryptionEnabled`/`SSEEnabled` only if the churn is acceptable.
- Deep scrub reports still use `SSE-S3` wording even for Vault-backed and SSE-KMS-labeled objects because the payload/envelope mechanism is shared. Decide whether to rename operational messages to “SSE encrypted” without changing JSON field names.
- Historical RFC `docs/sse-s3-rfc.md` still describes earlier MVP non-goals such as SSE-KMS being out of scope. Decide whether to keep it as a historical RFC or add a short status note pointing readers to current `docs/spec.md` / `docs/ops.md`.

##### First Recommended Cleanup Commits

1. Centralize the duplicated manifest/SSE key-entry adapter helpers used by `internal/storage/engine` and `internal/ops`.
2. Reuse the effective encryption result computed for policy evaluation in object write handlers, or explicitly document why recomputation is acceptable.
3. Rename misleading internal helpers such as `SSES3Enabled` if the change remains small and test-only churn is low.
4. Consolidate SSE response header setting across PUT, COPY, MPU initiation, UploadPart, and CompleteMultipartUpload.
5. Audit exported low-level local crypto helpers and either make them unexported or move remaining external test usage to provider-based helpers.

### Pass 2: Remove Dead Helpers And Transitional Paths

Remove clearly unused helpers and transitional code that is not referenced by stored data or public behavior. Keep this pass mechanical and low risk.

### Pass 3: Centralize Effective Encryption

Ensure PUT, CopyObject destination, and CreateMultipartUpload share one effective-encryption decision shape. The result should distinguish plaintext, SSE-S3, and SSE-KMS-compatible writes, including resolved provider key ID.

### Pass 4: Normalize Provider Boundaries

Move production envelope-key operations behind `KeyProvider`. Keep low-level crypto helpers as local-provider internals or focused test utilities.

### Pass 5: Test Consolidation

Keep tests that protect public behavior, persisted format compatibility, and ops safety. Remove or simplify tests that duplicate the same assertion at multiple layers without distinct risk coverage.

### Pass 6: Documentation Alignment

Update wording so docs describe the current model: S3-facing SSE-S3/SSE-KMS-compatible modes over internal key providers, with local and Vault Transit backends.

## Verification

Each cleanup pass should run focused tests for touched packages. The full stabilization branch should finish with:

```sh
go test -count=1 ./internal/sse ./internal/storage/manifest ./internal/storage/engine ./internal/meta ./internal/s3 ./internal/ops ./internal/repl ./cmd/seglake
make check
make test-e2e
```

If replication, rewrap, deep scrub, or provider wiring changes, also run a targeted manual smoke:

1. write plaintext object and read it back;
2. write SSE-S3 object and read/range it back;
3. write SSE-KMS-compatible object through the configured provider and read/range it back;
4. rewrap an encrypted object and verify old ciphertext remains readable with the new key;
5. run shallow scrub and deep encrypted scrub;
6. replicate encrypted object and rewrap metadata to a second node when replication code is touched.

## Acceptance Criteria

- No public S3 behavior changes unless explicitly approved.
- Existing plaintext, SSE-S3, and SSE-KMS-compatible objects remain readable.
- Manifest v1/v2/v3 compatibility is preserved.
- Unsupported encryption features still fail closed with the existing status/code semantics.
- Support-bundle and stats diagnostics remain redacted.
- `make check` and `make test-e2e` pass.
