# SSE Readiness Validation

This checklist is the release-readiness gate for the current SSE stack:
SSE-S3, SSE-KMS-compatible API behavior, local KEKs, Vault Transit,
bucket defaults, require-encryption policies, rewrap, deep scrub,
manifest GC, replication, and redacted diagnostics.

## Scope

The readiness pass verifies current behavior only. It does not add new
encryption modes, AWS KMS integration, SSE-C, DSSE-KMS, S3 Bucket Keys,
or new manifest formats.

## Automated Validation

Run these from the repository root:

```sh
go test -count=1 ./internal/sse ./internal/storage/manifest ./internal/storage/engine ./internal/meta ./internal/s3 ./internal/ops ./internal/repl ./cmd/seglake
make check
make test-e2e
```

Expected result:
- focused package tests pass without flakes;
- `make check` passes lint, build, and test compilation;
- `make test-e2e` passes externally visible S3 behavior checks.

## Manual Smoke Matrix

Use a disposable data directory and test keys. Do not use production KEKs
or production Vault tokens.

| Area | Validation | Expected result |
| --- | --- | --- |
| Plaintext compatibility | PUT, GET, HEAD, and range-read a plaintext object. | No SSE response headers; payload and range bytes match. |
| Explicit SSE-S3 | PUT with `x-amz-server-side-encryption: AES256`; GET, HEAD, and range-read. | Object is encrypted at rest; responses return `AES256`; plaintext-compatible ETag and size remain stable. |
| Bucket default SSE-S3 | Configure `?encryption` with `AES256`; PUT without an SSE header. | New object is SSE-S3 encrypted; existing objects are unchanged. |
| Explicit SSE-KMS-compatible | PUT with `x-amz-server-side-encryption: aws:kms` and a configured provider key ID. | Object is encrypted; GET/HEAD return `aws:kms` and the resolved key ID. |
| Bucket default SSE-KMS-compatible | Configure `?encryption` with `aws:kms` and `KMSMasterKeyID`; PUT without an SSE header. | New object is SSE-KMS-labeled and readable through the configured provider. |
| Header precedence | PUT `AES256` into a bucket with KMS default, and PUT `aws:kms` into a bucket with AES256 default. | Explicit request headers override bucket defaults. |
| Unsupported headers | Try SSE-C, DSSE-KMS, KMS encryption context, and S3 Bucket Keys. | Requests fail closed with the documented `400` or `501` behavior. |
| CORS preflight | OPTIONS request for object write paths with default CORS settings. | Allowed headers include SSE-S3 and SSE-KMS request headers. |
| Require-encryption policy | Apply `require_sse_s3` and `require_encryption` policies. | Plaintext writes are denied; matching explicit or default encryption satisfies policy. |
| Multipart upload | Initiate MPU with explicit SSE and via bucket default; upload parts and complete. | Final object is readable and preserves MPU ETag behavior. |
| CopyObject | Copy plaintext to encrypted, encrypted to plaintext when destination has no default, and encrypted to encrypted. | Destination encryption follows explicit destination headers or destination bucket default. |
| Rewrap | Rewrap an encrypted object to a new local or Vault provider key. | Segment ciphertext, version ID, size, and ETag stay unchanged; new manifest path is readable with the target key. |
| Deep scrub | Run shallow scrub, then `scrub -scrub-deep-encrypted` with provider credentials. | Shallow scrub needs no keys; deep scrub verifies DEK unwrap and AEAD tags without returning plaintext. |
| Manifest GC | Run manifest GC after rewrap with a short TTL in a disposable data dir. | Orphan manifest files are removed; live objects remain readable. |
| Replication | Replicate encrypted objects, bucket defaults, policies, diagnostics, and a rewrap-only update to a second node. | Peer fetches manifests/chunks as needed, converges on metadata, and reads only with the referenced keys. |
| Diagnostics | Call `/v1/meta/stats` and generate `support-bundle`. | `sse_diagnostics` contains counts, key IDs, and short fingerprint prefixes only; no KEKs, DEKs, raw EDEKs, Vault tokens, or nonce bytes. |

## Vault Dev Smoke

When validating Vault Transit locally, run Vault only in dev mode with
throwaway data and a throwaway token:

```sh
vault server -dev -dev-root-token-id=dev-root

export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=dev-root
vault secrets enable transit
vault write -f transit/keys/seglake-test
```

Then start Seglake with `-sse-s3-provider vault-transit` and validate:

1. explicit `aws:kms` write with `seglake-test`;
2. bucket default `aws:kms` write without object headers;
3. GET/HEAD response headers;
4. range-read;
5. deep encrypted scrub with Vault configured.

Never pass production Vault tokens on the command line. Prefer
environment variables or `-sse-s3-vault-token-file`.

## Release Checklist

- [ ] Automated validation passed.
- [ ] Manual smoke matrix passed for local provider.
- [ ] Vault dev smoke passed when Vault-backed SSE is in release scope.
- [ ] No secret material appears in logs, stats, support bundles, plans, or test artifacts.
- [ ] Existing plaintext, SSE-S3, SSE-KMS-compatible local, and Vault-backed objects remain readable.
- [ ] Manifest v1/v2/v3 compatibility is preserved.
- [ ] Unsupported encryption features fail closed with documented status/code semantics.
- [ ] Roadmap and operational docs reflect the validated scope and remaining non-goals.
