# RFC: SSE-S3 key provider interface

Status: Accepted / Phase 1, Vault Transit phase 2, and later SSE-KMS-compatible API implemented
Scope: SSE-S3 crypto/key abstraction, local KEK provider compatibility, future external key-provider backends.  
Target: Provider-interface refactor plus Vault Transit backend before the SSE-KMS-compatible API phase.

---

## 1) Summary

Seglake already supports SSE-S3 with local KEKs, envelope encryption, KEK rewrap, bucket defaults, require-encryption policy controls, deep encrypted scrub, and replication-aware manifest rewrap. The provider interface makes the local KEK implementation one backend rather than the only crypto integration point.

Phase 1 should not add Vault, AWS KMS, SSE-KMS request headers, or any new externally visible S3 API. Existing `x-amz-server-side-encryption: AES256`, bucket default encryption, rewrap, scrub, replication, and manifest v3 behavior should remain compatible. The implementation goal is to move local KEK wrapping/unwrapping behind a provider interface that can later support external backends.

Phase 2 adds a Vault Transit backend using the same provider interface. Vault Transit is a good fit because it supports encryption-as-a-service, data-key generation for envelope encryption, decrypt, rewrap, key rotation, and ACLs that can separate key administration from application encrypt/decrypt access.

Sources:
- [Vault Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit)
- [Vault Transit HTTP API](https://developer.hashicorp.com/vault/api-docs/secret/transit)
- [Vault Transit envelope encryption](https://developer.hashicorp.com/vault/docs/secrets/transit/envelope-encryption)
- [Vault Transit rewrap tutorial](https://developer.hashicorp.com/vault/tutorials/encryption-as-a-service/eaas-transit-rewrap)

---

## 2) Goals

- Introduce an internal SSE key-provider interface with operations for generating/wrapping DEKs, decrypting EDEKs, and rewrapping EDEKs.
- Keep the current local KEK behavior unchanged by implementing the new interface with the existing local provider.
- Preserve manifest v3 compatibility for existing encrypted objects.
- Keep local-only ops paths safe: rewrap and deep scrub must still avoid logging or reporting KEKs, DEKs, and raw EDEKs.
- Keep replication behavior unchanged: encrypted chunks replicate as ciphertext, rewrap replication fetches manifests only, and peers need the referenced key material/provider access.
- Make Vault Transit implementable later without another storage-engine refactor.

---

## 3) Non-goals

- Implementing Vault Transit in phase 1.
- Implementing AWS KMS, GCP KMS, Azure Key Vault, PKCS#11, HSMs, or a generic HTTP KMS plugin in phase 1.
- Adding S3 SSE-KMS API compatibility (`aws:kms`) in phase 1.
- Changing the public SSE-S3 API, bucket encryption XML behavior, policy condition names, ETags, object sizes, or manifest chunk layout.
- Migrating existing objects automatically.
- Removing support for local file/env KEKs.

---

## 4) Proposed Provider Model

Add a provider abstraction inside the SSE package or a storage-local crypto package. The exact Go names can be adjusted during implementation, but the interface should model these capabilities:

```go
type KeyProvider interface {
    GenerateDataKey(ctx context.Context, req GenerateDataKeyRequest) (GenerateDataKeyResult, error)
    DecryptDataKey(ctx context.Context, req DecryptDataKeyRequest) (DecryptDataKeyResult, error)
    RewrapDataKey(ctx context.Context, req RewrapDataKeyRequest) (RewrapDataKeyResult, error)
    DescribeKey(ctx context.Context, keyID string) (KeyDescription, error)
}
```

`GenerateDataKey` returns a plaintext DEK for immediate AES-GCM payload encryption plus an encrypted DEK blob for manifest storage. The local provider should generate the DEK locally, wrap it with the active local KEK, and return the existing local wrap metadata. A future Vault provider may call Vault data-key APIs or use Vault encrypt/decrypt primitives depending on the final backend design.

`DecryptDataKey` returns a plaintext DEK from manifest EDEK metadata. It is used by GET, range GET, deep scrub, and any read path that needs AES-GCM authentication. If the referenced key is missing, disabled, denied, or unreachable, reads fail closed.

`RewrapDataKey` returns a new encrypted DEK blob for the same plaintext DEK without changing object ciphertext, chunk refs, ETags, version IDs, or object visibility. The local provider can unwrap and wrap locally. A future external provider should prefer native rewrap when available, so Seglake does not need to expose the plaintext DEK during rewrap if the backend supports that.

`DescribeKey` provides non-secret diagnostics: provider type, key ID, whether the key is usable for write/read/rewrap, and optional version/fingerprint summaries. It must not return key material.

---

## 5) Manifest and Metadata Compatibility

Phase 1 should keep existing manifest v3 objects readable. Existing local EDEKs must continue to decode without migration. New local-provider writes may keep the current local wrap metadata shape, but the code should treat EDEK metadata as provider-owned data behind a typed envelope.

The manifest should continue to store:
- encryption mode and algorithm;
- key entries with stable `key_ref`;
- provider/key identifier sufficient to route decrypt/rewrap;
- encrypted DEK bytes and wrap metadata;
- redacted EDEK fingerprint used by rewrap plans and diagnostics.

If implementation needs an explicit provider identifier, use a backward-compatible default for existing manifests, for example `provider: "local"` when the field is absent. Do not require manifest v4 for phase 1 unless existing v3 cannot safely represent provider-owned EDEKs.

SQLite encryption summary fields should remain summaries only: mode, algorithm, key IDs/provider key IDs, and fingerprint summaries. Full EDEKs stay in manifests.

---

## 6) Configuration

Phase 1 keeps current local KEK flags/env:
- `-sse-s3-enabled`
- `-sse-s3-active-key`
- repeatable `-sse-s3-kek <key-id>=file:...|env:...`
- `SEGLAKE_SSE_S3_ENABLED`
- `SEGLAKE_SSE_S3_ACTIVE_KEY`
- `SEGLAKE_SSE_S3_KEKS`
- `SEGLAKE_SSE_S3_KEK_B64`

Internally, this config should produce a `local` key provider. No new provider-selection flag is required in phase 1. If a provider selector is added for clarity, default it to `local` and reject any other value until the corresponding backend exists.

Ops modes that need read-only key access, such as deep scrub, should keep using read-only provider construction without requiring an active writer key. Rewrap continues to require the target key and selected source keys.

---

## 7) Behavior by Subsystem

### Storage engine

Encrypted writes ask the provider for a DEK/EDEK, encrypt chunks locally with AES-256-GCM as today, then write manifest v3 metadata. Encrypted reads locate the key entry by `key_ref`, ask the provider to decrypt the EDEK, validate AEAD tags, and return plaintext only after authentication succeeds.

The provider interface must not change ciphertext chunk storage. AAD must remain stable across GC rewrite and replication, and must not include segment ID or offset.

### Rewrap

Rewrap plan/run should call provider rewrap operations instead of local unwrap/wrap helpers directly. Plans remain redacted and stale-plan checks continue to use key IDs, key refs, manifest paths, and short EDEK fingerprints.

For the local provider, phase 1 behavior should be byte-compatible in meaning but not necessarily byte-identical because wrap nonces are fresh. Segment ciphertext must remain unchanged.

### Deep scrub

Deep scrub should use provider decrypt operations to verify EDEK unwrap and AES-GCM tags. Missing provider access, denied decrypt, corrupted EDEK, malformed metadata, and AEAD failures remain errors and mark affected encrypted versions `DAMAGED` where current behavior does so.

### Replication

Replication remains provider-agnostic. Peers replicate manifest bytes and ciphertext chunks. A peer can read encrypted objects only if its configured provider can decrypt the referenced EDEK. Rewrap replication continues to fetch the new manifest and no chunks when ciphertext is already present.

### S3 API

No public API change in phase 1. SSE-S3 remains exposed as `AES256`. SSE-KMS headers and bucket KMS configs remain `501 NotImplemented`.

---

## 8) Phase 2: Vault Transit Backend

Vault Transit is available as a backend after the provider interface. The design is still SSE-S3-compatible from the S3 client perspective: clients request `AES256`, while Seglake uses Vault as the key-provider backend. SSE-KMS-compatible API can be considered later as a separate feature.

Vault config surface:
- `-sse-s3-provider vault-transit` / `SEGLAKE_SSE_S3_PROVIDER=vault-transit`;
- Vault address from `-sse-s3-vault-addr`, `SEGLAKE_SSE_S3_VAULT_ADDR`, or `VAULT_ADDR`;
- mount path from `-sse-s3-vault-mount`, default `transit`;
- active Transit key name from `-sse-s3-active-key`;
- token source from `-sse-s3-vault-token-file`, `SEGLAKE_SSE_S3_VAULT_TOKEN`, or `VAULT_TOKEN`;
- timeout from `-sse-s3-vault-timeout`, default `5s`;
- optional namespace from `-sse-s3-vault-namespace`.

Implemented design points:
- encrypted writes use Vault Transit `datakey/plaintext` with 256-bit DEKs;
- Vault ciphertext EDEKs are stored in manifest v3 `EncryptedDEK`, with `WrapAlgorithm=vault-transit-v1` and no local wrap nonce;
- encrypted reads use Vault Transit `decrypt`;
- same-key Vault rewrap uses Transit `rewrap`; cross-key Vault rewrap decrypts and encrypts the same DEK under the target key;
- provider routing can keep legacy local objects readable when local KEKs are configured alongside an active Vault provider;
- startup validates configuration shape but does not require Vault key metadata read permissions; runtime Vault failures fail reads/writes closed.

---

## 9) Failure Modes

- Missing provider or disabled SSE-S3: encrypted writes fail as they do today.
- Missing active writer key: startup/config validation fails for write-capable local provider.
- Missing read key/provider access: encrypted reads fail closed and do not return partial plaintext.
- Provider timeout/unavailable: reads/writes fail with internal errors; retries should be bounded and should not hide persistent key-provider failure.
- Rewrap partial failure: no object version should point at a new manifest unless the manifest write and SQLite transaction both succeed.
- Diagnostics must never include KEKs, plaintext DEKs, raw EDEKs, Vault tokens, or provider credentials.

---

## 10) Implementation Plan

1. Add provider request/result types and the `KeyProvider` interface.
2. Refactor the current local KEK provider to implement the interface while keeping existing constructors or compatibility wrappers.
3. Update storage write/read/range paths to call provider methods instead of direct local wrap/unwrap helpers.
4. Update rewrap and deep scrub to call provider decrypt/rewrap methods.
5. Keep old local helper functions only where useful for tests or internal provider implementation.
6. Add compatibility tests proving existing manifest v3 fixtures still decrypt.
7. Add a fake provider for unit tests to verify provider call boundaries and error handling without Vault.
8. Update docs after phase 1 implementation to describe the internal provider model while still documenting only local KEK configuration as supported.

---

## 11) Test Plan

- Local provider round trips:
  - generate DEK, encrypt object, read object;
  - decrypt existing manifest v3 local EDEKs;
  - rewrap local key `local:v1` to `local:v2`;
  - wrong/missing key fails closed.
- Storage behavior:
  - encrypted PUT/GET/range behavior unchanged;
  - ciphertext chunks and ETags remain compatible;
  - AAD stability across GC rewrite remains unchanged.
- Ops behavior:
  - rewrap plan/run still redacts secrets and changes only manifest EDEKs;
  - deep scrub succeeds with correct local provider and reports missing provider/key failures.
- Replication behavior:
  - encrypted object replication still transfers ciphertext chunks;
  - rewrap replication still fetches only the new manifest when chunks are present.
- Compatibility:
  - old plaintext v1/v2 manifests remain readable;
  - existing v3 local-encrypted manifests remain readable without manifest migration.
- Verification:
  - focused `go test ./internal/sse ./internal/storage/engine ./internal/ops ./internal/repl ./internal/s3`;
  - finish with `make check`;
  - manual smoke: write encrypted object before refactor, read/rewrap/deep-scrub after refactor using local KEKs.

---

## 12) Decisions

- Do not add an explicit provider identifier to new manifests in phase 1. Existing and new local-provider key entries continue to imply the local provider. Add explicit provider metadata only when an external backend needs it.
- Add a small normalized provider error taxonomy for internal handling and redacted diagnostics: missing key, provider unavailable, decrypt failed, invalid envelope, and permission denied.
- Add fake providers for tests where they materially improve boundary coverage. Keep them local to package tests initially; move them to a shared test helper only if repetition becomes noisy.
- Design the provider interface so a future Vault backend can use native rewrap without exposing plaintext DEKs during rotation, but do not require or implement Vault behavior in phase 1.
- Keep Vault-backed encryption under the existing SSE-S3 `AES256` API at first. Treat SSE-KMS-compatible client API as a separate future compatibility feature, useful only if clients need to select KMS key IDs per request or reuse AWS KMS-oriented tooling.
