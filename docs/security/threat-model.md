# Threat Model

This is a lightweight threat model for Seglake.
Update assumptions and add mitigations/tests as the system evolves.

## Scope
- In scope: S3 API, metadata (SQLite), storage engine, replication, ops endpoints.
- Out of scope: cloud infra, OS hardening, TLS termination if handled by external proxy.

## Assumptions (current)
- Deployment exposure: public internet for S3 API; internal-only for /v1/meta/* and /v1/replication/* via proxy allowlist/mTLS.
- Auth: SigV4 required when API keys are configured; public buckets may allow unsigned requests if policy permits.
- TLS: native TLS supported, but deployment expects TLS termination at a proxy; TLS is required at the edge for any public exposure.
- Rate limiting: partial (failed-auth limiter, per-key inflight, MPU Complete limiter; no global RPS limit by default).
- Request limits (recommended): max object size 5 GiB (app); proxy body limit 5.1 GiB; header size 16–32 KB; URL/query length 16–32 KB; timeouts tuned to expected throughput.
- Proxy/WAF global rate limits (baseline): per-IP 200 RPS (burst 400), per-key 500 RPS (burst 1000), global 2000 RPS (burst 4000).
- Replication peers: auth required if API keys exist; repl client can presign via repl-access-key/secret-key.
 - Compatibility-first defaults: replay protection is off unless explicitly enabled; Content-MD5 is optional; SigV4 UNSIGNED-PAYLOAD is allowed by default for client compatibility.

## Assets
- Object data (confidentiality, integrity, availability).
- Metadata (policies, versions, oplog, replication state).
- Credentials (API keys, secrets).
- Service availability.

## Threats and Mitigations

| Asset | Entry point | Threat | Example attack | Mitigation | Enforcement | Default | Notes/Config | Test/Verify |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Object data | S3 API (PUT/GET) | Tampering/Info disclosure | Read/write without auth | SigV4; public buckets require explicit allow + policy | code | auth enabled only when keys exist; public buckets off | `-access-key/-secret-key` or stored API keys; `-public-buckets` (default empty) | `internal/s3/e2e_test.go`, `internal/s3/policy_integration_test.go`, `internal/s3/authz_test.go` |
| Metadata | On-disk formats | Tampering/DoS | Corrupt manifest/segment -> crash | Checksums, decoder validation | code | on | N/A | `internal/storage/manifest/codec_fuzz_test.go`, `internal/storage/segment/format_fuzz_test.go`, `internal/storage/segment/index_fuzz_test.go`, `internal/ops/crash_harness_test.go` |
| Credentials | Config/env/API keys | Spoofing/Disclosure | Key leak -> full access | Least privilege; operational rotation and secret handling | code + ops | least privilege on; rotation ops-only | Ops practices in `docs/ops.md` | Ops runbook (`docs/ops.md`) + periodic secrets review |
| Auth (SigV4) | Headers/query | Spoofing/Replay | Bad canonicalization | Strict canonicalization; optional replay cache for presigned requests (opt-in) | code | canonicalization on; replay off | Replay defaults: `-replay-ttl=0`, `-replay-block=false` | `internal/s3/auth_test.go`, `internal/s3/sigv4_test.go`, `internal/s3/handler_replay_test.go`, `internal/s3/replay_test.go`, `internal/s3/auth_fuzz_test.go` |
| Policies | Policy JSON | Elevation/Disclosure | Policy parse bug bypass; read policies without auth | Validate inputs, deny-by-default; authz on GetBucketPolicy | code | on | `GET /<bucket>?policy` requires policy allow | `internal/s3/policy_test.go`, `internal/s3/policy_integration_test.go` |
| Bucket names | ListBuckets (`GET /`) | Info disclosure | Enumerate buckets outside tenant scope | Policy enforcement + per-key bucket allowlist filter | code | allowlist filter applies when configured; otherwise all buckets visible | Allowlist set via `keys allow-bucket` | `internal/s3/policy_integration_test.go` |
| Availability | All inputs | DoS | Large bodies, range explosion | Size/time limits; global RPS limit at proxy/WAF (see recommended limits above) | code + deploy | size limits on; global RPS at proxy | Defaults: `-max-object-size=5GiB`, `-max-url-length=32KiB`, `-max-header-bytes=32KiB`, `-read-timeout=30s`, `-write-timeout=30s`, `-idle-timeout=2m`; Auth limiter 5 req/s burst 5 per IP/key; inflight per-key 32; MPU complete 4. Proxy/WAF RPS/timeout limits per deploy. | `internal/s3/put_validation_test.go`, `internal/s3/ratelimit_test.go` |
| Replication | Replication API | Spoofing/Tampering | Fake peer or public exposure of replication endpoints | Require auth (SigV4 when enabled), network allowlist/mTLS at proxy | code + deploy | auth depends on keys; allowlist/mTLS off | Proxy/WAF allowlist/mTLS; SigV4 only when keys exist | `internal/s3/replication_test.go`, `cmd/seglake/replication_test.go` |
| Ops endpoints | /v1/meta/*, ops | Elevation/DoS | Unauth access to ops | Require auth (SigV4 when enabled), network allowlist/mTLS at proxy | code + deploy | auth depends on keys; allowlist/mTLS off | Proxy/WAF allowlist/mTLS; SigV4 only when keys exist | `internal/s3/policy_integration_test.go`, `scripts/curl_security_smoke.sh` |

## Decisions
- Public exposure is limited to S3 API; /v1/meta/* and /v1/replication/* are internal-only via proxy allowlist/mTLS.
- Request/body limits follow the recommended limits in Assumptions.
- TLS requirement is edge-only (proxy or native TLS); internal HTTP behind the proxy is acceptable.
- Proxy/WAF global rate limits follow the baseline values in Assumptions (deployment may tune).
 - Compatibility-first defaults are used for client-facing auth features; hardening knobs are opt-in per deployment.
