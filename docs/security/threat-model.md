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
- Request limits (recommended): max object size 5 GiB (app); proxy body limit 5.1 GiB; header size 16–32 KB; URL/query length 8–16 KB; timeouts tuned to expected throughput.
- Proxy/WAF global rate limits (baseline): per-IP 200 RPS (burst 400), per-key 500 RPS (burst 1000), global 2000 RPS (burst 4000).
- Replication peers: auth required if API keys exist; repl client can presign via repl-access-key/secret-key.

## Assets
- Object data (confidentiality, integrity, availability).
- Metadata (policies, versions, oplog, replication state).
- Credentials (API keys, secrets).
- Service availability.

## Threats and Mitigations

| Asset | Entry point | Threat | Example attack | Mitigation (current/planned) | Test/Verify |
| --- | --- | --- | --- | --- | --- |
| Object data | S3 API (PUT/GET) | Tampering/Info disclosure | Read/write without auth | SigV4; public buckets require explicit allow + policy | `internal/s3/e2e_test.go`, `internal/s3/policy_integration_test.go`, `internal/s3/authz_test.go` |
| Metadata | On-disk formats | Tampering/DoS | Corrupt manifest/segment -> crash | Checksums, decoder validation | `internal/storage/manifest/codec_fuzz_test.go`, `internal/storage/segment/format_fuzz_test.go`, `internal/storage/segment/index_fuzz_test.go`, `internal/ops/crash_harness_test.go` |
| Credentials | Config/env/API keys | Spoofing/Disclosure | Key leak -> full access | Least privilege; operational rotation and secret handling | Ops runbook (`docs/ops.md`) + periodic secrets review |
| Auth (SigV4) | Headers/query | Spoofing/Replay | Bad canonicalization | Strict canonicalization; replay cache for presigned requests | `internal/s3/auth_test.go`, `internal/s3/sigv4_test.go`, `internal/s3/handler_replay_test.go`, `internal/s3/replay_test.go`, `internal/s3/auth_fuzz_test.go` |
| Policies | Policy JSON | Elevation | Policy parse bug bypass | Validate inputs, deny-by-default | `internal/s3/policy_test.go`, `internal/s3/policy_integration_test.go` |
| Availability | All inputs | DoS | Large bodies, range explosion | Size/time limits; global RPS limit at proxy/WAF (see recommended limits above) | `internal/s3/put_validation_test.go`, `internal/s3/ratelimit_test.go` |
| Replication | Replication API | Spoofing/Tampering | Fake peer or public exposure of replication endpoints | Require auth (SigV4 when enabled), network allowlist/mTLS at proxy | `internal/s3/replication_test.go`, `cmd/seglake/replication_test.go` |
| Ops endpoints | /v1/meta/*, ops | Elevation/DoS | Unauth access to ops | Require auth (SigV4 when enabled), network allowlist/mTLS at proxy | `internal/s3/policy_integration_test.go`, `scripts/curl_security_smoke.sh` |

## Decisions
- Public exposure is limited to S3 API; /v1/meta/* and /v1/replication/* are internal-only via proxy allowlist/mTLS.
- Request/body limits follow the recommended limits in Assumptions.
- TLS requirement is edge-only (proxy or native TLS); internal HTTP behind the proxy is acceptable.
- Proxy/WAF global rate limits follow the baseline values in Assumptions (deployment may tune).
