# Threat Model (Draft)

This is a lightweight threat model for Seglake. It is a starting point, not an audit.
Update assumptions and add mitigations/tests as the system evolves.

## Scope
- In scope: S3 API, metadata (SQLite), storage engine, replication, ops endpoints.
- Out of scope: cloud infra, OS hardening, TLS termination if handled by external proxy.

## Assumptions (current)
- Deployment exposure: public internet.
- Auth: SigV4 required for all endpoints, except explicitly configured public buckets with policy.
- TLS termination: proxy (typical); native TLS supported as an alternative mode.
- Rate limiting: partial (failed-auth limiter, per-key inflight, MPU Complete limiter; no global RPS limit by default).
- Replication peers: auth required if API keys exist; repl client can presign via repl-access-key/secret-key.

## Assets
- Object data (confidentiality, integrity, availability).
- Metadata (policies, versions, oplog, replication state).
- Credentials (API keys, secrets).
- Service availability.

## Threats and Mitigations (Draft)

| Asset | Entry point | Threat | Example attack | Mitigation (current/planned) | Test/Verify |
| --- | --- | --- | --- | --- | --- |
| Object data | S3 API (PUT/GET) | Tampering/Info disclosure | Read/write without auth | SigV4; public buckets require explicit allow + policy | E2E auth + policy tests |
| Metadata | On-disk formats | Tampering/DoS | Corrupt manifest/segment -> crash | Checksums, decoder validation | Fuzz decode + crash harness |
| Credentials | Config/env/API keys | Spoofing/Disclosure | Key leak -> full access | Least privilege, rotation | Secrets review |
| Auth (SigV4) | Headers/query | Spoofing/Replay | Bad canonicalization | Strict canonicalization, replay cache | Fuzz canonicalization + e2e |
| Policies | Policy JSON | Elevation | Policy parse bug bypass | Validate inputs, deny-by-default | Fuzz ParsePolicy |
| Availability | All inputs | DoS | Large bodies, range explosion | Size/time limits; global RPS limit at proxy/WAF | Load test + fuzz |
| Replication | Replication API | Spoofing/Tampering | Fake peer, oplog injection | SigV4 required if API keys exist; allowlist/mTLS at proxy | Repl integration tests |
| Ops endpoints | /v1/meta/*, ops | Elevation/DoS | Unauth access to ops | Require auth, network allowlist | Ops auth tests |

## Open questions
- Which endpoints are exposed publicly?
- What are the max request/body limits?
- Do we require TLS everywhere (proxy and/or native TLS)?
- Confirm proxy/WAF global RPS limits.
