# Ops / Deployment Notes

Threat model: `docs/security/threat-model.md`.

## Production hardening checklist

- Terminate TLS (proxy or native TLS), enforce HTTPS, and restrict trusted proxy IPs.
- Store data on durable storage (separate volume), monitor disk usage, and alert on low space.
- Enable access logging and ship logs to central storage (request IDs are included).
- Set appropriate request size limits at the proxy and tune timeouts for large objects.
- Configure periodic snapshots of `meta.db` and test restores regularly.
- Monitor `/v1/meta/stats` and add external metrics/alerts (latency, errors, replay_detected, replication lag).
- Use separate API keys per app/service and restrict buckets via allow-list + policies.
- Keep a GC/MPU GC schedule and review reclaim reports before delete modes.
- Validate replication health (repl-validate) and plan for conflict review workflows.
- Keep compatibility-first defaults unless you have validated client behavior; enable hardening knobs intentionally.

Recommended limits (baseline):
- Max object size (app): 5 GiB via `-max-object-size`.
- Max header bytes (app): 32 KB via `-max-header-bytes` (0 uses Go default).
- Max URL/query length (app): 32 KB via `-max-url-length` (0 disables).
- Proxy request body limit: 5.1 GiB (slightly above max object size).
- Proxy header size: 16–32 KB.
- Proxy URL/query length: 16–32 KB (presigned URLs can be long).
- Proxy timeouts: header ~10s; body based on max object size and expected throughput.
- Proxy/WAF rate limits (baseline): per-IP 200 RPS (burst 400), per-key 500 RPS (burst 1000), global 2000 RPS (burst 4000).

Secrets handling (ops runbook expectations):
- Store API keys and secrets in a dedicated secret manager (or encrypted env file on disk).
- Rotate secrets on a fixed schedule (e.g., every 90 days) and on incident.
- Audit access to secrets and remove unused keys.
- For SSE-S3 with the local provider, back up KEKs separately from object data and metadata. Losing a KEK makes all object versions wrapped by that key unreadable; leaking a KEK compromises all DEKs wrapped by it.
- For SSE-S3 with Vault Transit, back up and operate Vault independently. Seglake stores Vault ciphertext EDEKs in manifests, not Vault tokens or plaintext DEKs.

## Online backups (recommended flow)

For consistent backups, avoid copying the data directory while writes are in flight. The safe pattern is:

1) Enable maintenance (write-quiesce).
2) Run `snapshot` (copies `meta.db` + WAL/SHM).
3) Backup the data directory (segments + manifests + snapshot output).
4) Disable maintenance.

If you must back up live data without maintenance, be aware of potential meta/data skew. Restores may require `rebuild-index`, and missing chunks can surface as read errors.

Even in maintenance mode, using `snapshot` is recommended because it captures `meta.db` together with WAL/SHM in a safe, consistent bundle and reduces operator error when copying metadata by hand.

Example (systemd drop-in idea):
```
ExecStartPre=/opt/seglake/build/seglake -mode maintenance -maintenance-action enable -data-dir /var/lib/seglake
ExecStartPre=/opt/seglake/build/seglake -mode snapshot -data-dir /var/lib/seglake -snapshot-dir /var/lib/seglake/snapshots/snapshot-$(date -u +%Y%m%dT%H%M%SZ)
ExecStartPost=/opt/seglake/build/seglake -mode maintenance -maintenance-action disable -data-dir /var/lib/seglake
```

Restore checklist (typical):
1) Restore the data directory (segments/manifests) and a snapshot directory.
2) Copy snapshot `meta.db` + `meta.db-wal` + `meta.db-shm` into the data dir.
3) Start the server. If metadata looks inconsistent, run `rebuild-index`.

## TLS reverse proxy checklist

1) Terminate TLS in a reverse proxy (nginx, Caddy, Envoy).
2) Keep Seglake on HTTP behind the proxy on a trusted network.
3) Enforce HTTPS at the edge (redirect or 301).
4) Pass through `Host` and `X-Forwarded-For` only from trusted IPs.
5) Virtual-hosted-style is enabled by default; ensure DNS and proxy routing by host.
6) Set request size limits at the proxy if needed (S3 SDKs may retry on 413).
7) Tune proxy timeouts/keepalive for large PUT/GET; disable buffering only if you need streaming behavior.
8) Keep access logs/metrics at the proxy (Seglake redacts presigned secrets).
9) Set CORS at Seglake if needed (see flags below).

Example (nginx, minimal):
```
server {
  listen 443 ssl;
  server_name s3.example.com;
  ssl_certificate /etc/ssl/cert.pem;
  ssl_certificate_key /etc/ssl/key.pem;

  location / {
    proxy_pass http://127.0.0.1:9000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_request_buffering off;
    proxy_buffering off;
  }
}
```

Example (Caddy):
```
# Path-style (bucket in URL path)
s3.example.com {
  reverse_proxy 127.0.0.1:9000
}

# Virtual-hosted-style (bucket as subdomain). Requires wildcard DNS.
# s3.example.com, *.s3.example.com {
#   reverse_proxy 127.0.0.1:9000
# }
```

Notes:
- The repo ships a ready-to-edit `examples/Caddyfile`.
- Uncomment the virtual-hosted block only if you have wildcard DNS configured.
- The hardening block in `examples/Caddyfile` must be enabled and configured (CIDR allowlist/TLS) to meet the threat model.
- The optional rate-limit example in `examples/Caddyfile` requires a Caddy plugin (e.g., `caddy-ratelimit`).

Example (Caddy, HTTPS redirect + request size limit):
```
http://s3.example.com {
  redir https://s3.example.com{uri}
}

s3.example.com {
  request_body {
    # Adjust to your largest expected PUT size.
    max_size 10GB
  }
  reverse_proxy 127.0.0.1:9000
}
```

## Native TLS (optional)

Seglake can serve HTTPS directly:
```
./build/seglake -tls -tls-cert certs/localhost.crt -tls-key certs/localhost.key
```

Notes:
- Self-signed certs require `--no-verify-ssl` or equivalent in clients.
- Certificates are hot-reloaded when the cert/key files change.
- Replay protection is disabled by default; enable with `-replay-ttl` (logs by default) and `-replay-block` to hard-block after validating clients.

## Compatibility vs hardening defaults

Compatibility-first defaults (safe for most clients):
- Allow SigV4 UNSIGNED-PAYLOAD (default).
- Do not require Content-MD5 (default).
- Replay protection disabled by default.

Hardening knobs (opt-in, may break some clients):
- Enable replay protection (`-replay-ttl`, optionally `-replay-block`).
- Require Content-MD5 (`-require-content-md5=true`).
- Disallow unsigned payloads (`-allow-unsigned-payload=false`).

## Environment variables (12-factor)

CLI flags override env vars; env vars override defaults. `-secrets-file` loads KEY=VALUE pairs that act like env defaults without exporting them to the process environment.

Server flags:
- `SEGLAKE_DATA_DIR` → `-data-dir`
- `SEGLAKE_ADDR` → `-addr`
- `SEGLAKE_ACCESS_KEY` → `-access-key`
- `SEGLAKE_SECRET_KEY` → `-secret-key`
- `SEGLAKE_REGION` → `-region`
- `SEGLAKE_TLS` → `-tls` (true/false)
- `SEGLAKE_TLS_CERT` → `-tls-cert`
- `SEGLAKE_TLS_KEY` → `-tls-key`
- `SEGLAKE_SSE_S3_ENABLED` → `-sse-s3-enabled`
- `SEGLAKE_SSE_S3_PROVIDER` → `-sse-s3-provider` (`local`, default, or `vault-transit`)
- `SEGLAKE_SSE_S3_ACTIVE_KEY` → `-sse-s3-active-key`
- `SEGLAKE_SSE_S3_KEKS` → comma-separated KEK specs: `key-id=env:ENV_NAME` or `key-id=file:/path/to/key`
- `SEGLAKE_SSE_S3_KEK_B64` → single-key convenience secret for the active key
- `SEGLAKE_SSE_S3_VAULT_ADDR` / `VAULT_ADDR` → `-sse-s3-vault-addr`
- `SEGLAKE_SSE_S3_VAULT_MOUNT` → `-sse-s3-vault-mount` (default `transit`)
- `SEGLAKE_SSE_S3_VAULT_TOKEN_FILE` → `-sse-s3-vault-token-file`
- `SEGLAKE_SSE_S3_VAULT_TOKEN` / `VAULT_TOKEN` → Vault token source
- `SEGLAKE_SSE_S3_VAULT_NAMESPACE` → `-sse-s3-vault-namespace`
- `SEGLAKE_SSE_S3_VAULT_TIMEOUT` → `-sse-s3-vault-timeout` (default `5s`)

SSE-S3 KEKs are base64-encoded 32-byte keys. CLI flags and env config support multiple KEKs so an old key can remain readable while a new active key handles writes:
```
SEGLAKE_SSE_S3_ENABLED=true
SEGLAKE_SSE_S3_ACTIVE_KEY=local:v2
SEGLAKE_SSE_S3_KEKS=local:v2=env:SEGLAKE_SSE_S3_KEK_V2_B64,local:v1=file:/etc/seglake/sse/local-v1.key
```

Do not place raw KEK values in logs or command histories. Prefer `file:` sources with restrictive permissions or env vars loaded from a secret manager.

The supported key-provider backends are `local` and `vault-transit`. The provider choice is transparent to S3 clients: encrypted writes, bucket defaults, HEAD/GET response headers, rewrap, scrub, and replication still use the SSE-S3 `AES256` surface. Vault-backed writes use Vault Transit data keys and store Vault ciphertext EDEKs in manifest v3; payload chunks are still encrypted locally with AES-256-GCM.

Example Vault Transit-backed server:
```
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=dev-root

./build/seglake -data-dir ./data \
  -sse-s3-enabled \
  -sse-s3-provider vault-transit \
  -sse-s3-active-key seglake-test \
  -sse-s3-vault-mount transit
```

Do not pass Vault tokens as command-line values. Use `SEGLAKE_SSE_S3_VAULT_TOKEN`, `VAULT_TOKEN`, or `-sse-s3-vault-token-file`.

Local Vault dev smoke setup:
```
vault server -dev -dev-root-token-id=dev-root

export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=dev-root
vault secrets enable transit
vault write -f transit/keys/seglake-test
```

Use this only for local smoke tests. Dev mode is not a production Vault configuration.

SSE-S3 KEK rewrap rotates EDEKs without rewriting segment ciphertext. Build a redacted plan first, then run it with the target and selected source KEKs configured:
```
./build/seglake -mode sse-rewrap-plan -data-dir ./data \
  -sse-s3-rewrap-target-key local:v2 \
  -sse-s3-kek local:v1=file:/etc/seglake/sse/local-v1.key \
  -sse-s3-kek local:v2=file:/etc/seglake/sse/local-v2.key \
  -sse-s3-rewrap-plan ./rewrap-v2.json

./build/seglake -mode sse-rewrap-run -data-dir ./data \
  -sse-s3-rewrap-target-key local:v2 \
  -sse-s3-kek local:v1=file:/etc/seglake/sse/local-v1.key \
  -sse-s3-kek local:v2=file:/etc/seglake/sse/local-v2.key \
  -sse-s3-rewrap-from-plan ./rewrap-v2.json
```

The plan contains version IDs, bucket/key names, manifest paths, key refs, key IDs, and short EDEK fingerprints only. It does not contain DEKs, KEKs, Vault tokens, or raw EDEKs. By default, all encrypted key entries not already wrapped by the target key are selected; use repeatable `-sse-s3-rewrap-source-key <key-id>` to limit the source keys. `sse-rewrap-run` is local-only and does not route through the admin socket so provider credentials are not sent to the running server process. Peers must have the target local KEK or Vault provider access before they can read rewrapped encrypted objects after replication.

Deep encrypted scrub verifies that SSE-S3 encrypted object chunks can unwrap their DEKs and pass AES-GCM authentication. Normal `scrub` remains shallow and needs no KEKs or Vault access. Deep scrub is local-only when key-provider config is supplied. With the local provider it uses the same `-sse-s3-kek` / `SEGLAKE_SSE_S3_KEKS` sources as rewrap:
```
./build/seglake -mode scrub -data-dir ./data \
  -scrub-deep-encrypted \
  -sse-s3-kek local:v2=file:/etc/seglake/sse/local-v2.key \
  -sse-s3-kek local:v1=file:/etc/seglake/sse/local-v1.key
```

For Vault-backed objects, pass `-sse-s3-provider vault-transit` plus the Vault address/token/mount flags or env vars. Missing KEKs, missing Vault provider access, EDEK unwrap failures, malformed encryption metadata, short encrypted reads, and AEAD tag failures are reported as scrub errors. Affected encrypted versions are marked `DAMAGED`. The report includes only redacted diagnostics such as key IDs and counters; it never includes KEKs, DEKs, Vault tokens, or raw EDEKs.

Manifest GC removes orphan manifest files left by operations such as SSE-S3 KEK rewrap. It never removes segments, SQLite rows, object versions, or MPU metadata. Build a plan first, then run it explicitly:
```
./build/seglake -mode manifest-gc-plan -data-dir ./data \
  -manifest-gc-ttl 168h \
  -manifest-gc-plan ./manifest-gc.json

./build/seglake -mode manifest-gc-run -data-dir ./data \
  -manifest-gc-from-plan ./manifest-gc.json \
  -manifest-gc-force
```

`manifest-gc-plan` treats current object manifests and active MPU part manifests as live. A run rechecks that each planned file is still orphaned and that size, mtime, and SHA-256 fingerprint still match before deleting it. Missing or changed planned files are skipped, not removed.

Ops/maintenance flags:
- `SEGLAKE_DATA_DIR` → `-data-dir` (modes: `ops`, `keys`, `bucket-policy`, `buckets`; when the server is running these use the admin socket + token in the data dir)
- `SEGLAKE_SSE_S3_REWRAP_TARGET_KEY` → `-sse-s3-rewrap-target-key` (modes: `sse-rewrap-plan`, `sse-rewrap-run`)

## awscli examples (SigV4)

List buckets:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls --endpoint-url http://localhost:9000
```

List objects:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls s3://demo --endpoint-url http://localhost:9000
```

PUT object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 cp ./file.bin s3://demo/file.bin --endpoint-url http://localhost:9000
```

PUT object with explicit SSE-S3:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 cp ./file.bin s3://demo/file.bin --endpoint-url http://localhost:9000 --sse AES256
```

Enable bucket default SSE-S3 for future writes without per-object `--sse`:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3api put-bucket-encryption \
  --bucket demo \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' \
  --endpoint-url http://localhost:9000
```

Inspect or clear bucket default SSE-S3:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3api get-bucket-encryption --bucket demo --endpoint-url http://localhost:9000
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3api delete-bucket-encryption --bucket demo --endpoint-url http://localhost:9000
```

GET object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 cp s3://demo/file.bin ./file.bin --endpoint-url http://localhost:9000
```

HTTPS with custom CA (no AWS config):
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 AWS_CA_BUNDLE=~/.aws/ca/custom-ca.pem aws s3 ls --endpoint-url https://s3.example.com
```

Force path-style via env (no AWS config):
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 AWS_S3_ADDRESSING_STYLE=path aws s3 ls --endpoint-url http://localhost:9000
```

Endpoint override via env (no AWS config):
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 AWS_ENDPOINT_URL_S3=http://localhost:9000 aws s3 ls
```

Endpoint + custom CA via env (no AWS config):
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 AWS_ENDPOINT_URL_S3=https://s3.example.com AWS_CA_BUNDLE=~/.aws/ca/custom-ca.pem aws s3 ls
```

Unsafe (debug only): skip TLS verification
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls --endpoint-url https://s3.example.com --no-verify-ssl
```

## Virtual-hosted vs path-style

Virtual-hosted-style is enabled by default (`-virtual-hosted=true`). Hostnames that are IPs, `localhost`, or lack a dot are ignored to keep path-style working locally.

Examples:
```
aws s3 ls s3://demo --endpoint-url http://localhost:9000
aws s3 ls s3://demo --endpoint-url http://demo.localhost:9000
```

## Smoke scripts (curl)

The repo includes three smoke scripts for quick checks against a running server:
- `scripts/curl_s3_smoke.sh` (basic S3 PUT/GET/HEAD/Range/conditions/CORS)
- `scripts/curl_security_smoke.sh` (auth/validation error responses; includes a brief sleep to avoid auth rate limiting)
- `scripts/curl_public_bucket_smoke.sh` (public bucket unsigned GET + signed setup)

Example:
```
./build/seglake -data-dir ./data -access-key test -secret-key testsecret
S3_ENDPOINT=http://localhost:9000 S3_HOST=localhost:9000 ./scripts/curl_s3_smoke.sh
S3_ENDPOINT=http://localhost:9000 S3_HOST=localhost:9000 ./scripts/curl_security_smoke.sh
```

Note:
`-mode server` (and `-mode repl-bootstrap`) will create `--data-dir` if it doesn't exist.
Other modes expect the directory to already exist and will return an error if it doesn't.

## Test strategy (matrix)

Below is a compact test map that helps catch regressions without deep knowledge of internals.
It works well as a checklist before release or after larger meta/storage changes.

Correctness (S3 contract):
- Unit: XML/query parsers, range parsing, ETag, SigV4 canonicalization.
- Integration: engine + meta (PUT/GET/DELETE/versioning).
- E2E: list v1/v2, multipart, copy, range/multi-range, conditional headers.
- Conformance: compare against reference S3/MinIO (differential).

Durability / crash consistency:
- Unit: write barrier + fsync ordering.
- Integration: crash after segment write before meta flush.
- E2E: crash harness (kill -9), fsck + rebuild-index, scrub after corruption.

Concurrency / contention:
- Unit: locks, barrier, SQLite busy handling.
- Integration: concurrent PUT/DELETE/MPU.
- Stress: `mpu_heavy`, `gc_pressure`, burst profiles.

Performance / latency:
- Bench: PUT/GET (1MiB, 64MiB, 1GiB).
- Stress: p95/p99 under increasing concurrency.
- Soak: long runs (1–24h) under steady load.

Security / auth:
- Unit: SigV4 validation, presign, replay cache.
- E2E: policies (allow/deny), bucket policy + header conditions.
- Negative: bad signatures, time skew, missing signed headers.

Metadata integrity:
- Integration: rebuild-index from manifests, fsck vs meta.
- E2E: list correctness during write/delete.
- Ops: snapshot/support-bundle.

Replication (when 2+ nodes are available):
- Integration: repl pull/push + oplog apply.
- E2E: repl-validate drift.
- Soak: long runs with conflicts.

Note: `scripts/stress_s3.sh` includes profiles for quickly surfacing contention
and performance regressions (especially MPU + DELETE).

## Server lock / heartbeat (CLI safety)

When `-mode server` starts, it creates a heartbeat file in `--data-dir`:
`.seglake.lock` (updated every 5s, considered stale after 15s).
This is used by CLI modes to detect a running server and prevent unsafe
operations on live data.

Behavior:
- Starting a second server on the same `--data-dir` is blocked if the heartbeat is fresh.
- “Unsafe live” modes prompt before running if a server is detected.
- Use `-yes` to skip the prompt.

Mode safety (when server is running):

Safe (no prompt):

| Mode | Note |
| --- | --- |
| `status`, `fsck`, `scrub`, `snapshot`, `gc-plan`, `gc-rewrite-plan`, `manifest-gc-plan`, `mpu-gc-plan`, `sse-rewrap-plan`, `support-bundle`, `keys`, `bucket-policy`, `buckets`, `maintenance`, `repl-validate` | Read-only or metadata changes only. |

Unsafe (prompt required, maintenance quiesced):

| Mode | Note |
| --- | --- |
| `rebuild-index`, `gc-run`, `gc-rewrite`, `gc-rewrite-run`, `manifest-gc-run`, `mpu-gc-run`, `sse-rewrap-run`, `db-integrity-check`, `db-reindex` | Touches meta or rewrites data/metadata; use maintenance window. |

Fsck/scrub scope:
- By default `fsck` and `scrub` scan **live manifests** from `meta.db` (plus active MPU parts) to avoid false “missing segment” reports after GC.
- Use `-fsck-all-manifests` / `-scrub-all-manifests` to scan every manifest file on disk (including orphans).
- Use `-scrub-deep-encrypted` to verify SSE-S3 DEK unwrap and AEAD tags for encrypted manifests selected by the same scrub scope.

Status:
- `status` reports `live_manifests` (from `meta.db` + MPU parts) when available; falls back to disk-only counts if meta can't be opened.

Examples:
```
./build/seglake -mode gc-run -data-dir /var/lib/seglake -gc-force
./build/seglake -mode gc-run -data-dir /var/lib/seglake -gc-force -yes
```

## Maintenance mode (read-only)

Toggle a global read-only switch that blocks write operations over HTTP while still allowing GET/HEAD/LIST:
```
./build/seglake -mode maintenance -maintenance-action status
./build/seglake -mode maintenance -maintenance-action enable
./build/seglake -mode maintenance -maintenance-action disable
```
Notes:
- States: `off` → `entering` → `quiesced` (writes drained) → `exiting` → `off`.
- Applies to S3 and replication write endpoints (writes only).
- Read-only operations continue to work.
- `maintenance-action enable` waits for `quiesced` and `disable` waits for `off` when a server is running (use `-maintenance-no-wait` to return immediately).
- Unsafe ops allowed without a live prompt when maintenance is `quiesced`: `gc-run`, `gc-rewrite`, `gc-rewrite-run`, `mpu-gc-run`.
- `maintenance status` reports `write_inflight` when the server is running.
- `/v1/meta/stats` includes `maintenance_state`, `maintenance_updated_at`, `write_inflight`, and `maintenance_transitions`.
- `/v1/meta/stats` includes `live_manifests` (count from meta + MPU parts) and `manifests_total` (all manifest files on disk).
- Smoke script: `scripts/maintenance_smoke.sh` (expects a running server and `SEGLAKE_DATA_DIR`).
- `segctl` helper:
  - `scripts/segctl maintenance status|enable|disable`
  - `scripts/segctl ops <mode> -- <flags>`
    - example: `scripts/segctl ops gc-run -- -gc-force`
  - `scripts/segctl bucket delete <bucket> --force` (deletes live objects first; delete markers remain)
  - `scripts/segctl stats --endpoint http://127.0.0.1:9000 --access test --secret testsecret`
  - `segctl` uses the admin socket when a live server heartbeat is detected.
  - `scripts/segctl db integrity-check` / `scripts/segctl db reindex [--table api_keys]`

Admin over Unix socket (server-side):
- When the server is running, admin commands that modify SQLite (ops, keys, buckets, bucket-policy, maintenance, repl) run via a local-only Unix socket.
- Socket path: `data/.seglake-admin.sock` (relative to `-data-dir`).
- Token path (required): `data/.seglake-admin.token` (relative to `-data-dir`, 0600). CLI reads the token and sends `X-Seglake-Admin-Token`.
- Windows limitation: Unix sockets are not available; local admin channel is unavailable there.

Example (maintenance + gc-run via admin socket):
```
./build/seglake -mode maintenance -maintenance-action enable
./build/seglake -mode gc-run -gc-force
./build/seglake -mode maintenance -maintenance-action disable
```

Example (skip waiting on enable/disable):
```
./build/seglake -mode maintenance -maintenance-action enable -maintenance-no-wait
./build/seglake -mode maintenance -maintenance-action disable -maintenance-no-wait
```

## GC/MPU guardrails

GC warnings and hard limits can be tuned:
```
./build/seglake -mode gc-plan -gc-warn-segments=100 -gc-warn-reclaim-bytes=$((100<<30))
./build/seglake -mode gc-run -gc-force -gc-max-segments=50 -gc-max-reclaim-bytes=$((10<<30))
./build/seglake -mode mpu-gc-plan -mpu-warn-uploads=1000 -mpu-warn-reclaim-bytes=$((10<<30))
./build/seglake -mode mpu-gc-run -mpu-force -mpu-max-uploads=500 -mpu-max-reclaim-bytes=$((5<<30))
```

## Replication (multi-site)

Pull oplog + fetch missing data:
```
./build/seglake -mode repl-pull -repl-remote http://peer:9000
```

Bootstrap a new node (snapshot + oplog):
```
./build/seglake -mode repl-bootstrap -repl-remote http://peer:9000 -repl-bootstrap-force
```

Continuous pull with backoff:
```
./build/seglake -mode repl-pull -repl-remote http://peer:9000 -repl-watch -repl-interval 5s -repl-backoff-max 1m -repl-retry-timeout 2m
```

Push local oplog:
```
./build/seglake -mode repl-push -repl-remote http://peer:9000
```

Continuous push:
```
./build/seglake -mode repl-push -repl-remote http://peer:9000 -repl-push-watch -repl-push-interval 5s -repl-push-backoff-max 1m
```

Notes:
- Watermarks are stored per-remote (pull and push separately).
- Replication endpoints are protected by policies (`ReplicationRead` / `ReplicationWrite`).
- `/v1/meta/stats` includes a `replication` section (lag and backlog).
- `/v1/meta/stats` also reports `replay_detected` (count of detected replays).

## Buckets (admin)

Manage bucket entries with `-mode buckets` (metadata only, bypasses S3 API):
```
./build/seglake -mode buckets -bucket-action list
./build/seglake -mode buckets -bucket-action create -bucket demo [-bucket-versioning enabled|suspended|disabled|unversioned]
./build/seglake -mode buckets -bucket-action exists -bucket demo
./build/seglake -mode buckets -bucket-action delete -bucket demo
```

## API keys / policies

Manage keys with `-mode keys`:
```
./build/seglake -mode keys -keys-action create -key-access=test -key-secret=testsecret -key-policy=rw -key-enabled=true -key-inflight=32
./build/seglake -mode keys -keys-action allow-bucket -key-access=test -key-bucket=demo
./build/seglake -mode keys -keys-action disallow-bucket -key-access=test -key-bucket=demo
./build/seglake -mode keys -keys-action list
./build/seglake -mode keys -keys-action list-buckets -key-access=test
./build/seglake -mode keys -keys-action enable -key-access=test
./build/seglake -mode keys -keys-action disable -key-access=test
./build/seglake -mode keys -keys-action delete -key-access=test
./build/seglake -mode keys -keys-action set-policy -key-access=test -key-policy='{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}'
```
Allow-list behavior:
- If an access key has one or more allowed buckets, `GET /` (ListBuckets) returns only those buckets.
- If the allow-list is empty, `GET /` returns all buckets (subject to policy).

Bucket policies:
```
./build/seglake -mode bucket-policy -bucket-policy-action set -bucket-policy-bucket=demo -bucket-policy='{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}]}]}'
./build/seglake -mode bucket-policy -bucket-policy-action get -bucket-policy-bucket=demo
./build/seglake -mode bucket-policy -bucket-policy-action delete -bucket-policy-bucket=demo
```

Public buckets (unsigned access):
- Enable on the server with `-public-buckets` (comma-separated bucket names).
- Unsigned requests are allowed **only** for those buckets **and** only if a bucket policy explicitly allows the action.
- Writes still require signed requests unless the policy explicitly allows them (not recommended).
- Listing all buckets (`GET /`) still requires signing.
Examples for systemd, Caddy, a public bucket policy, and `secrets.env` live in `examples/`.

Example (public read-only bucket):
```
./build/seglake -access-key test -secret-key testsecret -public-buckets public
./build/seglake -mode bucket-policy -bucket-policy-action set -bucket-policy-bucket=public -bucket-policy='{"version":"v1","statements":[{"effect":"allow","actions":["GetObject","HeadObject","ListBucket"],"resources":[{"bucket":"public"}]}]}'
curl http://localhost:9000/public/hello.txt
```

Bucket versioning:
- `PUT /<bucket>?versioning` supports `Enabled` and `Suspended` (AWS-compatible XML body).
- Create an unversioned bucket by sending `x-seglake-versioning: unversioned` on `PUT /<bucket>`.

Policies:
- `rw` (default): full access.
- `ro` / `read-only`: blocks PUT/POST/DELETE.

Custom JSON policy (stored in `api_keys.policy`):
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["GetObject", "ListBucket"],
      "resources": [
        { "bucket": "demo", "prefix": "public/" }
      ]
    }
  ]
}
```
Note: AWS-style policy JSON is accepted as input and mapped to Seglake policy (subset only; unsupported elements are rejected). Supported condition subset: IpAddress aws:SourceIp, DateGreaterThan/DateLessThan aws:CurrentTime, StringEquals/StringLike s3:prefix, StringEquals s3:delimiter, Bool aws:SecureTransport.

Example (AWS-style bucket policy input, allowed subset):
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::demo",
      "Condition": {
        "IpAddress": { "aws:SourceIp": ["10.0.0.0/8"] },
        "DateGreaterThan": { "aws:CurrentTime": "1970-01-01T00:00:00Z" },
        "DateLessThan": { "aws:CurrentTime": "2999-01-01T00:00:00Z" }
      }
    }
  ]
}
```

Actions: `ListBuckets`, `ListBucket`, `GetBucketLocation`, `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy`, `GetObject`, `HeadObject`, `PutObject`,
`DeleteObject`, `DeleteBucket`, `CopyObject`, `CreateMultipartUpload`, `UploadPart`,
`CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`, `ListMultipartParts`,
`GetMetaStats`, `GetMetaConflicts`, `ReplicationRead`, `ReplicationWrite`, `*`.

Conditions (optional) in statements:
- `source_ip`: list of CIDR blocks (e.g. `"10.0.0.0/8"`).
- `before` / `after`: RFC3339 time window.
- `headers`: exact match on request headers (lowercased keys).
- `require_sse_s3`: `true` requires effective SSE-S3 for PutObject, CopyObject destinations, and CreateMultipartUpload. Explicit `--sse AES256` and bucket default encryption both satisfy the requirement.

Example requiring SSE-S3 for future writes under a prefix:
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["PutObject", "CopyObject", "CreateMultipartUpload"],
      "resources": [
        { "bucket": "demo", "prefix": "secure/" }
      ],
      "conditions": {
        "require_sse_s3": true
      }
    },
    {
      "effect": "allow",
      "actions": ["UploadPart", "CompleteMultipartUpload", "AbortMultipartUpload"],
      "resources": [
        { "bucket": "demo", "prefix": "secure/" }
      ]
    }
  ]
}
```

Example with deny override:
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["GetObject"],
      "resources": [
        { "bucket": "demo" }
      ]
    },
    {
      "effect": "deny",
      "actions": ["GetObject"],
      "resources": [
        { "bucket": "demo", "prefix": "secret/" }
      ]
    }
  ]
}
```

Example read + list for a single bucket:
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["GetObject", "ListBucket"],
      "resources": [
        { "bucket": "demo" }
      ]
    }
  ]
}
```

## Request limits / CORS

Flags:
- `-max-object-size` (default 5 GiB, 0 = unlimited)
- `-require-content-md5` (default false)
- `-require-if-match-buckets` (comma-separated buckets or `*` to require `If-Match` on overwrite)

## HTTP timeouts / graceful shutdown

Flags:
- `-read-header-timeout` (default 10s)
- `-read-timeout` (default 30s)
- `-write-timeout` (default 30s)
- `-idle-timeout` (default 2m)
- `-shutdown-timeout` (default 10s)

Notes:
- For large PUT/GET, increase `-write-timeout` and `-read-timeout` to avoid disconnects.
- Graceful shutdown waits for in-flight requests up to `-shutdown-timeout`.
Example (large objects, slower clients):
```
./build/seglake -read-timeout 5m -write-timeout 5m -idle-timeout 5m -shutdown-timeout 30s
```

## Replay cache sizing

Replay protection uses an in-memory cache bounded by a max entries cap (default).
Use `-replay-ttl` to enable replay detection, `-replay-block` to enforce blocking on replays,
and `-replay-cache-max` to override the default cache size cap.

## Conflict visibility (MVP)

Endpoint:
- `GET /v1/meta/conflicts?bucket=...&prefix=...&limit=...&after_bucket=...&after_key=...&after_version=...`

Notes:
- Returns a JSON list of conflicting versions.
- GET/HEAD on an object with conflict returns `x-seglake-conflict: true`.
- `-replay-ttl` (default 0 = disabled)
- `-replay-block` (default false; block requests on replay detection)
- `-cors-origins` (default `*`, comma-separated list)
- `-cors-methods` (default `GET,PUT,HEAD,DELETE`)
- `-cors-headers` (default `authorization,content-md5,content-type,x-amz-date,x-amz-content-sha256`)
- `-cors-max-age` (default 86400)
- `-replay-ttl` (default 5m, 0 = disable replay protection)

## Curl smoke tests

S3 functionality:
```
./scripts/curl_s3_smoke.sh
```

Security checks:
```
./scripts/curl_security_smoke.sh
```

Both scripts can be configured via env vars:
```
S3_ENDPOINT=http://localhost:9000 S3_HOST=localhost:9000 S3_ACCESS_KEY=test S3_SECRET_KEY=testsecret S3_BUCKET=demo S3_OBJECT_KEY=spec.md S3_DATA_FILE=./docs/spec.md ./scripts/curl_s3_smoke.sh
```

Example with source IP + header condition:
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["GetObject"],
      "resources": [
        { "bucket": "demo", "prefix": "public/" }
      ],
      "conditions": {
        "source_ip": ["10.0.0.0/8"],
        "headers": { "x-tenant": "alpha" }
      }
    }
  ]
}
```

Example with time window:
```
{
  "version": "v1",
  "statements": [
    {
      "effect": "allow",
      "actions": ["GetObject"],
      "resources": [
        { "bucket": "demo" }
      ],
      "conditions": {
        "after": "2025-01-01T00:00:00Z",
        "before": "2026-01-01T00:00:00Z"
      }
    }
  ]
}
```

Proxy note:
- `X-Forwarded-For` is only trusted when the client IP matches `-trusted-proxies` CIDR list.

## s3cmd examples

List buckets:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 --access_key=test --secret_key=testsecret ls
```

List objects:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 --access_key=test --secret_key=testsecret ls s3://demo
```

PUT object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 --access_key=test --secret_key=testsecret put ./file.bin s3://demo/file.bin
```

GET object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 --access_key=test --secret_key=testsecret get s3://demo/file.bin ./file.bin
```
