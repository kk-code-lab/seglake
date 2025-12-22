# Ops / Deployment Notes

## TLS reverse proxy checklist

1) Terminate TLS in a reverse proxy (nginx, Caddy, Envoy).
2) Keep Seglake on HTTP behind the proxy on a trusted network.
3) Enforce HTTPS at the edge (redirect or 301).
4) Pass through `Host` and `X-Forwarded-For` only from trusted IPs.
5) Virtual-hosted-style is enabled by default; ensure DNS and proxy routing by host.
6) Set request size limits at the proxy if needed (S3 SDKs may retry on 413).
7) Tune proxy timeouts/keepalive for large PUT/GET; disable buffering only if you need streaming behavior.
8) Keep access logs/metrics at the proxy (Seglake redacts presigned secrets).

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

## Native TLS (optional)

Seglake can serve HTTPS directly:
```
./build/seglake -tls -tls-cert certs/localhost.crt -tls-key certs/localhost.key
```

Notes:
- Self-signed certs require `--no-verify-ssl` or equivalent in clients.
- Certificates are hot-reloaded when the cert/key files change.

## awscli examples (SigV4)

List buckets:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 ls --endpoint-url http://localhost:9000
```

List objects:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 ls s3://demo --endpoint-url http://localhost:9000
```

PUT object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp ./file.bin s3://demo/file.bin --endpoint-url http://localhost:9000
```

GET object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp s3://demo/file.bin ./file.bin --endpoint-url http://localhost:9000
```

## Virtual-hosted vs path-style

Virtual-hosted-style is enabled by default (`-virtual-hosted=true`). Hostnames that are IPs or `localhost` are ignored to keep path-style working locally.

Examples:
```
aws s3 ls --endpoint-url http://localhost:9000
aws s3 ls --endpoint-url http://bucket.localhost:9000
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

Continuous pull with backoff:
```
./build/seglake -mode repl-pull -repl-remote http://peer:9000 -repl-watch -repl-interval 5s -repl-backoff-max 1m
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
- Watermarki są przechowywane per-remote (pull i push osobno).
- Endpointy repl są chronione politykami (`ReplicationRead` / `ReplicationWrite`).
- `/v1/meta/stats` zawiera sekcję `replication` (lag i backlog).

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

Bucket policies:
```
./build/seglake -mode bucket-policy -bucket-policy-action set -bucket-policy-bucket=demo -bucket-policy='{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}]}]}'
./build/seglake -mode bucket-policy -bucket-policy-action get -bucket-policy-bucket=demo
./build/seglake -mode bucket-policy -bucket-policy-action delete -bucket-policy-bucket=demo
```

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

Actions: `ListBuckets`, `ListBucket`, `GetBucketLocation`, `GetObject`, `HeadObject`, `PutObject`,
`DeleteObject`, `DeleteBucket`, `CopyObject`, `CreateMultipartUpload`, `UploadPart`,
`CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`, `ListMultipartParts`,
`GetMetaStats`, `ReplicationRead`, `ReplicationWrite`, `*`.

Conditions (optional) in statements:
- `source_ip`: list of CIDR blocks (e.g. `"10.0.0.0/8"`).
- `before` / `after`: RFC3339 time window.
- `headers`: exact match on request headers (lowercased keys).

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
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret ls
```

List objects:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret ls s3://demo
```

PUT object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret put ./file.bin s3://demo/file.bin
```

GET object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret get s3://demo/file.bin ./file.bin
```
