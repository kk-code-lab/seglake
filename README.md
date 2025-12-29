# Seglake

A minimal, S3‑compatible object store focused on correctness and hard durability.
Implementation: append‑only segments + object manifests + metadata in SQLite (WAL, synchronous=FULL).

---

## Key features

- S3 API: PUT/GET/HEAD, ListObjects V1/V2, ListBuckets, Range GET (single and multi‑range)
- SigV4 + presigned URL (SigV2 not supported)
- SigV4 streaming uploads (`Content-Encoding: aws-chunked`) with chunk/trailer validation
- aws-chunked parser fuzz tests (SigV4 streaming)
- Multipart upload (init/upload/list/complete/abort)
- Durability contract: fsync segments + WAL commit before ACK
- Append‑only segments, 4 MiB chunking, BLAKE3 per chunk
- Ops tooling: fsck, scrub, rebuild-index, snapshot, gc-plan/run, gc-rewrite, mpu-gc, repl-validate
- Access policies (MVP): per-key + bucket policies + conditions
- Public buckets (unsigned access) via `-public-buckets` + bucket policy

---

## Quick start

### Build

```
make build
```

### Run (dev)

```
./build/seglake -data-dir ./data -access-key test -secret-key testsecret
```

Default address: `:9000`.

---

## Examples (awscli)

List buckets:

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 ls --endpoint-url http://localhost:9000
```

PUT/GET:

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp ./file.bin s3://demo/file.bin --endpoint-url http://localhost:9000

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp s3://demo/file.bin ./file.bin --endpoint-url http://localhost:9000
```

More: `docs/ops.md`.

---

## S3 compatibility (selected)

| Feature | Status | Notes |
| --- | --- | --- |
| ListBuckets | Yes | `GET /` |
| ListObjects V1 | Yes | `GET /<bucket>?prefix=...` |
| ListObjects V2 | Yes | `GET /<bucket>?list-type=2` |
| GetBucketLocation | Yes | `GET /<bucket>?location` |
| CreateBucket | Yes | `PUT /<bucket>` |
| DeleteBucket | Yes | Only if empty |
| PutObject | Yes | `PUT /<bucket>/<key>` |
| GetObject | Yes | `GET /<bucket>/<key>` |
| HeadObject | Yes | `HEAD /<bucket>/<key>` |
| DeleteObject | Yes | Idempotent |
| Versioned GET/HEAD/DELETE | Yes | `?versionId=...` |
| Range GET | Yes | Single + multi‑range |
| CopyObject | Yes | `x-amz-copy-source` |
| Multipart upload | Yes | init/upload/list/complete/abort/list uploads |
| SigV4 auth | Yes | Header + presigned |
| SigV4 streaming | Yes | `aws-chunked` + trailer checksum validation |
| SigV2 auth | No | Not supported |
| Presigned URLs | Yes | TTL 1..7 days |
| ETag behavior | Yes | Single = MD5, multipart = md5(part md5s) + "-N" |
| CORS / OPTIONS | Yes | Preflight supported |

Full scope: `docs/spec.md`.

---

## Architecture (short)

- **Chunking:** 4 MiB, BLAKE3 per chunk
- **Segments:** append‑only, ~1 GiB or 10 min idle
- **Manifests:** binary files describing object layout
- **Metadata:** SQLite (WAL, synchronous=FULL)
- **Durability barrier:** fsync segments → write manifest + meta tx → WAL flush → ACK

Full details: `docs/spec.md`.

---

## Operations and maintenance

Available modes: `status`, `fsck`, `scrub`, `rebuild-index`, `snapshot`, `support-bundle`,
`gc-plan`/`gc-run`, `gc-rewrite`, `mpu-gc-plan`/`mpu-gc-run`, `repl-validate`, `buckets`.

Checklists and examples: `docs/ops.md`  
Smoke scripts: `scripts/`  
Helper CLI: `scripts/segctl`

Quick examples:
- `scripts/segctl bucket list`
- `scripts/segctl key list`
- `scripts/segctl bucket-policy get`

Notes:
- Admin wrappers for keys, buckets, bucket policies.
- Uses `SEGLAKE_DATA_DIR` (default `./data`).

---

## Replication (multi‑site)

Model: LWW + tombstone, HLC for ordering.
Modes: `repl-pull`, `repl-push`, `repl-bootstrap`.

Examples and notes: `docs/ops.md`.

---

## Limits and API behavior (selected)

- Max object size: `-max-object-size` (default 5 GiB, 0 = unlimited)
- Multipart min part size: 5 MiB (except the last part)
- Presigned TTL: 1..7 days
- Virtual-hosted style enabled by default

Full list: `docs/spec.md`.

---

## Development

Key Makefile targets:

```
make build
make run
make test
make test-race
make fmt
make lint
make check
```

---

## Documentation

- `docs/spec.md` — spec/behavior and API scope
- `docs/ops.md` — deployment, TLS, policies, GC, repl
- `docs/optimization.md` — performance notes
- `docs/roadmap.md` — roadmap / planned work
- `examples/` — systemd, Caddy, public bucket policy

---

## License

`LICENSE`
