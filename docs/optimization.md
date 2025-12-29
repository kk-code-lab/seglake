# Optimization Notes

## Context
This document tracks performance investigations and the optimizations applied so far.

### Test Environment
- Host: Apple M1 Mac Air (local dev)
- Server: `go run ./cmd/seglake/ -data-dir ./data`
- Stress script: `scripts/stress_s3.sh`
- Note: results reflect local hardware constraints and are best used for relative comparisons.

## Stress Results (local)

### Baseline PUT-only (4MiB, 180s, conc=4)
- avg ~770 ms, p95 ~861 ms, p99 ~957 ms

### MPU-heavy (30% MPU 64MiB + GET/DEL, 180s, conc=4)
- PUT avg ~2.84 s, p95 ~7.53 s, p99 ~8.66 s
- GET avg ~626 ms, p95 ~720 ms

## Changes Applied

### 1) MPU complete without data compose (manifest-backed)
- **What**: `CompleteMultipartUpload` now builds a new manifest from existing part manifests, instead of streaming and re-writing data.
- **Where**:
  - `internal/s3/multipart.go` — compose manifest from part manifests
  - `internal/storage/engine/engine.go` — `PutManifestWithCommit` to store a manifest without new chunk writes
- **Why**: removes large data copy from the hot path; keeps read-path unchanged because manifests reference real chunk refs.

### 2) Hot-path auth usage updates throttled
- **What**: `RecordAPIKeyUse` is now throttled (default 30s per key) and executed async.
- **Where**: `internal/s3/handler.go`
- **Why**: reduces per-request meta writes in busy traffic.

### 3) MPU complete limiter
- **What**: global limiter for concurrent `CompleteMultipartUpload`.
- **Where**:
  - `internal/s3/ratelimit.go` — `Semaphore`
  - `internal/s3/multipart.go` — acquire/release in complete handler
  - `cmd/seglake/main.go` — `-mpu-complete-limit` flag (default 4)
- **Why**: optional safety valve under high concurrency.
- **Status**: did not materially improve results at conc=4; useful mainly at higher concurrency.

## Open Questions / Next Steps

- Add per-stage timing metrics for MPU complete (part manifest fetch, barrier wait, meta tx).
- Evaluate variant C further under higher concurrency and on production-like hardware.
- Consider read-path optimizations if virtual manifests increase read latency in real workloads.
- Reduce barrier pressure by combining meta updates in fewer transactions.
