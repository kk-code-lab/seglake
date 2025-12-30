# Roadmap

This is the single source of truth for planned work and open questions.

Tag legend (order used below):
- [api] API behavior and ergonomics
- [integrity] Durability, data safety, and correctness checks
- [observability] Metrics, diagnostics, and reporting
- [ops] Ops tooling and operational workflows
- [perf] Performance and latency improvements
- [repl] Replication and conflict handling
- [research] Exploratory or validation work

## Now
- [api][ops][repl] Expose conflict listings via an ops command or refine `/v1/meta/conflicts` UX for reviewing `CONFLICT` versions.
- [api][repl] Add API ergonomics to surface conflict presence (response headers or listing hints).
- [repl] Decide conflict handling for delete vs put (mark delete conflicts explicitly).

## Next
- [observability][repl] Add conflict metrics per bucket/key to highlight hotspots.
- [integrity][ops][repl] Add repl-validate deep mode (verify chunk hashes) for optional integrity checks.
- [integrity][repl] Add durable fsync for replication writes (fsync segment file after WriteSegmentRange) and only then mark SEALED in metadata.
- [api] Support `If-Match: *` semantics for overwrite guard (only if object exists).
- [api] Add object tagging (Get/Put/DeleteObjectTagging).

## Later / Research
- [observability][perf] Add per-stage timing metrics for MPU complete (part manifest fetch, barrier wait, meta tx).
- [perf][research] Evaluate variant C further under higher concurrency and on production-like hardware.
- [perf][research] Consider read-path optimizations if virtual manifests increase read latency in real workloads.
- [perf][research] Reduce barrier pressure by combining meta updates in fewer transactions.
