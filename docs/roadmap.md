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
- [api][repl] Add API ergonomics to surface conflict presence (response headers or listing hints).
- [repl] Decide conflict handling for delete vs put (mark delete conflicts explicitly).

## Next
- [observability][repl] Add conflict metrics per bucket/key to highlight hotspots.
- [integrity][ops][repl] Add repl-validate deep mode (verify chunk hashes) for optional integrity checks.
- [integrity][repl] Add durable fsync for replication writes (fsync segment file after WriteSegmentRange) and only then mark SEALED in metadata.
- [api] Add object tagging (Get/Put/DeleteObjectTagging).

## Later / Research
- [observability][perf] Add per-stage timing metrics for MPU complete (part manifest fetch, barrier wait, meta tx).
- [perf][research] Evaluate variant C further under higher concurrency and on production-like hardware.
- [perf][research] Consider read-path optimizations if virtual manifests increase read latency in real workloads.
- [perf][research] Reduce barrier pressure by combining meta updates in fewer transactions.
- [api][research] Evaluate SSE-C (client-provided keys) feasibility and operational risks.
- [storage][research] Evaluate a pending-MPU-DEK design so encrypted MPU-created objects can use one DEK for the final object without data rewrite.
- [storage][research] Evaluate AAD v2 with stronger object-context binding if MPU/GC paths can re-encrypt or preserve stable object context.
- [api][research] Evaluate optional opaque ETag mode for encrypted objects.
- [repl][research] Evaluate encrypted replication between peers with independent KEKs via rewrap or decrypt/re-encrypt flows.
