# TODO

- Expose conflict listings (ops command or /v1/meta/conflicts) to review CONFLICT versions.
- Support `If-Match: *` semantics for overwrite guard (only if object exists).
- Add conflict metrics per bucket/key to highlight hotspots.
- Add repl-validate deep mode (verify chunk hashes) for optional integrity checks.
- Decide conflict handling for delete vs put (mark delete conflicts explicitly).
- Add API ergonomics to surface conflict presence (response headers or listing hints).
- Add durable fsync for replication writes (fsync segment file after WriteSegmentRange) and only then mark SEALED in metadata.

## Performance (stress findings)

- Add metrics for MPU complete (parts fetch, compose time, barrier wait, meta tx).
- Consider virtual/compound MPU completes (defer full compose, read from parts on-demand).
- Make API key usage tracking async or batched to reduce hot-path meta updates.
- Add MPU-specific inflight limiter (separate from general inflight).
- Reduce barrier pressure by combining meta updates in fewer transactions.
