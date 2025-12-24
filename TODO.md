# TODO

- Expose conflict listings (ops command or /v1/meta/conflicts) to review CONFLICT versions.
- Support `If-Match: *` semantics for overwrite guard (only if object exists).
- Add conflict metrics per bucket/key to highlight hotspots.
- Add repl-validate deep mode (verify chunk hashes) for optional integrity checks.
- Decide conflict handling for delete vs put (mark delete conflicts explicitly).
- Add API ergonomics to surface conflict presence (response headers or listing hints).
