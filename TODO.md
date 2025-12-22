# TODO / Backlog (na bazie aktualnego stanu)

## 1) S3 API — luki kompatybilności
- Lepsze błędy AWS (pełniejsze pola, mapowania kodów).

## 2) Multipart — bezpieczeństwo danych
- Paginacja ListMultipartUploads (key-marker/upload-id-marker).

## 3) Storage / durability

## 4) Ops / observability
- Raporty trendów GC (historyka, reclaim rate).

## 5) Security / auth

## Done (2025-12-22)
- DELETE obiektu i bucketu.
- CopyObject.
- If-Match / If-None-Match.
- Virtual-hosted-style (za flagą).
- Multipart cleanup TTL (mpu-gc plan/run).
- GC-aware multipart (części traktowane jako live).
- Crash-consistency harness jako test opcjonalny (tag crashharness).
- Bloom + sparse index w footerze segmentu.
- E2E durability test (tag durability).
- `/v1/meta/stats` z ruchem i latencjami.
- Uspójnione raporty ops JSON (schema_version).
- Rate-limiting błędów auth (per IP/per key).
- Per-key limity inflight.
- Redakcja sekretów w logach (query z presigned).
- Checklist TLS i integracje awscli/s3cmd (`docs/ops.md`).
