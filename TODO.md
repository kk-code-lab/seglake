# TODO / Backlog (na bazie aktualnego stanu)

## 1) S3 API — luki kompatybilności
 - (puste)

## 2) Multipart — bezpieczeństwo danych
 - (puste)

## 3) Storage / durability

## 4) Ops / observability
- (puste)

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
- Dokładniejsze mapowanie AWS Errors (MethodNotAllowed vs InvalidRequest, bardziej AWS‑owe Message/Resource).
- S3 compat polish: konsekwentne nagłówki request‑id i region.
- Testy E2E SigV4/presigned dla edge‑case’ów (signed headers, UNSIGNED‑PAYLOAD, Range).
- Metryki per‑bucket/per‑key (p50/p95/p99 i status classes).
- Raporty trendów GC (historyka, reclaim rate).
- Uspójnione raporty ops JSON (schema_version).
- Rate-limiting błędów auth (per IP/per key).
- Per-key limity inflight.
- Redakcja sekretów w logach (query z presigned).
- Checklist TLS i integracje awscli/s3cmd (`docs/ops.md`).
- Paginacja ListMultipartUploads (key-marker/upload-id-marker).
- ListMultipartUploads: dopracowanie pełnej zgodności (edge‑case’y marker/prefix/delimiter).
- Lepsze błędy AWS (mapowania kodów i domyślne komunikaty).
