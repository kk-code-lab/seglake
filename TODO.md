# TODO / Backlog (na bazie aktualnego stanu)

## 1) S3 API — luki kompatybilności
- Lepsze błędy AWS (pełniejsze pola, mapowania kodów).

## 2) Multipart — bezpieczeństwo danych
- Paginacja ListMultipartUploads (key-marker/upload-id-marker).

## 3) Storage / durability
- Realny bloom + sparse index w footerze segmentu.
- Dodatkowe testy E2E durability (fsync / WAL semantics).

## 4) Ops / observability
- Rozszerzyć `/v1/meta/stats`: latencje, inflight, bytes in/out.
- Raporty trendów GC (historyka, reclaim rate).
- Uspójnienie raportów ops w JSON (np. schemat i versioning).

## 5) Security / auth
- Rate-limiting auth failures (per IP/per key).
- Per-key limity inflight.
- Pełna redakcja sekretów w logach.

## 6) Produkcyjność
- TLS przez reverse-proxy: checklist i example config.
- Dokumentacja integracji z awscli/s3cmd.

## Done (2025-12-22)
- DELETE obiektu i bucketu.
- CopyObject.
- If-Match / If-None-Match.
- Virtual-hosted-style (za flagą).
- Multipart cleanup TTL (mpu-gc plan/run).
- GC-aware multipart (części traktowane jako live).
- Crash-consistency harness jako test opcjonalny (SEGLAKE_CRASH_TEST).
