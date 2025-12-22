# TODO / Backlog (na bazie aktualnego stanu)

## 1) S3 API — luki kompatybilności
- DELETE obiektu i bucketu.
- CopyObject.
- If-Match / If-None-Match / inne warunkowe nagłówki.
- Virtual-hosted-style.
- Lepsze błędy AWS (pełniejsze pola, mapowania kodów).

## 2) Multipart — bezpieczeństwo danych
- TTL/cleanup uploadów (GC dla multipartów).
- GC-aware multipart (części traktowane jako live lub blokada GC).
- Paginacja ListMultipartUploads (key-marker/upload-id-marker).

## 3) Storage / durability
- Realny bloom + sparse index w footerze segmentu.
- Więcej testów crash-consistency (kill -9 + invariants).
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

