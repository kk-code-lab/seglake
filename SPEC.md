# SPEC: Seglake — stan aktualny implementacji

Wersja: v0.2 (spec odzwierciedla aktualny kod)  
Zakres: single-node, path-style S3, correctness > performance, minimalny narzut zasobów.

---

## 1) Podsumowanie

Seglake to prosty, zgodny z S3 (minimum użyteczne dla SDK/toolingu) object store oparty o:
- **append-only segmenty** z chunkami **4 MiB**,
- **manifesty obiektów** jako osobne pliki (binary codec),
- **metadane w SQLite (WAL, synchronous=FULL)**,
- **twardy kontrakt trwałości**: fsync segmentów + commit WAL zanim obiekt jest widoczny,
- **narzędzia ops**: status, fsck, scrub, rebuild-index, snapshot, support-bundle, GC plan/run, GC rewrite plan/run,
- **S3 API**: PUT/GET/HEAD, LIST (V1/V2), range GET (single i multi-range), SigV4 + presigned, multipart upload.

---

## 2) Status implementacji (faktycznie zrobione)

### 2.1 Storage core
- Chunking 4 MiB + BLAKE3 per chunk.
- Segmenty append-only z nagłówkiem i stopką (stopka z checksum, pola bloom/index na razie puste).
- Rotacja segmentów: **~1 GiB** lub **~10 min bezczynności** (co pierwsze).
- Reuse open segmentów; odzysk po crash (doszczelnianie open segmentów przy starcie).
- Manifesty: pliki binarne, ścieżka zwykle `data/objects/manifests/<versionID>` lub nazwa `<bucket>__<key>__<version>`.

### 2.2 Metadane
- SQLite WAL + synchronous=FULL + wal_checkpoint(TRUNCATE) przy flush.
- Tabele: buckets, versions, objects_current, manifests, segments, api_keys, api_key_bucket_allow,
  multipart_uploads, multipart_parts, rebuild_state, ops_runs.

### 2.3 S3 API
- Path-style: `/<bucket>/<key>`.
- PUT/GET/HEAD obiektu, ListObjectsV2, ListObjectsV1, ListBuckets, GetBucketLocation.
- Range GET: pojedynczy i multi-range (multipart/byteranges).
- SigV4 (Authorization oraz presigned) + fallback SigV2 **tylko** dla listowania.
- Presigned GET/PUT (TTL do 7 dni).
- Multipart: initiate, upload part, list parts, complete, abort, list multipart uploads.

### 2.4 Ops i observability
- Ops: status, fsck, scrub, rebuild-index, snapshot, support-bundle, gc-plan/gc-run,
  gc-rewrite-plan/gc-rewrite-run (throttle + pause file).
- `/v1/meta/stats` z podstawowymi licznikami + ruch i latencje.
- Request-id w logach i odpowiedziach.

---

## 3) Architektura i dane

### 3.1 Układ na dysku
- Root danych: `<data-dir>/objects/`
  - `segments/` — pliki segmentów
  - `manifests/` — pliki manifestów
- Metadane: `<data-dir>/meta.db` (+ WAL/SHM)

### 3.2 Chunking
- Stały rozmiar: **4 MiB** (ostatni chunk może być mniejszy).
- Hash chunku: **BLAKE3**.

### 3.3 Segmenty
- Format:
  - Header: magic + version.
  - Rekordy: `chunk_hash(32B) + len(u32) + data`.
- Footer: magic + version + offsety bloom/index + checksum (BLAKE3 po stopce).
- Stan: OPEN → SEALED.
- Rotacja: 1 GiB lub 10 min bezczynności.

### 3.4 Manifest obiektu
- Manifest zawiera: bucket, key, versionID, size, listę chunków (hash, segment_id, offset, len).
- Przechowywanie:
  - plik manifestu na dysku (binary codec),
  - ścieżka manifestu w SQLite (tabela `manifests`).

### 3.5 Metadane (SQLite)
- `objects_current` wskazuje aktualną wersję obiektu.
- `versions` przechowuje etag (MD5), size, last_modified_utc, state.
- `segments` przechowuje stan, size, checksum stopki.
- Multipart: `multipart_uploads`, `multipart_parts`.

### 3.6 Durability / barrier
- **Write barrier**:
  - `sync_interval` ~100ms
  - `sync_bytes` ~128MiB
- Kolejność: zapis segmentów → fsync segmentów → zapis manifestu + update metadanych w transakcji → WAL flush.
- ACK klienta po zakończeniu bariery.

### 3.7 Read path
- GET/HEAD: rozwiązywanie `objects_current` → manifest → strumień z segmentów.
- Range GET: pojedynczy range lub `multipart/byteranges` dla wielu zakresów.

### 3.8 Recovery
- Przy starcie: open segmenty są domykane (dopisywana stopka) lub oznaczane jako SEALED,
  jeśli footer już był poprawny.

---

## 4) S3 API — zakres

### 4.1 Endpoints
- `GET /` — ListBuckets.
- `GET /<bucket>?list-type=2` — ListObjectsV2.
- `GET /<bucket>?prefix=...` — ListObjectsV1 (marker).
- `GET /<bucket>?location` — GetBucketLocation.
- `PUT /<bucket>/<key>` — PUT object.
- `GET /<bucket>/<key>` — GET object.
- `HEAD /<bucket>/<key>` — HEAD object.
- `DELETE /<bucket>/<key>` — DELETE object (idempotentny).
- `DELETE /<bucket>` — DELETE bucket (tylko gdy pusty).
- `PUT /<bucket>/<key>` + `x-amz-copy-source` — CopyObject (pełny copy).
- Multipart:
  - `POST /<bucket>/<key>?uploads` — Initiate.
  - `PUT /<bucket>/<key>?partNumber=N&uploadId=...` — UploadPart.
  - `GET /<bucket>/<key>?uploadId=...` — ListParts.
  - `POST /<bucket>/<key>?uploadId=...` — Complete.
  - `DELETE /<bucket>/<key>?uploadId=...` — Abort.
- `GET /<bucket>?uploads` — ListMultipartUploads (bez paginacji markerami).
- `GET /<bucket>?uploads` — ListMultipartUploads (key-marker/upload-id-marker, max-uploads).

### 4.2 Auth
- SigV4: Authorization header lub presigned query.
- Presigned TTL: 1..7 dni.
- Dopuszczony SigV2 **tylko** dla listowania (`GET /` lub `GET /<bucket>`).
- `X-Amz-Content-Sha256` obsługiwany; `STREAMING-*` odrzucone.
- Request time skew: domyślnie ±5 min (konfigurowalne).
- Region `us` normalizowany do `us-east-1`.
- Rate limiting błędów auth per IP i per access key.
- Limity inflight per access key (domyślnie 32).
- Logi redagują sekrety w query (np. X-Amz-Signature/Credential).
- Referencje testów: `internal/s3/e2e_test.go`.

### 4.3 ETag
- Single PUT: `MD5` całego payloadu.
- Multipart: `md5(concat(md5(part_i))) + "-<partCount>"`.
- Referencje testów: `internal/s3/e2e_test.go`.

### 4.4 Range GET (zachowanie)
- `Range: bytes=a-b`, `bytes=a-`, `bytes=-n` wspierane.
- Multi-range → `multipart/byteranges` z boundary opartym o request-id.
- Nieobsługiwane/niepoprawne zakresy → `416 InvalidRange` + `Content-Range: bytes */<size>`.
- Referencje testów: `internal/s3/range_test.go`, `internal/s3/e2e_test.go`.

### 4.5 Conditional GET/HEAD
- `If-Match` → 412 `PreconditionFailed` gdy ETag się nie zgadza.
- `If-None-Match` → 304 `NotModified` gdy ETag się zgadza.

### 4.6 Błędy
- XML zgodny z AWS (`Code`, `Message`, `RequestId`, `HostId`, `Resource`).
- Przykłady walidowane w testach (m.in. `SignatureDoesNotMatch`, `RequestTimeTooSkewed`,
  `XAmzContentSHA256Mismatch`): `internal/s3/e2e_test.go`.

---

## 5) Ops / maintenance

### 5.1 Tryby
- `status` — liczba manifestów i segmentów.
- `fsck` — spójność manifestów i granic segmentów.
- `scrub` — weryfikacja hashy chunków; uszkodzone → `DAMAGED`.
- `rebuild-index` — odbudowa meta z manifestów.
- `snapshot` — kopia meta.db(+wal/shm) + raport.
- `support-bundle` — snapshot + fsck + scrub.
- `gc-plan`/`gc-run` — usuwa segmenty w 100% martwe.
- `gc-rewrite-plan`/`gc-rewrite-run` — rewrite segmentów częściowo martwych (throttle + pause file).
- `mpu-gc-plan`/`mpu-gc-run` — czyszczenie starych multipart uploadów (TTL).
  - GC segmentów uwzględnia części multipart jako live.

### 5.2 Stats API
`GET /v1/meta/stats` (JSON):
- objects, segments, bytes_live,
- ostatnie wyniki fsck/scrub/gc (czas + błędy + reclaim/rewritten),
- requests_total{op,status_class}, inflight{op},
- bytes_in_total, bytes_out_total,
- latency_ms{op}: p50/p95/p99,
- requests_total_by_bucket / latency_ms_by_bucket,
- requests_total_by_key / latency_ms_by_key,
- gc_trends: historia GC (mode, finished_at, errors, reclaimed/rewritten, reclaim_rate).

### 5.3 Crash harness
- Test integracyjny (opcjonalny): `go test -tags crashharness ./internal/ops -run TestCrashHarness`
  - Uruchamia binarkę i wykonuje PUT/multipart + kill -9 + fsck/rebuild-index.
  - `CRASH_CORRUPT=1` włącza kontrolowaną korupcję segmentu (oczekiwane błędy scrub/GET=500).
  - `CRASH_ITER` steruje liczbą iteracji (domyślnie 1).
- Test trwałości po crashu (opcjonalny): `go test -tags durability ./internal/ops -run TestDurabilityAfterCrash`

---

## 6) Limity i parametry

- Chunk: 4 MiB (stałe).
- Segment: ~1 GiB max, seal po ~10 min bezczynności.
- Barrier: 100ms / 128MiB.
- ListObjects max-keys: 1000.
- ListMultipartUploads max-uploads: 1000.
- Multipart min part size: 5 MiB poza ostatnim.
- Brak wymuszonego limitu rozmiaru obiektu w kodzie (praktycznie ogranicza storage).

## 6.1) Ops / TLS / tooling
- Checklist TLS i przykłady awscli/s3cmd: `docs/ops.md`.

---

## 7) Znane braki / ograniczenia (stan obecny)

 - Brak wersjonowania po API.
- Brak ACL/IAM/polityk, brak per-key limitów inflight.
- Brak TLS w aplikacji (zakładany reverse proxy).
- Virtual-hosted-style dostępny tylko za flagą.
- Brak replikacji / multi-site / oplogu / HLC.

---

## 8) Kolejne sensowne kroki (propozycje)

1) Dalsze guard‑rail’e dla MPU/GC (telemetria, ostrzeżenia, limity).
