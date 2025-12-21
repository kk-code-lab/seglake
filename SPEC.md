# Plan projektu: S3-compatible object store (MVP → multi-site async)

Wersja: v0.1 (specyfikacja robocza)  
Założenia: single-node MVP, docelowo multi-site async (faza C), prosty deploy/operacje, niski RAM/CPU, correctness > performance.

---

## 1) Streszczenie

Budujesz system S3‑kompatybilny, którego rdzeń stanowią:

- **append-only segmenty danych** (log‑structured) dzielone na **chunki 4 MiB**
- **metadane w SQLite (WAL, fsync)** + **manifesty jako pliki** (content‑addressed)
- **twardy kontrakt trwałości**: ACK dopiero po fsync danych + commit WAL
- **narzędzia operacyjne**: `status`, `scrub`, `fsck/rebuild-index`, `gc`, `snapshot`, support bundle
- **S3 API**: PUT/GET/HEAD, LIST (ListObjectsV2), delimiter/common prefixes, range GET, SigV4, presigned GET/PUT
- **Multipart** jako osobny milestone (MVP+1), z AWS‑style ETag i zasadą 5 MiB per part (poza ostatnim)
- **multi-site** w przyszłości: oplog (HLC+LWW), pull chunks po hash, HMAC→mTLS, watermarks, bootstrap przez snapshot

Kluczowe kompromisy:
- brak CAS (If‑Match/If‑None‑Match) – spójne z multi‑site bez consensus
- brak auto‑naprawy bitrot bez replik (jest wykrywanie + alarm + restore)
- GC i scrub manual (na start), z silnym “plan” i bezpiecznym domyślnym zachowaniem

---

## 2) Cele i nie‑cele

### 2.1 Cele MVP
- S3‑kompatybilność wystarczająca dla typowych SDK/toolingu:
  - SigV4 (zgodnie z AWS), path‑style, ListObjectsV2 XML, AWS Error XML
  - presigned GET/PUT
  - range GET
- “Correctness first”:
  - crash‑consistency: po kill -9 system wraca do spójnego stanu, bez ręcznego dłubania w DB
  - ACK = durable (fsync danych + metadanych)
- Prosty deploy:
  - jeden binarek + plik konfiguracyjny + katalog danych
  - TLS poza usługą (reverse proxy), w MVP dopuszczalny “http tylko na zaufanej sieci”
- Niski koszt zasobów:
  - brak dużych cache’y na start
  - ograniczenia concurrency i backpressure

### 2.2 Nie‑cele MVP
- pełna implementacja całego S3 (policy/IAM, ACLs, Object Lock, replication rules, lifecycle, inventory itp.)
- strong consistency LIST / globalny consensus
- erasure coding
- globalna deduplikacja chunków między obiektami (świadomie odłożone)

---

## 3) Założenia produktowe i ograniczenia

- skala: ≤ 1 TB, < 1e6 obiektów, typowo 32–512 MiB
- max obiekt: **1 GiB** (MVP)
- key:
  - percent‑decode → **wymagany poprawny UTF‑8**
  - brak normalizacji (NFC/NFD) – przechowujesz dokładnie po decode
- bucket: tworzony implicit przy pierwszym PUT (konfigowalny “strict mode” później)

---

## 4) Architektura wysokopoziomowa

### 4.1 Komponenty
1) **S3 Frontend (HTTP)**
   - SigV4, presigned, error XML, list XML
2) **Write path**
   - chunking 4 MiB, streaming, zapis do segmentów, bariera fsync
3) **Metadata store**
   - SQLite WAL (synchronous=FULL), indeks `objects_current`, rejestr segmentów, uploady multipart
4) **Manifest store**
   - pliki `data/manifests/<id>` (content-addressed), wykorzystywane do recovery i przyszłej replikacji
5) **Maintenance**
   - scrub (manual), GC (manual; v0 bez rewrite, v1 z rewrite)
6) **Recovery tooling**
   - fsck (online, read-only), rebuild-index (sqlite.new + atomic swap)
7) **Observability (minimalne)**
   - structured logs + request-id
   - minimalne metryki (bez Prometheus na start)
8) **(Faza C) Replikacja**
   - `/v1/repl/*` JSON, HMAC, pull chunks, handshake capabilities

### 4.2 API wersjonowanie
- S3 API: na `/` (żeby SDK działały bez kombinowania)
- Endpointy wewnętrzne: `/v1/*`
  - `/v1/meta/*`, `/v1/repl/*`, `/v1/admin/*`

---

## 5) Model danych: segmenty, chunki, manifesty

### 5.1 Chunking
- rozmiar chunka: **4 MiB**
- hash chunka: **BLAKE3**
- deduplikacja: **brak** (ani globalnie, ani w obrębie obiektu)
  - hash służy do integralności, scrub i przyszłego “pull” w replikacji

### 5.2 Segmenty (log‑structured)
- segment jest plikiem append‑only:
  - `OPEN` → dopisujesz rekordy
  - `SEALED` → zamrożony, ma footer z indeksem
- rotacja: **1 GiB lub 10 min** (cokolwiek pierwsze)

**Rekord chunka** (w segmencie):
- `chunk_hash` (32B BLAKE3)
- `len` (u32)
- `data` (len)
- (opcjonalnie w przyszłości: CRC32, ale nie w MVP)

**Footer (SEALED)**:
- `bloom filter` (target FP ~1%)
- `sparse index` (stride ~4 MiB danych)
- `footer checksum` + `format_version`

### 5.3 Manifest obiektu (content-addressed)
Manifest finalnej wersji obiektu to lista chunków w kolejności:
- `idx`
- `chunk_hash`
- `segment_id`, `offset`, `len`

Przechowywanie:
- w SQLite: szybki lookup (opcjonalnie pełna tabela manifestów lub referencja)
- na dysku: `data/manifests/<version_id>` (zalecane zawsze)

---

## 6) Metadane (SQLite WAL)

### 6.1 Tabele (minimum)
- `buckets(bucket, created_at)`
- `objects_current(bucket, key, version_id)` + indeks `(bucket, key)`
- `versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)`
  - `state`: ACTIVE / DAMAGED / (opcjonalnie EXPIRED)
- `manifests(version_id, idx, chunk_hash, segment_id, offset, len)`  
  (albo: trzymasz tylko referencję do pliku manifestu + lazy load)
- `segments(segment_id, path, state, created_at, sealed_at, size, footer_checksum)`
- `api_keys(access_key, secret_hash, salt, enabled, created_at, label, last_used_at)`
- `api_key_bucket_allow(access_key, bucket)` (allowlist)

### 6.2 LastModified i czas
- `last_modified_utc` = wall‑clock UTC w momencie commit (dla kompatybilności S3)
- HLC służy do deterministycznego porządku i LWW (pod multi-site)

---

## 7) Durability i spójność (kontrakt)

### 7.1 ACK = durable (twarda reguła)
Żaden obiekt nie może być “widoczny” w metadanych, jeśli jego dane nie są trwałe.

**Write barrier (group commit)**:
- `sync_interval = 100ms`
- `sync_bytes = 128MiB`
- podczas bariery:
  1) `fdatasync()` dla dotkniętych segmentów
  2) transakcja SQLite (WAL): wpis wersji + manifest + update `objects_current` (+ wpis do oplog w przyszłości)
  3) commit WAL (synchronous=FULL)
  4) zapis `hlc_state`

### 7.2 Read-after-write
- GET/HEAD po ACK zawsze widzi nową wersję

### 7.3 LIST
- lokalnie: query po `(bucket, key)` z prefixem, tokeny kontynuacji
- w multi-site eventual wynika z asynchronicznej replikacji

---

## 8) API: S3 kompatybilność

### 8.1 Auth: AWS SigV4
- canonical request zgodnie z AWS
- region konfigurowalny
- skew czasu ±5 min
- wspierasz `x-amz-content-sha256: UNSIGNED-PAYLOAD` (streaming PUT)
- path‑style: `/<bucket>/<key>` (tylko)

### 8.2 Presigned URLs
- GET i PUT obiektów
- max TTL 7 dni (konfig)
- signed headers: minimalnie `host`, opcjonalnie `range` dla GET

### 8.3 LIST (ListObjectsV2)
- XML jak AWS:
  - `IsTruncated`, `NextContinuationToken`, `CommonPrefixes`, `Contents` (`Key`, `ETag`, `Size`, `LastModified`, `StorageClass=STANDARD`)
- parametry:
  - `prefix`, `delimiter=/`, `max-keys` (domyślnie 1000, max 1000), `continuation-token`
- token: stateless, base64(last_key[, last_version_id])

### 8.4 Range GET
- `Range: bytes=...` mapowany na zakres chunków + offsety

### 8.5 Błędy (AWS Error XML)
- `NoSuchKey`, `NoSuchBucket`, `AccessDenied`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`, `InvalidArgument`, `InternalError`
- `RequestId` = request-id
- `HostId` = stały hash instancji (placeholder)

### 8.6 Request ID
- generujesz krótki losowy `x-request-id` oraz (opcjonalnie) `x-amz-request-id`
- logujesz go zawsze (INFO) i dołączasz do Error XML

---

## 9) ETag (maksymalna interoperacyjność)

### 9.1 Single PUT ETag = MD5
- podczas PUT liczysz MD5 strumienia (streaming)
- ETag w odpowiedziach i LIST = `"<md5hex>"`

### 9.2 Integrity nadal oparta o BLAKE3 per chunk
- BLAKE3 jest źródłem prawdy dla scrub/bitrot i przyszłego pull‑po‑hash
- MD5 służy jako kompatybilny ETag

---

## 10) Multipart Upload (MVP+1)

### 10.1 API
- Initiate: `POST ?uploads`
- UploadPart: `PUT ?partNumber=N&uploadId=...`
- Complete: `POST ?uploadId=...` (XML list parts)
- Abort: `DELETE ?uploadId=...`
- ListParts: tak (zalecane); ListMultipartUploads później

### 10.2 Trwałość UploadPart
- identycznie jak PUT: ACK po fsync danych + commit WAL

### 10.3 ETag multipart = AWS-style
- `ETag = md5(concat(md5(part_i))) + "-" + partCount`

### 10.4 Min part size
- 5 MiB poza ostatnim

### 10.5 Idempotencja i nadpisy
- nadpisywanie partNumber dozwolone
- Complete idempotentne

### 10.6 Cleanup
- TTL uploadów (np. 7 dni)
- CLI: `mpu gc plan/run`

### 10.7 Oplog
- do oplogu trafia tylko finalny PUT po Complete

---

## 11) Maintenance: scrub, fsck, rebuild-index, snapshot

### 11.1 Scrub (manual)
- domyślnie SEALED; OPEN tylko z flagą
- obiekty z brakami: `DAMAGED` (GET/HEAD 500 + `X-Error: DamagedObject`)

### 11.2 FSCK (online, read-only)
Raport:
- corrupted segments
- missing chunks for live versions
- oplog gaps/anomalies

### 11.3 Rebuild-index
- tworzy `sqlite.new` i atomowo podmienia
- odbudowa oparta o segmenty+manifesty; oplog do wyboru latest i kontroli gapów

### 11.4 Snapshot
- CLI `snapshot`: meta (sqlite+wal+hlc_state) + manifests + lista segmentów + checksums

---

## 12) GC i compaction (etapowanie)

### 12.1 GC v0 (MVP)
- `gc plan`: liczy live_bytes per segment iterując po manifestach żywych wersji
- usuwa tylko segmenty 100% martwe
- min_age 24h od seala
- “refuse unless --force”

### 12.2 GC v1 (po MVP)
- rewrite dla segmentów >50% martwe
- 2-phase, throttling, pause-if-load
- `gc plan` + `gc run`

---

## 13) Observability (minimalne metryki bez Prometheus)

### 13.1 Logi
- structured logs + request-id
- SigV4 debug tylko na DEBUG, z redakcją `Authorization` i `X-Amz-*`

### 13.2 Minimalne metryki
Endpoint: `GET /v1/meta/stats` (JSON):
- `requests_total{op,status_class}`
- `inflight_put`, `inflight_get`
- `bytes_in_total`, `bytes_out_total`
- latencje (rolling p50/p95/p99) per op
- segmenty (open/sealed), bytes total
- barrier stats (count, last_duration)
- auth failures, signature mismatches
- status ostatniego scrub/gc/fsck

---

## 14) Bezpieczeństwo

### 14.1 TLS
- MVP: HTTP na zaufanej sieci
- produkcyjnie: TLS w reverse proxy
- `trust_proxy_headers` tylko z allowlistą proxy IP/CIDR

### 14.2 API keys
- Argon2id/scrypt + salt
- allowlist bucketów per api_key
- rate limiting auth failures per IP i per key
- limity per key (inflight)

---

## 15) Multi-site async (faza C) – plan

- LWW tie-break `(hlc_ts, site_id)`
- per-site `op_seq`, sync “since seq”
- JSON batches + `protocol_version` + capabilities
- pull chunks po hash
- HMAC raw-body + timestamp TTL 30s → docelowo mTLS
- watermarks per peer blokują GC
- bootstrap przez snapshot; offline > TTL → re-seed

---

## 16) Roadmapa

### Milestone 0: storage core
- segmenty + manifesty + SQLite schema

### Milestone 1: S3 core
- SigV4, errors XML, list v2, range GET, ETag single PUT = MD5

### Milestone 2: ops/recovery
- fsck, rebuild-index, snapshot, support bundle, GC v0, scrub

### Milestone 3: multipart
- initiate/parts/complete/abort + ListParts + cleanup

### Milestone 4+: GC rewrite

### Faza C: multi-site

---

## 17) Alternatywy

- metadane: SQLite (MVP) → ewentualnie RocksDB (później)
- global dedup: odłożone
- consensus/strong consistency: nie
- erasure coding: odłożone

### 17.1 Minimalny stack (MVP, deps minimalne)
- wymagane zewnętrzne: `modernc.org/sqlite` (pure Go) + `github.com/zeebo/blake3`
- reszta: stdlib + własna implementacja (router/CLI/config/logi)

---

## 18) Ryzyka i mitigacje

- SigV4 canonicalization → test vectors + redacted debug
- crash-consistency → kill -9 tests + invariants
- GC rewrite correctness → etapowanie + 2-phase + fsck
- clock skew → HLC persist + wall-clock tylko dla LastModified
- fsync koszt → group commit + limity concurrency

---

## 19) Pozostałe elementy MVP (checklista)

### 19.1 Core durability i storage
- [x] Doprecyzowany group commit: sync_interval/sync_bytes z realnym batchingiem
- [ ] Reuse open segmentów (nie zawsze “segment per PUT”)
- [ ] Footer: bloom + sparse index (na razie stub)

### 19.2 Metadata / recovery
- [ ] FSCK: pełna spójność manifest↔segment↔DB (raporty + naprawy)
- [ ] Rebuild-index: weryfikacja wersji vs. stan w DB (DAMAGED/ACTIVE)

### 19.3 S3 polish
- [ ] SigV4 edge cases (canonicalization test vectors)
- [ ] ListObjectsV2: marker compatibility (AWS-style)

### 19.4 Observability
- [ ] `/v1/meta/stats` JSON endpoint
- [ ] Structured request logs z request-id

### 19.5 Ops polish
- [ ] GC plan file + `gc run --from-plan`
- [ ] Support bundle z redakcją sekretów
