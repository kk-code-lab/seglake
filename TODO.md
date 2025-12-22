# TODO / Backlog (na bazie aktualnego stanu)

## 1) S3 API — luki kompatybilności
 - (puste)

## 2) Multipart — bezpieczeństwo danych
 - (puste)

## 3) Storage / durability

## 4) Ops / observability
- (puste)

## 5) Security / auth

## 6) Replikacja / multi-site (plan)

Cel: asynchroniczna replikacja między węzłami z zachowaniem spójności metadanych,
bez wymogu silnej konsystencji globalnej (eventual consistency).

### Decyzje (wybrane) + ścieżki rozwoju
- Wybrany wariant: **Zestaw B (multi-site P2P, multi-writer, LWW + tombstone, HTTP JSON)**.
- Opcja rozwoju 1: tryb primary‑replica (prostsze operacje, mniej konfliktów).
- Opcja rozwoju 2: aktywne strumieniowanie chunków/segmentów + push (szybsza konwergencja).
- Opcja rozwoju 3: zachowanie konfliktów jako oddzielnych wersji (zaawansowane rozwiązywanie).

### Faza 0 — decyzje architektoniczne
- Model konfliktów: Last-Write-Wins po HLC (per obiekt/wersja) + jawny delete-tombstone.
- Granulat repliki: oplog na poziomie metadanych + strumieniowanie chunków/manifestów.
- Topologia: multi-site w trybie peer-to-peer z pull/push, start od 2-site.
- Gwarancje: brak transakcji globalnych; lokalny zapis jest natychmiast widoczny lokalnie.

### Faza 1 — fundamenty danych
- Zrobione: tabela `oplog`, HLC, wpisy w transakcjach, payloady PUT/DELETE.

### Faza 2 — replikacja danych i metadanych
- Zrobione: endpointy oplog/manifest/chunk, idempotencja, repl-pull/push, fetch braków.
- Otwarte: garbage/lease dla segmentów podczas replay.

### Faza 3 — bootstrap i recovery
- Zrobione: snapshot + oplog replay (`repl-bootstrap`), `repl-pull`, `repl-push`.
- Zrobione: rebuild-index z uwzględnieniem `oplog` i HLC (deterministyczne odtwarzanie).

### Faza 4 — spójność i edge-case
- Zrobione: konflikty LWW (PUT vs DELETE, PUT vs PUT, MPU complete vs DELETE) + tie-break po site_id.
- Zrobione: out-of-order apply (starszy PUT nie nadpisuje nowszego DELETE).
- Zrobione: replikacja ACL/polityk i API keys jako osobne typy operacji.

### Faza 5 — testy i observability
- Zrobione (opisane w SPEC).

### Poza zakresem pierwszej iteracji
- Silna konsystencja globalna.
- Cross-region locking lub transakcyjny rename.
- Zaawansowane polityki replikacji per-bucket (później).
