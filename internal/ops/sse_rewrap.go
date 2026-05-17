package ops

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

const sseRewrapPlanSchemaVersion = 1

// SSERewrapPlan contains only redacted metadata needed to replay a KEK rewrap.
type SSERewrapPlan struct {
	SchemaVersion        int                  `json:"schema_version"`
	CreatedAt            time.Time            `json:"created_at"`
	TargetKeyID          string               `json:"target_key_id"`
	SourceKeyIDs         []string             `json:"source_key_ids,omitempty"`
	Entries              []SSERewrapPlanEntry `json:"entries"`
	SkippedPlaintext     int                  `json:"skipped_plaintext"`
	SkippedAlreadyTarget int                  `json:"skipped_already_target"`
}

type SSERewrapPlanEntry struct {
	Bucket              string                  `json:"bucket"`
	Key                 string                  `json:"key"`
	VersionID           string                  `json:"version_id"`
	ManifestPath        string                  `json:"manifest_path"`
	ManifestFingerprint string                  `json:"manifest_fingerprint"`
	Keys                []SSERewrapKeyPlanEntry `json:"keys"`
}

type SSERewrapKeyPlanEntry struct {
	KeyRef          uint32 `json:"key_ref"`
	SourceKeyID     string `json:"source_key_id"`
	EDEKFingerprint string `json:"edek_fingerprint"`
}

func BuildSSERewrapPlan(layout fs.Layout, metaPath string, provider *ssecrypto.Provider, targetKeyID string, sourceKeyIDs []string) (*SSERewrapPlan, *Report, error) {
	targetKeyID = strings.TrimSpace(targetKeyID)
	if provider == nil {
		return nil, nil, fmt.Errorf("sse rewrap: SSE-S3 provider required")
	}
	if _, err := provider.LookupKey(targetKeyID); err != nil {
		return nil, nil, fmt.Errorf("sse rewrap: target key %q not configured: %w", targetKeyID, err)
	}
	report := newReport("sse-rewrap-plan")
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = store.Close() }()
	records, err := store.ListVersionManifestRecords(context.Background())
	if err != nil {
		return nil, nil, err
	}
	sourceFilter := keyIDSet(sourceKeyIDs)
	plan := &SSERewrapPlan{
		SchemaVersion: sseRewrapPlanSchemaVersion,
		CreatedAt:     now().UTC(),
		TargetKeyID:   targetKeyID,
		SourceKeyIDs:  normalizeKeyIDs(sourceKeyIDs),
		Entries:       []SSERewrapPlanEntry{},
	}
	codec := &manifest.BinaryCodec{}
	for _, rec := range records {
		report.Manifests++
		data, man, err := readManifestBytes(codec, rec.ManifestPath)
		if err != nil {
			return nil, nil, fmt.Errorf("sse rewrap: read manifest for version %s: %w", rec.VersionID, err)
		}
		if !man.Encrypted() {
			plan.SkippedPlaintext++
			report.SkippedManifests++
			continue
		}
		selected := make([]SSERewrapKeyPlanEntry, 0, len(man.Encryption.Keys))
		allTarget := true
		for _, keyEntry := range man.Encryption.Keys {
			if keyEntry.KeyID != targetKeyID {
				allTarget = false
			}
			if !selectRewrapKey(keyEntry.KeyID, targetKeyID, sourceFilter) {
				continue
			}
			sourceKey, err := provider.LookupKey(keyEntry.KeyID)
			if err != nil {
				return nil, nil, fmt.Errorf("sse rewrap: source key %q for version %s not configured: %w", keyEntry.KeyID, rec.VersionID, err)
			}
			if _, err := ssecrypto.UnwrapDEK(sourceKey, keyEntry.WrapNonce, keyEntry.EncryptedDEK, ssecrypto.WrapAAD(keyEntry.KeyID)); err != nil {
				return nil, nil, fmt.Errorf("sse rewrap: unwrap key_ref %d for version %s: %w", keyEntry.KeyRef, rec.VersionID, err)
			}
			selected = append(selected, SSERewrapKeyPlanEntry{
				KeyRef:          keyEntry.KeyRef,
				SourceKeyID:     keyEntry.KeyID,
				EDEKFingerprint: edekFingerprintHex(keyEntry),
			})
		}
		if len(selected) == 0 {
			if allTarget {
				plan.SkippedAlreadyTarget++
				report.SkippedManifests++
			}
			continue
		}
		plan.Entries = append(plan.Entries, SSERewrapPlanEntry{
			Bucket:              rec.Bucket,
			Key:                 rec.Key,
			VersionID:           rec.VersionID,
			ManifestPath:        rec.ManifestPath,
			ManifestFingerprint: manifestFingerprintHex(data),
			Keys:                selected,
		})
	}
	report.Candidates = len(plan.Entries)
	report.FinishedAt = now().UTC()
	return plan, report, nil
}

func RunSSERewrapPlan(layout fs.Layout, metaPath string, provider *ssecrypto.Provider, plan *SSERewrapPlan) (*Report, error) {
	if plan == nil {
		return nil, fmt.Errorf("sse rewrap: plan required")
	}
	if provider == nil {
		return nil, fmt.Errorf("sse rewrap: SSE-S3 provider required")
	}
	if plan.SchemaVersion != sseRewrapPlanSchemaVersion {
		return nil, fmt.Errorf("sse rewrap: unsupported plan schema %d", plan.SchemaVersion)
	}
	targetKey, err := provider.LookupKey(plan.TargetKeyID)
	if err != nil {
		return nil, fmt.Errorf("sse rewrap: target key %q not configured: %w", plan.TargetKeyID, err)
	}
	report := newReport("sse-rewrap-run")
	report.Candidates = len(plan.Entries)
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	codec := &manifest.BinaryCodec{}
	for _, entry := range plan.Entries {
		currentPath, err := store.ManifestPath(context.Background(), entry.VersionID)
		if err != nil {
			return nil, fmt.Errorf("sse rewrap: read current manifest path for version %s: %w", entry.VersionID, err)
		}
		if currentPath != entry.ManifestPath {
			return nil, fmt.Errorf("sse rewrap: stale plan for version %s: manifest path changed", entry.VersionID)
		}
		data, man, err := readManifestBytes(codec, entry.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("sse rewrap: read manifest for version %s: %w", entry.VersionID, err)
		}
		if got := manifestFingerprintHex(data); got != entry.ManifestFingerprint {
			return nil, fmt.Errorf("sse rewrap: stale plan for version %s: manifest fingerprint changed", entry.VersionID)
		}
		if man.VersionID != entry.VersionID || man.Bucket != entry.Bucket || man.Key != entry.Key {
			return nil, fmt.Errorf("sse rewrap: stale plan for version %s: manifest identity changed", entry.VersionID)
		}
		if !man.Encrypted() {
			return nil, fmt.Errorf("sse rewrap: version %s is no longer encrypted", entry.VersionID)
		}
		for _, plannedKey := range entry.Keys {
			keyIndex := findManifestKey(man, plannedKey.KeyRef)
			if keyIndex < 0 {
				return nil, fmt.Errorf("sse rewrap: key_ref %d missing in version %s", plannedKey.KeyRef, entry.VersionID)
			}
			keyEntry := &man.Encryption.Keys[keyIndex]
			if keyEntry.KeyID != plannedKey.SourceKeyID {
				return nil, fmt.Errorf("sse rewrap: stale plan for version %s key_ref %d: key id changed", entry.VersionID, plannedKey.KeyRef)
			}
			if edekFingerprintHex(*keyEntry) != plannedKey.EDEKFingerprint {
				return nil, fmt.Errorf("sse rewrap: stale plan for version %s key_ref %d: EDEK fingerprint changed", entry.VersionID, plannedKey.KeyRef)
			}
			sourceKey, err := provider.LookupKey(keyEntry.KeyID)
			if err != nil {
				return nil, fmt.Errorf("sse rewrap: source key %q for version %s not configured: %w", keyEntry.KeyID, entry.VersionID, err)
			}
			dek, err := ssecrypto.UnwrapDEK(sourceKey, keyEntry.WrapNonce, keyEntry.EncryptedDEK, ssecrypto.WrapAAD(keyEntry.KeyID))
			if err != nil {
				return nil, fmt.Errorf("sse rewrap: unwrap key_ref %d for version %s: %w", keyEntry.KeyRef, entry.VersionID, err)
			}
			wrapNonce, edek, err := ssecrypto.WrapDEK(targetKey, dek, ssecrypto.WrapAAD(targetKey.ID))
			if err != nil {
				return nil, fmt.Errorf("sse rewrap: wrap key_ref %d for version %s: %w", keyEntry.KeyRef, entry.VersionID, err)
			}
			sum := sha256.Sum256(edek)
			keyEntry.KeyID = targetKey.ID
			keyEntry.WrapNonce = wrapNonce
			keyEntry.EncryptedDEK = edek
			keyEntry.EDEKFingerprint = append(keyEntry.EDEKFingerprint[:0], sum[:ssecrypto.KeyFingerprintBytes]...)
		}
		newPath, err := writeRewrappedManifest(layout, codec, man)
		if err != nil {
			return nil, err
		}
		mode, algorithm, keyIDs, fingerprints := encryptionSummary(man.Encryption)
		err = store.WithTx(func(tx *sql.Tx) error {
			return store.RecordSSERewrapTx(tx, entry.Bucket, entry.Key, entry.VersionID, newPath, mode, algorithm, keyIDs, fingerprints)
		})
		if err != nil {
			return nil, err
		}
		report.Manifests++
		report.RebuiltObjects++
	}
	report.FinishedAt = now().UTC()
	return report, nil
}

func WriteSSERewrapPlan(path string, plan *SSERewrapPlan) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("sse rewrap: plan path required")
	}
	if plan == nil {
		return fmt.Errorf("sse rewrap: plan required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func ReadSSERewrapPlan(path string) (*SSERewrapPlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan SSERewrapPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func readManifestBytes(codec manifest.Codec, path string) ([]byte, *manifest.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	man, err := codec.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	return data, man, nil
}

func selectRewrapKey(keyID, targetKeyID string, sourceFilter map[string]struct{}) bool {
	if len(sourceFilter) > 0 {
		_, ok := sourceFilter[keyID]
		return ok
	}
	return keyID != targetKeyID
}

func keyIDSet(ids []string) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func normalizeKeyIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for id := range keyIDSet(ids) {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func manifestFingerprintHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func edekFingerprintHex(key manifest.KeyEntry) string {
	if len(key.EDEKFingerprint) > 0 {
		return hex.EncodeToString(key.EDEKFingerprint)
	}
	sum := sha256.Sum256(key.EncryptedDEK)
	return hex.EncodeToString(sum[:ssecrypto.KeyFingerprintBytes])
}

func findManifestKey(man *manifest.Manifest, keyRef uint32) int {
	if man == nil || man.Encryption == nil {
		return -1
	}
	for i := range man.Encryption.Keys {
		if man.Encryption.Keys[i].KeyRef == keyRef {
			return i
		}
	}
	return -1
}

func encryptionSummary(enc *manifest.Encryption) (mode, algorithm, keyIDs, fingerprints string) {
	if enc == nil {
		return "", "", "", ""
	}
	seen := make(map[string]struct{})
	ids := make([]string, 0, len(enc.Keys))
	fps := make([]string, 0, len(enc.Keys))
	for _, key := range enc.Keys {
		if _, ok := seen[key.KeyID]; !ok {
			seen[key.KeyID] = struct{}{}
			ids = append(ids, key.KeyID)
		}
		if len(key.EDEKFingerprint) > 0 {
			fps = append(fps, hex.EncodeToString(key.EDEKFingerprint))
		}
	}
	return enc.Mode, enc.Algorithm, strings.Join(ids, ","), strings.Join(fps, ",")
}

func writeRewrappedManifest(layout fs.Layout, codec manifest.Codec, man *manifest.Manifest) (string, error) {
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		return "", err
	}
	suffix, err := randomHex(8)
	if err != nil {
		return "", err
	}
	path := filepath.Join(layout.ManifestsDir, fmt.Sprintf("%s.rewrap.%d.%s", man.VersionID, now().UTC().UnixNano(), suffix))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	if err := codec.Encode(file, man); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	return path, nil
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
