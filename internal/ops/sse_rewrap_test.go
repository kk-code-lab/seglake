package ops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestSSERewrapPlanAndRun(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	oldKey := testSSEKey("local:v1", 1)
	newKey := testSSEKey("local:v2", 2)
	oldProvider, err := ssecrypto.NewProvider(oldKey.ID, []ssecrypto.Key{oldKey})
	if err != nil {
		t.Fatalf("NewProvider old: %v", err)
	}
	bothProvider, err := ssecrypto.NewProvider(newKey.ID, []ssecrypto.Key{oldKey, newKey})
	if err != nil {
		t.Fatalf("NewProvider both: %v", err)
	}
	eng, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: oldProvider})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	payload := bytes.Repeat([]byte("rewrap-me-"), 1024)
	man, result, err := eng.PutObjectSSES3(context.Background(), "bucket", "key", "", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	oldPath, err := store.ManifestPath(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("ManifestPath: %v", err)
	}
	oldManifestData, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read old manifest: %v", err)
	}
	oldSegmentData := readSegment(t, layout, man.Chunks[0].SegmentID)

	plan, report, err := BuildSSERewrapPlan(layout, metaPath, bothProvider, newKey.ID, nil)
	if err != nil {
		t.Fatalf("BuildSSERewrapPlan: %v", err)
	}
	if report.Candidates != 1 || len(plan.Entries) != 1 {
		t.Fatalf("expected one candidate, report=%+v plan=%+v", report, plan)
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}
	edek := man.Encryption.Keys[0].EncryptedDEK
	if strings.Contains(string(planJSON), base64.StdEncoding.EncodeToString(edek)) || strings.Contains(string(planJSON), hex.EncodeToString(edek)) {
		t.Fatalf("plan leaked raw EDEK")
	}

	runReport, err := RunSSERewrapPlan(layout, metaPath, bothProvider, plan)
	if err != nil {
		t.Fatalf("RunSSERewrapPlan: %v", err)
	}
	if runReport.RebuiltObjects != 1 {
		t.Fatalf("expected one rewrapped object, got %+v", runReport)
	}
	newPath, err := store.ManifestPath(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("ManifestPath new: %v", err)
	}
	if newPath == oldPath {
		t.Fatalf("expected new manifest path")
	}
	if got := readSegment(t, layout, man.Chunks[0].SegmentID); !bytes.Equal(got, oldSegmentData) {
		t.Fatalf("segment ciphertext changed")
	}
	newManifestData, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatalf("read new manifest: %v", err)
	}
	if bytes.Equal(newManifestData, oldManifestData) {
		t.Fatalf("manifest bytes did not change")
	}
	newManifest, err := (&manifest.BinaryCodec{}).Decode(bytes.NewReader(newManifestData))
	if err != nil {
		t.Fatalf("decode new manifest: %v", err)
	}
	if newManifest.Encryption.Keys[0].KeyID != newKey.ID {
		t.Fatalf("expected target key id, got %q", newManifest.Encryption.Keys[0].KeyID)
	}
	if newManifest.Chunks[0] != man.Chunks[0] {
		t.Fatalf("chunk ref changed: old=%+v new=%+v", man.Chunks[0], newManifest.Chunks[0])
	}

	targetOnly, err := ssecrypto.NewProvider(newKey.ID, []ssecrypto.Key{newKey})
	if err != nil {
		t.Fatalf("NewProvider target: %v", err)
	}
	targetEngine, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: targetOnly})
	if err != nil {
		t.Fatalf("engine.New target: %v", err)
	}
	reader, _, err := targetEngine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get with target key: %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll target: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch after rewrap")
	}

	oldOnlyEngine, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: oldProvider})
	if err != nil {
		t.Fatalf("engine.New old-only: %v", err)
	}
	if reader, _, err := oldOnlyEngine.Get(context.Background(), result.VersionID); err == nil {
		_, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr == nil {
			t.Fatalf("expected old-only provider read failure")
		}
	}

	if _, err := RunSSERewrapPlan(layout, metaPath, bothProvider, plan); err == nil {
		t.Fatalf("expected stale plan failure")
	}
}

func TestSSERewrapPlanSkipsPlaintextAndTarget(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	key := testSSEKey("local:v2", 2)
	provider, err := ssecrypto.NewProvider(key.ID, []ssecrypto.Key{key})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	eng, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: provider})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	if _, _, err := eng.PutObject(context.Background(), "bucket", "plain", "", strings.NewReader("plain")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, _, err := eng.PutObjectSSES3(context.Background(), "bucket", "encrypted", "", strings.NewReader("encrypted")); err != nil {
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	plan, report, err := BuildSSERewrapPlan(layout, metaPath, provider, key.ID, nil)
	if err != nil {
		t.Fatalf("BuildSSERewrapPlan: %v", err)
	}
	if len(plan.Entries) != 0 || report.Candidates != 0 {
		t.Fatalf("expected no candidates, plan=%+v report=%+v", plan, report)
	}
	if plan.SkippedPlaintext != 1 || plan.SkippedAlreadyTarget != 1 {
		t.Fatalf("unexpected skip counts: %+v", plan)
	}
}

func TestSSERewrapPlanRequiresTargetAndSourceKeys(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	oldKey := testSSEKey("local:v1", 1)
	targetKey := testSSEKey("local:v2", 2)
	oldProvider, err := ssecrypto.NewProvider(oldKey.ID, []ssecrypto.Key{oldKey})
	if err != nil {
		t.Fatalf("NewProvider old: %v", err)
	}
	eng, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: oldProvider})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	if _, _, err := eng.PutObjectSSES3(context.Background(), "bucket", "key", "", strings.NewReader("encrypted")); err != nil {
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	if _, _, err := BuildSSERewrapPlan(layout, metaPath, oldProvider, targetKey.ID, nil); err == nil {
		t.Fatalf("expected missing target key failure")
	}
	targetOnly, err := ssecrypto.NewProvider(targetKey.ID, []ssecrypto.Key{targetKey})
	if err != nil {
		t.Fatalf("NewProvider target: %v", err)
	}
	if _, _, err := BuildSSERewrapPlan(layout, metaPath, targetOnly, targetKey.ID, nil); err == nil {
		t.Fatalf("expected missing source key failure")
	}
}

func TestSSERewrapMultiKeyManifestPreservesRefs(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}
	oldA := testSSEKey("local:v1a", 1)
	oldB := testSSEKey("local:v1b", 2)
	target := testSSEKey("local:v2", 3)
	provider, err := ssecrypto.NewProvider(target.ID, []ssecrypto.Key{oldA, oldB, target})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	entryA := testWrappedKeyEntry(t, oldA, 10)
	entryB := testWrappedKeyEntry(t, oldB, 20)
	chunkA := manifest.ChunkRef{Index: 0, SegmentID: "seg-a", Offset: 11, Len: 32, PlainLen: 16, KeyRef: 10}
	chunkB := manifest.ChunkRef{Index: 1, SegmentID: "seg-b", Offset: 22, Len: 48, PlainLen: 32, KeyRef: 20}
	man := &manifest.Manifest{
		Bucket:    "bucket",
		Key:       "mpu",
		VersionID: "v-mpu",
		Size:      48,
		Chunks:    []manifest.ChunkRef{chunkA, chunkB},
		Encryption: &manifest.Encryption{
			Mode:          ssecrypto.ModeSSES3,
			Algorithm:     ssecrypto.AlgorithmAES256GCM,
			WrapAlgorithm: ssecrypto.WrapAES256GCM,
			AADScheme:     ssecrypto.AADSchemeV1,
			Keys:          []manifest.KeyEntry{entryA, entryB},
		},
	}
	manifestPath := filepath.Join(layout.ManifestsDir, "v-mpu")
	if err := writeManifest(manifestPath, man); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	if err := store.RecordPut(context.Background(), "bucket", "mpu", "v-mpu", "etag", 48, manifestPath, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	plan, _, err := BuildSSERewrapPlan(layout, metaPath, provider, target.ID, nil)
	if err != nil {
		t.Fatalf("BuildSSERewrapPlan: %v", err)
	}
	if len(plan.Entries) != 1 || len(plan.Entries[0].Keys) != 2 {
		t.Fatalf("expected two planned keys, got %+v", plan)
	}
	if _, err := RunSSERewrapPlan(layout, metaPath, provider, plan); err != nil {
		t.Fatalf("RunSSERewrapPlan: %v", err)
	}
	newPath, err := store.ManifestPath(context.Background(), "v-mpu")
	if err != nil {
		t.Fatalf("ManifestPath: %v", err)
	}
	newData, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatalf("read new manifest: %v", err)
	}
	got, err := (&manifest.BinaryCodec{}).Decode(bytes.NewReader(newData))
	if err != nil {
		t.Fatalf("decode new manifest: %v", err)
	}
	if got.Chunks[0] != chunkA || got.Chunks[1] != chunkB {
		t.Fatalf("chunk refs changed: %+v", got.Chunks)
	}
	if got.Encryption.Keys[0].KeyRef != 10 || got.Encryption.Keys[1].KeyRef != 20 {
		t.Fatalf("key refs changed: %+v", got.Encryption.Keys)
	}
	if got.Encryption.Keys[0].KeyID != target.ID || got.Encryption.Keys[1].KeyID != target.ID {
		t.Fatalf("expected target key IDs: %+v", got.Encryption.Keys)
	}
}

func testSSEKey(id string, seed byte) ssecrypto.Key {
	key := ssecrypto.Key{ID: id}
	copy(key.Bytes[:], bytes.Repeat([]byte{seed}, 32))
	return key
}

func testWrappedKeyEntry(t *testing.T, kek ssecrypto.Key, keyRef uint32) manifest.KeyEntry {
	t.Helper()
	var dek [32]byte
	copy(dek[:], bytes.Repeat([]byte{byte(keyRef)}, 32))
	nonce, edek, err := ssecrypto.WrapDEK(kek, dek, ssecrypto.WrapAAD(kek.ID))
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	sum := sha256.Sum256(edek)
	return manifest.KeyEntry{
		KeyRef:          keyRef,
		KeyID:           kek.ID,
		EncryptedDEK:    edek,
		WrapNonce:       nonce,
		NoncePrefix:     bytes.Repeat([]byte{byte(keyRef + 1)}, 8),
		NonceScheme:     ssecrypto.NonceSchemeV1,
		EDEKFingerprint: sum[:ssecrypto.KeyFingerprintBytes],
	}
}

func readSegment(t *testing.T, layout fs.Layout, segmentID string) []byte {
	t.Helper()
	data, err := os.ReadFile(layout.SegmentPath(segmentID))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	return data
}
