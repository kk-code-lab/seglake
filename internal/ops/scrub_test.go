package ops

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func TestScrubMarksDamagedVersion(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}

	man, result, err := eng.PutObject(context.Background(), "bucket", "key", "", strings.NewReader("hello world"))
	if err != nil {
		_ = store.Close()
		t.Fatalf("PutObject: %v", err)
	}
	if len(man.Chunks) == 0 {
		_ = store.Close()
		t.Fatalf("expected chunks")
	}

	segPath := layout.SegmentPath(man.Chunks[0].SegmentID)
	f, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
	if err != nil {
		_ = store.Close()
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, man.Chunks[0].Offset); err != nil {
		_ = f.Close()
		_ = store.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		_ = store.Close()
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := Scrub(layout, metaPath, true)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if report.Errors == 0 {
		t.Fatalf("expected scrub errors")
	}

	store2, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	metaObj, err := store2.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != result.VersionID {
		t.Fatalf("version mismatch: %s", metaObj.VersionID)
	}
	if metaObj.State != "DAMAGED" {
		t.Fatalf("expected DAMAGED, got %s", metaObj.State)
	}
}

func TestScrubReportsShortRead(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}

	man, _, err := eng.PutObject(context.Background(), "bucket", "key", "", strings.NewReader("hello world"))
	if err != nil {
		_ = store.Close()
		t.Fatalf("PutObject: %v", err)
	}
	if len(man.Chunks) == 0 {
		_ = store.Close()
		t.Fatalf("expected chunks")
	}

	segPath := layout.SegmentPath(man.Chunks[0].SegmentID)
	fi, err := os.Stat(segPath)
	if err != nil {
		_ = store.Close()
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(segPath, fi.Size()-2); err != nil {
		_ = store.Close()
		t.Fatalf("Truncate: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := Scrub(layout, metaPath, true)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if report.Errors == 0 {
		t.Fatalf("expected scrub errors")
	}
}

func TestDeepEncryptedScrubPassesAndShallowNeedsNoKEK(t *testing.T) {
	layout, metaPath, provider, _, cleanup := newEncryptedScrubFixture(t, "secret payload")
	defer cleanup()

	shallow, err := Scrub(layout, metaPath, true)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if shallow.Errors != 0 || shallow.EncryptedChunks != 0 {
		t.Fatalf("unexpected shallow report: %+v", shallow)
	}

	deep, err := ScrubWithOptions(layout, metaPath, ScrubOptions{
		LiveOnly:      true,
		DeepEncrypted: true,
		SSEProvider:   provider,
	})
	if err != nil {
		t.Fatalf("ScrubWithOptions: %v", err)
	}
	if deep.Errors != 0 || deep.EncryptedManifests != 1 || deep.EncryptedChunks == 0 {
		t.Fatalf("unexpected deep report: %+v", deep)
	}
}

func TestDeepEncryptedScrubMissingKEKMarksDamaged(t *testing.T) {
	layout, metaPath, _, result, cleanup := newEncryptedScrubFixture(t, "secret payload")
	defer cleanup()

	report, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: true, DeepEncrypted: true})
	if err != nil {
		t.Fatalf("ScrubWithOptions: %v", err)
	}
	if report.Errors == 0 || report.MissingKEKs == 0 {
		t.Fatalf("expected missing KEK error, got %+v", report)
	}
	assertVersionDamaged(t, metaPath, result.VersionID)
}

func TestDeepEncryptedScrubCorruptEDEKMarksDamaged(t *testing.T) {
	layout, metaPath, provider, result, cleanup := newEncryptedScrubFixture(t, "secret payload")
	defer cleanup()
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	manifestPath, err := store.ManifestPath(context.Background(), result.VersionID)
	if err != nil {
		_ = store.Close()
		t.Fatalf("ManifestPath: %v", err)
	}
	_ = store.Close()
	man := readManifest(t, manifestPath)
	man.Encryption.Keys[0].EncryptedDEK[0] ^= 0xff
	if err := writeManifest(manifestPath, man); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	report, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: true, DeepEncrypted: true, SSEProvider: provider})
	if err != nil {
		t.Fatalf("ScrubWithOptions: %v", err)
	}
	if report.Errors == 0 || report.EDEKUnwrapFailures == 0 {
		t.Fatalf("expected EDEK unwrap error, got %+v", report)
	}
	assertVersionDamaged(t, metaPath, result.VersionID)
}

func TestDeepEncryptedScrubAEADFailureMarksDamaged(t *testing.T) {
	layout, metaPath, provider, result, cleanup := newEncryptedScrubFixture(t, "secret payload")
	defer cleanup()
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	manifestPath, err := store.ManifestPath(context.Background(), result.VersionID)
	if err != nil {
		_ = store.Close()
		t.Fatalf("ManifestPath: %v", err)
	}
	_ = store.Close()
	man := readManifest(t, manifestPath)
	ch := man.Chunks[0]
	segPath := layout.SegmentPath(ch.SegmentID)
	ciphertext, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	ciphertext[ch.Offset] ^= 0xff
	if err := os.WriteFile(segPath, ciphertext, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	tampered := make([]byte, ch.Len)
	copy(tampered, ciphertext[ch.Offset:ch.Offset+int64(ch.Len)])
	man.Chunks[0].Hash = segment.HashChunk(tampered)
	if err := writeManifest(manifestPath, man); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	report, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: true, DeepEncrypted: true, SSEProvider: provider})
	if err != nil {
		t.Fatalf("ScrubWithOptions: %v", err)
	}
	if report.Errors == 0 || report.AEADFailures == 0 {
		t.Fatalf("expected AEAD error, got %+v", report)
	}
	assertVersionDamaged(t, metaPath, result.VersionID)
}

func TestDeepEncryptedScrubMultiKeyManifest(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	keyA := testSSEKey("local:v1a", 1)
	keyB := testSSEKey("local:v1b", 2)
	providerA, err := ssecrypto.NewProvider(keyA.ID, []ssecrypto.Key{keyA})
	if err != nil {
		t.Fatalf("NewProvider A: %v", err)
	}
	providerB, err := ssecrypto.NewProvider(keyB.ID, []ssecrypto.Key{keyB})
	if err != nil {
		t.Fatalf("NewProvider B: %v", err)
	}
	engA, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: providerA})
	if err != nil {
		t.Fatalf("engine.New A: %v", err)
	}
	manA, _, err := engA.PutObjectSSES3(context.Background(), "bucket", "part-a", "", bytes.NewReader([]byte("part-a")))
	if err != nil {
		t.Fatalf("PutObjectSSES3 A: %v", err)
	}
	engB, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: providerB})
	if err != nil {
		t.Fatalf("engine.New B: %v", err)
	}
	manB, _, err := engB.PutObjectSSES3(context.Background(), "bucket", "part-b", "", bytes.NewReader([]byte("part-b")))
	if err != nil {
		t.Fatalf("PutObjectSSES3 B: %v", err)
	}
	final := &manifest.Manifest{
		Bucket:    "bucket",
		Key:       "multi",
		VersionID: "multi-version",
		Size:      manA.Size + manB.Size,
		Encryption: &manifest.Encryption{
			Mode:          manA.Encryption.Mode,
			Algorithm:     manA.Encryption.Algorithm,
			WrapAlgorithm: manA.Encryption.WrapAlgorithm,
			AADScheme:     manA.Encryption.AADScheme,
			Keys: []manifest.KeyEntry{
				manA.Encryption.Keys[0],
				manB.Encryption.Keys[0],
			},
		},
	}
	final.Encryption.Keys[1].KeyRef = 1
	chA := manA.Chunks[0]
	chB := manB.Chunks[0]
	chB.KeyRef = 1
	final.Chunks = []manifest.ChunkRef{chA, chB}
	if err := writeManifest(layout.ManifestPath(final.VersionID), final); err != nil {
		t.Fatalf("writeManifest final: %v", err)
	}
	readOnlyProvider, err := ssecrypto.NewLookupProvider([]ssecrypto.Key{keyA, keyB})
	if err != nil {
		t.Fatalf("NewLookupProvider: %v", err)
	}
	report, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: false, DeepEncrypted: true, SSEProvider: readOnlyProvider})
	if err != nil {
		t.Fatalf("ScrubWithOptions: %v", err)
	}
	if report.Errors != 0 || report.EncryptedChunks < 2 {
		t.Fatalf("unexpected multi-key scrub report: %+v", report)
	}
}

func TestDeepEncryptedScrubAllManifestsIncludesOrphans(t *testing.T) {
	layout, metaPath, provider, result, cleanup := newEncryptedScrubFixture(t, "secret payload")
	defer cleanup()
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	manifestPath, err := store.ManifestPath(context.Background(), result.VersionID)
	if err != nil {
		_ = store.Close()
		t.Fatalf("ManifestPath: %v", err)
	}
	if _, err := store.DeleteObject(context.Background(), "bucket", "key"); err != nil {
		_ = store.Close()
		t.Fatalf("DeleteObject: %v", err)
	}
	_ = store.Close()
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected orphan manifest file: %v", err)
	}

	liveReport, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: true, DeepEncrypted: true, SSEProvider: provider})
	if err != nil {
		t.Fatalf("live ScrubWithOptions: %v", err)
	}
	allReport, err := ScrubWithOptions(layout, metaPath, ScrubOptions{LiveOnly: false, DeepEncrypted: true, SSEProvider: provider})
	if err != nil {
		t.Fatalf("all ScrubWithOptions: %v", err)
	}
	if liveReport.EncryptedManifests != 0 {
		t.Fatalf("expected live scrub to skip orphan, got %+v", liveReport)
	}
	if allReport.EncryptedManifests == 0 {
		t.Fatalf("expected all-manifests scrub to include orphan, got %+v", allReport)
	}
}

func newEncryptedScrubFixture(t *testing.T, payload string) (fs.Layout, string, *ssecrypto.Provider, *engine.PutResult, func()) {
	t.Helper()
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	key := testSSEKey("local:v1", 1)
	provider, err := ssecrypto.NewProvider(key.ID, []ssecrypto.Key{key})
	if err != nil {
		_ = store.Close()
		t.Fatalf("NewProvider: %v", err)
	}
	eng, err := engine.New(engine.Options{Layout: layout, MetaStore: store, SSE: provider})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}
	_, result, err := eng.PutObjectSSES3(context.Background(), "bucket", "key", "", strings.NewReader(payload))
	if err != nil {
		_ = store.Close()
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	return layout, metaPath, provider, result, func() { _ = store.Close() }
}

func readManifest(t *testing.T, path string) *manifest.Manifest {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open manifest: %v", err)
	}
	defer func() { _ = file.Close() }()
	man, err := (&manifest.BinaryCodec{}).Decode(file)
	if err != nil {
		t.Fatalf("Decode manifest: %v", err)
	}
	return man
}

func assertVersionDamaged(t *testing.T, metaPath, versionID string) {
	t.Helper()
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	obj, err := store.GetObjectVersion(context.Background(), "bucket", "key", versionID)
	if err != nil {
		t.Fatalf("GetObjectVersion: %v", err)
	}
	if obj.State != meta.VersionStateDamaged {
		t.Fatalf("expected DAMAGED, got %s", obj.State)
	}
}
