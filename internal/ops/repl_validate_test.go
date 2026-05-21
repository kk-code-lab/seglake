package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func TestReplValidateNoDiff(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA := filepath.Join(layoutA.ManifestsDir, "bucket__key__v1")
	pathB := filepath.Join(layoutB.ManifestsDir, "bucket__key__v1")
	if err := writeManifest(pathA, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest A: %v", err)
	}
	if err := writeManifest(pathB, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest B: %v", err)
	}

	storeA, err := meta.Open(metaA)
	if err != nil {
		t.Fatalf("meta.Open A: %v", err)
	}
	if err := storeA.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathA, ""); err != nil {
		t.Fatalf("RecordPut A: %v", err)
	}
	_ = storeA.Close()

	storeB, err := meta.Open(metaB)
	if err != nil {
		t.Fatalf("meta.Open B: %v", err)
	}
	if err := storeB.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathB, ""); err != nil {
		t.Fatalf("RecordPut B: %v", err)
	}
	_ = storeB.Close()

	report, err := ReplValidate(layoutA, metaA, dataB)
	if err != nil {
		t.Fatalf("ReplValidate: %v", err)
	}
	if report.Errors != 0 {
		t.Fatalf("expected no errors, got %d", report.Errors)
	}
	if report.CompareManifestsExtra != 0 || report.CompareManifestsMissing != 0 {
		t.Fatalf("unexpected manifest diffs: %+v", report)
	}
	if report.CompareLiveExtra != 0 || report.CompareLiveMissing != 0 {
		t.Fatalf("unexpected live diffs: %+v", report)
	}
	if report.CompareVersionsExtra != 0 || report.CompareVersionsMissing != 0 {
		t.Fatalf("unexpected version diffs: %+v", report)
	}
}

func TestReplValidateDetectsDiff(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	if err := os.MkdirAll(layoutB.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	pathA := filepath.Join(layoutA.ManifestsDir, "bucket__key__v1")
	if err := writeManifest(pathA, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest A: %v", err)
	}

	storeA, err := meta.Open(metaA)
	if err != nil {
		t.Fatalf("meta.Open A: %v", err)
	}
	if err := storeA.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathA, ""); err != nil {
		t.Fatalf("RecordPut A: %v", err)
	}
	_ = storeA.Close()

	storeB, err := meta.Open(metaB)
	if err != nil {
		t.Fatalf("meta.Open B: %v", err)
	}
	_ = storeB.Close()

	report, err := ReplValidate(layoutA, metaA, dataB)
	if err != nil {
		t.Fatalf("ReplValidate: %v", err)
	}
	if report.CompareManifestsExtra != 1 {
		t.Fatalf("expected 1 extra manifest, got %d", report.CompareManifestsExtra)
	}
	if report.CompareLiveExtra != 1 {
		t.Fatalf("expected 1 extra live entry, got %d", report.CompareLiveExtra)
	}
	if report.CompareVersionsExtra != 1 {
		t.Fatalf("expected 1 extra version entry, got %d", report.CompareVersionsExtra)
	}
	if report.Errors == 0 {
		t.Fatalf("expected errors")
	}
}

func TestReplValidateDeepVerifiesChunkHashes(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA, man := writeReplValidateObject(t, layoutA, "v1", []byte("replicated bytes"))
	pathB, _ := writeReplValidateObject(t, layoutB, "v1", []byte("replicated bytes"))
	if err := writeManifest(pathB, man); err != nil {
		t.Fatalf("rewrite remote manifest: %v", err)
	}
	recordPut(t, metaA, pathA, man)
	recordPut(t, metaB, pathB, man)

	report, err := ReplValidateWithOptions(layoutA, metaA, dataB, ReplValidateOptions{Deep: true})
	if err != nil {
		t.Fatalf("ReplValidateWithOptions: %v", err)
	}
	if report.Errors != 0 {
		t.Fatalf("expected no errors, got %+v", report)
	}
	if report.CompareChunksChecked != 2 {
		t.Fatalf("expected two checked chunks, got %d", report.CompareChunksChecked)
	}
}

func TestReplValidateShallowIgnoresChunkHashMismatch(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA, man := writeReplValidateObject(t, layoutA, "v1", []byte("good bytes"))
	pathB, _ := writeReplValidateObject(t, layoutB, "v1", []byte("bad! bytes"))
	if err := writeManifest(pathB, man); err != nil {
		t.Fatalf("rewrite remote manifest: %v", err)
	}
	recordPut(t, metaA, pathA, man)
	recordPut(t, metaB, pathB, man)

	shallow, err := ReplValidate(layoutA, metaA, dataB)
	if err != nil {
		t.Fatalf("ReplValidate: %v", err)
	}
	if shallow.Errors != 0 || shallow.CompareChunksChecked != 0 {
		t.Fatalf("expected shallow validation to skip chunks, got %+v", shallow)
	}

	deep, err := ReplValidateWithOptions(layoutA, metaA, dataB, ReplValidateOptions{Deep: true})
	if err != nil {
		t.Fatalf("ReplValidateWithOptions: %v", err)
	}
	if deep.CompareChunksInvalid != 1 || deep.Errors == 0 {
		t.Fatalf("expected deep validation hash error, got %+v", deep)
	}
}

func TestReplValidateDeepDetectsMissingSegment(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA, man := writeReplValidateObject(t, layoutA, "v1", []byte("replicated bytes"))
	pathB, _ := writeReplValidateObject(t, layoutB, "v1", []byte("replicated bytes"))
	if err := writeManifest(pathB, man); err != nil {
		t.Fatalf("rewrite remote manifest: %v", err)
	}
	if err := os.Remove(layoutB.SegmentPath(man.Chunks[0].SegmentID)); err != nil {
		t.Fatalf("remove remote segment: %v", err)
	}
	recordPut(t, metaA, pathA, man)
	recordPut(t, metaB, pathB, man)

	report, err := ReplValidateWithOptions(layoutA, metaA, dataB, ReplValidateOptions{Deep: true})
	if err != nil {
		t.Fatalf("ReplValidateWithOptions: %v", err)
	}
	if report.MissingSegments != 1 || report.CompareChunksInvalid != 1 || report.Errors == 0 {
		t.Fatalf("expected missing segment error, got %+v", report)
	}
}

func TestReplValidateDeepDetectsOutOfBoundsChunk(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA, man := writeReplValidateObject(t, layoutA, "v1", []byte("replicated bytes"))
	pathB, _ := writeReplValidateObject(t, layoutB, "v1", []byte("replicated bytes"))
	remoteMan := *man
	remoteMan.Chunks = append([]manifest.ChunkRef(nil), man.Chunks...)
	remoteMan.Chunks[0].Offset += 1 << 20
	if err := writeManifest(pathB, &remoteMan); err != nil {
		t.Fatalf("rewrite remote manifest: %v", err)
	}
	recordPut(t, metaA, pathA, man)
	recordPut(t, metaB, pathB, &remoteMan)

	report, err := ReplValidateWithOptions(layoutA, metaA, dataB, ReplValidateOptions{Deep: true})
	if err != nil {
		t.Fatalf("ReplValidateWithOptions: %v", err)
	}
	if report.OutOfBoundsChunks != 1 || report.CompareChunksInvalid != 1 || report.Errors == 0 {
		t.Fatalf("expected out-of-bounds chunk error, got %+v", report)
	}
}

func writeReplValidateObject(t *testing.T, layout fs.Layout, versionID string, data []byte) (string, *manifest.Manifest) {
	t.Helper()
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	segID := "seg-" + versionID
	segPath := layout.SegmentPath(segID)
	hash := segment.HashChunk(data)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	offset, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: hash, Len: uint32(len(data))}, data)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if err := writer.Seal(segment.FinalizeFooter(segment.NewFooter(1))); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	man := &manifest.Manifest{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: versionID,
		Size:      int64(len(data)),
		Chunks: []manifest.ChunkRef{
			{Index: 0, Hash: hash, SegmentID: segID, Offset: offset, Len: uint32(len(data))},
		},
	}
	path := layout.ManifestPath(versionID)
	if err := writeManifest(path, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path, man
}

func recordPut(t *testing.T, metaPath, manifestPath string, man *manifest.Manifest) {
	t.Helper()
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.RecordPut(context.Background(), man.Bucket, man.Key, man.VersionID, "", man.Size, manifestPath, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
}
