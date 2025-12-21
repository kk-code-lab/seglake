package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func TestFsckWithValidSegment(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	segID := "seg-1"
	segPath := layout.SegmentPath(segID)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	data := []byte("hello")
	offset, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: [32]byte{1}, Len: uint32(len(data))}, data)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	footer := segment.FinalizeFooter(segment.NewFooter(1))
	if err := writer.Seal(footer); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "b",
		Key:       "k",
		VersionID: "v1",
		Size:      int64(len(data)),
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: segID, Offset: offset, Len: uint32(len(data))},
		},
	}
	if err := writeManifest(layout.ManifestPath(man.VersionID), man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	report, err := Fsck(layout)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	if report.MissingSegments != 0 {
		t.Fatalf("expected no missing segments, got %d", report.MissingSegments)
	}
	if report.OutOfBoundsChunks != 0 {
		t.Fatalf("expected no out-of-bounds chunks, got %d", report.OutOfBoundsChunks)
	}
}

func TestFsckReportsMissingSegment(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "b",
		Key:       "k",
		VersionID: "v1",
		Size:      4,
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: "seg-missing", Offset: segment.SegmentHeaderLen(), Len: 4},
		},
	}
	if err := writeManifest(layout.ManifestPath(man.VersionID), man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	report, err := Fsck(layout)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	if report.MissingSegments == 0 {
		t.Fatalf("expected missing segments")
	}
}

func TestFsckReportsInvalidFooter(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	segID := "seg-bad"
	segPath := layout.SegmentPath(segID)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	data := []byte("hello")
	offset, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: [32]byte{1}, Len: uint32(len(data))}, data)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "b",
		Key:       "k",
		VersionID: "v1",
		Size:      int64(len(data)),
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: segID, Offset: offset, Len: uint32(len(data))},
		},
	}
	if err := writeManifest(layout.ManifestPath(man.VersionID), man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	report, err := Fsck(layout)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	if report.Errors == 0 {
		t.Fatalf("expected fsck errors for invalid footer")
	}
	if report.MissingSegments != 0 {
		t.Fatalf("expected no missing segments, got %d", report.MissingSegments)
	}
}

func TestGCPlanAndRun(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	metaPath := filepath.Join(layout.Root, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	liveSegID := "seg-live"
	deadSegID := "seg-dead"
	liveSegPath, liveOffset, liveSize := createSegment(t, layout, liveSegID)
	deadSegPath, _, deadSize := createSegment(t, layout, deadSegID)

	if err := store.RecordSegment(context.Background(), liveSegID, liveSegPath, "SEALED", liveSize, nil); err != nil {
		t.Fatalf("RecordSegment live: %v", err)
	}
	if err := store.RecordSegment(context.Background(), deadSegID, deadSegPath, "SEALED", deadSize, nil); err != nil {
		t.Fatalf("RecordSegment dead: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "b",
		Key:       "k",
		VersionID: "v1",
		Size:      5,
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: liveSegID, Offset: liveOffset, Len: 5},
		},
	}
	manPath := layout.ManifestPath(man.VersionID)
	if err := writeManifest(manPath, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := store.RecordPut(context.Background(), man.Bucket, man.Key, man.VersionID, "", man.Size, manPath); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	report, candidates, err := GCPlan(layout, metaPath, 0)
	if err != nil {
		t.Fatalf("GCPlan: %v", err)
	}
	if report.Candidates != 1 || len(candidates) != 1 || candidates[0].ID != deadSegID {
		t.Fatalf("expected dead segment candidate, got %+v", candidates)
	}

	if _, err := GCRun(layout, metaPath, 0, true); err != nil {
		t.Fatalf("GCRun: %v", err)
	}
	if _, err := os.Stat(deadSegPath); !os.IsNotExist(err) {
		t.Fatalf("expected dead segment removed")
	}

	store2, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	segments, err := store2.ListSegments(context.Background())
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].ID != liveSegID {
		t.Fatalf("expected only live segment, got %+v", segments)
	}
}

func createSegment(t *testing.T, layout fs.Layout, segID string) (string, int64, int64) {
	t.Helper()
	segPath := layout.SegmentPath(segID)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	data := []byte("hello")
	offset, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: [32]byte{2}, Len: uint32(len(data))}, data)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	footer := segment.FinalizeFooter(segment.NewFooter(1))
	if err := writer.Seal(footer); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return segPath, offset, info.Size()
}

func writeManifest(path string, man *manifest.Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return (&manifest.BinaryCodec{}).Encode(file, man)
}

func TestGCPlanMinAgeRespected(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	metaPath := filepath.Join(layout.Root, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	deadSegID := "seg-dead"
	deadSegPath, _, deadSize := createSegment(t, layout, deadSegID)
	if err := store.RecordSegment(context.Background(), deadSegID, deadSegPath, "SEALED", deadSize, nil); err != nil {
		t.Fatalf("RecordSegment: %v", err)
	}

	report, candidates, err := GCPlan(layout, metaPath, 24*time.Hour)
	if err != nil {
		t.Fatalf("GCPlan: %v", err)
	}
	if report.Candidates != 0 || len(candidates) != 0 {
		t.Fatalf("expected no candidates due to minAge")
	}
}
