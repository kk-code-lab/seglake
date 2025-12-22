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

func TestSnapshotWritesFiles(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	metaPath := filepath.Join(layout.Root, "meta.db")
	t.Logf("metaPath=%s root=%s segments=%s manifests=%s", metaPath, layout.Root, layout.SegmentsDir, layout.ManifestsDir)
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	outDir := filepath.Join(dir, "snapshot")
	report, err := Snapshot(layout, metaPath, outDir)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if report.Mode != "snapshot" {
		t.Fatalf("expected snapshot mode, got %s", report.Mode)
	}
	if _, err := os.Stat(filepath.Join(outDir, "snapshot.json")); err != nil {
		t.Fatalf("snapshot.json missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "meta.db")); err != nil {
		t.Fatalf("meta.db missing: %v", err)
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

	report, candidates, err := GCPlan(layout, metaPath, 0, GCGuardrails{})
	if err != nil {
		t.Fatalf("GCPlan: %v", err)
	}
	if report.Candidates != 1 || len(candidates) != 1 || candidates[0].ID != deadSegID {
		t.Fatalf("expected dead segment candidate, got %+v", candidates)
	}

	if _, err := GCRun(layout, metaPath, 0, true, GCGuardrails{}); err != nil {
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

func TestMPUGCPlanAndRun(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	uploadID := "upload-1"
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "key", uploadID); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := store.PutMultipartPart(context.Background(), uploadID, 1, "v1", "etag", 123); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	report, uploads, err := MPUGCPlan(metaPath, time.Nanosecond, MPUGCGuardrails{})
	if err != nil {
		t.Fatalf("MPUGCPlan: %v", err)
	}
	if report.Candidates == 0 || len(uploads) == 0 {
		t.Fatalf("expected candidates")
	}

	report, err = MPUGCRun(metaPath, time.Nanosecond, true, MPUGCGuardrails{})
	if err != nil {
		t.Fatalf("MPUGCRun: %v", err)
	}
	if report.Deleted == 0 {
		t.Fatalf("expected deleted uploads")
	}
	if _, err := store.GetMultipartUpload(context.Background(), uploadID); err == nil {
		t.Fatalf("expected upload deleted")
	}
}

func TestGCPlanIncludesMultipartParts(t *testing.T) {
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

	segID := "seg-mpu"
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

	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := store.RecordSegment(context.Background(), segID, segPath, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:]); err != nil {
		t.Fatalf("RecordSegment: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "",
		Key:       "",
		VersionID: "v-mpu",
		Size:      int64(len(data)),
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: segID, Offset: offset, Len: uint32(len(data))},
		},
	}
	manPath := layout.ManifestPath(man.VersionID)
	if err := writeManifest(manPath, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := store.RecordManifest(context.Background(), man.VersionID, manPath); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "key", "upload-2"); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := store.PutMultipartPart(context.Background(), "upload-2", 1, man.VersionID, "etag", int64(len(data))); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}

	report, candidates, err := GCPlan(layout, metaPath, 0, GCGuardrails{})
	if err != nil {
		t.Fatalf("GCPlan: %v", err)
	}
	if report.Manifests == 0 {
		t.Fatalf("expected manifests counted")
	}
	if len(candidates) != 0 {
		t.Fatalf("expected no GC candidates, got %d", len(candidates))
	}
}

func TestGCRewritePlanRun(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	segID := "seg-gc"
	segPath := layout.SegmentPath(segID)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	liveData := []byte("hello")
	offset, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: [32]byte{1}, Len: uint32(len(liveData))}, liveData)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	deadData := []byte("dead!")
	if _, err := writer.AppendRecord(segment.ChunkRecordHeader{Hash: [32]byte{2}, Len: uint32(len(deadData))}, deadData); err != nil {
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
	if err := store.RecordSegment(context.Background(), segID, segPath, "SEALED", info.Size(), footer.ChecksumHash[:]); err != nil {
		t.Fatalf("RecordSegment: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "b",
		Key:       "k1",
		VersionID: "v1",
		Size:      int64(len(liveData)),
		Chunks: []manifest.ChunkRef{
			{Index: 0, SegmentID: segID, Offset: offset, Len: uint32(len(liveData))},
		},
	}
	manPath := layout.ManifestPath(man.VersionID)
	if err := writeManifest(manPath, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := store.RecordPut(context.Background(), "b", "k1", man.VersionID, "", man.Size, manPath); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	plan, report, err := GCRewritePlanBuild(layout, metaPath, 0, 1.0)
	if err != nil {
		t.Fatalf("GCRewritePlanBuild: %v", err)
	}
	if report.Candidates == 0 || len(plan.Candidates) == 0 {
		t.Fatalf("expected candidates")
	}

	path := filepath.Join(dir, "gc-plan.json")
	if err := WriteGCRewritePlan(path, plan); err != nil {
		t.Fatalf("WriteGCRewritePlan: %v", err)
	}
	readPlan, err := ReadGCRewritePlan(path)
	if err != nil {
		t.Fatalf("ReadGCRewritePlan: %v", err)
	}

	gcReport, err := GCRewriteFromPlan(layout, metaPath, readPlan, true, 0, "")
	if err != nil {
		t.Fatalf("GCRewriteFromPlan: %v", err)
	}
	if gcReport.RewrittenSegments == 0 {
		t.Fatalf("expected rewritten segments")
	}

	manFile, err := os.Open(manPath)
	if err != nil {
		t.Fatalf("Open manifest: %v", err)
	}
	updated, err := (&manifest.BinaryCodec{}).Decode(manFile)
	_ = manFile.Close()
	if err != nil {
		t.Fatalf("Decode manifest: %v", err)
	}
	if len(updated.Chunks) != 1 {
		t.Fatalf("unexpected chunks: %d", len(updated.Chunks))
	}
	if updated.Chunks[0].SegmentID == segID {
		t.Fatalf("expected chunk to move to new segment")
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

	report, candidates, err := GCPlan(layout, metaPath, 24*time.Hour, GCGuardrails{})
	if err != nil {
		t.Fatalf("GCPlan: %v", err)
	}
	if report.Candidates != 0 || len(candidates) != 0 {
		t.Fatalf("expected no candidates due to minAge")
	}
}
