package ops

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

	report, err := Fsck(layout, "", true)
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

	report, err := Fsck(layout, "", true)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	if report.MissingSegments == 0 {
		t.Fatalf("expected missing segments")
	}
}

func TestSupportBundleWritesRedactedSSEDiagnostics(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segments: %v", err)
	}
	metaPath := filepath.Join(layout.Root, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "bucket", "plain", "v-plain", "etag", 1, filepath.Join(layout.ManifestsDir, "plain.man"), ""); err != nil {
		t.Fatalf("RecordPut plaintext: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "encrypted", "v-encrypted", "etag", 1, filepath.Join(layout.ManifestsDir, "encrypted.man"), ""); err != nil {
		t.Fatalf("RecordPut encrypted: %v", err)
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return store.SetVersionEncryptionTx(tx, "v-encrypted", "SSE-KMS", "aws:kms", "vault:orders", "abcdef1234567890")
	}); err != nil {
		t.Fatalf("SetVersionEncryption: %v", err)
	}

	outDir := filepath.Join(dir, "bundle")
	if _, err := SupportBundle(layout, metaPath, outDir); err != nil {
		t.Fatalf("SupportBundle: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "sse-diagnostics.json"))
	if err != nil {
		t.Fatalf("read sse-diagnostics.json: %v", err)
	}
	var diag meta.SSEDiagnostics
	if err := json.Unmarshal(data, &diag); err != nil {
		t.Fatalf("decode sse diagnostics: %v", err)
	}
	if diag.PlaintextActiveVersions != 1 || diag.EncryptedActiveVersions != 1 {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
	if diag.ByKeyID["vault:orders"] != 1 || diag.ByEDEKFingerprintPrefix["abcdef12"] != 1 {
		t.Fatalf("unexpected diagnostic maps: %+v", diag)
	}
	body := string(data)
	for _, forbidden := range []string{
		"abcdef1234567890",
		"encrypted_dek",
		"plain_dek",
		"kek",
		"vault_token",
		"wrap_nonce",
		"nonce_prefix",
	} {
		if strings.Contains(strings.ToLower(body), forbidden) {
			t.Fatalf("support bundle diagnostics leaked %q: %s", forbidden, body)
		}
	}
}

func TestSupportBundleWritesObjectTagCountsOnly(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		_ = store.Close()
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.SetObjectTags(context.Background(), "bucket", "key", "v1", []meta.ObjectTag{{Key: "secret-project", Value: "alpha"}}); err != nil {
		_ = store.Close()
		t.Fatalf("SetObjectTags: %v", err)
	}
	_ = store.Close()

	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	outDir := filepath.Join(dir, "bundle")
	if _, err := SupportBundle(layout, metaPath, outDir); err != nil {
		t.Fatalf("SupportBundle: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(outDir, "object-tags.json"))
	if err != nil {
		t.Fatalf("read object-tags.json: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "tagged_versions") || !strings.Contains(text, "tag_rows") {
		t.Fatalf("expected aggregate tag counts, got %s", text)
	}
	if strings.Contains(text, "secret-project") || strings.Contains(text, "alpha") {
		t.Fatalf("support bundle leaked tag values: %s", text)
	}
}

func TestSupportBundleWritesRedactedLifecycleDiagnostics(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "bucket"); err != nil {
		_ = store.Close()
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketLifecycle(ctx, meta.BucketLifecycleConfig{
		Bucket:            "bucket",
		XML:               `<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix>private/</Prefix></Filter></Rule></LifecycleConfiguration>`,
		NormalizedJSON:    `{"rules":[{"id":"expire","status":"Enabled","filter":{"prefix":"private/"}}]}`,
		ConfigFingerprint: "raw-fingerprint",
		RuleIDs:           `["expire"]`,
	}); err != nil {
		_ = store.Close()
		t.Fatalf("SetBucketLifecycle: %v", err)
	}
	_ = store.Close()

	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	outDir := filepath.Join(dir, "bundle")
	if _, err := SupportBundle(layout, metaPath, outDir); err != nil {
		t.Fatalf("SupportBundle: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(outDir, "lifecycle-diagnostics.json"))
	if err != nil {
		t.Fatalf("read lifecycle-diagnostics.json: %v", err)
	}
	var diag meta.LifecycleDiagnostics
	if err := json.Unmarshal(body, &diag); err != nil {
		t.Fatalf("decode lifecycle diagnostics: %v", err)
	}
	if diag.ConfiguredBuckets != 1 || diag.TotalRules != 1 || len(diag.Buckets) != 1 {
		t.Fatalf("unexpected lifecycle diagnostics: %+v", diag)
	}
	if diag.Buckets[0].Bucket != "bucket" || diag.Buckets[0].RuleIDs[0] != "expire" {
		t.Fatalf("unexpected bucket diagnostics: %+v", diag.Buckets[0])
	}
	text := string(body)
	for _, forbidden := range []string{"private/", "raw-fingerprint", "LifecycleConfiguration", "normalized_json"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("support bundle leaked %q: %s", forbidden, text)
		}
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

	report, err := Fsck(layout, "", true)
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
	if err := store.RecordPut(context.Background(), man.Bucket, man.Key, man.VersionID, "", man.Size, manPath, ""); err != nil {
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
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "key", uploadID, ""); err != nil {
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
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "key", "upload-2", ""); err != nil {
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
	if err := store.RecordPut(context.Background(), "b", "k1", man.VersionID, "", man.Size, manPath, ""); err != nil {
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
