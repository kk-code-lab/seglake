package meta

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreRecordPut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag", 12, "/tmp/manifest", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	got, err := store.CurrentVersion(ctx, "b1", "k1")
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if got != "v1" {
		t.Fatalf("version mismatch: %s", got)
	}
}

func TestRecordSegmentSealedAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordSegment(ctx, "seg-1", "/tmp/seg-1", "OPEN", 0, nil); err != nil {
		t.Fatalf("RecordSegment open: %v", err)
	}
	if err := store.RecordSegment(ctx, "seg-1", "/tmp/seg-1", "SEALED", 128, []byte{1, 2}); err != nil {
		t.Fatalf("RecordSegment sealed: %v", err)
	}
	seg, err := store.GetSegment(ctx, "seg-1")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if seg.State != "SEALED" {
		t.Fatalf("state mismatch: %s", seg.State)
	}
	if seg.SealedAt == "" {
		t.Fatalf("expected sealed_at")
	}
	if seg.Size != 128 {
		t.Fatalf("size mismatch: %d", seg.Size)
	}
}

func TestMarkDamaged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag", 12, "/tmp/manifest", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.MarkDamaged(ctx, "v1"); err != nil {
		t.Fatalf("MarkDamaged: %v", err)
	}
	var state string
	if err := store.db.QueryRowContext(ctx, "SELECT state FROM versions WHERE version_id=?", "v1").Scan(&state); err != nil {
		t.Fatalf("query state: %v", err)
	}
	if state != "DAMAGED" {
		t.Fatalf("expected DAMAGED, got %s", state)
	}
}

func TestStatsIncludesOpsRuns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.RecordOpsRun(context.Background(), "fsck", &ReportOps{
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Errors:     1,
	}); err != nil {
		t.Fatalf("RecordOpsRun fsck: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-rewrite", &ReportOps{
		FinishedAt:        time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
		Errors:            0,
		Deleted:           2,
		ReclaimedBytes:    1024,
		RewrittenSegments: 1,
		RewrittenBytes:    2048,
		NewSegments:       1,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc: %v", err)
	}

	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.LastFsckAt == "" || stats.LastGCAt == "" {
		t.Fatalf("expected last ops timestamps")
	}
	if stats.LastGCReclaimed != 1024 || stats.LastGCRewritten != 2048 || stats.LastGCNewSegments != 1 {
		t.Fatalf("unexpected gc stats: %+v", stats)
	}
}

func TestStatsIncludesReplBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.RecordReplBytes(context.Background(), 128); err != nil {
		t.Fatalf("RecordReplBytes: %v", err)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.ReplBytesInTotal != 128 {
		t.Fatalf("expected repl bytes 128, got %d", stats.ReplBytesInTotal)
	}
}

func TestListGCTrends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	base := time.Now().UTC()
	if err := store.RecordOpsRun(context.Background(), "gc-plan", &ReportOps{
		FinishedAt: base.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		Errors:     0,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-plan: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-run", &ReportOps{
		FinishedAt:     base.Add(-time.Hour).Format(time.RFC3339Nano),
		Errors:         1,
		ReclaimedBytes: 100,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-run: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-rewrite", &ReportOps{
		FinishedAt:     base.Format(time.RFC3339Nano),
		Errors:         0,
		ReclaimedBytes: 100,
		RewrittenBytes: 100,
		NewSegments:    2,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-rewrite: %v", err)
	}

	trends, err := store.ListGCTrends(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListGCTrends: %v", err)
	}
	if len(trends) != 2 {
		t.Fatalf("expected 2 trends, got %d", len(trends))
	}
	if trends[0].Mode != "gc-rewrite" || trends[1].Mode != "gc-run" {
		t.Fatalf("unexpected order: %+v", trends)
	}
	if trends[0].ReclaimRate < 0.49 || trends[0].ReclaimRate > 0.51 {
		t.Fatalf("unexpected reclaim rate: %f", trends[0].ReclaimRate)
	}
	if trends[1].ReclaimRate != 1 {
		t.Fatalf("unexpected reclaim rate: %f", trends[1].ReclaimRate)
	}
}

func TestListGCTrendsFiltersPlansAndLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	base := time.Now().UTC()
	if err := store.RecordOpsRun(context.Background(), "gc-plan", &ReportOps{
		FinishedAt: base.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		Errors:     0,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-plan: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-rewrite-plan", &ReportOps{
		FinishedAt: base.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		Errors:     0,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-rewrite-plan: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-run", &ReportOps{
		FinishedAt:     base.Add(-time.Hour).Format(time.RFC3339Nano),
		Errors:         0,
		ReclaimedBytes: 10,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-run: %v", err)
	}
	if err := store.RecordOpsRun(context.Background(), "gc-rewrite", &ReportOps{
		FinishedAt:     base.Format(time.RFC3339Nano),
		Errors:         0,
		ReclaimedBytes: 5,
		RewrittenBytes: 5,
	}); err != nil {
		t.Fatalf("RecordOpsRun gc-rewrite: %v", err)
	}

	trends, err := store.ListGCTrends(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListGCTrends: %v", err)
	}
	if len(trends) != 2 {
		t.Fatalf("expected 2 trends, got %d", len(trends))
	}
	if trends[0].Mode == "gc-plan" || trends[0].Mode == "gc-rewrite-plan" {
		t.Fatalf("unexpected plan in trends: %+v", trends[0])
	}
	limited, err := store.ListGCTrends(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListGCTrends limit: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 trend, got %d", len(limited))
	}
}

func TestDeleteObjectVersionPromotesPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	bucket := "b"
	key := "k"
	if err := store.RecordPut(ctx, bucket, key, "v1", "etag1", 1, "", ""); err != nil {
		t.Fatalf("RecordPut v1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := store.RecordPut(ctx, bucket, key, "v2", "etag2", 2, "", ""); err != nil {
		t.Fatalf("RecordPut v2: %v", err)
	}
	meta, err := store.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if meta.VersionID != "v2" {
		t.Fatalf("expected current v2, got %s", meta.VersionID)
	}
	deleted, err := store.DeleteObjectVersion(ctx, bucket, key, "v2")
	if err != nil {
		t.Fatalf("DeleteObjectVersion: %v", err)
	}
	if !deleted {
		t.Fatalf("expected delete to succeed")
	}
	meta, err = store.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObjectMeta after delete: %v", err)
	}
	if meta.VersionID != "v1" {
		t.Fatalf("expected current v1 after delete, got %s", meta.VersionID)
	}
	v2, err := store.GetObjectVersion(ctx, bucket, key, "v2")
	if err != nil {
		t.Fatalf("GetObjectVersion v2: %v", err)
	}
	if v2.State != "DELETED" {
		t.Fatalf("expected v2 state DELETED, got %s", v2.State)
	}
}
