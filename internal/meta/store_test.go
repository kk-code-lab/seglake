package meta

import (
	"context"
	"database/sql"
	"fmt"
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

func TestRecordMPUCompleteUpdatesETag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag-single", 12, "/tmp/manifest", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.RecordMPUComplete(ctx, "b1", "k1", "v1", "etag-mpu", 12); err != nil {
		t.Fatalf("RecordMPUComplete: %v", err)
	}
	meta, err := store.GetObjectMeta(ctx, "b1", "k1")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if meta.ETag != "etag-mpu" {
		t.Fatalf("expected mpu etag, got %q", meta.ETag)
	}
}

func TestHLCStateMonotonicAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	future := fmt.Sprintf("%019d-%010d", time.Now().UTC().Add(time.Hour).UnixNano(), 0)
	if err := store.setHLCState(ctx, future); err != nil {
		_ = store.Close()
		t.Fatalf("setHLCState: %v", err)
	}
	_ = store.Close()

	store, err = Open(path)
	if err != nil {
		t.Fatalf("Open reopen: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag", 1, "/tmp/manifest", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	maxHLC, err := store.MaxOplogHLC(ctx)
	if err != nil {
		t.Fatalf("MaxOplogHLC: %v", err)
	}
	if maxHLC <= future {
		t.Fatalf("expected hlc > %q, got %q", future, maxHLC)
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

func TestListConflicts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "a/1", "v1", "etag1", 1, "/tmp/m1", ""); err != nil {
		t.Fatalf("RecordPut v1: %v", err)
	}
	if err := store.RecordPut(ctx, "b1", "b/2", "v2", "etag2", 2, "/tmp/m2", ""); err != nil {
		t.Fatalf("RecordPut v2: %v", err)
	}
	if err := store.RecordPut(ctx, "b2", "a/3", "v3", "etag3", 3, "/tmp/m3", ""); err != nil {
		t.Fatalf("RecordPut v3: %v", err)
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return ExecTx(tx, "UPDATE versions SET state='CONFLICT' WHERE version_id IN (?, ?)", "v1", "v3")
	}); err != nil {
		t.Fatalf("mark conflicts: %v", err)
	}

	items, err := store.ListConflicts(ctx, "b1", "", "", "", "", 100)
	if err != nil {
		t.Fatalf("ListConflicts bucket: %v", err)
	}
	if len(items) != 1 || items[0].Bucket != "b1" || items[0].Key != "a/1" {
		t.Fatalf("unexpected bucket conflicts: %+v", items)
	}

	page1, err := store.ListConflicts(ctx, "", "a/", "", "", "", 1)
	if err != nil {
		t.Fatalf("ListConflicts page1: %v", err)
	}
	if len(page1) != 1 {
		t.Fatalf("expected 1 item, got %d", len(page1))
	}
	page2, err := store.ListConflicts(ctx, "", "a/", page1[0].Bucket, page1[0].Key, page1[0].VersionID, 10)
	if err != nil {
		t.Fatalf("ListConflicts page2: %v", err)
	}
	if len(page2) != 1 || page2[0].Bucket != "b2" || page2[0].Key != "a/3" {
		t.Fatalf("unexpected page2: %+v", page2)
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
