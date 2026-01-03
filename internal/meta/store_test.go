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

func TestMigrationV18AddsDeleteMarkersForOrphans(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS buckets (bucket TEXT PRIMARY KEY, created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS versions (
			version_id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			etag TEXT,
			size INTEGER NOT NULL,
			content_type TEXT,
			last_modified_utc TEXT NOT NULL,
			hlc_ts TEXT,
			site_id TEXT,
			state TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS objects_current (
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			version_id TEXT NOT NULL,
			PRIMARY KEY(bucket, key)
		)`,
		`CREATE TABLE IF NOT EXISTS manifests (version_id TEXT PRIMARY KEY, path TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS segments (segment_id TEXT PRIMARY KEY, path TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, sealed_at TEXT, size INTEGER, footer_checksum BLOB)`,
		`CREATE TABLE IF NOT EXISTS api_keys (access_key TEXT PRIMARY KEY, secret_hash TEXT NOT NULL, salt TEXT NOT NULL, enabled INTEGER NOT NULL, created_at TEXT NOT NULL, label TEXT, last_used_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS api_key_bucket_allow (access_key TEXT NOT NULL, bucket TEXT NOT NULL, PRIMARY KEY(access_key, bucket))`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS multipart_uploads (upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL, created_at TEXT NOT NULL, state TEXT NOT NULL, content_type TEXT)`,
		`CREATE TABLE IF NOT EXISTS multipart_parts (upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, version_id TEXT NOT NULL, etag TEXT NOT NULL, size INTEGER NOT NULL, last_modified_utc TEXT NOT NULL, PRIMARY KEY(upload_id, part_number))`,
		`CREATE TABLE IF NOT EXISTS bucket_policies (bucket TEXT PRIMARY KEY, policy TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS oplog (id INTEGER PRIMARY KEY AUTOINCREMENT, site_id TEXT NOT NULL, hlc_ts TEXT NOT NULL, op_type TEXT NOT NULL, bucket TEXT NOT NULL, key TEXT NOT NULL, version_id TEXT, payload TEXT, bytes INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS repl_state (id INTEGER PRIMARY KEY CHECK (id = 1), updated_at TEXT NOT NULL, last_pull_hlc TEXT NOT NULL DEFAULT '', last_push_hlc TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS repl_state_remote (remote TEXT PRIMARY KEY, updated_at TEXT NOT NULL, last_pull_hlc TEXT NOT NULL DEFAULT '', last_push_hlc TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS repl_metrics (id INTEGER PRIMARY KEY CHECK (id = 1), updated_at TEXT NOT NULL, conflict_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS hlc_state (id INTEGER PRIMARY KEY CHECK (id = 1), last_hlc TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.Exec(stmt); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	_, err = tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES(17, ?)`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.Exec(`INSERT INTO buckets(bucket, created_at) VALUES(?, ?)`, "b", now)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	_, err = tx.Exec(`INSERT INTO versions(version_id, bucket, key, etag, size, content_type, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`, "v1", "b", "k1", "etag", 1, "", now, "0000000000000000001-0000000001", "legacy")
	if err != nil {
		t.Fatalf("insert v1: %v", err)
	}
	_, err = tx.Exec(`INSERT INTO objects_current(bucket, key, version_id) VALUES(?, ?, ?)`, "b", "k1", "v1")
	if err != nil {
		t.Fatalf("objects_current k1: %v", err)
	}
	_, err = tx.Exec(`INSERT INTO versions(version_id, bucket, key, etag, size, content_type, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`, "v2", "b", "k2", "etag2", 1, "", now, "0000000000000000002-0000000001", "legacy")
	if err != nil {
		t.Fatalf("insert v2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	meta1, err := store.GetObjectMeta(context.Background(), "b", "k1")
	if err != nil {
		t.Fatalf("GetObjectMeta k1: %v", err)
	}
	if meta1.VersionID != "v1" {
		t.Fatalf("expected k1 current v1, got %s", meta1.VersionID)
	}

	meta2, err := store.GetObjectMeta(context.Background(), "b", "k2")
	if err != nil {
		t.Fatalf("GetObjectMeta k2: %v", err)
	}
	if meta2.State != VersionStateDeleteMarker {
		t.Fatalf("expected delete marker for k2, got %s", meta2.State)
	}
}
