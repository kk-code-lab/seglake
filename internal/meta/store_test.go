package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestStoreRecordPut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag", 12, "/tmp/manifest"); err != nil {
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
	defer store.Close()

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
	defer store.Close()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "b1", "k1", "v1", "etag", 12, "/tmp/manifest"); err != nil {
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
