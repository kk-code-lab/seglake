package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
)

func TestRunBucketsDeleteWithoutForceFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, metaPath := newTestMetaStore(t)

	if err := store.CreateBucket(ctx, "bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag", 1, "", "text/plain"); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	if err := runBuckets("delete", metaPath, "bucket", "", false, false); err == nil {
		t.Fatalf("expected error for non-empty bucket delete without force")
	}
}

func TestRunBucketsForceDeleteDisabledVersioning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, metaPath := newTestMetaStore(t)

	if err := store.CreateBucketWithVersioning(ctx, "bucket", meta.BucketVersioningDisabled); err != nil {
		t.Fatalf("CreateBucketWithVersioning: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag", 1, "", "text/plain"); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	if err := runBuckets("delete", metaPath, "bucket", "", true, false); err != nil {
		t.Fatalf("runBuckets force delete: %v", err)
	}
	exists, err := store.BucketExists(ctx, "bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Fatalf("expected bucket deleted")
	}
	objects, err := store.ListObjects(ctx, "bucket", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("expected no live objects, got %d", len(objects))
	}
	versions, err := store.ListObjectVersions(ctx, "bucket", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("expected no versions after unversioned delete, got %d", len(versions))
	}
}

func TestRunBucketsForceDeleteEnabledVersioning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, metaPath := newTestMetaStore(t)

	if err := store.CreateBucketWithVersioning(ctx, "bucket", meta.BucketVersioningEnabled); err != nil {
		t.Fatalf("CreateBucketWithVersioning: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag", 1, "", "text/plain"); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	if err := runBuckets("delete", metaPath, "bucket", "", true, false); err != nil {
		t.Fatalf("runBuckets force delete: %v", err)
	}
	exists, err := store.BucketExists(ctx, "bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Fatalf("expected bucket deleted")
	}
	objects, err := store.ListObjects(ctx, "bucket", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("expected no live objects, got %d", len(objects))
	}
	live, err := store.BucketHasLiveObjects(ctx, "bucket")
	if err != nil {
		t.Fatalf("BucketHasLiveObjects: %v", err)
	}
	if live {
		t.Fatalf("expected no live objects after force delete")
	}
}

func newTestMetaStore(t *testing.T) (*meta.Store, string) {
	t.Helper()
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, metaPath
}
