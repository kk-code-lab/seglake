package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMultipartUploadLifecycle(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.CreateMultipartUpload(ctx, "bucket", "prefix/key", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	uploads, err := store.ListMultipartUploads(ctx, "bucket", "prefix/", "", "", 100)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(uploads))
	}

	if err := store.AbortMultipartUpload(ctx, "u1"); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	uploads, err = store.ListMultipartUploads(ctx, "bucket", "prefix/", "", "", 100)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(uploads) != 0 {
		t.Fatalf("expected 0 uploads after abort, got %d", len(uploads))
	}

	if _, err := store.GetMultipartUpload(ctx, "u1"); err == nil {
		t.Fatalf("expected upload deleted after abort")
	}
}

func TestMultipartPartsReplaceAndOrder(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.CreateMultipartUpload(ctx, "bucket", "key", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	if err := store.PutMultipartPart(ctx, "u1", 2, "v2", "etag2", 200); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	if err := store.PutMultipartPart(ctx, "u1", 1, "v1", "etag1", 100); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	if err := store.PutMultipartPart(ctx, "u1", 1, "v1b", "etag1b", 150); err != nil {
		t.Fatalf("PutMultipartPart replace: %v", err)
	}

	parts, err := store.ListMultipartParts(ctx, "u1")
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if parts[0].PartNumber != 1 || parts[0].ETag != "etag1b" || parts[0].Size != 150 {
		t.Fatalf("part1 not replaced: %+v", parts[0])
	}
	if parts[1].PartNumber != 2 {
		t.Fatalf("expected part2 second, got %+v", parts[1])
	}
}
