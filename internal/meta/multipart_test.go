package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

func TestAbortMultipartUploadWritesAndAppliesOplog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source, err := Open(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if err := source.CreateMultipartUpload(ctx, "bucket", "tmp/key", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload source: %v", err)
	}
	if err := source.PutMultipartPart(ctx, "u1", 1, "part-v1", "etag", 100); err != nil {
		t.Fatalf("PutMultipartPart source: %v", err)
	}
	if err := source.AbortMultipartUpload(ctx, "u1"); err != nil {
		t.Fatalf("AbortMultipartUpload source: %v", err)
	}
	entries, err := source.ListOplog(ctx)
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 1 || entries[0].OpType != "mpu_abort" || entries[0].VersionID != "u1" {
		t.Fatalf("unexpected oplog entries: %+v", entries)
	}
	var payload oplogMPUAbortPayload
	if err := json.Unmarshal([]byte(entries[0].Payload), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.UploadID != "u1" {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	remote, err := Open(filepath.Join(dir, "remote.db"))
	if err != nil {
		t.Fatalf("Open remote: %v", err)
	}
	t.Cleanup(func() { _ = remote.Close() })
	if err := remote.CreateMultipartUpload(ctx, "bucket", "tmp/key", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload remote: %v", err)
	}
	if err := remote.PutMultipartPart(ctx, "u1", 1, "part-v1", "etag", 100); err != nil {
		t.Fatalf("PutMultipartPart remote: %v", err)
	}
	if _, err := remote.ApplyOplogEntries(ctx, entries); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if _, err := remote.GetMultipartUpload(ctx, "u1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected upload removed, err=%v", err)
	}
	parts, err := remote.ListMultipartParts(ctx, "u1")
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected parts removed, got %+v", parts)
	}
	if _, err := remote.ApplyOplogEntries(ctx, entries); err != nil {
		t.Fatalf("ApplyOplogEntries idempotent: %v", err)
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
