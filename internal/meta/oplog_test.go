package meta

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestOplogPutDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.SetSiteID("site-a")

	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 123, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if _, err := store.DeleteObject(context.Background(), "bucket", "key"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].OpType != "put" || entries[0].Bucket != "bucket" || entries[0].Key != "key" || entries[0].VersionID != "v1" {
		t.Fatalf("unexpected put entry: %+v", entries[0])
	}
	if entries[0].SiteID != "site-a" || entries[0].HLCTS == "" {
		t.Fatalf("expected site/hlc, got site=%q hlc=%q", entries[0].SiteID, entries[0].HLCTS)
	}
	if entries[1].OpType != "delete" || entries[1].Bucket != "bucket" || entries[1].Key != "key" || entries[1].VersionID != "v1" {
		t.Fatalf("unexpected delete entry: %+v", entries[1])
	}
	if entries[1].SiteID != "site-a" || entries[1].HLCTS == "" {
		t.Fatalf("expected site/hlc, got site=%q hlc=%q", entries[1].SiteID, entries[1].HLCTS)
	}
}

func TestOplogSinceLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.SetSiteID("site-a")

	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 123, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v2", "etag", 124, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	since := entries[0].HLCTS
	filtered, err := store.ListOplogSince(context.Background(), since, 10)
	if err != nil {
		t.Fatalf("ListOplogSince: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(filtered))
	}
	if filtered[0].VersionID != "v2" {
		t.Fatalf("expected v2, got %s", filtered[0].VersionID)
	}

	limited, err := store.ListOplogSince(context.Background(), "", 1)
	if err != nil {
		t.Fatalf("ListOplogSince limit: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(limited))
	}
}

func TestApplyOplogEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         123,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	put := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000001-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	applied, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{put})
	if err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected applied=1, got %d", applied)
	}
	meta, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if meta.VersionID != "v1" {
		t.Fatalf("expected current v1, got %s", meta.VersionID)
	}

	deletePayload, err := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	del := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000002-0000000001",
		OpType:    "delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(deletePayload),
	}
	applied, err = store.ApplyOplogEntries(context.Background(), []OplogEntry{del, del})
	if err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected applied=1 for delete, got %d", applied)
	}
	if _, err := store.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected object to be deleted")
	}
}

func TestMaxOplogHLC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hlc, err := store.MaxOplogHLC(context.Background())
	if err != nil {
		t.Fatalf("MaxOplogHLC: %v", err)
	}
	if hlc != "" {
		t.Fatalf("expected empty HLC, got %q", hlc)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	hlc, err = store.MaxOplogHLC(context.Background())
	if err != nil {
		t.Fatalf("MaxOplogHLC: %v", err)
	}
	if hlc == "" {
		t.Fatalf("expected HLC value")
	}
}
