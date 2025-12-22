package meta

import (
	"context"
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
