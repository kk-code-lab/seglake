package meta

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestObjectTagsSetGetDelete(t *testing.T) {
	t.Parallel()
	store := newObjectTagTestStore(t)
	ctx := context.Background()
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	tags := []ObjectTag{{Key: "env", Value: "dev"}, {Key: "project", Value: "alpha"}}
	if err := store.SetObjectTags(ctx, "bucket", "key", "v1", tags); err != nil {
		t.Fatalf("SetObjectTags: %v", err)
	}
	got, err := store.GetObjectTags(ctx, "v1")
	if err != nil {
		t.Fatalf("GetObjectTags: %v", err)
	}
	if len(got) != 2 || got[0].Key != "env" || got[1].Key != "project" {
		t.Fatalf("unexpected tags: %+v", got)
	}
	count, err := store.CountObjectTags(ctx, "v1")
	if err != nil {
		t.Fatalf("CountObjectTags: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
	if err := store.DeleteObjectTags(ctx, "bucket", "key", "v1"); err != nil {
		t.Fatalf("DeleteObjectTags: %v", err)
	}
	got, err = store.GetObjectTags(ctx, "v1")
	if err != nil {
		t.Fatalf("GetObjectTags after delete: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no tags, got %+v", got)
	}
}

func TestObjectTagsAreVersionScoped(t *testing.T) {
	t.Parallel()
	store := newObjectTagTestStore(t)
	ctx := context.Background()
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag1", 1, "", ""); err != nil {
		t.Fatalf("RecordPut v1: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "key", "v2", "etag2", 1, "", ""); err != nil {
		t.Fatalf("RecordPut v2: %v", err)
	}
	if err := store.SetObjectTags(ctx, "bucket", "key", "v1", []ObjectTag{{Key: "version", Value: "one"}}); err != nil {
		t.Fatalf("SetObjectTags v1: %v", err)
	}
	if err := store.SetObjectTags(ctx, "bucket", "key", "v2", []ObjectTag{{Key: "version", Value: "two"}}); err != nil {
		t.Fatalf("SetObjectTags v2: %v", err)
	}
	v1Tags, err := store.GetObjectTags(ctx, "v1")
	if err != nil {
		t.Fatalf("GetObjectTags v1: %v", err)
	}
	v2Tags, err := store.GetObjectTags(ctx, "v2")
	if err != nil {
		t.Fatalf("GetObjectTags v2: %v", err)
	}
	if v1Tags[0].Value != "one" || v2Tags[0].Value != "two" {
		t.Fatalf("tags not version-scoped: v1=%+v v2=%+v", v1Tags, v2Tags)
	}
}

func TestObjectTagsOplogApplyIdempotent(t *testing.T) {
	t.Parallel()
	target := newObjectTagTestStore(t)
	if err := target.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	payload, err := json.Marshal(oplogObjectTagsPayload{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Tags:      []ObjectTag{{Key: "project", Value: "alpha"}},
		UpdatedAt: "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	entry := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000001-0000000001",
		OpType:    "object_tags_set",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(payload),
	}
	applied, err := target.ApplyOplogEntries(context.Background(), []OplogEntry{entry, entry})
	if err != nil {
		t.Fatalf("ApplyOplogEntries set: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected one applied entry, got %d", applied)
	}
	tags, err := target.GetObjectTags(context.Background(), "v1")
	if err != nil {
		t.Fatalf("GetObjectTags: %v", err)
	}
	if len(tags) != 1 || tags[0].Key != "project" || tags[0].Value != "alpha" {
		t.Fatalf("unexpected tags: %+v", tags)
	}
	del := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000002-0000000001",
		OpType:    "object_tags_delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
	}
	applied, err = target.ApplyOplogEntries(context.Background(), []OplogEntry{del, del})
	if err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected one delete applied entry, got %d", applied)
	}
	tags, err = target.GetObjectTags(context.Background(), "v1")
	if err != nil {
		t.Fatalf("GetObjectTags after delete: %v", err)
	}
	if len(tags) != 0 {
		t.Fatalf("expected tags cleared, got %+v", tags)
	}
}

func TestObjectTagDiagnosticsCountRowsOnly(t *testing.T) {
	t.Parallel()
	store := newObjectTagTestStore(t)
	ctx := context.Background()
	if err := store.RecordPut(ctx, "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.SetObjectTags(ctx, "bucket", "key", "v1", []ObjectTag{{Key: "project", Value: "alpha"}, {Key: "env", Value: "dev"}}); err != nil {
		t.Fatalf("SetObjectTags: %v", err)
	}
	versions, rows, err := store.CountObjectTagRows(ctx)
	if err != nil {
		t.Fatalf("CountObjectTagRows: %v", err)
	}
	if versions != 1 || rows != 2 {
		t.Fatalf("unexpected counts versions=%d rows=%d", versions, rows)
	}
}

func newObjectTagTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
