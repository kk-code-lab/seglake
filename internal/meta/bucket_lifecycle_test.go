package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestBucketLifecycleCRUD(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := store.GetBucketLifecycle(ctx, "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no lifecycle config, got %v", err)
	}
	if err := store.SetBucketLifecycle(ctx, BucketLifecycleConfig{Bucket: "missing", XML: "<LifecycleConfiguration/>", NormalizedJSON: "{}", ConfigFingerprint: "fp"}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected missing bucket error, got %v", err)
	}
	cfg := BucketLifecycleConfig{
		Bucket:            "demo",
		XML:               "<LifecycleConfiguration><Rule><ID>expire</ID></Rule></LifecycleConfiguration>",
		NormalizedJSON:    `{"rules":[{"id":"expire"}]}`,
		ConfigFingerprint: "abc123",
		RuleIDs:           `["expire"]`,
	}
	if err := store.SetBucketLifecycle(ctx, cfg); err != nil {
		t.Fatalf("SetBucketLifecycle: %v", err)
	}
	got, err := store.GetBucketLifecycle(ctx, "demo")
	if err != nil {
		t.Fatalf("GetBucketLifecycle: %v", err)
	}
	if got.Bucket != "demo" || got.XML != cfg.XML || got.NormalizedJSON != cfg.NormalizedJSON || got.ConfigFingerprint != "abc123" || got.RuleIDs != `["expire"]` {
		t.Fatalf("unexpected config: %+v", got)
	}
	listed, err := store.ListBucketLifecycle(ctx)
	if err != nil {
		t.Fatalf("ListBucketLifecycle: %v", err)
	}
	if listed["demo"].ConfigFingerprint != "abc123" {
		t.Fatalf("expected listed lifecycle config, got %+v", listed)
	}
	if err := store.DeleteBucketLifecycle(ctx, "demo"); err != nil {
		t.Fatalf("DeleteBucketLifecycle: %v", err)
	}
	if _, err := store.GetBucketLifecycle(ctx, "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected config deleted, got %v", err)
	}
}

func TestApplyOplogBucketLifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	payload, err := json.Marshal(oplogBucketLifecyclePayload{
		Bucket:            "demo",
		XML:               "<LifecycleConfiguration><Rule><ID>expire</ID></Rule></LifecycleConfiguration>",
		NormalizedJSON:    `{"rules":[{"id":"expire"}]}`,
		ConfigFingerprint: "abc123",
		RuleIDs:           `["expire"]`,
		UpdatedAt:         "2026-05-17T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	entries := []OplogEntry{
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000200-0000000001",
			OpType:  "bucket_lifecycle",
			Bucket:  "demo",
			Key:     "demo",
			Payload: string(payload),
		},
		{
			SiteID: "site-a",
			HLCTS:  "0000000000000000201-0000000001",
			OpType: "bucket_lifecycle_delete",
			Bucket: "demo",
			Key:    "demo",
		},
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[:1]); err != nil {
		t.Fatalf("ApplyOplogEntries set: %v", err)
	}
	cfg, err := store.GetBucketLifecycle(context.Background(), "demo")
	if err != nil {
		t.Fatalf("GetBucketLifecycle: %v", err)
	}
	if cfg.ConfigFingerprint != "abc123" || cfg.RuleIDs != `["expire"]` {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[1:]); err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if _, err := store.GetBucketLifecycle(context.Background(), "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected config deleted, got %v", err)
	}
}
