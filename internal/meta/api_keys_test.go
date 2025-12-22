package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestAPIKeyLifecycleAndBuckets(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.UpsertAPIKey(ctx, "k1", "s1", "rw", true, 10); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	if err := store.AllowBucketForKey(ctx, "k1", "b1"); err != nil {
		t.Fatalf("AllowBucketForKey: %v", err)
	}
	if err := store.AllowBucketForKey(ctx, "k1", "b2"); err != nil {
		t.Fatalf("AllowBucketForKey: %v", err)
	}
	buckets, err := store.ListAllowedBuckets(ctx, "k1")
	if err != nil {
		t.Fatalf("ListAllowedBuckets: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}
	if err := store.DisallowBucketForKey(ctx, "k1", "b1"); err != nil {
		t.Fatalf("DisallowBucketForKey: %v", err)
	}
	buckets, err = store.ListAllowedBuckets(ctx, "k1")
	if err != nil {
		t.Fatalf("ListAllowedBuckets after disallow: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket after disallow, got %d", len(buckets))
	}
	if err := store.SetAPIKeyEnabled(ctx, "k1", false); err != nil {
		t.Fatalf("SetAPIKeyEnabled: %v", err)
	}
	key, err := store.GetAPIKey(ctx, "k1")
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if key.Enabled {
		t.Fatalf("expected key disabled")
	}
	if err := store.SetAPIKeyEnabled(ctx, "k1", true); err != nil {
		t.Fatalf("SetAPIKeyEnabled enable: %v", err)
	}
	key, err = store.GetAPIKey(ctx, "k1")
	if err != nil {
		t.Fatalf("GetAPIKey enable: %v", err)
	}
	if !key.Enabled {
		t.Fatalf("expected key enabled")
	}
	if err := store.DeleteAPIKey(ctx, "k1"); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	if _, err := store.GetAPIKey(ctx, "k1"); err == nil {
		t.Fatalf("expected key deleted")
	}
}
