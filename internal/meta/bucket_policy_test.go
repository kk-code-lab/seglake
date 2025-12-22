package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBucketPolicyCRUD(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}]}]}`
	if err := store.SetBucketPolicy(ctx, "demo", policy); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	got, err := store.GetBucketPolicy(ctx, "demo")
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if got != policy {
		t.Fatalf("unexpected policy: %q", got)
	}
	if err := store.DeleteBucketPolicy(ctx, "demo"); err != nil {
		t.Fatalf("DeleteBucketPolicy: %v", err)
	}
	if _, err := store.GetBucketPolicy(ctx, "demo"); err == nil {
		t.Fatalf("expected policy deleted")
	}
}
