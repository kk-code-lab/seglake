package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestBucketEncryptionCRUD(t *testing.T) {
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
	if _, err := store.GetBucketEncryption(ctx, "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no encryption config, got %v", err)
	}
	if err := store.SetBucketEncryption(ctx, "missing", BucketEncryptionModeSSES3, BucketEncryptionAlgorithmAES256); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected missing bucket error, got %v", err)
	}
	if err := store.SetBucketEncryption(ctx, "demo", BucketEncryptionModeSSES3, BucketEncryptionAlgorithmAES256); err != nil {
		t.Fatalf("SetBucketEncryption: %v", err)
	}
	cfg, err := store.GetBucketEncryption(ctx, "demo")
	if err != nil {
		t.Fatalf("GetBucketEncryption: %v", err)
	}
	if cfg.Bucket != "demo" || cfg.Mode != BucketEncryptionModeSSES3 || cfg.Algorithm != BucketEncryptionAlgorithmAES256 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.KeyID != "" {
		t.Fatalf("expected SSE-S3 key id to be empty, got %q", cfg.KeyID)
	}
	if err := store.SetBucketEncryptionWithKey(ctx, "demo", BucketEncryptionModeSSEKMS, BucketEncryptionAlgorithmAWSKMS, "vault-key"); err != nil {
		t.Fatalf("SetBucketEncryptionWithKey: %v", err)
	}
	cfg, err = store.GetBucketEncryption(ctx, "demo")
	if err != nil {
		t.Fatalf("GetBucketEncryption KMS: %v", err)
	}
	if cfg.Mode != BucketEncryptionModeSSEKMS || cfg.Algorithm != BucketEncryptionAlgorithmAWSKMS || cfg.KeyID != "vault-key" {
		t.Fatalf("unexpected KMS config: %+v", cfg)
	}
	listed, err := store.ListBucketEncryption(ctx)
	if err != nil {
		t.Fatalf("ListBucketEncryption: %v", err)
	}
	if listed["demo"].Algorithm != BucketEncryptionAlgorithmAWSKMS || listed["demo"].KeyID != "vault-key" {
		t.Fatalf("expected listed config, got %+v", listed)
	}
	if err := store.DeleteBucketEncryption(ctx, "demo"); err != nil {
		t.Fatalf("DeleteBucketEncryption: %v", err)
	}
	if _, err := store.GetBucketEncryption(ctx, "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected config deleted, got %v", err)
	}
}

func TestApplyOplogBucketEncryption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	payload, err := json.Marshal(oplogBucketEncryptionPayload{
		Bucket:    "demo",
		Mode:      BucketEncryptionModeSSEKMS,
		Algorithm: BucketEncryptionAlgorithmAWSKMS,
		KeyID:     "vault-key",
		UpdatedAt: "2026-05-17T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	entries := []OplogEntry{
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000200-0000000001",
			OpType:  "bucket_encryption",
			Bucket:  "demo",
			Key:     "demo",
			Payload: string(payload),
		},
		{
			SiteID: "site-a",
			HLCTS:  "0000000000000000201-0000000001",
			OpType: "bucket_encryption_delete",
			Bucket: "demo",
			Key:    "demo",
		},
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[:1]); err != nil {
		t.Fatalf("ApplyOplogEntries set: %v", err)
	}
	cfg, err := store.GetBucketEncryption(context.Background(), "demo")
	if err != nil {
		t.Fatalf("GetBucketEncryption: %v", err)
	}
	if cfg.Mode != BucketEncryptionModeSSEKMS || cfg.Algorithm != BucketEncryptionAlgorithmAWSKMS || cfg.KeyID != "vault-key" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[1:]); err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if _, err := store.GetBucketEncryption(context.Background(), "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected config deleted, got %v", err)
	}
}
