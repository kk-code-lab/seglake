package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetLifecycleDiagnosticsRedactsRuleContent(t *testing.T) {
	t.Parallel()
	store, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	for _, bucket := range []string{"alpha", "beta"} {
		if err := store.CreateBucket(ctx, bucket); err != nil {
			t.Fatalf("CreateBucket %s: %v", bucket, err)
		}
	}
	configs := []BucketLifecycleConfig{
		{
			Bucket:            "beta",
			XML:               `<LifecycleConfiguration><Rule><ID>archive-secret-prefix</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`,
			NormalizedJSON:    `{"rules":[{"id":"expire-beta","status":"Enabled","prefix":"private/"}]}`,
			ConfigFingerprint: "fingerprint-beta",
			RuleIDs:           `["expire-beta"]`,
		},
		{
			Bucket:            "alpha",
			XML:               `<LifecycleConfiguration><Rule><ID>expire-alpha</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`,
			NormalizedJSON:    `{"rules":[{"id":"expire-alpha","status":"Enabled"},{"status":"Disabled","filter":{"tag":{"key":"secret-tag","value":"secret-value"}}}]}`,
			ConfigFingerprint: "fingerprint-alpha",
			RuleIDs:           `["expire-alpha"]`,
		},
	}
	for _, cfg := range configs {
		if err := store.SetBucketLifecycle(ctx, cfg); err != nil {
			t.Fatalf("SetBucketLifecycle %s: %v", cfg.Bucket, err)
		}
	}

	diag, err := store.GetLifecycleDiagnostics(ctx)
	if err != nil {
		t.Fatalf("GetLifecycleDiagnostics: %v", err)
	}
	if diag.ConfiguredBuckets != 2 || diag.TotalRules != 3 {
		t.Fatalf("unexpected lifecycle totals: %+v", diag)
	}
	if len(diag.Buckets) != 2 || diag.Buckets[0].Bucket != "alpha" || diag.Buckets[0].RuleCount != 2 {
		t.Fatalf("unexpected ordered bucket diagnostics: %+v", diag.Buckets)
	}
	if len(diag.Buckets[0].RuleIDs) != 1 || diag.Buckets[0].RuleIDs[0] != "expire-alpha" {
		t.Fatalf("unexpected rule IDs: %+v", diag.Buckets[0].RuleIDs)
	}
	body, err := json.Marshal(diag)
	if err != nil {
		t.Fatalf("Marshal diagnostics: %v", err)
	}
	for _, forbidden := range []string{"private/", "secret-tag", "secret-value", "fingerprint-alpha", "LifecycleConfiguration"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("diagnostics leaked %q: %s", forbidden, body)
		}
	}
}

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
