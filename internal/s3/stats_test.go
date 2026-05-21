package s3

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
)

func TestStatsIncludesReplayDetected(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	metrics := NewMetrics()
	metrics.IncReplayDetected()

	handler := &Handler{
		Meta:    store,
		Metrics: metrics,
	}

	rec := httptest.NewRecorder()
	handler.handleStats(context.Background(), rec, "req-1", "/v1/meta/stats")

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if resp.ReplayDetected != 1 {
		t.Fatalf("expected replay_detected=1, got %d", resp.ReplayDetected)
	}
}

func TestStatsIncludesRedactedSSEDiagnostics(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.RecordPut(ctx, "bucket", "plain", "v-plain", "etag", 1, "/tmp/plain", ""); err != nil {
		t.Fatalf("RecordPut plaintext: %v", err)
	}
	if err := store.RecordPut(ctx, "bucket", "encrypted", "v-encrypted", "etag", 1, "/tmp/encrypted", ""); err != nil {
		t.Fatalf("RecordPut encrypted: %v", err)
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return store.SetVersionEncryptionTx(tx, "v-encrypted", "SSE-KMS", "aws:kms", "vault:orders", "abcdef1234567890")
	}); err != nil {
		t.Fatalf("SetVersionEncryption: %v", err)
	}

	handler := &Handler{Meta: store, Metrics: NewMetrics()}
	rec := httptest.NewRecorder()
	handler.handleStats(ctx, rec, "req-1", "/v1/meta/stats")

	body := rec.Body.String()
	var resp statsResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if resp.Objects == 0 && resp.BytesLive == 0 {
		t.Fatalf("expected existing stats fields to remain populated: %+v", resp)
	}
	if resp.SSEDiagnostics.PlaintextActiveVersions != 1 || resp.SSEDiagnostics.EncryptedActiveVersions != 1 {
		t.Fatalf("unexpected sse diagnostics: %+v", resp.SSEDiagnostics)
	}
	if resp.SSEDiagnostics.ByMode["SSE-KMS"] != 1 || resp.SSEDiagnostics.ByKeyID["vault:orders"] != 1 {
		t.Fatalf("unexpected sse diagnostic maps: %+v", resp.SSEDiagnostics)
	}
	if strings.Contains(body, "abcdef1234567890") {
		t.Fatalf("stats leaked full fingerprint: %s", body)
	}
	if !strings.Contains(body, "abcdef12") {
		t.Fatalf("stats missing redacted fingerprint prefix: %s", body)
	}
}

func TestStatsIncludesConflictHotspots(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	for _, version := range []string{"v1", "v2"} {
		if err := store.RecordPut(ctx, "bucket", "key", version, "etag", 1, "/tmp/"+version, ""); err != nil {
			t.Fatalf("RecordPut %s: %v", version, err)
		}
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return meta.ExecTx(tx, "UPDATE versions SET state='CONFLICT' WHERE version_id IN (?, ?)", "v1", "v2")
	}); err != nil {
		t.Fatalf("mark conflicts: %v", err)
	}

	handler := &Handler{Meta: store, Metrics: NewMetrics()}
	rec := httptest.NewRecorder()
	handler.handleStats(ctx, rec, "req-1", "/v1/meta/stats")

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if len(resp.ConflictHotspots) != 1 {
		t.Fatalf("expected one conflict hotspot, got %+v", resp.ConflictHotspots)
	}
	hotspot := resp.ConflictHotspots[0]
	if hotspot.Bucket != "bucket" || hotspot.Key != "key" || hotspot.Conflicts != 2 {
		t.Fatalf("unexpected hotspot: %+v", hotspot)
	}
}
