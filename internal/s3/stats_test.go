package s3

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
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
