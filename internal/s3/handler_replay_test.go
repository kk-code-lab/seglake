package s3

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func newReplayTestHandler(t *testing.T, replayTTL time.Duration, replayBlock bool) *Handler {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertAPIKey(t.Context(), "ak", "sk", "", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "data")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}

	return &Handler{
		Engine:         eng,
		Meta:           store,
		Auth:           &AuthConfig{AccessKey: "ak", SecretKey: "sk", Region: "us-east-1"},
		Metrics:        NewMetrics(),
		ReplayCacheTTL: replayTTL,
		ReplayBlock:    replayBlock,
	}
}

func TestReplaySoftDoesNotBlock(t *testing.T) {
	handler := newReplayTestHandler(t, time.Minute, false)
	rawURL := "http://localhost:9000/bucket/key"
	signed, err := handler.Auth.Presign(http.MethodGet, rawURL, time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}

	req1 := httptest.NewRequest(http.MethodGet, signed, nil)
	req1.Host = "localhost:9000"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	req2 := httptest.NewRequest(http.MethodGet, signed, nil)
	req2.Host = "localhost:9000"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code == http.StatusForbidden {
		t.Fatalf("expected replay to be logged only, got %d", w2.Code)
	}
	_, _, _, _, replayDetected, _, _, _, _, _, _ := handler.Metrics.Snapshot()
	if replayDetected != 1 {
		t.Fatalf("expected replay detected 1, got %d", replayDetected)
	}
}

func TestReplayHardBlocks(t *testing.T) {
	handler := newReplayTestHandler(t, time.Minute, true)
	rawURL := "http://localhost:9000/bucket/key"
	signed, err := handler.Auth.Presign(http.MethodGet, rawURL, time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}

	req1 := httptest.NewRequest(http.MethodGet, signed, nil)
	req1.Host = "localhost:9000"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	req2 := httptest.NewRequest(http.MethodGet, signed, nil)
	req2.Host = "localhost:9000"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusForbidden {
		t.Fatalf("expected replay to be blocked, got %d", w2.Code)
	}
}
