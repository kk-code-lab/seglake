package admin

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestOpsRunRequiresQuiesced(t *testing.T) {
	h := newTestHandler(t)
	reqBody := []byte(`{"mode":"status"}`)

	req := httptest.NewRequest(http.MethodPost, "/admin/ops/run", bytes.NewReader(reqBody))
	req.Header.Set(TokenHeader(), h.AuthToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func TestOpsRunUnsafeRequiresQuiesced(t *testing.T) {
	h := newTestHandler(t)
	reqBody := []byte(`{"mode":"gc-run","gc_force":true}`)

	req := httptest.NewRequest(http.MethodPost, "/admin/ops/run", bytes.NewReader(reqBody))
	req.Header.Set(TokenHeader(), h.AuthToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected maintenance check, got %d", w.Code)
	}

	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/admin/ops/run", bytes.NewReader(reqBody))
	req.Header.Set(TokenHeader(), h.AuthToken)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func TestOpsRunRequiresToken(t *testing.T) {
	h := newTestHandler(t)
	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	reqBody := []byte(`{"mode":"status"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/ops/run", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d", w.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/admin/ops/run", bytes.NewReader(reqBody))
	req.Header.Set(TokenHeader(), h.AuthToken)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return &Handler{
		DataDir:   dir,
		Meta:      store,
		Engine:    eng,
		AuthToken: "admin-token",
	}
}
