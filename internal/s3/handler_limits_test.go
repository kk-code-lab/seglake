package s3

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestPrepareRequestMaxURLLength(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	h := &Handler{
		Engine:       eng,
		Meta:         store,
		MaxURLLength: 10,
	}

	t.Run("rejects_over_limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/"+strings.Repeat("a", 11), nil)
		rec := httptest.NewRecorder()
		_, ok := h.prepareRequest(rec, req)
		if ok {
			t.Fatalf("expected request to be rejected")
		}
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d got %d", http.StatusBadRequest, rec.Code)
		}
	})

	t.Run("accepts_within_limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/"+strings.Repeat("a", 5), nil)
		rec := httptest.NewRecorder()
		_, ok := h.prepareRequest(rec, req)
		if !ok {
			t.Fatalf("expected request to be accepted")
		}
	})
}
