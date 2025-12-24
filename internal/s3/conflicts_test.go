package s3

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestConflictHeaderOnGet(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	_, result, err := eng.PutObject(ctx, "bucket", "key", "text/plain", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return meta.ExecTx(tx, "UPDATE versions SET state='CONFLICT' WHERE version_id=?", result.VersionID)
	}); err != nil {
		t.Fatalf("mark conflict: %v", err)
	}

	handler := &Handler{Engine: eng, Meta: store}
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status: %d", rec.Code)
	}
	if got := rec.Header().Get("x-seglake-conflict"); got != "true" {
		t.Fatalf("expected conflict header, got %q", got)
	}
}

func TestMetaConflictsEndpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	_, result, err := eng.PutObject(ctx, "bucket", "conflict-key", "text/plain", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return meta.ExecTx(tx, "UPDATE versions SET state='CONFLICT' WHERE version_id=?", result.VersionID)
	}); err != nil {
		t.Fatalf("mark conflict: %v", err)
	}

	handler := &Handler{Engine: eng, Meta: store}
	req := httptest.NewRequest(http.MethodGet, "/v1/meta/conflicts?bucket=bucket&prefix=conflict", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflicts status: %d", rec.Code)
	}
	var resp conflictsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(resp.Items))
	}
	if resp.Items[0].Bucket != "bucket" || resp.Items[0].Key != "conflict-key" {
		t.Fatalf("unexpected item: %+v", resp.Items[0])
	}
}
