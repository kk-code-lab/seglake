package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestReplicationOplogEndpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/oplog?limit=1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp oplogResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(resp.Entries))
	}
	if resp.Entries[0].OpType != "put" {
		t.Fatalf("expected put, got %s", resp.Entries[0].OpType)
	}
}

func TestReplicationOplogApplyEndpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"etag":              "etag",
		"size":              10,
		"last_modified_utc": "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	reqBody, err := json.Marshal(oplogApplyRequest{
		Entries: []meta.OplogEntry{{
			SiteID:    "site-a",
			HLCTS:     "0000000000000000001-0000000001",
			OpType:    "put",
			Bucket:    "bucket",
			Key:       "key",
			VersionID: "v1",
			Payload:   string(payload),
		}},
	})
	if err != nil {
		t.Fatalf("body: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/replication/oplog", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	metaObj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != "v1" {
		t.Fatalf("expected current v1, got %s", metaObj.VersionID)
	}
}
