package repl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestReplPullAppliesObjectTagsWithoutDataFetch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"bucket":     "bucket",
		"key":        "key",
		"version_id": "v1",
		"tags": []map[string]string{
			{"key": "project", "value": "alpha"},
		},
		"updated_at": "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var manifestCalls int32
	var chunkCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			resp := replOplogResponse{
				Entries: []meta.OplogEntry{{
					SiteID:    "site-a",
					HLCTS:     "0000000000000000002-0000000001",
					OpType:    "object_tags_set",
					Bucket:    "bucket",
					Key:       "key",
					VersionID: "v1",
					Payload:   string(payload),
				}},
				LastHLC: "0000000000000000002-0000000001",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/manifest":
			atomic.AddInt32(&manifestCalls, 1)
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/chunk":
			atomic.AddInt32(&chunkCalls, 1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := &replClient{base: mustParseURL(t, server.URL), client: server.Client()}
	if _, _, err := runReplPullOnce(context.Background(), client, "", 100, true, store, eng, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("runReplPullOnce: %v", err)
	}
	tags, err := store.GetObjectTags(context.Background(), "v1")
	if err != nil {
		t.Fatalf("GetObjectTags: %v", err)
	}
	if len(tags) != 1 || tags[0].Key != "project" || tags[0].Value != "alpha" {
		t.Fatalf("unexpected replicated tags: %+v", tags)
	}
	if atomic.LoadInt32(&manifestCalls) != 0 || atomic.LoadInt32(&chunkCalls) != 0 {
		t.Fatalf("object tag replication fetched data: manifests=%d chunks=%d", manifestCalls, chunkCalls)
	}
}
