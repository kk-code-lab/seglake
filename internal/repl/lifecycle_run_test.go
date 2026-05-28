package repl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestReplPullAppliesMPUAbortWithoutDataFetch(t *testing.T) {
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
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "tmp/key", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := store.PutMultipartPart(context.Background(), "u1", 1, "part-v1", "etag", 100); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"upload_id":  "u1",
		"created_at": "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	server, manifestCalls, chunkCalls := newMetadataOnlyReplServer(t, meta.OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000002-0000000001",
		OpType:    "mpu_abort",
		Bucket:    "bucket",
		Key:       "tmp/key",
		VersionID: "u1",
		Payload:   string(payload),
	})

	client := &replClient{base: mustParseURL(t, server.URL), client: server.Client()}
	if _, _, err := runReplPullOnce(context.Background(), client, "", 100, true, store, eng, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("runReplPullOnce: %v", err)
	}
	if _, err := store.GetMultipartUpload(context.Background(), "u1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected upload removed, err=%v", err)
	}
	parts, err := store.ListMultipartParts(context.Background(), "u1")
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected parts removed, got %+v", parts)
	}
	if atomic.LoadInt32(manifestCalls) != 0 || atomic.LoadInt32(chunkCalls) != 0 {
		t.Fatalf("mpu abort replication fetched data: manifests=%d chunks=%d", atomic.LoadInt32(manifestCalls), atomic.LoadInt32(chunkCalls))
	}
}
