package repl

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func TestReplPullRetriesChunk(t *testing.T) {
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

	var chunkCalls int32
	manBytes := mustManifestBytes(t, "bucket", "key", "v1", "seg-test", []byte("data"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			resp := replOplogResponse{
				Entries: []meta.OplogEntry{{
					SiteID:    "site-a",
					HLCTS:     "0000000000000000002-0000000001",
					OpType:    "put",
					Bucket:    "bucket",
					Key:       "key",
					VersionID: "v1",
				}},
				LastHLC: "0000000000000000002-0000000001",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/manifest":
			_, _ = w.Write(manBytes)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/chunk":
			if atomic.AddInt32(&chunkCalls, 1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("fail"))
				return
			}
			_, _ = w.Write([]byte("data"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := &replClient{base: mustParseURL(t, server.URL), client: server.Client()}
	cache := newReplMissingCache()
	if _, _, err := runReplPullOnce(context.Background(), client, "", 100, true, store, eng, cache, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("runReplPullOnce: %v", err)
	}
	data, err := eng.ReadSegmentRange("seg-test", 0, 4)
	if err != nil {
		t.Fatalf("ReadSegmentRange: %v", err)
	}
	if string(data) != "data" {
		t.Fatalf("expected data, got %q", string(data))
	}
	if atomic.LoadInt32(&chunkCalls) < 2 {
		t.Fatalf("expected retry, calls=%d", atomic.LoadInt32(&chunkCalls))
	}
}

func TestReplMissingCache(t *testing.T) {
	t.Parallel()
	cache := newReplMissingCache()
	cache.addChunk(replMissingChunk{SegmentID: "seg", Offset: 1, Length: 2})
	if len(cache.snapshot()) != 1 {
		t.Fatalf("expected cache size 1")
	}
	cache.clear()
	if len(cache.snapshot()) != 0 {
		t.Fatalf("expected cache empty")
	}
}

func TestReplPullRetryDeadline(t *testing.T) {
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

	manBytes := mustManifestBytes(t, "bucket", "key", "v1", "seg-test", []byte("data"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			resp := replOplogResponse{
				Entries: []meta.OplogEntry{{
					SiteID:    "site-a",
					HLCTS:     "0000000000000000002-0000000001",
					OpType:    "put",
					Bucket:    "bucket",
					Key:       "key",
					VersionID: "v1",
				}},
				LastHLC: "0000000000000000002-0000000001",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/manifest":
			_, _ = w.Write(manBytes)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/chunk":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("fail"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := &replClient{base: mustParseURL(t, server.URL), client: server.Client()}
	cache := newReplMissingCache()
	_, _, err = runReplPullOnce(context.Background(), client, "", 100, true, store, eng, cache, time.Now())
	if err == nil {
		t.Fatalf("expected deadline error")
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return parsed
}

func mustManifestBytes(t *testing.T, bucket, key, versionID, segmentID string, data []byte) []byte {
	t.Helper()
	hash := segment.HashChunk(data)
	man := &manifest.Manifest{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Size:      int64(len(data)),
		Chunks: []manifest.ChunkRef{{
			Index:     0,
			Hash:      hash,
			SegmentID: segmentID,
			Offset:    0,
			Len:       uint32(len(data)),
		}},
	}
	buf := &bytes.Buffer{}
	if err := (&manifest.BinaryCodec{}).Encode(buf, man); err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return buf.Bytes()
}
