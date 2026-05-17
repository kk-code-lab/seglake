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

func TestReplPullFetchesRewrappedManifest(t *testing.T) {
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
	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 0, filepath.Join(dir, "old-manifest"), ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	var manifestCalls int32
	manBytes := mustEmptyManifestBytes(t, "bucket", "key", "v1")
	payload, err := json.Marshal(map[string]string{
		"last_modified_utc":            "2026-01-01T00:00:00Z",
		"encryption_mode":              "SSE-S3",
		"encryption_algorithm":         "AES-256-GCM",
		"encryption_key_ids":           "local:v2",
		"encryption_edek_fingerprints": "beef",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			resp := replOplogResponse{
				Entries: []meta.OplogEntry{{
					SiteID:    "site-a",
					HLCTS:     "0000000000000000002-0000000001",
					OpType:    "sse_rewrap",
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
			_, _ = w.Write(manBytes)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := &replClient{base: mustParseURL(t, server.URL), client: server.Client()}
	if _, _, err := runReplPullOnce(context.Background(), client, "", 100, true, store, eng, nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("runReplPullOnce: %v", err)
	}
	if got := atomic.LoadInt32(&manifestCalls); got != 1 {
		t.Fatalf("expected one manifest fetch, got %d", got)
	}
	if _, err := eng.GetManifest(context.Background(), "v1"); err != nil {
		t.Fatalf("GetManifest after rewrap pull: %v", err)
	}
	obj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if obj.EncryptionKeyIDs != "local:v2" {
		t.Fatalf("encryption summary not updated: %+v", obj)
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

func mustEmptyManifestBytes(t *testing.T, bucket, key, versionID string) []byte {
	t.Helper()
	man := &manifest.Manifest{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	}
	buf := &bytes.Buffer{}
	if err := (&manifest.BinaryCodec{}).Encode(buf, man); err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return buf.Bytes()
}
