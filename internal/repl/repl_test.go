package repl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/s3"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
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

func TestReplPullBucketDefaultEncryptionConverges(t *testing.T) {
	ctx := context.Background()
	key := replTestSSEKey("local:v1", 1)
	provider := replTestProvider(t, key.ID, key)
	source := newReplTestNode(t, "site-a", provider)
	target := newReplTestNode(t, "site-b", provider)
	sourceServer := source.startReplicationServer(t)

	if err := source.store.CreateBucket(ctx, "bucket"); err != nil {
		t.Fatalf("CreateBucket source: %v", err)
	}
	if err := source.store.SetBucketEncryption(ctx, "bucket", meta.BucketEncryptionModeSSES3, meta.BucketEncryptionAlgorithmAES256); err != nil {
		t.Fatalf("SetBucketEncryption source: %v", err)
	}

	pullFromNode(t, target, sourceServer, "")
	cfg, err := target.store.GetBucketEncryption(ctx, "bucket")
	if err != nil {
		t.Fatalf("GetBucketEncryption target: %v", err)
	}
	if cfg.Mode != meta.BucketEncryptionModeSSES3 || cfg.Algorithm != meta.BucketEncryptionAlgorithmAES256 {
		t.Fatalf("unexpected replicated config: %+v", cfg)
	}

	putResp := serveNodeRequest(t, target, http.MethodPut, "/bucket/defaulted.txt", "replicated default")
	if putResp.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", putResp.Code, putResp.Body.String())
	}
	if got := putResp.Header().Get("x-amz-server-side-encryption"); got != ssecrypto.ServerSideHeaderS3 {
		t.Fatalf("expected SSE-S3 response header, got %q", got)
	}
	obj, err := target.store.GetObjectMeta(ctx, "bucket", "defaulted.txt")
	if err != nil {
		t.Fatalf("GetObjectMeta target: %v", err)
	}
	if obj.EncryptionMode != ssecrypto.ModeSSES3 {
		t.Fatalf("expected encrypted target object, got %+v", obj)
	}
}

func TestReplPullRequireSSES3PolicySatisfiedByReplicatedDefault(t *testing.T) {
	ctx := context.Background()
	key := replTestSSEKey("local:v1", 1)
	provider := replTestProvider(t, key.ID, key)
	source := newReplTestNode(t, "site-a", provider)
	target := newReplTestNode(t, "site-b", provider)
	target.enableUnsignedPublicBucketPolicyAuth(t, "bucket")
	sourceServer := source.startReplicationServer(t)

	if err := source.store.CreateBucket(ctx, "bucket"); err != nil {
		t.Fatalf("CreateBucket source: %v", err)
	}
	requirePolicy := `{"version":"v1","statements":[{"effect":"allow","actions":["PutObject"],"resources":[{"bucket":"bucket"}],"conditions":{"require_sse_s3":true}}]}`
	if err := source.store.SetBucketPolicy(ctx, "bucket", requirePolicy); err != nil {
		t.Fatalf("SetBucketPolicy source: %v", err)
	}

	lastHLC := pullFromNode(t, target, sourceServer, "")
	denied := serveNodeRequest(t, target, http.MethodPut, "/bucket/plain.txt", "plaintext")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("expected replicated require_sse_s3 policy to deny plaintext, got status=%d body=%s", denied.Code, denied.Body.String())
	}

	if err := source.store.SetBucketEncryption(ctx, "bucket", meta.BucketEncryptionModeSSES3, meta.BucketEncryptionAlgorithmAES256); err != nil {
		t.Fatalf("SetBucketEncryption source: %v", err)
	}
	pullFromNode(t, target, sourceServer, lastHLC)
	allowed := serveNodeRequest(t, target, http.MethodPut, "/bucket/defaulted.txt", "encrypted by default")
	if allowed.Code != http.StatusOK {
		t.Fatalf("expected replicated default to satisfy policy, got status=%d body=%s", allowed.Code, allowed.Body.String())
	}
	if got := allowed.Header().Get("x-amz-server-side-encryption"); got != ssecrypto.ServerSideHeaderS3 {
		t.Fatalf("expected SSE-S3 response header, got %q", got)
	}
}

func TestReplPullEncryptedObjectAndRewrapPreservesCiphertext(t *testing.T) {
	ctx := context.Background()
	oldKey := replTestSSEKey("local:v1", 1)
	newKey := replTestSSEKey("local:v2", 2)
	oldProvider := replTestProvider(t, oldKey.ID, oldKey)
	bothProvider := replTestProvider(t, newKey.ID, oldKey, newKey)
	newProvider := replTestProvider(t, newKey.ID, newKey)

	source := newReplTestNode(t, "site-a", oldProvider)
	target := newReplTestNode(t, "site-b", oldProvider)
	payload := []byte(strings.Repeat("encrypted replication payload ", 16))
	man, result, err := source.eng.PutObjectSSES3(ctx, "bucket", "key", "", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("PutObjectSSES3 source: %v", err)
	}
	if len(man.Chunks) == 0 {
		t.Fatalf("expected encrypted chunks")
	}
	sourceRaw := readNodeChunk(t, source, man.Chunks[0])
	if bytes.Contains(sourceRaw, payload) {
		t.Fatalf("ciphertext chunk unexpectedly contains plaintext payload")
	}

	initialServer := source.startReplicationServer(t)
	lastHLC := pullFromNode(t, target, initialServer, "")
	if gotChunks := atomic.LoadInt32(&initialServer.chunkCalls); gotChunks == 0 {
		t.Fatalf("expected initial encrypted replication to fetch chunks")
	}
	targetRaw := readNodeChunk(t, target, man.Chunks[0])
	if !bytes.Equal(targetRaw, sourceRaw) {
		t.Fatalf("replicated chunk bytes differ from source ciphertext")
	}
	got := readVersionWithEngine(t, target.eng, result.VersionID)
	if !bytes.Equal(got, payload) {
		t.Fatalf("replicated payload mismatch: got %q", string(got))
	}

	plan, _, err := ops.BuildSSERewrapPlan(source.layout, source.metaPath, bothProvider, newKey.ID, nil)
	if err != nil {
		t.Fatalf("BuildSSERewrapPlan: %v", err)
	}
	if len(plan.Entries) != 1 {
		t.Fatalf("expected one rewrap entry, got %+v", plan)
	}
	if _, err := ops.RunSSERewrapPlan(source.layout, source.metaPath, bothProvider, plan); err != nil {
		t.Fatalf("RunSSERewrapPlan: %v", err)
	}

	rewrapServer := source.startReplicationServer(t)
	pullFromNode(t, target, rewrapServer, lastHLC)
	if gotManifests := atomic.LoadInt32(&rewrapServer.manifestCalls); gotManifests != 1 {
		t.Fatalf("expected one rewrapped manifest fetch, got %d", gotManifests)
	}
	if gotChunks := atomic.LoadInt32(&rewrapServer.chunkCalls); gotChunks != 0 {
		t.Fatalf("expected no chunk fetches for rewrap-only replication, got %d", gotChunks)
	}
	afterRewrapRaw := readNodeChunk(t, target, man.Chunks[0])
	if !bytes.Equal(afterRewrapRaw, sourceRaw) {
		t.Fatalf("rewrap replication changed ciphertext chunks")
	}
	newOnlyEngine := target.engineWithProvider(t, newProvider)
	got = readVersionWithEngine(t, newOnlyEngine, result.VersionID)
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch after replicated rewrap: got %q", string(got))
	}
	oldOnlyEngine := target.engineWithProvider(t, oldProvider)
	if _, err := readVersionWithEngineErr(oldOnlyEngine, result.VersionID); err == nil {
		t.Fatalf("expected old-only provider to fail after replicated rewrap")
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

type replTestNode struct {
	dir      string
	metaPath string
	layout   fs.Layout
	store    *meta.Store
	eng      *engine.Engine
	handler  *s3.Handler
}

type replTestServer struct {
	server        *httptest.Server
	client        *replClient
	manifestCalls int32
	chunkCalls    int32
}

func newReplTestNode(t *testing.T, siteID string, provider *ssecrypto.Provider) *replTestNode {
	t.Helper()
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	store.SetSiteID(siteID)
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
		SSE:       provider,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}
	node := &replTestNode{
		dir:      dir,
		metaPath: metaPath,
		layout:   layout,
		store:    store,
		eng:      eng,
	}
	node.handler = &s3.Handler{
		Engine: eng,
		Meta:   store,
		Auth: &s3.AuthConfig{
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
		},
		PublicBuckets: make(map[string]struct{}),
	}
	t.Cleanup(func() { _ = store.Close() })
	return node
}

func (n *replTestNode) startReplicationServer(t *testing.T) *replTestServer {
	t.Helper()
	rs := &replTestServer{}
	rs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/replication/manifest":
			atomic.AddInt32(&rs.manifestCalls, 1)
		case "/v1/replication/chunk":
			atomic.AddInt32(&rs.chunkCalls, 1)
		}
		n.handler.ServeHTTP(w, r)
	}))
	rs.client = &replClient{base: mustParseURL(t, rs.server.URL), client: rs.server.Client()}
	t.Cleanup(rs.server.Close)
	return rs
}

func (n *replTestNode) enableUnsignedPublicBucketPolicyAuth(t *testing.T, bucket string) {
	t.Helper()
	if err := n.store.UpsertAPIKey(context.Background(), "ak", "sk", "rw", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	n.handler.Auth.SecretLookup = n.store.LookupAPISecret
	n.handler.PublicBuckets[bucket] = struct{}{}
}

func (n *replTestNode) engineWithProvider(t *testing.T, provider *ssecrypto.Provider) *engine.Engine {
	t.Helper()
	eng, err := engine.New(engine.Options{
		Layout:    n.layout,
		MetaStore: n.store,
		SSE:       provider,
	})
	if err != nil {
		t.Fatalf("engine.New with provider: %v", err)
	}
	return eng
}

func pullFromNode(t *testing.T, target *replTestNode, source *replTestServer, since string) string {
	t.Helper()
	lastHLC, _, err := runReplPullOnce(context.Background(), source.client, since, 1000, true, target.store, target.eng, newReplMissingCache(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("runReplPullOnce: %v", err)
	}
	return lastHLC
}

func serveNodeRequest(t *testing.T, node *replTestNode, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	node.handler.ServeHTTP(rec, req)
	return rec
}

func readNodeChunk(t *testing.T, node *replTestNode, ref manifest.ChunkRef) []byte {
	t.Helper()
	data, err := node.eng.ReadSegmentRange(ref.SegmentID, ref.Offset, int64(ref.Len))
	if err != nil {
		t.Fatalf("ReadSegmentRange: %v", err)
	}
	return data
}

func readVersionWithEngine(t *testing.T, eng *engine.Engine, versionID string) []byte {
	t.Helper()
	data, err := readVersionWithEngineErr(eng, versionID)
	if err != nil {
		t.Fatalf("read version %s: %v", versionID, err)
	}
	return data
}

func readVersionWithEngineErr(eng *engine.Engine, versionID string) ([]byte, error) {
	reader, _, err := eng.Get(context.Background(), versionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

func replTestSSEKey(id string, seed byte) ssecrypto.Key {
	key := ssecrypto.Key{ID: id}
	for i := range key.Bytes {
		key.Bytes[i] = seed + byte(i)
	}
	return key
}

func replTestProvider(t *testing.T, active string, keys ...ssecrypto.Key) *ssecrypto.Provider {
	t.Helper()
	provider, err := ssecrypto.NewProvider(active, keys)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	return provider
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
