package s3

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
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

func TestReplicationManifestEndpoint(t *testing.T) {
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

	_, putResult, err := eng.PutObject(context.Background(), "bucket", "key", strings.NewReader("hello world"))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/manifest?versionId="+putResult.VersionID, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var codec manifest.BinaryCodec
	got, err := codec.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.VersionID != putResult.VersionID || got.Bucket != "bucket" || got.Key != "key" {
		t.Fatalf("unexpected manifest: %+v", got)
	}
	if got.Size != int64(len("hello world")) {
		t.Fatalf("unexpected size: %d", got.Size)
	}
}

func TestReplicationChunkEndpoint(t *testing.T) {
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

	data := "hello world"
	man, _, err := eng.PutObject(context.Background(), "bucket", "key", strings.NewReader(data))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if len(man.Chunks) == 0 {
		t.Fatalf("expected chunks")
	}
	ch := man.Chunks[0]

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/chunk?segmentId="+ch.SegmentID+
		"&offset="+strconv.FormatInt(ch.Offset, 10)+
		"&len="+strconv.FormatInt(int64(ch.Len), 10), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != data {
		t.Fatalf("expected %q, got %q", data, rec.Body.String())
	}
}

func TestReplicationSnapshotEndpoint(t *testing.T) {
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

	if _, _, err := eng.PutObject(context.Background(), "bucket", "key", strings.NewReader("hello")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/snapshot", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/gzip") {
		t.Fatalf("expected gzip content type")
	}
	names, err := listTarGz(rec.Body.Bytes())
	if err != nil {
		t.Fatalf("list tar: %v", err)
	}
	found := false
	for _, name := range names {
		if name == "meta.db" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("meta.db not found in snapshot")
	}
}

func listTarGz(data []byte) ([]string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var out []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		out = append(out, hdr.Name)
	}
	return out, nil
}

func TestReplicationOplogLimitCap(t *testing.T) {
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

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/oplog?limit=999999", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestReplicationChunkLimit(t *testing.T) {
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

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/replication/chunk?segmentId=seg-x&offset=0&len=99999999", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
