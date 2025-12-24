package engine

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestEnginePutGetRoundTrip(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(dir, "data")),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := bytes.Repeat([]byte("abcd"), 1024)
	manifest, result, err := engine.Put(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if manifest.Size != int64(len(input)) {
		t.Fatalf("manifest size mismatch: %d", manifest.Size)
	}
	if result.ETag == "" {
		t.Fatalf("expected ETag")
	}

	reader, gotManifest, err := engine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if gotManifest.VersionID != manifest.VersionID {
		t.Fatalf("manifest id mismatch")
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("data mismatch")
	}
}

func TestEnginePutObjectRecordsMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	engine, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "data")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, result, err := engine.PutObject(context.Background(), "bucket1", "key1", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got, err := store.CurrentVersion(context.Background(), "bucket1", "key1")
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if got != result.VersionID {
		t.Fatalf("version mismatch: %s", got)
	}

	reader, _, err := engine.GetObject(context.Background(), "bucket1", "key1")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("data mismatch")
	}
}

func TestEngineGetRange(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(dir, "data")),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := bytes.Repeat([]byte("abcd"), 8)
	_, result, err := engine.Put(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	reader, _, err := engine.GetRange(context.Background(), result.VersionID, 3, 7)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(input[3:10]) {
		t.Fatalf("range mismatch: %q", string(got))
	}
}

func TestEngineGetManifestFallbackToWalk(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	layout := fs.NewLayout(filepath.Join(dir, "data"))
	engine, err := New(Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
	}
	manifestPath := layout.ManifestPath(formatManifestName(man.Bucket, man.Key, man.VersionID))
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := writeManifestFile(manifestPath, &manifest.BinaryCodec{}, man); err != nil {
		t.Fatalf("writeManifestFile: %v", err)
	}
	stalePath := layout.ManifestPath("missing-" + man.VersionID)
	if err := store.RecordManifest(context.Background(), man.VersionID, stalePath); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	got, err := engine.GetManifest(context.Background(), man.VersionID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if got.Bucket != man.Bucket || got.Key != man.Key || got.VersionID != man.VersionID {
		t.Fatalf("manifest mismatch: %+v", got)
	}
}

func TestEngineGetManifestMissingReturnsNotExist(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	layout := fs.NewLayout(filepath.Join(dir, "data"))
	engine, err := New(Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	versionID := "missing-v1"
	stalePath := layout.ManifestPath(versionID)
	if err := store.RecordManifest(context.Background(), versionID, stalePath); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	_, err = engine.GetManifest(context.Background(), versionID)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got %v", err)
	}
}
