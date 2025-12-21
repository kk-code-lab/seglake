package engine

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
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
	defer reader.Close()

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
	defer store.Close()

	engine, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "data")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, result, err := engine.PutObject(context.Background(), "bucket1", "key1", bytes.NewReader([]byte("hello")))
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
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("data mismatch")
	}
}
