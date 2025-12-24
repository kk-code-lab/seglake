package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestStoreManifestBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	man, _, err := eng.PutObject(context.Background(), "bucket", "key", "", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	manifestBytes, err := eng.ManifestBytes(context.Background(), man.VersionID)
	if err != nil {
		t.Fatalf("ManifestBytes: %v", err)
	}
	manifestPath := eng.layout.ManifestPath(formatManifestName(man.Bucket, man.Key, man.VersionID))
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	if _, err := eng.StoreManifestBytes(context.Background(), manifestBytes); err != nil {
		t.Fatalf("StoreManifestBytes: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
}

func TestWriteSegmentRange(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	data := []byte("chunk-data")
	if err := eng.WriteSegmentRange(context.Background(), "seg-test", 10, data); err != nil {
		t.Fatalf("WriteSegmentRange: %v", err)
	}
	path := eng.layout.SegmentPath("seg-test")
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if got := string(buf[10 : 10+len(data)]); got != string(data) {
		t.Fatalf("expected %q got %q", string(data), got)
	}
	seg, err := store.GetSegment(context.Background(), "seg-test")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if seg.Size < int64(10+len(data)) {
		t.Fatalf("expected size >= %d got %d", 10+len(data), seg.Size)
	}
}

func TestWriteSegmentRangeMetaFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	eng, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("New: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := eng.WriteSegmentRange(context.Background(), "seg-test", 0, []byte("data")); err == nil {
		t.Fatalf("expected error from RecordSegment failure")
	}
}
