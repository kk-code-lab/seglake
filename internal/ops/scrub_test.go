package ops

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestScrubMarksDamagedVersion(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}

	man, result, err := eng.PutObject(context.Background(), "bucket", "key", strings.NewReader("hello world"))
	if err != nil {
		_ = store.Close()
		t.Fatalf("PutObject: %v", err)
	}
	if len(man.Chunks) == 0 {
		_ = store.Close()
		t.Fatalf("expected chunks")
	}

	segPath := layout.SegmentPath(man.Chunks[0].SegmentID)
	f, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
	if err != nil {
		_ = store.Close()
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, man.Chunks[0].Offset); err != nil {
		_ = f.Close()
		_ = store.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		_ = store.Close()
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := Scrub(layout, metaPath)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if report.Errors == 0 {
		t.Fatalf("expected scrub errors")
	}

	store2, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	metaObj, err := store2.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != result.VersionID {
		t.Fatalf("version mismatch: %s", metaObj.VersionID)
	}
	if metaObj.State != "DAMAGED" {
		t.Fatalf("expected DAMAGED, got %s", metaObj.State)
	}
}

func TestScrubReportsShortRead(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.Root, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}

	man, _, err := eng.PutObject(context.Background(), "bucket", "key", strings.NewReader("hello world"))
	if err != nil {
		_ = store.Close()
		t.Fatalf("PutObject: %v", err)
	}
	if len(man.Chunks) == 0 {
		_ = store.Close()
		t.Fatalf("expected chunks")
	}

	segPath := layout.SegmentPath(man.Chunks[0].SegmentID)
	fi, err := os.Stat(segPath)
	if err != nil {
		_ = store.Close()
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(segPath, fi.Size()-2); err != nil {
		_ = store.Close()
		t.Fatalf("Truncate: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := Scrub(layout, metaPath)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if report.Errors == 0 {
		t.Fatalf("expected scrub errors")
	}
}
