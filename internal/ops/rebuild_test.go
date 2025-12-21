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
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestRebuildIndexReconstructsSegments(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	layout := fs.NewLayout(filepath.Join(dataDir, "objects"))
	metaPath := filepath.Join(dataDir, "meta.db")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	if _, _, err := eng.PutObject(context.Background(), "b", "k", strings.NewReader("data")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Rebuild into fresh meta.
	if _, err := RebuildIndex(layout, metaPath); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	store2, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()

	segments, err := store2.ListSegments(context.Background())
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segments) == 0 {
		t.Fatalf("expected segments")
	}
}

func TestRebuildIndexUsesManifestFilename(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	man := &manifest.Manifest{
		VersionID: "v1",
		Size:      4,
	}
	path := filepath.Join(layout.ManifestsDir, "bucket__key__v1")
	if err := writeManifest(path, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if _, err := RebuildIndex(layout, metaPath); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	got, err := store.CurrentVersion(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if got != "v1" {
		t.Fatalf("expected version v1, got %s", got)
	}
}

func TestRebuildIndexSkipsManifestWithoutKey(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	man := &manifest.Manifest{
		VersionID: "v1",
		Size:      4,
	}
	path := filepath.Join(layout.ManifestsDir, "v1")
	if err := writeManifest(path, man); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	report, err := RebuildIndex(layout, metaPath)
	if err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if report.SkippedManifests != 1 {
		t.Fatalf("expected skipped manifests, got %d", report.SkippedManifests)
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.CurrentVersion(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected no current version")
	}
}

func TestRebuildIndexSkipsCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	metaPath := filepath.Join(layout.Root, "meta.db")
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	path := filepath.Join(layout.ManifestsDir, "bucket__key__v1")
	if err := os.WriteFile(path, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := RebuildIndex(layout, metaPath); err == nil {
		t.Fatalf("expected rebuild error for corrupt manifest")
	}
}
