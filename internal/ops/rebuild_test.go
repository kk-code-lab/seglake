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
