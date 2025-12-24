package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestReplValidateNoDiff(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	pathA := filepath.Join(layoutA.ManifestsDir, "bucket__key__v1")
	pathB := filepath.Join(layoutB.ManifestsDir, "bucket__key__v1")
	if err := writeManifest(pathA, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest A: %v", err)
	}
	if err := writeManifest(pathB, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest B: %v", err)
	}

	storeA, err := meta.Open(metaA)
	if err != nil {
		t.Fatalf("meta.Open A: %v", err)
	}
	if err := storeA.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathA, ""); err != nil {
		t.Fatalf("RecordPut A: %v", err)
	}
	_ = storeA.Close()

	storeB, err := meta.Open(metaB)
	if err != nil {
		t.Fatalf("meta.Open B: %v", err)
	}
	if err := storeB.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathB, ""); err != nil {
		t.Fatalf("RecordPut B: %v", err)
	}
	_ = storeB.Close()

	report, err := ReplValidate(layoutA, metaA, dataB)
	if err != nil {
		t.Fatalf("ReplValidate: %v", err)
	}
	if report.Errors != 0 {
		t.Fatalf("expected no errors, got %d", report.Errors)
	}
	if report.CompareManifestsExtra != 0 || report.CompareManifestsMissing != 0 {
		t.Fatalf("unexpected manifest diffs: %+v", report)
	}
	if report.CompareLiveExtra != 0 || report.CompareLiveMissing != 0 {
		t.Fatalf("unexpected live diffs: %+v", report)
	}
	if report.CompareVersionsExtra != 0 || report.CompareVersionsMissing != 0 {
		t.Fatalf("unexpected version diffs: %+v", report)
	}
}

func TestReplValidateDetectsDiff(t *testing.T) {
	dir := t.TempDir()
	dataA := filepath.Join(dir, "a")
	dataB := filepath.Join(dir, "b")
	layoutA := fs.NewLayout(filepath.Join(dataA, "objects"))
	layoutB := fs.NewLayout(filepath.Join(dataB, "objects"))
	metaA := filepath.Join(dataA, "meta.db")
	metaB := filepath.Join(dataB, "meta.db")

	if err := os.MkdirAll(layoutB.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	pathA := filepath.Join(layoutA.ManifestsDir, "bucket__key__v1")
	if err := writeManifest(pathA, &manifest.Manifest{VersionID: "v1", Size: 4}); err != nil {
		t.Fatalf("write manifest A: %v", err)
	}

	storeA, err := meta.Open(metaA)
	if err != nil {
		t.Fatalf("meta.Open A: %v", err)
	}
	if err := storeA.RecordPut(context.Background(), "bucket", "key", "v1", "", 4, pathA, ""); err != nil {
		t.Fatalf("RecordPut A: %v", err)
	}
	_ = storeA.Close()

	storeB, err := meta.Open(metaB)
	if err != nil {
		t.Fatalf("meta.Open B: %v", err)
	}
	_ = storeB.Close()

	report, err := ReplValidate(layoutA, metaA, dataB)
	if err != nil {
		t.Fatalf("ReplValidate: %v", err)
	}
	if report.CompareManifestsExtra != 1 {
		t.Fatalf("expected 1 extra manifest, got %d", report.CompareManifestsExtra)
	}
	if report.CompareLiveExtra != 1 {
		t.Fatalf("expected 1 extra live entry, got %d", report.CompareLiveExtra)
	}
	if report.CompareVersionsExtra != 1 {
		t.Fatalf("expected 1 extra version entry, got %d", report.CompareVersionsExtra)
	}
	if report.Errors == 0 {
		t.Fatalf("expected errors")
	}
}
