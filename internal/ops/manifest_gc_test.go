package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestManifestGCPlanAndRun(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}

	livePath := writeTestManifest(t, layout, "live")
	if err := store.RecordPut(context.Background(), "bucket", "live", "live", "etag", 1, livePath, ""); err != nil {
		t.Fatalf("RecordPut live: %v", err)
	}
	mpuPath := writeTestManifest(t, layout, "mpu-part")
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "mpu", "upload-1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := store.RecordManifest(context.Background(), "mpu-part", mpuPath); err != nil {
		t.Fatalf("RecordManifest mpu: %v", err)
	}
	if err := store.PutMultipartPart(context.Background(), "upload-1", 1, "mpu-part", "etag", 1); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	oldOrphan := writeTestManifest(t, layout, "old-orphan")
	youngOrphan := writeTestManifest(t, layout, "young-orphan")
	oldTime := time.Now().Add(-48 * time.Hour).UTC()
	setMTime(t, oldOrphan, oldTime)
	setMTime(t, livePath, oldTime)
	setMTime(t, mpuPath, oldTime)

	plan, report, err := ManifestGCPlanBuild(layout, metaPath, 24*time.Hour)
	if err != nil {
		t.Fatalf("ManifestGCPlanBuild: %v", err)
	}
	if report.Manifests != 4 || report.LiveManifests != 2 || report.Candidates != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].Path != oldOrphan {
		t.Fatalf("unexpected candidates: %+v", plan.Candidates)
	}
	if plan.CandidateBytes <= 0 || plan.CandidateBytes != report.CandidateBytes {
		t.Fatalf("candidate bytes mismatch: plan=%d report=%d", plan.CandidateBytes, report.CandidateBytes)
	}

	if _, err := ManifestGCRun(layout, metaPath, plan, false); err == nil {
		t.Fatalf("expected force failure")
	}
	runReport, err := ManifestGCRun(layout, metaPath, plan, true)
	if err != nil {
		t.Fatalf("ManifestGCRun: %v", err)
	}
	if runReport.Deleted != 1 || runReport.Reclaimed != plan.CandidateBytes || runReport.SkippedManifests != 0 {
		t.Fatalf("unexpected run report: %+v", runReport)
	}
	if _, err := os.Stat(oldOrphan); !os.IsNotExist(err) {
		t.Fatalf("expected old orphan removed, stat err=%v", err)
	}
	for _, path := range []string{livePath, mpuPath, youngOrphan} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s retained: %v", path, err)
		}
	}
}

func TestManifestGCRunSkipsStaleCandidates(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := os.MkdirAll(layout.ManifestsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll manifests: %v", err)
	}
	becameLive := writeTestManifest(t, layout, "became-live")
	changed := writeTestManifest(t, layout, "changed")
	missing := writeTestManifest(t, layout, "missing")
	oldTime := time.Now().Add(-48 * time.Hour).UTC()
	for _, path := range []string{becameLive, changed, missing} {
		setMTime(t, path, oldTime)
	}
	plan, _, err := ManifestGCPlanBuild(layout, metaPath, 24*time.Hour)
	if err != nil {
		t.Fatalf("ManifestGCPlanBuild: %v", err)
	}
	if len(plan.Candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %+v", plan.Candidates)
	}
	if err := store.RecordPut(context.Background(), "bucket", "became-live", "became-live", "etag", 1, becameLive, ""); err != nil {
		t.Fatalf("RecordPut became live: %v", err)
	}
	if err := os.WriteFile(changed, []byte("changed"), 0o644); err != nil {
		t.Fatalf("WriteFile changed: %v", err)
	}
	if err := os.Remove(missing); err != nil {
		t.Fatalf("Remove missing candidate: %v", err)
	}
	report, err := ManifestGCRun(layout, metaPath, plan, true)
	if err != nil {
		t.Fatalf("ManifestGCRun: %v", err)
	}
	if report.Deleted != 0 || report.SkippedManifests != 3 || report.Errors != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if _, err := os.Stat(becameLive); err != nil {
		t.Fatalf("became live should remain: %v", err)
	}
	if _, err := os.Stat(changed); err != nil {
		t.Fatalf("changed should remain: %v", err)
	}
}

func TestManifestGCPlanRoundTrip(t *testing.T) {
	dir := t.TempDir()
	plan := &ManifestGCPlan{
		SchemaVersion:  manifestGCPlanSchemaVersion,
		GeneratedAt:    time.Now().UTC(),
		TTL:            7 * 24 * time.Hour,
		CandidateBytes: 5,
		Candidates: []ManifestGCCandidate{{
			Path:              filepath.Join(dir, "manifest"),
			Size:              5,
			ModTime:           time.Now().UTC(),
			FingerprintSHA256: "abc123",
		}},
	}
	path := filepath.Join(dir, "plan.json")
	if err := WriteManifestGCPlan(path, plan); err != nil {
		t.Fatalf("WriteManifestGCPlan: %v", err)
	}
	got, err := ReadManifestGCPlan(path)
	if err != nil {
		t.Fatalf("ReadManifestGCPlan: %v", err)
	}
	if got.SchemaVersion != plan.SchemaVersion || got.TTL != plan.TTL || got.CandidateBytes != plan.CandidateBytes || len(got.Candidates) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if got.Candidates[0].Path != plan.Candidates[0].Path || got.Candidates[0].FingerprintSHA256 != plan.Candidates[0].FingerprintSHA256 {
		t.Fatalf("candidate mismatch: %+v", got.Candidates[0])
	}
}

func writeTestManifest(t *testing.T, layout fs.Layout, versionID string) string {
	t.Helper()
	path := layout.ManifestPath(versionID)
	man := &manifest.Manifest{Bucket: "bucket", Key: versionID, VersionID: versionID, Size: 1}
	if err := writeManifest(path, man); err != nil {
		t.Fatalf("writeManifest %s: %v", versionID, err)
	}
	return path
}

func setMTime(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("Chtimes %s: %v", path, err)
	}
}
