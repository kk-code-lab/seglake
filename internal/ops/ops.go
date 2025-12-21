package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

// Report summarizes an ops run.
type Report struct {
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	Mode              string    `json:"mode"`
	Manifests         int       `json:"manifests"`
	Segments          int       `json:"segments"`
	Errors            int       `json:"errors"`
	ErrorSample       []string  `json:"error_sample,omitempty"`
	Candidates        int       `json:"candidates,omitempty"`
	Deleted           int       `json:"deleted,omitempty"`
	Reclaimed         int64     `json:"reclaimed_bytes,omitempty"`
	CandidateIDs      []string  `json:"candidate_ids,omitempty"`
	MissingSegments   int       `json:"missing_segments,omitempty"`
	InvalidManifests  int       `json:"invalid_manifests,omitempty"`
	OutOfBoundsChunks int       `json:"out_of_bounds_chunks,omitempty"`
	RebuiltObjects    int       `json:"rebuilt_objects,omitempty"`
	SkippedManifests  int       `json:"skipped_manifests,omitempty"`
}

// Status collects basic counts about storage state.
func Status(layout fs.Layout) (*Report, error) {
	report := &Report{Mode: "status", StartedAt: time.Now().UTC()}
	manifests, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	segments, err := listFiles(layout.SegmentsDir)
	if err != nil {
		return nil, err
	}
	report.Manifests = len(manifests)
	report.Segments = len(segments)
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

// Fsck validates manifests and segment boundaries.
func Fsck(layout fs.Layout) (*Report, error) {
	report := &Report{Mode: "fsck", StartedAt: time.Now().UTC()}
	manifests, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	report.Manifests = len(manifests)

	segmentInfo := make(map[string]os.FileInfo)
	segmentSeen := make(map[string]struct{})
	store, err := meta.Open(filepath.Join(layout.Root, "meta.db"))
	if err != nil {
		store = nil
	}
	if store != nil {
		defer store.Close()
	}
	addError := func(err error) {
		report.Errors++
		if len(report.ErrorSample) < 5 {
			report.ErrorSample = append(report.ErrorSample, err.Error())
		}
	}

	for _, path := range manifests {
		file, err := os.Open(path)
		if err != nil {
			addError(err)
			continue
		}
		man, err := (&manifest.BinaryCodec{}).Decode(file)
		_ = file.Close()
		if err != nil {
			report.InvalidManifests++
			addError(err)
			continue
		}
		for _, ch := range man.Chunks {
			info, ok := segmentInfo[ch.SegmentID]
			if !ok {
				segPath := layout.SegmentPath(ch.SegmentID)
				info, err = os.Stat(segPath)
				if err != nil {
					report.MissingSegments++
					addError(fmt.Errorf("missing segment %s", ch.SegmentID))
					continue
				}
				reader, err := segment.NewReader(segPath)
				if err != nil {
					addError(fmt.Errorf("segment header invalid %s", ch.SegmentID))
					continue
				}
				if _, err := reader.ReadFooter(); err != nil {
					// Treat missing/invalid footer as OPEN segment.
				}
				_ = reader.Close()
				segmentInfo[ch.SegmentID] = info
				report.Segments++
			}
			segmentSeen[ch.SegmentID] = struct{}{}
			dataEnd := info.Size()
			if dataEnd > segment.FooterLen() {
				dataEnd = info.Size() - segment.FooterLen()
			}
			if ch.Offset < segment.SegmentHeaderLen() || ch.Offset+int64(ch.Len) > dataEnd {
				report.OutOfBoundsChunks++
				addError(fmt.Errorf("chunk out of bounds segment=%s offset=%d len=%d", ch.SegmentID, ch.Offset, ch.Len))
			}
		}
	}

	if store != nil {
		segments, err := store.ListSegments(context.Background())
		if err == nil {
			for _, seg := range segments {
				if _, ok := segmentSeen[seg.ID]; !ok && seg.State == string(segment.StateSealed) {
					report.MissingSegments++
				}
			}
		}
	}

	report.FinishedAt = time.Now().UTC()
	return report, nil
}

// Scrub verifies chunk hashes against stored data.
func Scrub(layout fs.Layout, metaPath string) (*Report, error) {
	report := &Report{Mode: "scrub", StartedAt: time.Now().UTC()}
	manifests, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	report.Manifests = len(manifests)

	var store *meta.Store
	if metaPath != "" {
		store, err = meta.Open(metaPath)
		if err != nil {
			return nil, err
		}
		defer store.Close()
	}

	addError := func(err error) {
		report.Errors++
		if len(report.ErrorSample) < 5 {
			report.ErrorSample = append(report.ErrorSample, err.Error())
		}
	}

	for _, path := range manifests {
		file, err := os.Open(path)
		if err != nil {
			addError(err)
			continue
		}
		man, err := (&manifest.BinaryCodec{}).Decode(file)
		_ = file.Close()
		if err != nil {
			addError(err)
			continue
		}
		for _, ch := range man.Chunks {
			segPath := layout.SegmentPath(ch.SegmentID)
			f, err := os.Open(segPath)
			if err != nil {
				addError(err)
				continue
			}
			buf := make([]byte, ch.Len)
			n, err := f.ReadAt(buf, ch.Offset)
			_ = f.Close()
			if err != nil && err != io.EOF {
				addError(err)
				continue
			}
			if n != int(ch.Len) {
				addError(fmt.Errorf("short read segment=%s", ch.SegmentID))
				continue
			}
			if segmentHash := segment.HashChunk(buf); segmentHash != ch.Hash {
				addError(fmt.Errorf("hash mismatch segment=%s", ch.SegmentID))
				if store != nil {
					_ = store.MarkDamaged(context.Background(), man.VersionID)
				}
			}
		}
	}

	report.FinishedAt = time.Now().UTC()
	return report, nil
}

// Snapshot writes a minimal snapshot manifest and copies the SQLite files.
func Snapshot(layout fs.Layout, metaPath string, outDir string) (*Report, error) {
	if outDir == "" {
		return nil, errors.New("ops: snapshot output dir required")
	}
	report := &Report{Mode: "snapshot", StartedAt: time.Now().UTC()}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	if metaPath != "" {
		_ = copyFile(metaPath, filepath.Join(outDir, "meta.db"))
		_ = copyFile(metaPath+"-wal", filepath.Join(outDir, "meta.db-wal"))
		_ = copyFile(metaPath+"-shm", filepath.Join(outDir, "meta.db-shm"))
	}
	manifests, _ := listFiles(layout.ManifestsDir)
	segments, _ := listFiles(layout.SegmentsDir)
	report.Manifests = len(manifests)
	report.Segments = len(segments)
	report.FinishedAt = time.Now().UTC()
	snapshotPath := filepath.Join(outDir, "snapshot.json")
	if err := writeJSON(snapshotPath, report); err != nil {
		return nil, err
	}
	return report, nil
}

// Rebuild runs rebuild-index.
func Rebuild(layout fs.Layout, metaPath string) (*Report, error) {
	return RebuildIndex(layout, metaPath)
}

// GCPlan computes which sealed segments can be removed.
func GCPlan(layout fs.Layout, metaPath string, minAge time.Duration) (*Report, []meta.Segment, error) {
	report := &Report{Mode: "gc-plan", StartedAt: time.Now().UTC()}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer store.Close()

	livePaths, err := store.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, nil, err
	}
	report.Manifests = len(livePaths)

	liveBytes := make(map[string]int64)
	for _, path := range livePaths {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		man, err := (&manifest.BinaryCodec{}).Decode(file)
		_ = file.Close()
		if err != nil {
			continue
		}
		for _, ch := range man.Chunks {
			liveBytes[ch.SegmentID] += int64(ch.Len)
		}
	}

	segments, err := store.ListSegments(context.Background())
	if err != nil {
		return nil, nil, err
	}
	report.Segments = len(segments)

	var candidates []meta.Segment
	for _, seg := range segments {
		if seg.State != string(segment.StateSealed) {
			continue
		}
		if seg.SealedAt == "" {
			continue
		}
		sealedAt, err := time.Parse(time.RFC3339Nano, seg.SealedAt)
		if err != nil {
			continue
		}
		if time.Since(sealedAt) < minAge {
			continue
		}
		if liveBytes[seg.ID] == 0 {
			candidates = append(candidates, seg)
		}
	}
	report.Candidates = len(candidates)
	for _, seg := range candidates {
		report.CandidateIDs = append(report.CandidateIDs, seg.ID)
	}
	report.FinishedAt = time.Now().UTC()
	return report, candidates, nil
}

// GCRun deletes candidate segments after verifying the plan.
func GCRun(layout fs.Layout, metaPath string, minAge time.Duration, force bool) (*Report, error) {
	if !force {
		return nil, errors.New("gc: refuse to run without --force")
	}
	report, candidates, err := GCPlan(layout, metaPath, minAge)
	if err != nil {
		return nil, err
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	for _, seg := range candidates {
		if err := os.Remove(seg.Path); err != nil {
			report.Errors++
			continue
		}
		_ = store.DeleteSegment(context.Background(), seg.ID)
		report.Deleted++
		report.Reclaimed += seg.Size
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func listFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		out = append(out, filepath.Join(dir, entry.Name()))
	}
	return out, nil
}

func writeJSON(path string, v any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
