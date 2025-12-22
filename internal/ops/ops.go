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
	SchemaVersion     int       `json:"schema_version"`
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
	RewrittenSegments int       `json:"rewritten_segments,omitempty"`
	RewrittenBytes    int64     `json:"rewritten_bytes,omitempty"`
	NewSegments       int       `json:"new_segments,omitempty"`
	CandidateIDs      []string  `json:"candidate_ids,omitempty"`
	MissingSegments   int       `json:"missing_segments,omitempty"`
	InvalidManifests  int       `json:"invalid_manifests,omitempty"`
	OutOfBoundsChunks int       `json:"out_of_bounds_chunks,omitempty"`
	RebuiltObjects    int       `json:"rebuilt_objects,omitempty"`
	SkippedManifests  int       `json:"skipped_manifests,omitempty"`
	MissingSegmentIDs []string  `json:"missing_segment_ids,omitempty"`
}

const reportSchemaVersion = 1

func newReport(mode string) *Report {
	return &Report{
		SchemaVersion: reportSchemaVersion,
		Mode:          mode,
		StartedAt:     time.Now().UTC(),
	}
}

// Status collects basic counts about storage state.
func Status(layout fs.Layout) (*Report, error) {
	report := newReport("status")
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
	report := newReport("fsck")
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
		defer func() { _ = store.Close() }()
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
			segPath := layout.SegmentPath(ch.SegmentID)
			if !ok {
				info, err = os.Stat(segPath)
				if err != nil {
					report.MissingSegments++
					if len(report.MissingSegmentIDs) < 100 {
						report.MissingSegmentIDs = append(report.MissingSegmentIDs, ch.SegmentID)
					}
					addError(fmt.Errorf("missing segment %s", ch.SegmentID))
					continue
				}
				reader, err := segment.NewReader(segPath)
				if err != nil {
					addError(fmt.Errorf("segment header invalid %s", ch.SegmentID))
					continue
				}
				if _, err := reader.ReadFooter(); err != nil {
					addError(fmt.Errorf("segment footer invalid %s: %w", ch.SegmentID, err))
				}
				_ = reader.Close()
				segmentInfo[ch.SegmentID] = info
				report.Segments++
			}
			segmentSeen[ch.SegmentID] = struct{}{}
			dataEnd := info.Size()
			if reader, err := segment.NewReader(segPath); err == nil {
				if footer, err := reader.ReadFooter(); err == nil && footer.BloomOffset > 0 {
					dataEnd = footer.BloomOffset
				} else if dataEnd > segment.FooterLen() {
					dataEnd = info.Size() - segment.FooterLen()
				}
				_ = reader.Close()
			} else if dataEnd > segment.FooterLen() {
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
					if len(report.MissingSegmentIDs) < 100 {
						report.MissingSegmentIDs = append(report.MissingSegmentIDs, seg.ID)
					}
				}
			}
		}
	}

	report.FinishedAt = time.Now().UTC()
	if store != nil {
		_ = store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
	}
	return report, nil
}

// Scrub verifies chunk hashes against stored data.
func Scrub(layout fs.Layout, metaPath string) (*Report, error) {
	report := newReport("scrub")
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
		defer func() { _ = store.Close() }()
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
	if store != nil {
		_ = store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
	}
	return report, nil
}

// Snapshot writes a minimal snapshot manifest and copies the SQLite files.
func Snapshot(layout fs.Layout, metaPath string, outDir string) (*Report, error) {
	if outDir == "" {
		return nil, errors.New("ops: snapshot output dir required")
	}
	report := newReport("snapshot")
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

// SupportBundle gathers snapshot and reports into a directory.
func SupportBundle(layout fs.Layout, metaPath string, outDir string) (*Report, error) {
	if outDir == "" {
		return nil, errors.New("ops: support bundle output dir required")
	}
	report := newReport("support-bundle")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	snapDir := filepath.Join(outDir, "snapshot")
	_, _ = Snapshot(layout, metaPath, snapDir)
	fsck, _ := Fsck(layout)
	_ = writeJSON(filepath.Join(outDir, "fsck.json"), fsck)
	scrub, _ := Scrub(layout, metaPath)
	_ = writeJSON(filepath.Join(outDir, "scrub.json"), scrub)
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

// GCPlan computes which sealed segments can be removed.
func GCPlan(layout fs.Layout, metaPath string, minAge time.Duration) (*Report, []meta.Segment, error) {
	report := newReport("gc-plan")
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = store.Close() }()

	livePaths, err := store.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, nil, err
	}
	mpuPaths, err := store.ListMultipartPartManifestPaths(context.Background())
	if err != nil {
		return nil, nil, err
	}
	livePaths = mergeUniquePaths(livePaths, mpuPaths)
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
	defer func() { _ = store.Close() }()

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
	_ = store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
	return report, nil
}

func reportOpsFrom(report *Report) *meta.ReportOps {
	if report == nil || report.FinishedAt.IsZero() {
		return nil
	}
	return &meta.ReportOps{
		FinishedAt:        report.FinishedAt.UTC().Format(time.RFC3339Nano),
		Errors:            report.Errors,
		Deleted:           report.Deleted,
		ReclaimedBytes:    report.Reclaimed,
		RewrittenSegments: report.RewrittenSegments,
		RewrittenBytes:    report.RewrittenBytes,
		NewSegments:       report.NewSegments,
	}
}

// MPUGCPlan computes which multipart uploads can be removed by TTL.
func MPUGCPlan(metaPath string, ttl time.Duration) (*Report, []meta.MultipartUpload, error) {
	if ttl <= 0 {
		return nil, nil, errors.New("mpu-gc: ttl must be > 0")
	}
	report := newReport("mpu-gc-plan")
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = store.Close() }()

	cutoff := time.Now().UTC().Add(-ttl)
	uploads, err := store.ListMultipartUploadsBefore(context.Background(), cutoff)
	if err != nil {
		return nil, nil, err
	}
	report.Candidates = len(uploads)
	for _, up := range uploads {
		report.CandidateIDs = append(report.CandidateIDs, up.UploadID)
	}
	report.FinishedAt = time.Now().UTC()
	return report, uploads, nil
}

// MPUGCRun deletes multipart uploads older than TTL.
func MPUGCRun(metaPath string, ttl time.Duration, force bool) (*Report, error) {
	if !force {
		return nil, errors.New("mpu-gc: refuse to run without --force")
	}
	report, uploads, err := MPUGCPlan(metaPath, ttl)
	if err != nil {
		return nil, err
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	for _, up := range uploads {
		_, bytes, err := store.DeleteMultipartUpload(context.Background(), up.UploadID)
		if err != nil {
			report.Errors++
			continue
		}
		report.Deleted++
		report.Reclaimed += bytes
	}
	report.Mode = "mpu-gc-run"
	report.FinishedAt = time.Now().UTC()
	_ = store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
	return report, nil
}

func mergeUniquePaths(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, p := range a {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, p := range b {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
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

func writeJSON(path string, v any) (err error) {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := in.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	_, err = io.Copy(out, in)
	return err
}
