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
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Mode        string    `json:"mode"`
	Manifests   int       `json:"manifests"`
	Segments    int       `json:"segments"`
	Errors      int       `json:"errors"`
	ErrorSample []string  `json:"error_sample,omitempty"`
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
			info, ok := segmentInfo[ch.SegmentID]
			if !ok {
				segPath := layout.SegmentPath(ch.SegmentID)
				info, err = os.Stat(segPath)
				if err != nil {
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
			dataEnd := info.Size()
			if dataEnd > segment.FooterLen() {
				dataEnd = info.Size() - segment.FooterLen()
			}
			if ch.Offset < segment.SegmentHeaderLen() || ch.Offset+int64(ch.Len) > dataEnd {
				addError(fmt.Errorf("chunk out of bounds segment=%s offset=%d len=%d", ch.SegmentID, ch.Offset, ch.Len))
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
