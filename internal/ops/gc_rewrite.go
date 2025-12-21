package ops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

const gcRewriteMaxSegmentBytes int64 = 1 << 30

// GCRewrite compacts partially-dead segments by rewriting live chunks into new segments.
func GCRewrite(layout fs.Layout, metaPath string, minAge time.Duration, liveThreshold float64, force bool) (*Report, error) {
	if !force {
		return nil, errors.New("gc: refuse to run without --force")
	}
	if liveThreshold <= 0 || liveThreshold > 1 {
		return nil, errors.New("gc: live threshold must be (0,1]")
	}
	report := &Report{Mode: "gc-rewrite", StartedAt: time.Now().UTC()}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	livePaths, err := store.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, err
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
		return nil, err
	}
	report.Segments = len(segments)

	rewriteCandidates := make(map[string]meta.Segment)
	var fullyDead []meta.Segment
	for _, seg := range segments {
		if seg.State != string(segment.StateSealed) || seg.SealedAt == "" {
			continue
		}
		sealedAt, err := time.Parse(time.RFC3339Nano, seg.SealedAt)
		if err != nil {
			continue
		}
		if time.Since(sealedAt) < minAge {
			continue
		}
		live := liveBytes[seg.ID]
		if live == 0 {
			fullyDead = append(fullyDead, seg)
			continue
		}
		if seg.Size <= 0 {
			continue
		}
		ratio := float64(live) / float64(seg.Size)
		if ratio <= liveThreshold {
			rewriteCandidates[seg.ID] = seg
		}
	}
	report.Candidates = len(rewriteCandidates) + len(fullyDead)
	for id := range rewriteCandidates {
		report.CandidateIDs = append(report.CandidateIDs, id)
	}
	for _, seg := range fullyDead {
		report.CandidateIDs = append(report.CandidateIDs, seg.ID)
	}

	if len(rewriteCandidates) > 0 {
		if err := rewriteSegments(layout, store, livePaths, rewriteCandidates, report); err != nil {
			return report, err
		}
	}

	for _, seg := range fullyDead {
		if err := os.Remove(seg.Path); err != nil {
			report.Errors++
			continue
		}
		_ = store.DeleteSegment(context.Background(), seg.ID)
		report.Deleted++
		report.Reclaimed += seg.Size
	}

	for _, seg := range rewriteCandidates {
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

type gcWriter struct {
	layout fs.Layout
	writer *segment.Writer
	id     string
	size   int64
	closed bool
}

func (w *gcWriter) ensure() error {
	if w.writer != nil {
		return nil
	}
	id, err := newID()
	if err != nil {
		return err
	}
	path := w.layout.SegmentPath(id)
	writer, err := segment.NewWriter(path, 1)
	if err != nil {
		return err
	}
	w.writer = writer
	w.id = id
	w.size = 0
	return nil
}

func (w *gcWriter) append(hash [32]byte, data []byte) (string, int64, error) {
	if err := w.ensure(); err != nil {
		return "", 0, err
	}
	if w.size+int64(len(data))+segment.RecordHeaderLen() > gcRewriteMaxSegmentBytes {
		if err := w.seal(); err != nil {
			return "", 0, err
		}
		if err := w.ensure(); err != nil {
			return "", 0, err
		}
	}
	offset, err := w.writer.AppendRecord(segment.ChunkRecordHeader{Hash: hash, Len: uint32(len(data))}, data)
	if err != nil {
		return "", 0, err
	}
	w.size += int64(len(data)) + segment.RecordHeaderLen()
	return w.id, offset, nil
}

func (w *gcWriter) seal() error {
	if w.writer == nil || w.closed {
		return nil
	}
	footer := segment.FinalizeFooter(segment.NewFooter(1))
	if err := w.writer.Seal(footer); err != nil {
		return err
	}
	if err := w.writer.Sync(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func rewriteSegments(layout fs.Layout, store *meta.Store, livePaths []string, candidates map[string]meta.Segment, report *Report) error {
	sourceFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range sourceFiles {
			_ = f.Close()
		}
	}()
	writer := &gcWriter{layout: layout}
	newSegments := make(map[string]int64)

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
		changed := false
		for idx, ch := range man.Chunks {
			if _, ok := candidates[ch.SegmentID]; !ok {
				continue
			}
			sf := sourceFiles[ch.SegmentID]
			if sf == nil {
				segPath := layout.SegmentPath(ch.SegmentID)
				f, err := os.Open(segPath)
				if err != nil {
					return err
				}
				sourceFiles[ch.SegmentID] = f
				sf = f
			}
			data := make([]byte, ch.Len)
			if _, err := sf.ReadAt(data, ch.Offset); err != nil && err != io.EOF {
				return err
			}
			newSegID, newOffset, err := writer.append(ch.Hash, data)
			if err != nil {
				return err
			}
			man.Chunks[idx].SegmentID = newSegID
			man.Chunks[idx].Offset = newOffset
			newSegments[newSegID] = 0
			report.RewrittenBytes += int64(ch.Len)
			changed = true
		}
		if !changed {
			continue
		}
		if err := writeManifestAtomic(path, man); err != nil {
			return err
		}
		report.RewrittenSegments++
	}

	if err := writer.seal(); err != nil {
		return err
	}

	for id := range newSegments {
		path := layout.SegmentPath(id)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		reader, err := segment.NewReader(path)
		if err != nil {
			return err
		}
		footer, err := reader.ReadFooter()
		_ = reader.Close()
		if err != nil {
			return err
		}
		if err := store.RecordSegment(context.Background(), id, path, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:]); err != nil {
			return err
		}
		report.NewSegments++
	}
	return nil
}

func writeManifestAtomic(path string, man *manifest.Manifest) error {
	tmp := path + ".gc"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := (&manifest.BinaryCodec{}).Encode(file, man); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(path)
		return os.Rename(tmp, path)
	}
	return nil
}

func newID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
