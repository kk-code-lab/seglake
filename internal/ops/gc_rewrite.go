package ops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

type GCRewritePlan struct {
	GeneratedAt   time.Time            `json:"generated_at"`
	MinAge        time.Duration        `json:"min_age"`
	LiveThreshold float64              `json:"live_threshold"`
	Candidates    []GCRewriteCandidate `json:"candidates"`
}

type GCRewriteCandidate struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	LiveBytes int64  `json:"live_bytes"`
}

// GCRewritePlanBuild computes a rewrite plan for partially-dead segments.
func GCRewritePlanBuild(layout fs.Layout, metaPath string, minAge time.Duration, liveThreshold float64) (*GCRewritePlan, *Report, error) {
	if liveThreshold <= 0 || liveThreshold > 1 {
		return nil, nil, errors.New("gc: live threshold must be (0,1]")
	}
	report := &Report{Mode: "gc-rewrite-plan", StartedAt: time.Now().UTC()}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = store.Close() }()

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

	var candidates []GCRewriteCandidate
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
		if seg.Size <= 0 || live == 0 {
			continue
		}
		ratio := float64(live) / float64(seg.Size)
		if ratio <= liveThreshold {
			candidates = append(candidates, GCRewriteCandidate{
				ID:        seg.ID,
				Path:      seg.Path,
				Size:      seg.Size,
				LiveBytes: live,
			})
		}
	}
	report.Candidates = len(candidates)
	for _, cand := range candidates {
		report.CandidateIDs = append(report.CandidateIDs, cand.ID)
	}
	report.FinishedAt = time.Now().UTC()

	plan := &GCRewritePlan{
		GeneratedAt:   time.Now().UTC(),
		MinAge:        minAge,
		LiveThreshold: liveThreshold,
		Candidates:    candidates,
	}
	return plan, report, nil
}

// WriteGCRewritePlan writes plan JSON to a file.
func WriteGCRewritePlan(path string, plan *GCRewritePlan) error {
	if plan == nil || path == "" {
		return errors.New("gc: plan and path required")
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ReadGCRewritePlan reads plan JSON from a file.
func ReadGCRewritePlan(path string) (*GCRewritePlan, error) {
	if path == "" {
		return nil, errors.New("gc: plan path required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan GCRewritePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

// GCRewrite compacts partially-dead segments by rewriting live chunks into new segments.
func GCRewrite(layout fs.Layout, metaPath string, minAge time.Duration, liveThreshold float64, force bool, throttleBps int64, pauseFile string) (*Report, error) {
	plan, _, err := GCRewritePlanBuild(layout, metaPath, minAge, liveThreshold)
	if err != nil {
		return nil, err
	}
	return GCRewriteFromPlan(layout, metaPath, plan, force, throttleBps, pauseFile)
}

// GCRewriteFromPlan executes a rewrite using the provided plan.
func GCRewriteFromPlan(layout fs.Layout, metaPath string, plan *GCRewritePlan, force bool, throttleBps int64, pauseFile string) (*Report, error) {
	if !force {
		return nil, errors.New("gc: refuse to run without --force")
	}
	if plan == nil {
		return nil, errors.New("gc: plan required")
	}
	report := &Report{Mode: "gc-rewrite", StartedAt: time.Now().UTC(), Candidates: len(plan.Candidates)}
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

	rewriteCandidates := make(map[string]meta.Segment)
	for _, cand := range plan.Candidates {
		seg, err := store.GetSegment(context.Background(), cand.ID)
		if err != nil {
			report.Errors++
			continue
		}
		if seg.State != string(segment.StateSealed) {
			continue
		}
		rewriteCandidates[cand.ID] = *seg
		report.CandidateIDs = append(report.CandidateIDs, cand.ID)
	}

	if len(rewriteCandidates) > 0 {
		if err := rewriteSegments(layout, store, livePaths, rewriteCandidates, report, throttleBps, pauseFile); err != nil {
			return report, err
		}
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
	layout    fs.Layout
	writer    *segment.Writer
	id        string
	size      int64
	closed    bool
	throttle  *gcThrottle
	pauseFile string
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
	if w.pauseFile != "" {
		for {
			if _, err := os.Stat(w.pauseFile); err != nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
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
	if w.throttle != nil {
		w.throttle.wait(int64(len(data)))
	}
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

func rewriteSegments(layout fs.Layout, store *meta.Store, livePaths []string, candidates map[string]meta.Segment, report *Report, throttleBps int64, pauseFile string) error {
	sourceFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range sourceFiles {
			_ = f.Close()
		}
	}()
	writer := &gcWriter{layout: layout, throttle: newGCThrottle(throttleBps), pauseFile: pauseFile}
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

type gcThrottle struct {
	bytesPerSec int64
	last        time.Time
	accum       int64
}

func newGCThrottle(bps int64) *gcThrottle {
	if bps <= 0 {
		return nil
	}
	return &gcThrottle{bytesPerSec: bps, last: time.Now()}
}

func (t *gcThrottle) wait(n int64) {
	if t == nil || n <= 0 {
		return
	}
	t.accum += n
	elapsed := time.Since(t.last)
	if elapsed <= 0 {
		return
	}
	allowed := int64(float64(t.bytesPerSec) * elapsed.Seconds())
	if t.accum <= allowed {
		return
	}
	sleepFor := time.Duration(float64(t.accum-allowed)/float64(t.bytesPerSec)) * time.Second
	if sleepFor > 0 {
		time.Sleep(sleepFor)
	}
	t.last = time.Now()
	t.accum = 0
}
