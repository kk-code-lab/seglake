package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

type segmentManager struct {
	layout         fs.Layout
	segmentVersion uint32
	metaStore      *meta.Store
	maxBytes       int64
	maxAge         time.Duration

	mu        sync.Mutex
	writer    *segment.Writer
	segmentID string
	createdAt time.Time
	size      int64
}

func newSegmentManager(layout fs.Layout, version uint32, metaStore *meta.Store, maxBytes int64, maxAge time.Duration) *segmentManager {
	if maxBytes <= 0 {
		maxBytes = 1 << 30
	}
	if maxAge <= 0 {
		maxAge = 10 * time.Minute
	}
	return &segmentManager{
		layout:         layout,
		segmentVersion: version,
		metaStore:      metaStore,
		maxBytes:       maxBytes,
		maxAge:         maxAge,
	}
}

func (m *segmentManager) appendChunk(ctx context.Context, hash [32]byte, data []byte) (string, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureSegment(ctx, int64(len(data))); err != nil {
		return "", 0, err
	}
	offset, err := m.writer.AppendRecord(segment.ChunkRecordHeader{
		Hash: hash,
		Len:  uint32(len(data)),
	}, data)
	if err != nil {
		return "", 0, err
	}
	m.size += int64(len(data)) + int64(segment.RecordHeaderLen())
	return m.segmentID, offset, nil
}

func (m *segmentManager) sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return nil
	}
	return m.writer.Sync()
}

func (m *segmentManager) ensureSegment(ctx context.Context, nextBytes int64) error {
	if m.writer == nil {
		return m.openNewSegment(ctx)
	}
	age := time.Since(m.createdAt)
	if m.size+nextBytes >= m.maxBytes || age >= m.maxAge {
		if err := m.sealCurrent(ctx); err != nil {
			return err
		}
		return m.openNewSegment(ctx)
	}
	return nil
}

func (m *segmentManager) openNewSegment(ctx context.Context) error {
	segmentID := "seg-" + newID()
	segmentPath := m.layout.SegmentPath(segmentID)
	writer, err := segment.NewWriter(segmentPath, m.segmentVersion)
	if err != nil {
		return err
	}
	m.writer = writer
	m.segmentID = segmentID
	m.createdAt = time.Now().UTC()
	info, _ := os.Stat(segmentPath)
	if info != nil {
		m.size = info.Size()
	} else {
		m.size = 0
	}
	if m.metaStore != nil {
		if err := m.metaStore.RecordSegment(ctx, segmentID, segmentPath, string(segment.StateOpen), m.size, nil); err != nil {
			return err
		}
	}
	return nil
}

func (m *segmentManager) sealCurrent(ctx context.Context) error {
	if m.writer == nil {
		return nil
	}
	footer := segment.FinalizeFooter(segment.NewFooter(m.segmentVersion))
	if err := m.writer.Seal(footer); err != nil {
		return err
	}
	if err := m.writer.Sync(); err != nil {
		return err
	}
	if err := m.writer.Close(); err != nil {
		return err
	}
	if m.metaStore != nil {
		path := m.layout.SegmentPath(m.segmentID)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := m.metaStore.RecordSegment(ctx, m.segmentID, path, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:]); err != nil {
			return err
		}
	}
	m.writer = nil
	m.segmentID = ""
	m.size = 0
	return nil
}

func (m *segmentManager) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return nil
	}
	return m.writer.Close()
}

func (m *segmentManager) pathForSegment(segmentID string) string {
	return filepath.Join(m.layout.SegmentsDir, segmentID)
}

var errNoSegment = errors.New("engine: no open segment")
