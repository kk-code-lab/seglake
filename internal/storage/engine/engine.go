package engine

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/chunk"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

// PutResult captures metadata for a successful write.
type PutResult struct {
	VersionID   string
	ETag        string
	Size        int64
	CommittedAt time.Time
}

// Options configures the storage engine.
type Options struct {
	Layout          fs.Layout
	SegmentVersion  uint32
	Splitter        chunk.Splitter
	ManifestCodec   manifest.Codec
	MetaStore       *meta.Store
	SegmentMaxBytes int64
	SegmentMaxAge   time.Duration
	BarrierInterval time.Duration
	BarrierMaxBytes int64
}

// Engine owns the storage read/write path.
type Engine struct {
	layout         fs.Layout
	segmentVersion uint32
	splitter       chunk.Splitter
	manifestCodec  manifest.Codec
	metaStore      *meta.Store
	segments       *segmentManager
	barrier        *writeBarrier
}

// New creates a storage engine instance.
func New(opts Options) (*Engine, error) {
	if opts.Layout.Root == "" {
		return nil, errors.New("engine: layout root required")
	}
	if opts.SegmentVersion == 0 {
		opts.SegmentVersion = 1
	}
	if opts.Splitter == nil {
		opts.Splitter = chunk.NewFixedSplitter(chunk.DefaultSize)
	}
	if opts.ManifestCodec == nil {
		opts.ManifestCodec = &manifest.BinaryCodec{}
	}
	engine := &Engine{
		layout:         opts.Layout,
		segmentVersion: opts.SegmentVersion,
		splitter:       opts.Splitter,
		manifestCodec:  opts.ManifestCodec,
		metaStore:      opts.MetaStore,
		segments:       newSegmentManager(opts.Layout, opts.SegmentVersion, opts.MetaStore, opts.SegmentMaxBytes, opts.SegmentMaxAge),
	}
	engine.barrier = newWriteBarrier(engine, opts.BarrierInterval, opts.BarrierMaxBytes)
	if err := engine.ensureDirs(); err != nil {
		return nil, err
	}
	return engine, nil
}

// Put stores an object stream and returns manifest metadata.
func (e *Engine) Put(ctx context.Context, r io.Reader) (*manifest.Manifest, *PutResult, error) {
	return e.PutObject(ctx, "", "", r)
}

// PutObject stores an object stream and returns manifest metadata.
func (e *Engine) PutObject(ctx context.Context, bucket, key string, r io.Reader) (*manifest.Manifest, *PutResult, error) {
	if err := e.ensureDirs(); err != nil {
		return nil, nil, err
	}
	versionID := newID()

	man := &manifest.Manifest{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	}
	var size int64
	hasher := md5.New()
	splitErr := e.splitter.Split(io.TeeReader(r, hasher), func(ch chunk.Chunk) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		segmentID, offset, err := e.segments.appendChunk(ctx, ch.Hash, ch.Data)
		if err != nil {
			return err
		}
		e.barrier.addBytes(int64(len(ch.Data)))
		man.Chunks = append(man.Chunks, manifest.ChunkRef{
			Index:     ch.Index,
			Hash:      ch.Hash,
			SegmentID: segmentID,
			Offset:    offset,
			Len:       uint32(len(ch.Data)),
		})
		size += int64(len(ch.Data))
		return nil
	})
	if splitErr != nil {
		return nil, nil, splitErr
	}
	man.Size = size

	result := &PutResult{
		VersionID: versionID,
		ETag:      hex.EncodeToString(hasher.Sum(nil)),
		Size:      size,
	}
	manifestPath := e.layout.ManifestPath(formatManifestName(bucket, key, versionID))
	commit := func(tx *sql.Tx) error {
		if err := writeManifestFile(manifestPath, e.manifestCodec, man); err != nil {
			return err
		}
		if e.metaStore != nil && bucket != "" && key != "" {
			if err := e.metaStore.RecordPutTx(tx, bucket, key, versionID, result.ETag, size, manifestPath); err != nil {
				return err
			}
		}
		return nil
	}
	if err := e.barrier.register(commit); err != nil {
		return nil, nil, err
	}
	if err := e.segments.sync(); err != nil {
		return nil, nil, err
	}
	if err := e.barrier.wait(ctx); err != nil {
		return nil, nil, err
	}
	result.CommittedAt = time.Now().UTC()
	return man, result, nil
}

// Get retrieves an object stream by version id.
func (e *Engine) Get(ctx context.Context, versionID string) (io.ReadCloser, *manifest.Manifest, error) {
	if err := e.ensureDirs(); err != nil {
		return nil, nil, err
	}
	file, man, err := e.openManifestByVersion(versionID)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	reader := newManifestReader(e.layout, man)
	if ctx != nil {
		reader.ctx = ctx
	}
	return reader, man, nil
}

// GetRange retrieves a byte range for a version id.
func (e *Engine) GetRange(ctx context.Context, versionID string, start, length int64) (io.ReadCloser, *manifest.Manifest, error) {
	if err := e.ensureDirs(); err != nil {
		return nil, nil, err
	}
	if length <= 0 {
		return nil, nil, errors.New("engine: invalid range length")
	}
	file, man, err := e.openManifestByVersion(versionID)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	reader, err := newRangeReader(e.layout, man, start, length)
	if err != nil {
		return nil, nil, err
	}
	if ctx != nil {
		reader.ctx = ctx
	}
	return reader, man, nil
}

func (e *Engine) openManifestByVersion(versionID string) (*os.File, *manifest.Manifest, error) {
	paths := []string{
		e.layout.ManifestPath(versionID),
	}
	if entries, err := os.ReadDir(e.layout.ManifestsDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(name, "__"+versionID) {
				paths = append(paths, filepath.Join(e.layout.ManifestsDir, name))
			}
		}
	}
	var lastErr error
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			lastErr = err
			continue
		}
		man, err := e.manifestCodec.Decode(file)
		if err != nil {
			_ = file.Close()
			lastErr = err
			continue
		}
		return file, man, nil
	}
	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return nil, nil, lastErr
}

// GetObject resolves the current version id using metadata and returns the stream.
func (e *Engine) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *manifest.Manifest, error) {
	if e.metaStore == nil {
		return nil, nil, errors.New("engine: meta store not configured")
	}
	versionID, err := e.metaStore.CurrentVersion(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	return e.Get(ctx, versionID)
}

func (e *Engine) ensureDirs() error {
	if err := os.MkdirAll(e.layout.SegmentsDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(e.layout.ManifestsDir, 0o755); err != nil {
		return err
	}
	return nil
}

func (e *Engine) flushMeta(commits []func(tx *sql.Tx) error) error {
	if e.metaStore == nil {
		for _, commit := range commits {
			if err := commit(nil); err != nil {
				return err
			}
		}
		return nil
	}
	return e.metaStore.FlushWith(commits)
}

func writeManifestFile(path string, codec manifest.Codec, man *manifest.Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return codec.Encode(file, man)
}

func newID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("engine: rand failure: %v", err))
	}
	return hex.EncodeToString(buf[:])
}

func formatManifestName(bucket, key, versionID string) string {
	if bucket == "" || key == "" {
		return versionID
	}
	return bucket + "__" + key + "__" + versionID
}
