package engine

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/chunk"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
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

// Layout returns the engine storage layout.
func (e *Engine) Layout() fs.Layout {
	return e.layout
}

// MissingChunk describes a missing segment range for replication.
type MissingChunk struct {
	SegmentID string
	Offset    int64
	Length    int64
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
	if err := engine.recoverOpenSegments(context.Background()); err != nil {
		return nil, err
	}
	return engine, nil
}

// Put stores an object stream and returns manifest metadata.
func (e *Engine) Put(ctx context.Context, r io.Reader) (*manifest.Manifest, *PutResult, error) {
	return e.PutObject(ctx, "", "", "", r)
}

// PutObject stores an object stream and returns manifest metadata.
func (e *Engine) PutObject(ctx context.Context, bucket, key, contentType string, r io.Reader) (*manifest.Manifest, *PutResult, error) {
	return e.PutObjectWithCommit(ctx, bucket, key, contentType, r, nil)
}

// CommitMeta schedules a meta-only commit through the write barrier.
func (e *Engine) CommitMeta(ctx context.Context, commit func(tx *sql.Tx) error) error {
	if commit == nil {
		return nil
	}
	if e.metaStore == nil {
		return errors.New("engine: meta store not configured")
	}
	if err := e.barrier.register(commit); err != nil {
		return err
	}
	return e.barrier.wait(ctx)
}

// PutObjectWithCommit stores an object stream and runs an optional meta commit in the barrier transaction.
func (e *Engine) PutObjectWithCommit(ctx context.Context, bucket, key, contentType string, r io.Reader, extraCommit func(tx *sql.Tx, result *PutResult, manifestPath string) error) (*manifest.Manifest, *PutResult, error) {
	if err := e.ensureDirs(); err != nil {
		return nil, nil, err
	}
	versionID, err := newID()
	if err != nil {
		return nil, nil, err
	}

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
		if e.metaStore != nil {
			if bucket != "" && key != "" {
				if err := e.metaStore.RecordPutTx(tx, bucket, key, versionID, result.ETag, size, manifestPath, contentType); err != nil {
					return err
				}
			} else {
				if err := e.metaStore.RecordManifestTx(tx, versionID, manifestPath); err != nil {
					return err
				}
			}
		}
		if extraCommit != nil {
			if tx == nil {
				return errors.New("engine: extra commit requires transaction")
			}
			if err := extraCommit(tx, result, manifestPath); err != nil {
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
	file, man, err := e.openManifestByVersion(ctx, versionID)
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
	file, man, err := e.openManifestByVersion(ctx, versionID)
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

func (e *Engine) openManifestByVersion(ctx context.Context, versionID string) (*os.File, *manifest.Manifest, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	seen := make(map[string]struct{})
	paths := make([]string, 0, 4)
	addPath := func(path string) {
		if path == "" {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	tryFrom := func(start int) (*os.File, *manifest.Manifest, bool) {
		for i := start; i < len(paths); i++ {
			path := paths[i]
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
			return file, man, true
		}
		return nil, nil, false
	}

	foundMetaPath := false
	if e.metaStore != nil {
		path, err := e.metaStore.ManifestPath(ctx, versionID)
		if err == nil && path != "" {
			addPath(path)
			foundMetaPath = true
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			lastErr = err
		}
	}

	addPath(e.layout.ManifestPath(versionID))

	start := 0
	if file, man, ok := tryFrom(start); ok {
		return file, man, nil
	}
	start = len(paths)
	if !foundMetaPath || lastErr != nil {
		if err := filepath.WalkDir(e.layout.ManifestsDir, func(path string, d iofs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(d.Name(), "__"+versionID) {
				addPath(path)
			}
			return nil
		}); err != nil && lastErr == nil {
			lastErr = err
		}
		if file, man, ok := tryFrom(start); ok {
			return file, man, nil
		}
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

// GetManifest returns the decoded manifest for a version id.
func (e *Engine) GetManifest(ctx context.Context, versionID string) (*manifest.Manifest, error) {
	if versionID == "" {
		return nil, errors.New("engine: version id required")
	}
	file, man, err := e.openManifestByVersion(ctx, versionID)
	if file != nil {
		_ = file.Close()
	}
	if err != nil {
		return nil, err
	}
	return man, nil
}

// ManifestBytes returns the encoded manifest for a version id.
func (e *Engine) ManifestBytes(ctx context.Context, versionID string) ([]byte, error) {
	if versionID == "" {
		return nil, errors.New("engine: version id required")
	}
	man, err := e.GetManifest(ctx, versionID)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := e.manifestCodec.Encode(&buf, man); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadSegmentRange reads raw bytes from a segment file at offset for length.
func (e *Engine) ReadSegmentRange(segmentID string, offset, length int64) ([]byte, error) {
	if segmentID == "" {
		return nil, errors.New("engine: segment id required")
	}
	if offset < 0 || length <= 0 {
		return nil, errors.New("engine: invalid segment range")
	}
	path := e.layout.SegmentPath(segmentID)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	buf := make([]byte, length)
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(n) != length {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

// MissingChunks reports missing segments for the given manifest.
func (e *Engine) MissingChunks(man *manifest.Manifest) ([]MissingChunk, error) {
	if man == nil {
		return nil, errors.New("engine: manifest required")
	}
	missing := make([]MissingChunk, 0)
	for _, ch := range man.Chunks {
		path := e.layout.SegmentPath(ch.SegmentID)
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				missing = append(missing, MissingChunk{
					SegmentID: ch.SegmentID,
					Offset:    ch.Offset,
					Length:    int64(ch.Len),
				})
				continue
			}
			return nil, err
		}
		if info.Size() < ch.Offset+int64(ch.Len) {
			missing = append(missing, MissingChunk{
				SegmentID: ch.SegmentID,
				Offset:    ch.Offset,
				Length:    int64(ch.Len),
			})
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, ch.Len)
		n, err := file.ReadAt(buf, ch.Offset)
		_ = file.Close()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n != int(ch.Len) || segment.HashChunk(buf) != ch.Hash {
			missing = append(missing, MissingChunk{
				SegmentID: ch.SegmentID,
				Offset:    ch.Offset,
				Length:    int64(ch.Len),
			})
		}
	}
	return missing, nil
}

// StoreManifestBytes decodes and stores a manifest blob on disk and in metadata.
func (e *Engine) StoreManifestBytes(ctx context.Context, data []byte) (*manifest.Manifest, error) {
	if len(data) == 0 {
		return nil, errors.New("engine: manifest bytes required")
	}
	if err := e.ensureDirs(); err != nil {
		return nil, err
	}
	man, err := e.manifestCodec.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	manifestPath := e.layout.ManifestPath(formatManifestName(man.Bucket, man.Key, man.VersionID))
	if err := writeManifestFile(manifestPath, e.manifestCodec, man); err != nil {
		return nil, err
	}
	if e.metaStore != nil {
		if err := e.metaStore.RecordManifest(ctx, man.VersionID, manifestPath); err != nil {
			return nil, err
		}
	}
	return man, nil
}

// WriteSegmentRange writes raw bytes into a segment file at the given offset.
func (e *Engine) WriteSegmentRange(ctx context.Context, segmentID string, offset int64, data []byte) error {
	if segmentID == "" {
		return errors.New("engine: segment id required")
	}
	if offset < 0 || len(data) == 0 {
		return errors.New("engine: invalid segment range")
	}
	if err := e.ensureDirs(); err != nil {
		return err
	}
	path := e.layout.SegmentPath(segmentID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteAt(data, offset); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if e.metaStore != nil {
		if err := e.metaStore.RecordSegment(ctx, segmentID, path, "SEALED", info.Size(), nil); err != nil {
			return err
		}
	}
	return nil
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
	if err := codec.Encode(file, man); err != nil {
		return err
	}
	return file.Sync()
}

func newID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("engine: rand failure: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func formatManifestName(bucket, key, versionID string) string {
	if bucket == "" || key == "" {
		return versionID
	}
	b := base64.RawURLEncoding.EncodeToString([]byte(bucket))
	k := base64.RawURLEncoding.EncodeToString([]byte(key))
	return b + "__" + k + "__" + versionID
}
