package engine

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	Layout         fs.Layout
	SegmentVersion uint32
	Splitter       chunk.Splitter
	ManifestCodec  manifest.Codec
	MetaStore      *meta.Store
}

// Engine owns the storage read/write path.
type Engine struct {
	layout         fs.Layout
	segmentVersion uint32
	splitter       chunk.Splitter
	manifestCodec  manifest.Codec
	metaStore      *meta.Store
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
	}
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
	segmentID := "seg-" + newID()
	segmentPath := e.layout.SegmentPath(segmentID)

	writer, err := segment.NewWriter(segmentPath, e.segmentVersion)
	if err != nil {
		return nil, nil, err
	}
	defer writer.Close()

	if e.metaStore != nil {
		if err := e.metaStore.RecordSegment(ctx, segmentID, segmentPath, string(segment.StateOpen), 0, nil); err != nil {
			return nil, nil, err
		}
	}

	man := &manifest.Manifest{
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
		offset, err := writer.AppendRecord(segment.ChunkRecordHeader{
			Hash: ch.Hash,
			Len:  uint32(len(ch.Data)),
		}, ch.Data)
		if err != nil {
			return err
		}
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

	footer := segment.NewFooter(e.segmentVersion)
	footer = segment.FinalizeFooter(footer)
	if err := writer.Seal(footer); err != nil {
		return nil, nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, nil, err
	}

	if e.metaStore != nil {
		info, err := os.Stat(segmentPath)
		if err != nil {
			return nil, nil, err
		}
		if err := e.metaStore.RecordSegment(ctx, segmentID, segmentPath, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:]); err != nil {
			return nil, nil, err
		}
	}

	manifestPath := e.layout.ManifestPath(versionID)
	if err := writeManifestFile(manifestPath, e.manifestCodec, man); err != nil {
		return nil, nil, err
	}

	result := &PutResult{
		VersionID:   versionID,
		ETag:        hex.EncodeToString(hasher.Sum(nil)),
		Size:        size,
		CommittedAt: time.Now().UTC(),
	}
	if e.metaStore != nil && bucket != "" && key != "" {
		if err := e.metaStore.RecordPut(ctx, bucket, key, versionID, result.ETag, size, manifestPath); err != nil {
			return nil, nil, err
		}
	}
	return man, result, nil
}

// Get retrieves an object stream by version id.
func (e *Engine) Get(ctx context.Context, versionID string) (io.ReadCloser, *manifest.Manifest, error) {
	if err := e.ensureDirs(); err != nil {
		return nil, nil, err
	}
	manifestPath := e.layout.ManifestPath(versionID)
	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	man, err := e.manifestCodec.Decode(file)
	if err != nil {
		return nil, nil, err
	}
	reader := newManifestReader(e.layout, man)
	if ctx != nil {
		reader.ctx = ctx
	}
	return reader, man, nil
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

func writeManifestFile(path string, codec manifest.Codec, man *manifest.Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return codec.Encode(file, man)
}

func newID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("engine: rand failure: %v", err))
	}
	return hex.EncodeToString(buf[:])
}
