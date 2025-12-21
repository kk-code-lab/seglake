package engine

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

type manifestReader struct {
	layout   fs.Layout
	manifest *manifest.Manifest
	index    int
	buf      []byte
	bufOff   int
	segID    string
	segFile  *os.File
	ctx      context.Context
}

func newManifestReader(layout fs.Layout, man *manifest.Manifest) *manifestReader {
	return &manifestReader{
		layout:   layout,
		manifest: man,
		ctx:      context.Background(),
	}
}

func (r *manifestReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.checkContext(); err != nil {
		return 0, err
	}

	n := 0
	for n < len(p) {
		if r.buf == nil || r.bufOff >= len(r.buf) {
			if err := r.loadNextChunk(); err != nil {
				if errors.Is(err, io.EOF) && n > 0 {
					return n, nil
				}
				return n, err
			}
		}
		copied := copy(p[n:], r.buf[r.bufOff:])
		n += copied
		r.bufOff += copied
		if err := r.checkContext(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (r *manifestReader) Close() error {
	if r.segFile != nil {
		return r.segFile.Close()
	}
	return nil
}

func (r *manifestReader) loadNextChunk() error {
	if r.index >= len(r.manifest.Chunks) {
		return io.EOF
	}
	ref := r.manifest.Chunks[r.index]
	if ref.Len == 0 {
		return errors.New("engine: zero-length chunk")
	}
	if err := r.openSegment(ref.SegmentID); err != nil {
		return err
	}
	buf := make([]byte, ref.Len)
	n, err := r.segFile.ReadAt(buf, ref.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n != int(ref.Len) {
		return io.ErrUnexpectedEOF
	}
	r.buf = buf
	r.bufOff = 0
	r.index++
	return nil
}

func (r *manifestReader) openSegment(segmentID string) error {
	if r.segFile != nil && r.segID == segmentID {
		return nil
	}
	if r.segFile != nil {
		_ = r.segFile.Close()
		r.segFile = nil
	}
	path := r.layout.SegmentPath(segmentID)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	r.segID = segmentID
	r.segFile = file
	return nil
}

func (r *manifestReader) checkContext() error {
	if r.ctx == nil {
		return nil
	}
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		return nil
	}
}

type rangePiece struct {
	segmentID string
	offset    int64
	length    int64
}

type rangeReader struct {
	layout  fs.Layout
	pieces  []rangePiece
	index   int
	buf     []byte
	bufOff  int
	segID   string
	segFile *os.File
	ctx     context.Context
}

func newRangeReader(layout fs.Layout, man *manifest.Manifest, start, length int64) (*rangeReader, error) {
	if start < 0 || length <= 0 {
		return nil, errors.New("engine: invalid range")
	}
	if man.Size < start+length {
		return nil, errors.New("engine: range out of bounds")
	}
	pieces := make([]rangePiece, 0)
	var pos int64
	end := start + length
	for _, ch := range man.Chunks {
		chStart := pos
		chEnd := pos + int64(ch.Len)
		if chEnd <= start {
			pos = chEnd
			continue
		}
		if chStart >= end {
			break
		}
		readStart := start
		if chStart > readStart {
			readStart = chStart
		}
		readEnd := end
		if chEnd < readEnd {
			readEnd = chEnd
		}
		pieces = append(pieces, rangePiece{
			segmentID: ch.SegmentID,
			offset:    ch.Offset + (readStart - chStart),
			length:    readEnd - readStart,
		})
		pos = chEnd
	}
	return &rangeReader{
		layout: layout,
		pieces: pieces,
		ctx:    context.Background(),
	}, nil
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.checkContext(); err != nil {
		return 0, err
	}
	n := 0
	for n < len(p) {
		if r.buf == nil || r.bufOff >= len(r.buf) {
			if err := r.loadNextPiece(); err != nil {
				if errors.Is(err, io.EOF) && n > 0 {
					return n, nil
				}
				return n, err
			}
		}
		copied := copy(p[n:], r.buf[r.bufOff:])
		n += copied
		r.bufOff += copied
		if err := r.checkContext(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (r *rangeReader) Close() error {
	if r.segFile != nil {
		return r.segFile.Close()
	}
	return nil
}

func (r *rangeReader) loadNextPiece() error {
	if r.index >= len(r.pieces) {
		return io.EOF
	}
	piece := r.pieces[r.index]
	if err := r.openSegment(piece.segmentID); err != nil {
		return err
	}
	buf := make([]byte, piece.length)
	n, err := r.segFile.ReadAt(buf, piece.offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n != int(piece.length) {
		return io.ErrUnexpectedEOF
	}
	r.buf = buf
	r.bufOff = 0
	r.index++
	return nil
}

func (r *rangeReader) openSegment(segmentID string) error {
	if r.segFile != nil && r.segID == segmentID {
		return nil
	}
	if r.segFile != nil {
		_ = r.segFile.Close()
		r.segFile = nil
	}
	path := r.layout.SegmentPath(segmentID)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	r.segID = segmentID
	r.segFile = file
	return nil
}

func (r *rangeReader) checkContext() error {
	if r.ctx == nil {
		return nil
	}
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		return nil
	}
}
