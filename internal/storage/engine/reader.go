package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func setReaderContext(r io.ReadCloser, ctx context.Context) {
	switch v := r.(type) {
	case *manifestReader:
		v.ctx = ctx
	case *rangeReader:
		v.ctx = ctx
	case *encryptedManifestReader:
		v.ctx = ctx
		v.state.ctx = ctx
	case *encryptedRangeReader:
		v.ctx = ctx
		v.state.ctx = ctx
	}
}

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
		return fmt.Errorf("engine: zero-length chunk")
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

type encryptedState struct {
	layout   fs.Layout
	manifest *manifest.Manifest
	provider ssecrypto.KeyProvider
	keys     map[uint32][32]byte
	segID    string
	segFile  *os.File
	ctx      context.Context
}

func newEncryptedState(layout fs.Layout, man *manifest.Manifest, provider ssecrypto.KeyProvider) *encryptedState {
	return &encryptedState{layout: layout, manifest: man, provider: provider, keys: make(map[uint32][32]byte), ctx: context.Background()}
}

func (s *encryptedState) close() error {
	if s.segFile != nil {
		return s.segFile.Close()
	}
	return nil
}

func (s *encryptedState) openSegment(segmentID string) error {
	if s.segFile != nil && s.segID == segmentID {
		return nil
	}
	if s.segFile != nil {
		_ = s.segFile.Close()
		s.segFile = nil
	}
	file, err := os.Open(s.layout.SegmentPath(segmentID))
	if err != nil {
		return err
	}
	s.segID = segmentID
	s.segFile = file
	return nil
}

func (s *encryptedState) decryptChunk(ref manifest.ChunkRef) ([]byte, error) {
	if s.provider == nil {
		return nil, ssecrypto.ErrDisabled
	}
	keyEntry, err := manifestKey(s.manifest, ref.KeyRef)
	if err != nil {
		return nil, err
	}
	dek, ok := s.keys[ref.KeyRef]
	if !ok {
		result, err := s.provider.DecryptDataKey(s.ctx, ssecrypto.DecryptDataKeyRequest{KeyEntry: sseKeyEntryFromManifestWithWrap(s.manifest.Encryption.WrapAlgorithm, keyEntry)})
		if err != nil {
			return nil, err
		}
		dek = result.PlaintextDEK
		s.keys[ref.KeyRef] = dek
	}
	if err := s.openSegment(ref.SegmentID); err != nil {
		return nil, err
	}
	ciphertext := make([]byte, ref.Len)
	n, err := s.segFile.ReadAt(ciphertext, ref.Offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != int(ref.Len) {
		return nil, io.ErrUnexpectedEOF
	}
	aead, err := ssecrypto.NewGCM(dek)
	if err != nil {
		return nil, err
	}
	nonce, err := ssecrypto.ChunkNonce(keyEntry.NoncePrefix, uint32(ref.Index))
	if err != nil {
		return nil, err
	}
	plainLen := int(ref.PlainLength())
	return aead.Open(nil, nonce, ciphertext, ssecrypto.ChunkAAD(ref.Index, plainLen))
}

func manifestKey(man *manifest.Manifest, keyRef uint32) (manifest.KeyEntry, error) {
	if man == nil || man.Encryption == nil {
		return manifest.KeyEntry{}, errors.New("engine: encrypted manifest metadata missing")
	}
	for _, key := range man.Encryption.Keys {
		if key.KeyRef == keyRef {
			return key, nil
		}
	}
	return manifest.KeyEntry{}, fmt.Errorf("engine: encrypted key ref %d missing", keyRef)
}

type encryptedManifestReader struct {
	state  *encryptedState
	index  int
	buf    []byte
	bufOff int
	ctx    context.Context
}

func newEncryptedManifestReader(layout fs.Layout, man *manifest.Manifest, provider ssecrypto.KeyProvider) *encryptedManifestReader {
	return &encryptedManifestReader{state: newEncryptedState(layout, man, provider), ctx: context.Background()}
}

func (r *encryptedManifestReader) Read(p []byte) (int, error) {
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

func (r *encryptedManifestReader) Close() error {
	return r.state.close()
}

func (r *encryptedManifestReader) loadNextChunk() error {
	if r.index >= len(r.state.manifest.Chunks) {
		return io.EOF
	}
	ref := r.state.manifest.Chunks[r.index]
	if ref.Len == 0 {
		return fmt.Errorf("engine: zero-length chunk")
	}
	plain, err := r.state.decryptChunk(ref)
	if err != nil {
		return err
	}
	if len(plain) != int(ref.PlainLength()) {
		return fmt.Errorf("engine: decrypted chunk length mismatch")
	}
	r.buf = plain
	r.bufOff = 0
	r.index++
	return nil
}

func (r *encryptedManifestReader) checkContext() error {
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

type encryptedRangePiece struct {
	ref       manifest.ChunkRef
	sliceFrom int64
	sliceLen  int64
}

type encryptedRangeReader struct {
	state  *encryptedState
	pieces []encryptedRangePiece
	index  int
	buf    []byte
	bufOff int
	ctx    context.Context
}

func newEncryptedRangeReader(layout fs.Layout, man *manifest.Manifest, provider ssecrypto.KeyProvider, start, length int64) (*encryptedRangeReader, error) {
	if start < 0 || length <= 0 {
		return nil, errors.New("engine: invalid range")
	}
	if man.Size < start+length {
		return nil, errors.New("engine: range out of bounds")
	}
	pieces := make([]encryptedRangePiece, 0)
	var pos int64
	end := start + length
	for _, ch := range man.Chunks {
		chLen := int64(ch.PlainLength())
		chStart := pos
		chEnd := pos + chLen
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
		pieces = append(pieces, encryptedRangePiece{
			ref:       ch,
			sliceFrom: readStart - chStart,
			sliceLen:  readEnd - readStart,
		})
		pos = chEnd
	}
	return &encryptedRangeReader{state: newEncryptedState(layout, man, provider), pieces: pieces, ctx: context.Background()}, nil
}

func (r *encryptedRangeReader) Read(p []byte) (int, error) {
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

func (r *encryptedRangeReader) Close() error {
	return r.state.close()
}

func (r *encryptedRangeReader) loadNextPiece() error {
	if r.index >= len(r.pieces) {
		return io.EOF
	}
	piece := r.pieces[r.index]
	plain, err := r.state.decryptChunk(piece.ref)
	if err != nil {
		return err
	}
	from := int(piece.sliceFrom)
	to := from + int(piece.sliceLen)
	if from < 0 || to > len(plain) || from > to {
		return fmt.Errorf("engine: invalid encrypted range slice")
	}
	r.buf = plain[from:to]
	r.bufOff = 0
	r.index++
	return nil
}

func (r *encryptedRangeReader) checkContext() error {
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
