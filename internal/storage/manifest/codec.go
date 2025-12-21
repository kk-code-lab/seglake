package manifest

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/zeebo/blake3"
)

const (
	magic       = 0x53474c4d // "SGLM"
	versionV1   = 1
	headerLen   = 4 + 4
	checksumLen = 32
)

// Codec serializes and deserializes manifests.
type Codec interface {
	Encode(w io.Writer, m *Manifest) error
	Decode(r io.Reader) (*Manifest, error)
}

// BinaryCodec implements a compact binary manifest format.
type BinaryCodec struct{}

// Encode writes a manifest with a header and checksum.
func (c *BinaryCodec) Encode(w io.Writer, m *Manifest) error {
	if m == nil {
		return errors.New("manifest: nil manifest")
	}
	buf := make([]byte, 0, 256)
	buf = appendU32(buf, magic)
	buf = appendU32(buf, versionV1)
	buf = appendString(buf, m.VersionID)
	buf = appendU64(buf, uint64(m.Size))
	buf = appendU32(buf, uint32(len(m.Chunks)))
	for _, ch := range m.Chunks {
		buf = appendU32(buf, uint32(ch.Index))
		buf = append(buf, ch.Hash[:]...)
		buf = appendString(buf, ch.SegmentID)
		buf = appendU64(buf, uint64(ch.Offset))
		buf = appendU32(buf, ch.Len)
	}
	checksum := blake3.Sum256(buf[headerLen:])
	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := w.Write(checksum[:])
	return err
}

// Decode reads a manifest, validates header and checksum, and returns the manifest.
func (c *BinaryCodec) Decode(r io.Reader) (*Manifest, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) < headerLen+checksumLen {
		return nil, errors.New("manifest: truncated")
	}
	body := data[:len(data)-checksumLen]
	checksum := data[len(data)-checksumLen:]
	sum := blake3.Sum256(body[headerLen:])
	if !equalBytes(sum[:], checksum) {
		return nil, errors.New("manifest: checksum mismatch")
	}
	if binary.LittleEndian.Uint32(body[0:4]) != magic {
		return nil, errors.New("manifest: bad magic")
	}
	if binary.LittleEndian.Uint32(body[4:8]) != versionV1 {
		return nil, errors.New("manifest: unsupported version")
	}
	offset := headerLen
	versionID, n, err := readString(body[offset:])
	if err != nil {
		return nil, err
	}
	offset += n
	if offset+8+4 > len(body) {
		return nil, errors.New("manifest: truncated body")
	}
	size := int64(binary.LittleEndian.Uint64(body[offset:]))
	offset += 8
	chunkCount := int(binary.LittleEndian.Uint32(body[offset:]))
	offset += 4
	chunks := make([]ChunkRef, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		if offset+4+32 > len(body) {
			return nil, errors.New("manifest: truncated chunk")
		}
		index := int(binary.LittleEndian.Uint32(body[offset:]))
		offset += 4
		var hash [32]byte
		copy(hash[:], body[offset:offset+32])
		offset += 32
		segmentID, n, err := readString(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
		if offset+8+4 > len(body) {
			return nil, errors.New("manifest: truncated chunk")
		}
		off := int64(binary.LittleEndian.Uint64(body[offset:]))
		offset += 8
		length := binary.LittleEndian.Uint32(body[offset:])
		offset += 4
		chunks = append(chunks, ChunkRef{
			Index:     index,
			Hash:      hash,
			SegmentID: segmentID,
			Offset:    off,
			Len:       length,
		})
	}
	if offset != len(body) {
		return nil, errors.New("manifest: trailing bytes")
	}
	return &Manifest{
		VersionID: versionID,
		Size:      size,
		Chunks:    chunks,
	}, nil
}

func appendU32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendU64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendString(buf []byte, v string) []byte {
	if len(v) > int(^uint32(0)) {
		panic("manifest: string too large")
	}
	buf = appendU32(buf, uint32(len(v)))
	return append(buf, v...)
}

func readString(data []byte) (string, int, error) {
	if len(data) < 4 {
		return "", 0, errors.New("manifest: truncated string length")
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	if len(data) < 4+n {
		return "", 0, errors.New("manifest: truncated string")
	}
	return string(data[4 : 4+n]), 4 + n, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
