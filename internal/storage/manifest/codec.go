package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

const (
	magic       = 0x53474c4d // "SGLM"
	versionV1   = 1
	versionV2   = 2
	versionV3   = 3
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
		return fmt.Errorf("manifest: nil manifest")
	}
	buf := make([]byte, 0, 256)
	buf = appendU32(buf, magic)
	version := versionV1
	if m.Encrypted() {
		version = versionV3
	} else if m.Bucket != "" || m.Key != "" {
		version = versionV2
	}
	buf = appendU32(buf, uint32(version))
	if version == versionV2 || version == versionV3 {
		var err error
		buf, err = appendString(buf, m.Bucket)
		if err != nil {
			return err
		}
		buf, err = appendString(buf, m.Key)
		if err != nil {
			return err
		}
	}
	var err error
	buf, err = appendString(buf, m.VersionID)
	if err != nil {
		return err
	}
	buf = appendU64(buf, uint64(m.Size))
	buf = appendU32(buf, uint32(len(m.Chunks)))
	for _, ch := range m.Chunks {
		buf = appendU32(buf, uint32(ch.Index))
		buf = append(buf, ch.Hash[:]...)
		buf, err = appendString(buf, ch.SegmentID)
		if err != nil {
			return err
		}
		buf = appendU64(buf, uint64(ch.Offset))
		buf = appendU32(buf, ch.Len)
		if version == versionV3 {
			buf = appendU32(buf, ch.PlainLen)
			buf = appendU32(buf, ch.KeyRef)
		}
	}
	if version == versionV3 {
		if m.Encryption == nil {
			return fmt.Errorf("manifest: v3 requires encryption metadata")
		}
		buf, err = appendEncryption(buf, m.Encryption)
		if err != nil {
			return err
		}
	}
	checksum := blake3.Sum256(buf[headerLen:])
	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err = w.Write(checksum[:])
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
	version := binary.LittleEndian.Uint32(body[4:8])
	if version != versionV1 && version != versionV2 && version != versionV3 {
		return nil, errors.New("manifest: unsupported version")
	}
	offset := headerLen
	bucket := ""
	key := ""
	if version == versionV2 || version == versionV3 {
		var n int
		var err error
		bucket, n, err = readString(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
		key, n, err = readString(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
	}
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
		plainLen := uint32(0)
		keyRef := uint32(0)
		if version == versionV3 {
			if offset+8 > len(body) {
				return nil, errors.New("manifest: truncated encrypted chunk")
			}
			plainLen = binary.LittleEndian.Uint32(body[offset:])
			offset += 4
			keyRef = binary.LittleEndian.Uint32(body[offset:])
			offset += 4
		}
		chunks = append(chunks, ChunkRef{
			Index:     index,
			Hash:      hash,
			SegmentID: segmentID,
			Offset:    off,
			Len:       length,
			PlainLen:  plainLen,
			KeyRef:    keyRef,
		})
	}
	var enc *Encryption
	if version == versionV3 {
		var n int
		enc, n, err = readEncryption(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
	}
	if offset != len(body) {
		return nil, errors.New("manifest: trailing bytes")
	}
	return &Manifest{
		Bucket:     bucket,
		Key:        key,
		VersionID:  versionID,
		Size:       size,
		Chunks:     chunks,
		Encryption: enc,
	}, nil
}

func appendEncryption(buf []byte, enc *Encryption) ([]byte, error) {
	var err error
	buf, err = appendString(buf, enc.Mode)
	if err != nil {
		return nil, err
	}
	buf, err = appendString(buf, enc.Algorithm)
	if err != nil {
		return nil, err
	}
	buf, err = appendString(buf, enc.WrapAlgorithm)
	if err != nil {
		return nil, err
	}
	buf, err = appendString(buf, enc.AADScheme)
	if err != nil {
		return nil, err
	}
	buf = appendU32(buf, uint32(len(enc.Keys)))
	for _, key := range enc.Keys {
		buf = appendU32(buf, key.KeyRef)
		buf, err = appendString(buf, key.KeyID)
		if err != nil {
			return nil, err
		}
		buf, err = appendBytes(buf, key.EncryptedDEK)
		if err != nil {
			return nil, err
		}
		buf, err = appendBytes(buf, key.WrapNonce)
		if err != nil {
			return nil, err
		}
		buf, err = appendBytes(buf, key.NoncePrefix)
		if err != nil {
			return nil, err
		}
		buf, err = appendString(buf, key.NonceScheme)
		if err != nil {
			return nil, err
		}
		buf, err = appendBytes(buf, key.EDEKFingerprint)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func readEncryption(data []byte) (*Encryption, int, error) {
	offset := 0
	mode, n, err := readString(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += n
	algorithm, n, err := readString(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += n
	wrapAlgorithm, n, err := readString(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += n
	aadScheme, n, err := readString(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += n
	if offset+4 > len(data) {
		return nil, 0, errors.New("manifest: truncated encryption keys")
	}
	keyCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	keys := make([]KeyEntry, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		if offset+4 > len(data) {
			return nil, 0, errors.New("manifest: truncated encryption key")
		}
		keyRef := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		keyID, n, err := readString(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		edek, n, err := readBytes(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		wrapNonce, n, err := readBytes(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		noncePrefix, n, err := readBytes(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		nonceScheme, n, err := readString(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		fp, n, err := readBytes(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += n
		keys = append(keys, KeyEntry{
			KeyRef:          keyRef,
			KeyID:           keyID,
			EncryptedDEK:    edek,
			WrapNonce:       wrapNonce,
			NoncePrefix:     noncePrefix,
			NonceScheme:     nonceScheme,
			EDEKFingerprint: fp,
		})
	}
	return &Encryption{Mode: mode, Algorithm: algorithm, WrapAlgorithm: wrapAlgorithm, AADScheme: aadScheme, Keys: keys}, offset, nil
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

func appendString(buf []byte, v string) ([]byte, error) {
	if len(v) > int(^uint32(0)) {
		return nil, errors.New("manifest: string too large")
	}
	buf = appendU32(buf, uint32(len(v)))
	return append(buf, v...), nil
}

func appendBytes(buf []byte, v []byte) ([]byte, error) {
	if len(v) > int(^uint32(0)) {
		return nil, errors.New("manifest: bytes too large")
	}
	buf = appendU32(buf, uint32(len(v)))
	return append(buf, v...), nil
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

func readBytes(data []byte) ([]byte, int, error) {
	if len(data) < 4 {
		return nil, 0, errors.New("manifest: truncated bytes length")
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	if len(data) < 4+n {
		return nil, 0, errors.New("manifest: truncated bytes")
	}
	out := make([]byte, n)
	copy(out, data[4:4+n])
	return out, 4 + n, nil
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
