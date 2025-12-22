package segment

import (
	"encoding/binary"
	"errors"
	"math"
)

// IndexEntry maps a chunk hash to its data offset in a segment.
type IndexEntry struct {
	Offset int64
	Hash   [32]byte
}

// BuildBloom builds a bloom filter for the provided entries.
func BuildBloom(entries []IndexEntry) []byte {
	if len(entries) == 0 {
		return nil
	}
	n := float64(len(entries))
	const fpRate = 0.01
	mBits := int(math.Ceil(-n * math.Log(fpRate) / (math.Ln2 * math.Ln2)))
	if mBits < 1024 {
		mBits = 1024
	}
	mBytes := (mBits + 7) / 8
	k := int(math.Round((float64(mBits) / n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 8 {
		k = 8
	}
	bloom := make([]byte, mBytes)
	for _, entry := range entries {
		h1 := binary.LittleEndian.Uint64(entry.Hash[0:8])
		h2 := binary.LittleEndian.Uint64(entry.Hash[8:16])
		if h2 == 0 {
			h2 = 0x9e3779b97f4a7c15
		}
		for i := 0; i < k; i++ {
			bit := (h1 + uint64(i)*h2) % uint64(mBits)
			bloom[bit/8] |= 1 << (bit % 8)
		}
	}
	return bloom
}

// EncodeIndex encodes index entries as a binary blob.
func EncodeIndex(entries []IndexEntry) []byte {
	if len(entries) == 0 {
		return nil
	}
	const entrySize = 8 + 32
	size := 4 + len(entries)*entrySize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(entries)))
	off := 4
	for _, entry := range entries {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(entry.Offset))
		off += 8
		copy(buf[off:off+32], entry.Hash[:])
		off += 32
	}
	return buf
}

// DecodeIndex decodes a binary index blob.
func DecodeIndex(data []byte) ([]IndexEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.New("segment: index too short")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	const entrySize = 8 + 32
	want := 4 + count*entrySize
	if len(data) != want {
		return nil, errors.New("segment: index size mismatch")
	}
	out := make([]IndexEntry, 0, count)
	off := 4
	for i := 0; i < count; i++ {
		var entry IndexEntry
		entry.Offset = int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		copy(entry.Hash[:], data[off:off+32])
		off += 32
		out = append(out, entry)
	}
	return out, nil
}
