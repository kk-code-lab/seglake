package manifest

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func FuzzBinaryCodecDecode(f *testing.F) {
	// Future: extend with structured generation to hit version-v2 fields more often.
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, data []byte) {
		codec := &BinaryCodec{}
		_, _ = codec.Decode(bytes.NewReader(data))

		manifest := randomManifest(data)
		var buf bytes.Buffer
		if err := codec.Encode(&buf, manifest); err != nil {
			return
		}
		got, err := codec.Decode(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("decode after encode failed: %v", err)
		}
		if !reflect.DeepEqual(manifest, got) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

func randomManifest(seed []byte) *Manifest {
	r := rand.New(rand.NewSource(seedToInt64(seed)))
	chunkCount := r.Intn(6)
	chunks := make([]ChunkRef, 0, chunkCount)
	var total uint64
	for i := 0; i < chunkCount; i++ {
		var hash [32]byte
		_, _ = r.Read(hash[:])
		length := uint32(r.Intn(1<<16) + 1)
		chunks = append(chunks, ChunkRef{
			Index:     i,
			Hash:      hash,
			SegmentID: randString(r, 24),
			Offset:    int64(total),
			Len:       length,
		})
		total += uint64(length)
	}
	return &Manifest{
		Bucket:    randString(r, 12),
		Key:       randString(r, 24),
		VersionID: randString(r, 16),
		Size:      int64(total),
		Chunks:    chunks,
	}
}

func seedToInt64(seed []byte) int64 {
	if len(seed) == 0 {
		return 0
	}
	var v int64
	for i := 0; i < len(seed) && i < 8; i++ {
		v |= int64(seed[i]) << (8 * i)
	}
	return v
}

func randString(r *rand.Rand, max int) string {
	if max <= 0 {
		return ""
	}
	n := r.Intn(max + 1)
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_"
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(buf)
}
