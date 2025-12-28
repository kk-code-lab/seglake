package segment

import (
	"math/rand"
	"reflect"
	"testing"
)

func FuzzIndexDecode(f *testing.F) {
	// Future: add seeds with malformed sizes to exercise size-mismatch errors more.
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeIndex(data)

		entries := randomIndexEntries(data)
		encoded := EncodeIndex(entries)
		decoded, err := DecodeIndex(encoded)
		if err != nil {
			t.Fatalf("decode after encode failed: %v", err)
		}
		if !reflect.DeepEqual(entries, decoded) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

func randomIndexEntries(seed []byte) []IndexEntry {
	r := rand.New(rand.NewSource(seedToInt64(seed)))
	count := r.Intn(8)
	if count == 0 {
		return nil
	}
	out := make([]IndexEntry, 0, count)
	for i := 0; i < count; i++ {
		var hash [32]byte
		_, _ = r.Read(hash[:])
		out = append(out, IndexEntry{
			Offset: int64(r.Int63()),
			Hash:   hash,
		})
	}
	return out
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
