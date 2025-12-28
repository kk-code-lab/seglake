package chunk

import (
	"bytes"
	"testing"
)

func FuzzFixedSplitter(f *testing.F) {
	// Future: add seed corpus with sizes around DefaultSize boundaries.
	f.Add([]byte("hello"), 3)
	f.Add([]byte("hello"), 0)
	f.Fuzz(func(t *testing.T, data []byte, size int) {
		if size > 1<<20 {
			size = 1 << 20
		}
		if size < 0 && size > -1<<20 {
			size = 0
		}
		splitter := NewFixedSplitter(size)
		reader := bytes.NewReader(data)
		var (
			lastIndex = -1
			total     int
		)
		err := splitter.Split(reader, func(ch Chunk) error {
			if ch.Index <= lastIndex {
				t.Fatalf("chunk index not increasing: %d <= %d", ch.Index, lastIndex)
			}
			lastIndex = ch.Index
			total += len(ch.Data)
			return nil
		})
		if err != nil {
			return
		}
		if total != len(data) {
			t.Fatalf("splitter total=%d want=%d", total, len(data))
		}
	})
}
