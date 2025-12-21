package chunk

import (
	"bytes"
	"testing"
)

func TestFixedSplitterBoundaries(t *testing.T) {
	size := 8
	cases := []struct {
		name      string
		inputSize int
		wantCnt   int
	}{
		{name: "empty", inputSize: 0, wantCnt: 0},
		{name: "one", inputSize: 1, wantCnt: 1},
		{name: "size-1", inputSize: size - 1, wantCnt: 1},
		{name: "size", inputSize: size, wantCnt: 1},
		{name: "size+1", inputSize: size + 1, wantCnt: 2},
		{name: "double+tail", inputSize: size*2 + 3, wantCnt: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]byte, tc.inputSize)
			for i := range input {
				input[i] = byte(i % 251)
			}
			var got []Chunk
			splitter := NewFixedSplitter(size)
			err := splitter.Split(bytes.NewReader(input), func(c Chunk) error {
				got = append(got, c)
				return nil
			})
			if err != nil {
				t.Fatalf("Split: %v", err)
			}
			if len(got) != tc.wantCnt {
				t.Fatalf("expected %d chunks, got %d", tc.wantCnt, len(got))
			}
			var rebuilt []byte
			for i, c := range got {
				if c.Index != i {
					t.Fatalf("chunk index mismatch: got %d want %d", c.Index, i)
				}
				if c.Hash != Hash(c.Data) {
					t.Fatalf("hash mismatch for chunk %d", i)
				}
				rebuilt = append(rebuilt, c.Data...)
			}
			if !bytes.Equal(rebuilt, input) {
				t.Fatalf("rebuild mismatch")
			}
		})
	}
}
