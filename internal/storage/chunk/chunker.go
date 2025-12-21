package chunk

import "io"

// DefaultSize is the default chunk size (4 MiB).
const DefaultSize = 4 << 20

// Chunk is a unit produced by the chunker.
type Chunk struct {
	Index int
	Hash  [32]byte
	Data  []byte
}

// Splitter streams chunks to a callback.
type Splitter interface {
	Split(r io.Reader, fn func(Chunk) error) error
}

// FixedSplitter splits streams into fixed-size chunks.
type FixedSplitter struct {
	Size int
}

// NewFixedSplitter creates a fixed-size splitter.
func NewFixedSplitter(size int) *FixedSplitter {
	if size <= 0 {
		size = DefaultSize
	}
	return &FixedSplitter{Size: size}
}

// Split streams chunks to the callback; the final chunk may be smaller.
func (s *FixedSplitter) Split(r io.Reader, fn func(Chunk) error) error {
	buf := make([]byte, s.Size)
	index := 0
	for {
		n, err := io.ReadFull(r, buf)
		if err == io.EOF {
			return nil
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		if n == 0 {
			return nil
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		chunk := Chunk{
			Index: index,
			Hash:  Hash(data),
			Data:  data,
		}
		if err := fn(chunk); err != nil {
			return err
		}
		index++
		if err == io.ErrUnexpectedEOF {
			return nil
		}
	}
}
