package chunk

import "github.com/zeebo/blake3"

// Hash computes BLAKE3 digest for a chunk.
func Hash(data []byte) [32]byte {
	return blake3.Sum256(data)
}
