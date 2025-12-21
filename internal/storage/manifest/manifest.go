package manifest

// ChunkRef points to a chunk stored in a segment.
type ChunkRef struct {
	Index     int
	Hash      [32]byte
	SegmentID string
	Offset    int64
	Len       uint32
}

// Manifest describes the layout of an object version.
type Manifest struct {
	Bucket    string
	Key       string
	VersionID string
	Size      int64
	Chunks    []ChunkRef
}
