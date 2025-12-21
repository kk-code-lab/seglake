package chunk

// Span describes a byte range within an object.
type Span struct {
	Offset int64
	Len    int64
}
