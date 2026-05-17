package manifest

// ChunkRef points to a chunk stored in a segment.
type ChunkRef struct {
	Index     int
	Hash      [32]byte
	SegmentID string
	Offset    int64
	// Len is the number of stored bytes in the segment. For encrypted chunks
	// this is ciphertext+tag length; PlainLen carries plaintext length.
	Len      uint32
	PlainLen uint32
	KeyRef   uint32
}

// Manifest describes the layout of an object version.
type Manifest struct {
	Bucket     string
	Key        string
	VersionID  string
	Size       int64
	Chunks     []ChunkRef
	Encryption *Encryption
}

type Encryption struct {
	Mode          string
	Algorithm     string
	WrapAlgorithm string
	AADScheme     string
	Keys          []KeyEntry
}

type KeyEntry struct {
	KeyRef          uint32
	KeyID           string
	EncryptedDEK    []byte
	WrapNonce       []byte
	NoncePrefix     []byte
	NonceScheme     string
	EDEKFingerprint []byte
}

func (m *Manifest) Encrypted() bool {
	return m != nil && m.Encryption != nil && m.Encryption.Mode != ""
}

func (c ChunkRef) PlainLength() uint32 {
	if c.PlainLen > 0 {
		return c.PlainLen
	}
	return c.Len
}
