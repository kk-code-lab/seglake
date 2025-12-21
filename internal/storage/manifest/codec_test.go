package manifest

import (
	"bytes"
	"testing"
)

func TestBinaryCodecRoundTrip(t *testing.T) {
	c := &BinaryCodec{}
	manifest := &Manifest{
		VersionID: "v1",
		Size:      12,
		Chunks: []ChunkRef{
			{
				Index:     0,
				Hash:      [32]byte{1, 2, 3},
				SegmentID: "seg-0001",
				Offset:    64,
				Len:       4,
			},
			{
				Index:     1,
				Hash:      [32]byte{4, 5, 6},
				SegmentID: "seg-0001",
				Offset:    96,
				Len:       8,
			},
		},
	}

	var buf bytes.Buffer
	if err := c.Encode(&buf, manifest); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := c.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got.VersionID != manifest.VersionID || got.Size != manifest.Size || len(got.Chunks) != len(manifest.Chunks) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	for i := range manifest.Chunks {
		if got.Chunks[i] != manifest.Chunks[i] {
			t.Fatalf("chunk %d mismatch: %+v", i, got.Chunks[i])
		}
	}
}

func TestBinaryCodecChecksumMismatch(t *testing.T) {
	c := &BinaryCodec{}
	manifest := &Manifest{
		VersionID: "v1",
		Size:      1,
	}
	var buf bytes.Buffer
	if err := c.Encode(&buf, manifest); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data := buf.Bytes()
	data[len(data)-1] ^= 0xff
	if _, err := c.Decode(bytes.NewReader(data)); err == nil {
		t.Fatalf("expected checksum error")
	}
}
