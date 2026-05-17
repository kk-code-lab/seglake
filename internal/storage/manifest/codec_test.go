package manifest

import (
	"bytes"
	"testing"
)

func TestBinaryCodecRoundTrip(t *testing.T) {
	c := &BinaryCodec{}
	manifest := &Manifest{
		Bucket:    "bucket",
		Key:       "key",
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
	if got.Bucket != manifest.Bucket || got.Key != manifest.Key {
		t.Fatalf("bucket/key mismatch: %+v", got)
	}
	for i := range manifest.Chunks {
		if got.Chunks[i] != manifest.Chunks[i] {
			t.Fatalf("chunk %d mismatch: %+v", i, got.Chunks[i])
		}
	}
}

func TestBinaryCodecV3EncryptionRoundTrip(t *testing.T) {
	c := &BinaryCodec{}
	manifest := &Manifest{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Size:      5,
		Chunks: []ChunkRef{{
			Index:     0,
			Hash:      [32]byte{1},
			SegmentID: "seg",
			Offset:    8,
			Len:       21,
			PlainLen:  5,
			KeyRef:    7,
		}},
		Encryption: &Encryption{
			Mode:          "SSE-S3",
			Algorithm:     "AES-256-GCM",
			WrapAlgorithm: "AES-256-GCM",
			AADScheme:     "seglake-sse-s3-aad-v1",
			Keys: []KeyEntry{{
				KeyRef:          7,
				KeyID:           "local:v1",
				EncryptedDEK:    []byte{1, 2, 3},
				WrapNonce:       []byte{4, 5, 6},
				NoncePrefix:     []byte{7, 8, 9},
				NonceScheme:     "random64-counter32-v1",
				EDEKFingerprint: []byte{10, 11},
			}},
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
	if !got.Encrypted() {
		t.Fatalf("expected encrypted manifest")
	}
	if got.Chunks[0].PlainLen != manifest.Chunks[0].PlainLen || got.Chunks[0].KeyRef != manifest.Chunks[0].KeyRef {
		t.Fatalf("chunk encryption metadata mismatch: %+v", got.Chunks[0])
	}
	if len(got.Encryption.Keys) != 1 || got.Encryption.Keys[0].KeyID != "local:v1" {
		t.Fatalf("key metadata mismatch: %+v", got.Encryption)
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
