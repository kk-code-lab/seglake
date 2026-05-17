package sse

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestProviderValidationAndWrap(t *testing.T) {
	raw := strings.Repeat("a", 32)
	key, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(raw)))
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	provider, err := NewProvider("local:v1", []Key{key})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	active, err := provider.ActiveKey()
	if err != nil {
		t.Fatalf("ActiveKey: %v", err)
	}
	dek, err := GenerateDEK()
	if err != nil {
		t.Fatalf("GenerateDEK: %v", err)
	}
	nonce, edek, err := WrapDEK(active, dek, WrapAAD(active.ID))
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	got, err := UnwrapDEK(active, nonce, edek, WrapAAD(active.ID))
	if err != nil {
		t.Fatalf("UnwrapDEK: %v", err)
	}
	if got != dek {
		t.Fatalf("DEK mismatch")
	}
	wrong := active
	wrong.ID = "local:v2"
	wrong.Bytes[0] ^= 0xff
	if _, err := UnwrapDEK(wrong, nonce, edek, WrapAAD(active.ID)); err == nil {
		t.Fatalf("expected unwrap failure with wrong KEK")
	}
}

func TestChunkNonceUnique(t *testing.T) {
	prefix := []byte("12345678")
	a, err := ChunkNonce(prefix, 1)
	if err != nil {
		t.Fatalf("ChunkNonce: %v", err)
	}
	b, err := ChunkNonce(prefix, 2)
	if err != nil {
		t.Fatalf("ChunkNonce: %v", err)
	}
	if string(a) == string(b) {
		t.Fatalf("expected unique nonces")
	}
}

func TestRejectsInvalidKeyIDs(t *testing.T) {
	for _, id := range []string{"", "a,b", "a=b", "a b"} {
		if err := ValidateKeyID(id); err == nil {
			t.Fatalf("expected invalid key id %q", id)
		}
	}
}
