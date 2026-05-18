package sse

import (
	"context"
	"encoding/base64"
	"errors"
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

func TestLookupProviderDoesNotRequireActiveKey(t *testing.T) {
	key, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32))))
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	provider, err := NewLookupProvider([]Key{key})
	if err != nil {
		t.Fatalf("NewLookupProvider: %v", err)
	}
	got, err := provider.LookupKey("local:v1")
	if err != nil {
		t.Fatalf("LookupKey: %v", err)
	}
	if got.ID != key.ID || got.Bytes != key.Bytes {
		t.Fatalf("lookup mismatch")
	}
	if _, err := provider.ActiveKey(); err == nil {
		t.Fatalf("expected ActiveKey to fail without active writer key")
	}
}

func TestProviderDataKeyRoundTrip(t *testing.T) {
	key, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	provider, err := NewProvider(key.ID, []Key{key})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	generated, err := provider.GenerateDataKey(context.Background(), GenerateDataKeyRequest{KeyRef: 7})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if generated.KeyEntry.KeyRef != 7 || generated.KeyEntry.KeyID != key.ID {
		t.Fatalf("unexpected key entry: %+v", generated.KeyEntry)
	}
	if len(generated.KeyEntry.EncryptedDEK) == 0 || len(generated.KeyEntry.WrapNonce) == 0 || len(generated.KeyEntry.EDEKFingerprint) != KeyFingerprintBytes {
		t.Fatalf("incomplete key entry: %+v", generated.KeyEntry)
	}
	decrypted, err := provider.DecryptDataKey(context.Background(), DecryptDataKeyRequest{KeyEntry: generated.KeyEntry})
	if err != nil {
		t.Fatalf("DecryptDataKey: %v", err)
	}
	if decrypted.PlaintextDEK != generated.PlaintextDEK {
		t.Fatalf("DEK mismatch")
	}
}

func TestProviderGenerateDataKeyWithRequestedKeyID(t *testing.T) {
	active, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 32))))
	if err != nil {
		t.Fatalf("DecodeKey active: %v", err)
	}
	target, err := DecodeKey("local:v2", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", 32))))
	if err != nil {
		t.Fatalf("DecodeKey target: %v", err)
	}
	provider, err := NewProvider(active.ID, []Key{active, target})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	generated, err := provider.GenerateDataKey(context.Background(), GenerateDataKeyRequest{KeyRef: 3, KeyID: target.ID})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if generated.KeyEntry.KeyID != target.ID || generated.KeyEntry.KeyRef != 3 {
		t.Fatalf("unexpected requested key entry: %+v", generated.KeyEntry)
	}
	if _, err := provider.GenerateDataKey(context.Background(), GenerateDataKeyRequest{KeyID: "local:missing"}); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("expected missing requested key error, got %v", err)
	}
}

func TestProviderRewrapDataKey(t *testing.T) {
	oldKey, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("o", 32))))
	if err != nil {
		t.Fatalf("DecodeKey old: %v", err)
	}
	newKey, err := DecodeKey("local:v2", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("n", 32))))
	if err != nil {
		t.Fatalf("DecodeKey new: %v", err)
	}
	oldProvider, err := NewProvider(oldKey.ID, []Key{oldKey})
	if err != nil {
		t.Fatalf("NewProvider old: %v", err)
	}
	bothProvider, err := NewProvider(newKey.ID, []Key{oldKey, newKey})
	if err != nil {
		t.Fatalf("NewProvider both: %v", err)
	}
	generated, err := oldProvider.GenerateDataKey(context.Background(), GenerateDataKeyRequest{KeyRef: 1})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	rewrapped, err := bothProvider.RewrapDataKey(context.Background(), RewrapDataKeyRequest{
		KeyEntry:    generated.KeyEntry,
		TargetKeyID: newKey.ID,
	})
	if err != nil {
		t.Fatalf("RewrapDataKey: %v", err)
	}
	if rewrapped.KeyEntry.KeyID != newKey.ID || rewrapped.KeyEntry.KeyRef != generated.KeyEntry.KeyRef {
		t.Fatalf("unexpected rewrapped key entry: %+v", rewrapped.KeyEntry)
	}
	if string(rewrapped.KeyEntry.EncryptedDEK) == string(generated.KeyEntry.EncryptedDEK) {
		t.Fatalf("expected EDEK to change")
	}
	decrypted, err := bothProvider.DecryptDataKey(context.Background(), DecryptDataKeyRequest(rewrapped))
	if err != nil {
		t.Fatalf("DecryptDataKey rewrapped: %v", err)
	}
	if decrypted.PlaintextDEK != generated.PlaintextDEK {
		t.Fatalf("DEK mismatch after rewrap")
	}
	if _, err := oldProvider.DecryptDataKey(context.Background(), DecryptDataKeyRequest(rewrapped)); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("expected old provider missing-key error, got %v", err)
	}
}

func TestProviderDataKeyErrors(t *testing.T) {
	key, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("e", 32))))
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	lookup, err := NewLookupProvider([]Key{key})
	if err != nil {
		t.Fatalf("NewLookupProvider: %v", err)
	}
	if _, err := lookup.GenerateDataKey(context.Background(), GenerateDataKeyRequest{}); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("expected read-only provider write failure as missing key, got %v", err)
	}
	if _, err := lookup.DecryptDataKey(context.Background(), DecryptDataKeyRequest{KeyEntry: KeyEntry{KeyID: key.ID}}); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("expected invalid envelope, got %v", err)
	}
	provider, err := NewProvider(key.ID, []Key{key})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	generated, err := provider.GenerateDataKey(context.Background(), GenerateDataKeyRequest{})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	generated.KeyEntry.EncryptedDEK[0] ^= 0xff
	if _, err := provider.DecryptDataKey(context.Background(), DecryptDataKeyRequest{KeyEntry: generated.KeyEntry}); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("expected decrypt failure, got %v", err)
	}
}
