package sse

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	ModeSSES3           = "SSE-S3"
	AlgorithmAES256GCM  = "AES-256-GCM"
	WrapAES256GCM       = "AES-256-GCM"
	NonceSchemeV1       = "random64-counter32-v1"
	AADSchemeV1         = "seglake-sse-s3-aad-v1"
	ServerSideHeaderS3  = "AES256"
	KeyFingerprintBytes = 8
)

var (
	ErrDisabled   = errors.New("sse: disabled")
	ErrNoSuchKey  = errors.New("sse: key not configured")
	ErrBadKeySpec = errors.New("sse: invalid key spec")
)

type Key struct {
	ID    string
	Bytes [32]byte
}

type Provider struct {
	active string
	keys   map[string]Key
}

func NewProvider(active string, keys []Key) (*Provider, error) {
	active = strings.TrimSpace(active)
	if active == "" {
		return nil, fmt.Errorf("%w: active key required", ErrBadKeySpec)
	}
	out := &Provider{active: active, keys: make(map[string]Key, len(keys))}
	for _, key := range keys {
		if err := ValidateKeyID(key.ID); err != nil {
			return nil, err
		}
		if _, ok := out.keys[key.ID]; ok {
			return nil, fmt.Errorf("%w: duplicate key id %q", ErrBadKeySpec, key.ID)
		}
		out.keys[key.ID] = key
	}
	if _, ok := out.keys[active]; !ok {
		return nil, fmt.Errorf("%w: active key %q", ErrNoSuchKey, active)
	}
	return out, nil
}

func (p *Provider) Enabled() bool {
	return p != nil
}

func (p *Provider) ActiveKey() (Key, error) {
	if p == nil {
		return Key{}, ErrDisabled
	}
	key, ok := p.keys[p.active]
	if !ok {
		return Key{}, ErrNoSuchKey
	}
	return key, nil
}

func (p *Provider) LookupKey(id string) (Key, error) {
	if p == nil {
		return Key{}, ErrDisabled
	}
	key, ok := p.keys[id]
	if !ok {
		return Key{}, ErrNoSuchKey
	}
	return key, nil
}

func ValidateKeyID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("%w: empty key id", ErrBadKeySpec)
	}
	if strings.ContainsAny(id, "=, \t\r\n") {
		return fmt.Errorf("%w: key id %q contains reserved characters", ErrBadKeySpec, id)
	}
	return nil
}

func DecodeKey(id, b64 string) (Key, error) {
	if err := ValidateKeyID(id); err != nil {
		return Key{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return Key{}, fmt.Errorf("%w: key %q is not base64", ErrBadKeySpec, id)
	}
	if len(raw) != 32 {
		return Key{}, fmt.Errorf("%w: key %q decoded to %d bytes, want 32", ErrBadKeySpec, id, len(raw))
	}
	var key Key
	key.ID = strings.TrimSpace(id)
	copy(key.Bytes[:], raw)
	return key, nil
}

func RandomBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func NewGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func GenerateDEK() ([32]byte, error) {
	var dek [32]byte
	if _, err := rand.Read(dek[:]); err != nil {
		return [32]byte{}, err
	}
	return dek, nil
}

func WrapDEK(kek Key, dek [32]byte, aad []byte) (nonce, encrypted []byte, err error) {
	aead, err := NewGCM(kek.Bytes)
	if err != nil {
		return nil, nil, err
	}
	nonce, err = RandomBytes(aead.NonceSize())
	if err != nil {
		return nil, nil, err
	}
	return nonce, aead.Seal(nil, nonce, dek[:], aad), nil
}

func UnwrapDEK(kek Key, nonce, encrypted, aad []byte) ([32]byte, error) {
	aead, err := NewGCM(kek.Bytes)
	if err != nil {
		return [32]byte{}, err
	}
	plain, err := aead.Open(nil, nonce, encrypted, aad)
	if err != nil {
		return [32]byte{}, err
	}
	if len(plain) != 32 {
		return [32]byte{}, fmt.Errorf("sse: invalid dek length %d", len(plain))
	}
	var dek [32]byte
	copy(dek[:], plain)
	return dek, nil
}

func ChunkNonce(prefix []byte, index uint32) ([]byte, error) {
	if len(prefix) != 8 {
		return nil, fmt.Errorf("sse: nonce prefix must be 8 bytes")
	}
	nonce := make([]byte, 12)
	copy(nonce, prefix)
	nonce[8] = byte(index >> 24)
	nonce[9] = byte(index >> 16)
	nonce[10] = byte(index >> 8)
	nonce[11] = byte(index)
	return nonce, nil
}

func WrapAAD(keyID string) []byte {
	return []byte("seglake:sse-s3:dek-wrap:v1:" + keyID)
}

func ChunkAAD(chunkIndex int, plainLen int) []byte {
	return []byte(fmt.Sprintf("seglake:sse-s3:v1\nchunk:%d\nplain-len:%d\n", chunkIndex, plainLen))
}
