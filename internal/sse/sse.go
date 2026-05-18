package sse

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	ModeSSES3            = "SSE-S3"
	AlgorithmAES256GCM   = "AES-256-GCM"
	WrapAES256GCM        = "AES-256-GCM"
	WrapVaultTransitV1   = "vault-transit-v1"
	NonceSchemeV1        = "random64-counter32-v1"
	AADSchemeV1          = "seglake-sse-s3-aad-v1"
	ServerSideHeaderS3   = "AES256"
	KeyFingerprintBytes  = 8
	ProviderLocal        = "local"
	ProviderVaultTransit = "vault-transit"
)

var (
	ErrDisabled            = errors.New("sse: disabled")
	ErrNoSuchKey           = errors.New("sse: key not configured")
	ErrBadKeySpec          = errors.New("sse: invalid key spec")
	ErrProviderUnavailable = errors.New("sse: provider unavailable")
	ErrMissingKey          = ErrNoSuchKey
	ErrDecryptFailed       = errors.New("sse: decrypt failed")
	ErrInvalidEnvelope     = errors.New("sse: invalid key envelope")
	ErrPermissionDenied    = errors.New("sse: permission denied")
)

type Key struct {
	ID    string
	Bytes [32]byte
}

type KeyProvider interface {
	GenerateDataKey(ctx context.Context, req GenerateDataKeyRequest) (GenerateDataKeyResult, error)
	DecryptDataKey(ctx context.Context, req DecryptDataKeyRequest) (DecryptDataKeyResult, error)
	WrapDataKey(ctx context.Context, req WrapDataKeyRequest) (WrapDataKeyResult, error)
	RewrapDataKey(ctx context.Context, req RewrapDataKeyRequest) (RewrapDataKeyResult, error)
	DescribeKey(ctx context.Context, keyID string) (KeyDescription, error)
}

type GenerateDataKeyRequest struct {
	KeyRef uint32
}

type GenerateDataKeyResult struct {
	PlaintextDEK [32]byte
	KeyEntry     KeyEntry
}

type DecryptDataKeyRequest struct {
	KeyEntry KeyEntry
}

type DecryptDataKeyResult struct {
	PlaintextDEK [32]byte
}

type WrapDataKeyRequest struct {
	PlaintextDEK [32]byte
	KeyEntry     KeyEntry
	TargetKeyID  string
}

type WrapDataKeyResult struct {
	KeyEntry KeyEntry
}

type RewrapDataKeyRequest struct {
	KeyEntry    KeyEntry
	TargetKeyID string
}

type RewrapDataKeyResult struct {
	KeyEntry KeyEntry
}

type KeyDescription struct {
	ProviderType string
	KeyID        string
	CanEncrypt   bool
	CanDecrypt   bool
	CanRewrap    bool
}

type KeyEntry struct {
	WrapAlgorithm   string
	KeyRef          uint32
	KeyID           string
	EncryptedDEK    []byte
	WrapNonce       []byte
	NoncePrefix     []byte
	NonceScheme     string
	EDEKFingerprint []byte
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
	out, err := NewLookupProvider(keys)
	if err != nil {
		return nil, err
	}
	out.active = active
	if _, ok := out.keys[active]; !ok {
		return nil, fmt.Errorf("%w: active key %q", ErrNoSuchKey, active)
	}
	return out, nil
}

// NewLookupProvider builds a read-only provider for operations that only need key lookup.
func NewLookupProvider(keys []Key) (*Provider, error) {
	out := &Provider{keys: make(map[string]Key, len(keys))}
	for _, key := range keys {
		if err := ValidateKeyID(key.ID); err != nil {
			return nil, err
		}
		if _, ok := out.keys[key.ID]; ok {
			return nil, fmt.Errorf("%w: duplicate key id %q", ErrBadKeySpec, key.ID)
		}
		out.keys[key.ID] = key
	}
	if len(out.keys) == 0 {
		return nil, fmt.Errorf("%w: at least one key required", ErrBadKeySpec)
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

func (p *Provider) GenerateDataKey(_ context.Context, req GenerateDataKeyRequest) (GenerateDataKeyResult, error) {
	activeKey, err := p.ActiveKey()
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	dek, err := GenerateDEK()
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	wrapNonce, edek, err := WrapDEK(activeKey, dek, WrapAAD(activeKey.ID))
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	noncePrefix, err := RandomBytes(8)
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	sum := sha256.Sum256(edek)
	return GenerateDataKeyResult{
		PlaintextDEK: dek,
		KeyEntry: KeyEntry{
			WrapAlgorithm:   WrapAES256GCM,
			KeyRef:          req.KeyRef,
			KeyID:           activeKey.ID,
			EncryptedDEK:    edek,
			WrapNonce:       wrapNonce,
			NoncePrefix:     noncePrefix,
			NonceScheme:     NonceSchemeV1,
			EDEKFingerprint: sum[:KeyFingerprintBytes],
		},
	}, nil
}

func (p *Provider) DecryptDataKey(_ context.Context, req DecryptDataKeyRequest) (DecryptDataKeyResult, error) {
	if !isLocalWrapAlgorithm(req.KeyEntry.WrapAlgorithm) {
		return DecryptDataKeyResult{}, fmt.Errorf("%w: unsupported wrap algorithm %q", ErrInvalidEnvelope, req.KeyEntry.WrapAlgorithm)
	}
	if req.KeyEntry.KeyID == "" || len(req.KeyEntry.EncryptedDEK) == 0 || len(req.KeyEntry.WrapNonce) == 0 {
		return DecryptDataKeyResult{}, ErrInvalidEnvelope
	}
	kek, err := p.LookupKey(req.KeyEntry.KeyID)
	if err != nil {
		return DecryptDataKeyResult{}, err
	}
	dek, err := UnwrapDEK(kek, req.KeyEntry.WrapNonce, req.KeyEntry.EncryptedDEK, WrapAAD(req.KeyEntry.KeyID))
	if err != nil {
		return DecryptDataKeyResult{}, fmt.Errorf("%w: %v", ErrDecryptFailed, err)
	}
	return DecryptDataKeyResult{PlaintextDEK: dek}, nil
}

func (p *Provider) WrapDataKey(_ context.Context, req WrapDataKeyRequest) (WrapDataKeyResult, error) {
	targetKeyID := strings.TrimSpace(req.TargetKeyID)
	if targetKeyID == "" {
		return WrapDataKeyResult{}, fmt.Errorf("%w: target key required", ErrInvalidEnvelope)
	}
	targetKey, err := p.LookupKey(targetKeyID)
	if err != nil {
		return WrapDataKeyResult{}, err
	}
	wrapNonce, edek, err := WrapDEK(targetKey, req.PlaintextDEK, WrapAAD(targetKey.ID))
	if err != nil {
		return WrapDataKeyResult{}, err
	}
	noncePrefix := req.KeyEntry.NoncePrefix
	if len(noncePrefix) == 0 {
		noncePrefix, err = RandomBytes(8)
		if err != nil {
			return WrapDataKeyResult{}, err
		}
	}
	nonceScheme := req.KeyEntry.NonceScheme
	if nonceScheme == "" {
		nonceScheme = NonceSchemeV1
	}
	sum := sha256.Sum256(edek)
	entry := req.KeyEntry
	entry.WrapAlgorithm = WrapAES256GCM
	entry.KeyID = targetKey.ID
	entry.WrapNonce = wrapNonce
	entry.EncryptedDEK = edek
	entry.NoncePrefix = noncePrefix
	entry.NonceScheme = nonceScheme
	entry.EDEKFingerprint = sum[:KeyFingerprintBytes]
	return WrapDataKeyResult{KeyEntry: entry}, nil
}

func (p *Provider) RewrapDataKey(ctx context.Context, req RewrapDataKeyRequest) (RewrapDataKeyResult, error) {
	targetKeyID := strings.TrimSpace(req.TargetKeyID)
	if targetKeyID == "" {
		return RewrapDataKeyResult{}, fmt.Errorf("%w: target key required", ErrInvalidEnvelope)
	}
	decrypted, err := p.DecryptDataKey(ctx, DecryptDataKeyRequest{KeyEntry: req.KeyEntry})
	if err != nil {
		return RewrapDataKeyResult{}, err
	}
	wrapped, err := p.WrapDataKey(ctx, WrapDataKeyRequest{
		PlaintextDEK: decrypted.PlaintextDEK,
		KeyEntry:     req.KeyEntry,
		TargetKeyID:  targetKeyID,
	})
	if err != nil {
		return RewrapDataKeyResult{}, err
	}
	return RewrapDataKeyResult(wrapped), nil
}

func (p *Provider) DescribeKey(_ context.Context, keyID string) (KeyDescription, error) {
	keyID = strings.TrimSpace(keyID)
	if _, err := p.LookupKey(keyID); err != nil {
		return KeyDescription{}, err
	}
	return KeyDescription{
		ProviderType: ProviderLocal,
		KeyID:        keyID,
		CanEncrypt:   p != nil && p.active == keyID,
		CanDecrypt:   true,
		CanRewrap:    true,
	}, nil
}

func (p *Provider) WrapAlgorithm() string {
	return WrapAES256GCM
}

func NormalizeWrapAlgorithm(algorithm string) string {
	algorithm = strings.TrimSpace(algorithm)
	if algorithm == "" {
		return WrapAES256GCM
	}
	return algorithm
}

func isLocalWrapAlgorithm(algorithm string) bool {
	return NormalizeWrapAlgorithm(algorithm) == WrapAES256GCM
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
