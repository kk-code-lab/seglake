package sse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultVaultTransitTimeout = 5 * time.Second

type VaultTransitConfig struct {
	Address   string
	Mount     string
	Token     string
	Namespace string
	ActiveKey string
	Timeout   time.Duration
	Client    *http.Client
}

type VaultTransitProvider struct {
	address   string
	mount     string
	token     string
	namespace string
	activeKey string
	client    *http.Client
}

func NewVaultTransitProvider(cfg VaultTransitConfig) (*VaultTransitProvider, error) {
	address := strings.TrimRight(strings.TrimSpace(cfg.Address), "/")
	if address == "" {
		return nil, fmt.Errorf("%w: vault address required", ErrBadKeySpec)
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("%w: invalid vault address", ErrBadKeySpec)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: vault address must use http or https", ErrBadKeySpec)
	}
	mount := strings.Trim(strings.TrimSpace(cfg.Mount), "/")
	if mount == "" {
		mount = "transit"
	}
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, fmt.Errorf("%w: vault token required", ErrBadKeySpec)
	}
	activeKey := strings.TrimSpace(cfg.ActiveKey)
	if activeKey != "" {
		if err := ValidateKeyID(activeKey); err != nil {
			return nil, err
		}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultVaultTransitTimeout
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	return &VaultTransitProvider{
		address:   address,
		mount:     mount,
		token:     token,
		namespace: strings.TrimSpace(cfg.Namespace),
		activeKey: activeKey,
		client:    client,
	}, nil
}

func (p *VaultTransitProvider) GenerateDataKey(ctx context.Context, req GenerateDataKeyRequest) (GenerateDataKeyResult, error) {
	if p == nil {
		return GenerateDataKeyResult{}, ErrProviderUnavailable
	}
	keyID := strings.TrimSpace(req.KeyID)
	if keyID == "" {
		keyID = p.activeKey
	}
	if keyID == "" {
		return GenerateDataKeyResult{}, fmt.Errorf("%w: active vault key required", ErrMissingKey)
	}
	var resp struct {
		Data struct {
			Plaintext  string `json:"plaintext"`
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	if err := p.post(ctx, "datakey/plaintext/"+url.PathEscape(keyID), map[string]int{"bits": 256}, &resp); err != nil {
		return GenerateDataKeyResult{}, err
	}
	dek, err := decodeVaultDEK(resp.Data.Plaintext)
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	if strings.TrimSpace(resp.Data.Ciphertext) == "" {
		return GenerateDataKeyResult{}, fmt.Errorf("%w: missing vault ciphertext", ErrInvalidEnvelope)
	}
	noncePrefix, err := RandomBytes(8)
	if err != nil {
		return GenerateDataKeyResult{}, err
	}
	edek := []byte(resp.Data.Ciphertext)
	sum := sha256.Sum256(edek)
	return GenerateDataKeyResult{
		PlaintextDEK: dek,
		KeyEntry: KeyEntry{
			WrapAlgorithm:   WrapVaultTransitV1,
			KeyRef:          req.KeyRef,
			KeyID:           keyID,
			EncryptedDEK:    edek,
			NoncePrefix:     noncePrefix,
			NonceScheme:     NonceSchemeV1,
			EDEKFingerprint: sum[:KeyFingerprintBytes],
		},
	}, nil
}

func (p *VaultTransitProvider) DefaultKeyID() string {
	if p == nil {
		return ""
	}
	return p.activeKey
}

func (p *VaultTransitProvider) DecryptDataKey(ctx context.Context, req DecryptDataKeyRequest) (DecryptDataKeyResult, error) {
	if p == nil {
		return DecryptDataKeyResult{}, ErrProviderUnavailable
	}
	if NormalizeWrapAlgorithm(req.KeyEntry.WrapAlgorithm) != WrapVaultTransitV1 {
		return DecryptDataKeyResult{}, fmt.Errorf("%w: unsupported wrap algorithm %q", ErrInvalidEnvelope, req.KeyEntry.WrapAlgorithm)
	}
	keyID := strings.TrimSpace(req.KeyEntry.KeyID)
	if keyID == "" || len(req.KeyEntry.EncryptedDEK) == 0 {
		return DecryptDataKeyResult{}, ErrInvalidEnvelope
	}
	var resp struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := p.post(ctx, "decrypt/"+url.PathEscape(keyID), map[string]string{"ciphertext": string(req.KeyEntry.EncryptedDEK)}, &resp); err != nil {
		return DecryptDataKeyResult{}, err
	}
	dek, err := decodeVaultDEK(resp.Data.Plaintext)
	if err != nil {
		return DecryptDataKeyResult{}, err
	}
	return DecryptDataKeyResult{PlaintextDEK: dek}, nil
}

func (p *VaultTransitProvider) WrapDataKey(ctx context.Context, req WrapDataKeyRequest) (WrapDataKeyResult, error) {
	if p == nil {
		return WrapDataKeyResult{}, ErrProviderUnavailable
	}
	targetKeyID := strings.TrimSpace(req.TargetKeyID)
	if targetKeyID == "" {
		targetKeyID = p.activeKey
	}
	if targetKeyID == "" {
		return WrapDataKeyResult{}, fmt.Errorf("%w: target vault key required", ErrInvalidEnvelope)
	}
	var resp struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	body := map[string]string{"plaintext": base64.StdEncoding.EncodeToString(req.PlaintextDEK[:])}
	if err := p.post(ctx, "encrypt/"+url.PathEscape(targetKeyID), body, &resp); err != nil {
		return WrapDataKeyResult{}, err
	}
	if strings.TrimSpace(resp.Data.Ciphertext) == "" {
		return WrapDataKeyResult{}, fmt.Errorf("%w: missing vault ciphertext", ErrInvalidEnvelope)
	}
	noncePrefix := req.KeyEntry.NoncePrefix
	var err error
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
	edek := []byte(resp.Data.Ciphertext)
	sum := sha256.Sum256(edek)
	entry := req.KeyEntry
	entry.WrapAlgorithm = WrapVaultTransitV1
	entry.KeyID = targetKeyID
	entry.EncryptedDEK = edek
	entry.WrapNonce = nil
	entry.NoncePrefix = noncePrefix
	entry.NonceScheme = nonceScheme
	entry.EDEKFingerprint = sum[:KeyFingerprintBytes]
	return WrapDataKeyResult{KeyEntry: entry}, nil
}

func (p *VaultTransitProvider) RewrapDataKey(ctx context.Context, req RewrapDataKeyRequest) (RewrapDataKeyResult, error) {
	targetKeyID := strings.TrimSpace(req.TargetKeyID)
	if targetKeyID == "" {
		return RewrapDataKeyResult{}, fmt.Errorf("%w: target key required", ErrInvalidEnvelope)
	}
	if NormalizeWrapAlgorithm(req.KeyEntry.WrapAlgorithm) == WrapVaultTransitV1 && req.KeyEntry.KeyID == targetKeyID {
		var resp struct {
			Data struct {
				Ciphertext string `json:"ciphertext"`
			} `json:"data"`
		}
		if err := p.post(ctx, "rewrap/"+url.PathEscape(targetKeyID), map[string]string{"ciphertext": string(req.KeyEntry.EncryptedDEK)}, &resp); err != nil {
			return RewrapDataKeyResult{}, err
		}
		if strings.TrimSpace(resp.Data.Ciphertext) == "" {
			return RewrapDataKeyResult{}, fmt.Errorf("%w: missing vault ciphertext", ErrInvalidEnvelope)
		}
		edek := []byte(resp.Data.Ciphertext)
		sum := sha256.Sum256(edek)
		entry := req.KeyEntry
		entry.WrapAlgorithm = WrapVaultTransitV1
		entry.KeyID = targetKeyID
		entry.EncryptedDEK = edek
		entry.WrapNonce = nil
		entry.EDEKFingerprint = sum[:KeyFingerprintBytes]
		return RewrapDataKeyResult{KeyEntry: entry}, nil
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

func (p *VaultTransitProvider) DescribeKey(_ context.Context, keyID string) (KeyDescription, error) {
	keyID = strings.TrimSpace(keyID)
	if err := ValidateKeyID(keyID); err != nil {
		return KeyDescription{}, err
	}
	return KeyDescription{
		ProviderType: ProviderVaultTransit,
		KeyID:        keyID,
		CanEncrypt:   p != nil && (p.activeKey == "" || p.activeKey == keyID),
		CanDecrypt:   true,
		CanRewrap:    true,
	}, nil
}

func (p *VaultTransitProvider) WrapAlgorithm() string {
	return WrapVaultTransitV1
}

func (p *VaultTransitProvider) post(ctx context.Context, path string, body any, out any) error {
	if p == nil {
		return ErrProviderUnavailable
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.address+"/v1/"+p.mount+"/"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vault-Token", p.token)
	if p.namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.namespace)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: vault request failed", ErrProviderUnavailable)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%w: read vault response", ErrProviderUnavailable)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return vaultStatusError(resp.StatusCode)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: decode vault response", ErrInvalidEnvelope)
	}
	return nil
}

func decodeVaultDEK(value string) ([32]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: invalid vault plaintext", ErrInvalidEnvelope)
	}
	if len(raw) != 32 {
		return [32]byte{}, fmt.Errorf("%w: vault plaintext decoded to %d bytes", ErrInvalidEnvelope, len(raw))
	}
	var dek [32]byte
	copy(dek[:], raw)
	return dek, nil
}

func vaultStatusError(status int) error {
	switch status {
	case http.StatusForbidden:
		return ErrPermissionDenied
	case http.StatusNotFound:
		return ErrMissingKey
	case http.StatusBadRequest:
		return ErrDecryptFailed
	default:
		if status >= 500 {
			return ErrProviderUnavailable
		}
		return fmt.Errorf("%w: vault status %d", ErrProviderUnavailable, status)
	}
}
