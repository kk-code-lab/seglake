package sse

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestVaultTransitProviderDataKeyRoundTrip(t *testing.T) {
	server := newFakeVaultTransit(t)
	defer server.Close()
	provider := newTestVaultProvider(t, server.URL, "seglake-test")

	generated, err := provider.GenerateDataKey(t.Context(), GenerateDataKeyRequest{KeyRef: 9})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if generated.KeyEntry.WrapAlgorithm != WrapVaultTransitV1 {
		t.Fatalf("wrap algorithm = %q", generated.KeyEntry.WrapAlgorithm)
	}
	if generated.KeyEntry.KeyID != "seglake-test" || generated.KeyEntry.KeyRef != 9 {
		t.Fatalf("unexpected key entry: %+v", generated.KeyEntry)
	}
	if len(generated.KeyEntry.WrapNonce) != 0 {
		t.Fatalf("vault key entry should not store local wrap nonce")
	}
	decrypted, err := provider.DecryptDataKey(t.Context(), DecryptDataKeyRequest{KeyEntry: generated.KeyEntry})
	if err != nil {
		t.Fatalf("DecryptDataKey: %v", err)
	}
	if decrypted.PlaintextDEK != generated.PlaintextDEK {
		t.Fatalf("DEK mismatch")
	}
}

func TestVaultTransitProviderRewrap(t *testing.T) {
	server := newFakeVaultTransit(t)
	defer server.Close()
	provider := newTestVaultProvider(t, server.URL, "v1")

	generated, err := provider.GenerateDataKey(t.Context(), GenerateDataKeyRequest{KeyRef: 1})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	sameKey, err := provider.RewrapDataKey(t.Context(), RewrapDataKeyRequest{
		KeyEntry:    generated.KeyEntry,
		TargetKeyID: "v1",
	})
	if err != nil {
		t.Fatalf("same-key RewrapDataKey: %v", err)
	}
	if sameKey.KeyEntry.KeyID != "v1" || string(sameKey.KeyEntry.EncryptedDEK) == string(generated.KeyEntry.EncryptedDEK) {
		t.Fatalf("unexpected same-key rewrap entry: %+v", sameKey.KeyEntry)
	}
	crossKey, err := provider.RewrapDataKey(t.Context(), RewrapDataKeyRequest{
		KeyEntry:    sameKey.KeyEntry,
		TargetKeyID: "v2",
	})
	if err != nil {
		t.Fatalf("cross-key RewrapDataKey: %v", err)
	}
	if crossKey.KeyEntry.KeyID != "v2" || crossKey.KeyEntry.WrapAlgorithm != WrapVaultTransitV1 {
		t.Fatalf("unexpected cross-key entry: %+v", crossKey.KeyEntry)
	}
	decrypted, err := provider.DecryptDataKey(t.Context(), DecryptDataKeyRequest(crossKey))
	if err != nil {
		t.Fatalf("DecryptDataKey cross-key: %v", err)
	}
	if decrypted.PlaintextDEK != generated.PlaintextDEK {
		t.Fatalf("DEK mismatch after rewrap")
	}
}

func TestVaultTransitProviderErrors(t *testing.T) {
	for status, want := range map[int]error{
		http.StatusForbidden:           ErrPermissionDenied,
		http.StatusNotFound:            ErrMissingKey,
		http.StatusInternalServerError: ErrProviderUnavailable,
		http.StatusBadGateway:          ErrProviderUnavailable,
		http.StatusServiceUnavailable:  ErrProviderUnavailable,
		http.StatusGatewayTimeout:      ErrProviderUnavailable,
		http.StatusBadRequest:          ErrDecryptFailed,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			provider := newTestVaultProvider(t, server.URL, "v1")
			_, err := provider.GenerateDataKey(t.Context(), GenerateDataKeyRequest{})
			if !errors.Is(err, want) {
				t.Fatalf("expected %v, got %v", want, err)
			}
		})
	}
}

func TestRoutingProviderReadsLegacyLocalAndWritesVault(t *testing.T) {
	localKey, err := DecodeKey("local:v1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("l", 32))))
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	localProvider, err := NewProvider(localKey.ID, []Key{localKey})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	localDataKey, err := localProvider.GenerateDataKey(t.Context(), GenerateDataKeyRequest{KeyRef: 1})
	if err != nil {
		t.Fatalf("local GenerateDataKey: %v", err)
	}

	server := newFakeVaultTransit(t)
	defer server.Close()
	vaultProvider := newTestVaultProvider(t, server.URL, "vault:v1")
	routing, err := NewRoutingProvider(vaultProvider, localProvider)
	if err != nil {
		t.Fatalf("NewRoutingProvider: %v", err)
	}
	generated, err := routing.GenerateDataKey(t.Context(), GenerateDataKeyRequest{KeyRef: 2})
	if err != nil {
		t.Fatalf("routing GenerateDataKey: %v", err)
	}
	if generated.KeyEntry.WrapAlgorithm != WrapVaultTransitV1 {
		t.Fatalf("expected vault write, got %q", generated.KeyEntry.WrapAlgorithm)
	}
	decryptedLocal, err := routing.DecryptDataKey(t.Context(), DecryptDataKeyRequest{KeyEntry: localDataKey.KeyEntry})
	if err != nil {
		t.Fatalf("routing DecryptDataKey local: %v", err)
	}
	if decryptedLocal.PlaintextDEK != localDataKey.PlaintextDEK {
		t.Fatalf("legacy local DEK mismatch")
	}
}

func newTestVaultProvider(t *testing.T, address, active string) *VaultTransitProvider {
	t.Helper()
	provider, err := NewVaultTransitProvider(VaultTransitConfig{
		Address:   address,
		Mount:     "transit",
		Token:     "test-token",
		ActiveKey: active,
	})
	if err != nil {
		t.Fatalf("NewVaultTransitProvider: %v", err)
	}
	return provider
}

type fakeVaultTransit struct {
	sync.Mutex
	counter int
	keys    map[string][32]byte
}

func newFakeVaultTransit(t *testing.T) *httptest.Server {
	t.Helper()
	state := &fakeVaultTransit{keys: map[string][32]byte{}}
	return httptest.NewServer(http.HandlerFunc(state.handle))
}

func (v *fakeVaultTransit) handle(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Vault-Token") == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/transit/")
	switch {
	case strings.HasPrefix(path, "datakey/plaintext/"):
		keyID := strings.TrimPrefix(path, "datakey/plaintext/")
		dek := v.nextDEK()
		ciphertext := v.store(keyID, dek)
		writeVaultJSON(w, map[string]any{"data": map[string]string{
			"plaintext":  base64.StdEncoding.EncodeToString(dek[:]),
			"ciphertext": ciphertext,
		}})
	case strings.HasPrefix(path, "decrypt/"):
		var req struct {
			Ciphertext string `json:"ciphertext"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		dek, ok := v.lookup(req.Ciphertext)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeVaultJSON(w, map[string]any{"data": map[string]string{
			"plaintext": base64.StdEncoding.EncodeToString(dek[:]),
		}})
	case strings.HasPrefix(path, "encrypt/"):
		keyID := strings.TrimPrefix(path, "encrypt/")
		var req struct {
			Plaintext string `json:"plaintext"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		raw, _ := base64.StdEncoding.DecodeString(req.Plaintext)
		var dek [32]byte
		copy(dek[:], raw)
		writeVaultJSON(w, map[string]any{"data": map[string]string{
			"ciphertext": v.store(keyID, dek),
		}})
	case strings.HasPrefix(path, "rewrap/"):
		keyID := strings.TrimPrefix(path, "rewrap/")
		var req struct {
			Ciphertext string `json:"ciphertext"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		dek, ok := v.lookup(req.Ciphertext)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeVaultJSON(w, map[string]any{"data": map[string]string{
			"ciphertext": v.store(keyID, dek),
		}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (v *fakeVaultTransit) nextDEK() [32]byte {
	v.Lock()
	defer v.Unlock()
	v.counter++
	var dek [32]byte
	for i := range dek {
		dek[i] = byte(v.counter + i)
	}
	return dek
}

func (v *fakeVaultTransit) store(keyID string, dek [32]byte) string {
	v.Lock()
	defer v.Unlock()
	v.counter++
	ciphertext := "vault:v1:" + keyID + ":" + base64.StdEncoding.EncodeToString([]byte{byte(v.counter)})
	v.keys[ciphertext] = dek
	return ciphertext
}

func (v *fakeVaultTransit) lookup(ciphertext string) ([32]byte, bool) {
	v.Lock()
	defer v.Unlock()
	dek, ok := v.keys[ciphertext]
	return dek, ok
}

func writeVaultJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
