package main

import (
	"encoding/base64"
	"strings"
	"testing"

	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
)

func TestBuildSSEProviderFromSingleEnv(t *testing.T) {
	t.Setenv("SEGLAKE_SSE_S3_KEK_B64", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32))))
	opts := &serverOptions{
		sseS3Enabled:      true,
		sseS3ActiveKey:    "local:v1",
		sseS3SingleKeyB64: envOrDefault("SEGLAKE_SSE_S3_KEK_B64", ""),
	}
	provider, err := buildSSEProvider(opts)
	if err != nil {
		t.Fatalf("buildSSEProvider: %v", err)
	}
	if _, err := provider.GenerateDataKey(t.Context(), ssecrypto.GenerateDataKeyRequest{}); err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
}

func TestBuildSSEProviderRejectsDuplicateKeys(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))
	opts := &serverOptions{
		sseS3Enabled:   true,
		sseS3ActiveKey: "local:v1",
		sseS3KEKs:      multiString{"local:v1=inline:" + encoded, "local:v1=inline:" + encoded},
	}
	if _, err := buildSSEProvider(opts); err == nil {
		t.Fatalf("expected duplicate key error")
	}
}

func TestBuildSSEProviderVaultTransitFromEnv(t *testing.T) {
	t.Setenv("SEGLAKE_SSE_S3_VAULT_TOKEN", "test-token")
	opts := &serverOptions{
		sseS3Enabled:      true,
		sseS3Provider:     ssecrypto.ProviderVaultTransit,
		sseS3ActiveKey:    "vault-test",
		sseS3VaultAddr:    "http://127.0.0.1:8200",
		sseS3VaultMount:   "transit",
		sseS3VaultTimeout: 5,
	}
	provider, err := buildSSEProvider(opts)
	if err != nil {
		t.Fatalf("buildSSEProvider vault: %v", err)
	}
	desc, err := provider.DescribeKey(t.Context(), "vault-test")
	if err != nil {
		t.Fatalf("DescribeKey: %v", err)
	}
	if desc.ProviderType != ssecrypto.ProviderVaultTransit {
		t.Fatalf("provider type = %q", desc.ProviderType)
	}
}

func TestBuildSSEProviderVaultTransitRequiresToken(t *testing.T) {
	t.Setenv("SEGLAKE_SSE_S3_VAULT_TOKEN", "")
	t.Setenv("VAULT_TOKEN", "")
	opts := &serverOptions{
		sseS3Enabled:    true,
		sseS3Provider:   ssecrypto.ProviderVaultTransit,
		sseS3ActiveKey:  "vault-test",
		sseS3VaultAddr:  "http://127.0.0.1:8200",
		sseS3VaultMount: "transit",
	}
	if _, err := buildSSEProvider(opts); err == nil {
		t.Fatalf("expected missing token error")
	}
}

func TestParseSSEKeySpecRejectsBadLength(t *testing.T) {
	if _, err := parseSSEKeySpec("local:v1=inline:" + base64.StdEncoding.EncodeToString([]byte("short"))); err == nil {
		t.Fatalf("expected invalid length")
	}
}
