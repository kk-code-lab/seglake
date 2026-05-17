package main

import (
	"encoding/base64"
	"strings"
	"testing"
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
	if _, err := provider.ActiveKey(); err != nil {
		t.Fatalf("ActiveKey: %v", err)
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

func TestParseSSEKeySpecRejectsBadLength(t *testing.T) {
	if _, err := parseSSEKeySpec("local:v1=inline:" + base64.StdEncoding.EncodeToString([]byte("short"))); err == nil {
		t.Fatalf("expected invalid length")
	}
}
