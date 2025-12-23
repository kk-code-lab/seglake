package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestAuthzPolicyAndBucketAllow(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.UpsertAPIKey(ctx, "ak", "sk", "read-only", true, 1); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	if err := store.AllowBucketForKey(ctx, "ak", "allowed"); err != nil {
		t.Fatalf("AllowBucketForKey: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	if _, _, err := eng.PutObject(ctx, "allowed", "k", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			Region:               "us-east-1",
			SecretLookup:         store.LookupAPISecret,
			AllowUnsignedPayload: true,
		},
		InflightLimiter: NewInflightLimiter(4),
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/allowed/k", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("unexpected body: %s", string(body))
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/allowed/new", bytes.NewReader([]byte("nope")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(putReq, "ak", "sk", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	_ = putResp.Body.Close()
	if putResp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	denyReq, err := http.NewRequest(http.MethodGet, server.URL+"/denied/k", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(denyReq, "ak", "sk", "us-east-1")
	denyResp, err := http.DefaultClient.Do(denyReq)
	if err != nil {
		t.Fatalf("GET denied error: %v", err)
	}
	_ = denyResp.Body.Close()
	if denyResp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET denied status: %d", denyResp.StatusCode)
	}
}

func TestSigV2ListRequestRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			SecretLookup:         store.LookupAPISecret,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "AWS ak:deadbeef")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		_ = resp.Body.Close()
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
	var errResp errorResponse
	if err := xml.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("decode error response: %v", err)
	}
	_ = resp.Body.Close()
	if errResp.Code != "SignatureDoesNotMatch" {
		t.Fatalf("expected SignatureDoesNotMatch, got %q", errResp.Code)
	}
}
