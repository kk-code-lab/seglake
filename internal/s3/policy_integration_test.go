package s3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func newPolicyServer(t *testing.T, policy string) (*httptest.Server, *Handler, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	if err := store.UpsertAPIKey(context.Background(), "ak", "sk", policy, true, 0); err != nil {
		_ = store.Close()
		t.Fatalf("UpsertAPIKey: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			Region:       "us-east-1",
			SecretLookup: store.LookupAPISecret,
		},
	}

	server := httptest.NewServer(handler)
	cleanup := func() {
		server.Close()
		_ = store.Close()
	}
	return server, handler, cleanup
}

func TestPolicyEnforcedOnRequests(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo","prefix":"public/"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/demo/public/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/demo/public/new", bytes.NewReader([]byte("nope")))
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
}

func TestPolicyDenyPrefixOverridesAllow(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]},{"effect":"deny","actions":["read"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "secret/x", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/demo/secret/x", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
}

func TestPolicyMPUDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]}]}`
	server, _, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/demo/key?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("MPU initiate status: %d", resp.StatusCode)
	}
}

func TestPolicyCopyDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "src", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req, err := http.NewRequest(http.MethodPut, server.URL+"/demo/dst", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Amz-Copy-Source", "/demo/src")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("COPY error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("COPY status: %d", resp.StatusCode)
	}
}

func TestPolicyMetaDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]}]}`
	server, _, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/meta/stats", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("meta stats error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("meta stats status: %d", resp.StatusCode)
	}
}

func TestPolicyListBucketsDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]}]}`
	server, _, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list buckets error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("list buckets status: %d", resp.StatusCode)
	}
}

func TestPolicyListObjectsDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo"}]}]}`
	server, _, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/demo?list-type=2", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list objects error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("list objects status: %d", resp.StatusCode)
	}
}
