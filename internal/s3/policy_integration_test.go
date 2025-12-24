package s3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

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
			Region:               "us-east-1",
			SecretLookup:         store.LookupAPISecret,
			AllowUnsignedPayload: true,
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]},{"effect":"deny","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "secret/x", "", bytes.NewReader([]byte("x"))); err != nil {
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "src", "", bytes.NewReader([]byte("x"))); err != nil {
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
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
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
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

func TestPolicyConditionsHeadersEnforced(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"headers":{"x-tenant":"alpha"}}}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/demo/public/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Tenant", "alpha")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}

	req2, err := http.NewRequest(http.MethodGet, server.URL+"/demo/public/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req2.Header.Set("X-Tenant", "beta")
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp2.StatusCode)
	}
}

func TestPolicyConditionsTimeWindowEnforced(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"after":"1970-01-01T00:00:00Z","before":"2999-01-01T00:00:00Z"}}]}`
	if pol, err := ParsePolicy(policy); err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	} else if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "public/ok", &PolicyContext{Now: time.Now().UTC()}); !allowed {
		t.Fatalf("expected policy to allow in time window")
	}
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/demo/public/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}

	policy2 := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"before":"1970-01-01T00:00:00Z"}}]}`
	server2, handler2, cleanup2 := newPolicyServer(t, policy2)
	defer cleanup2()

	if _, _, err := handler2.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}
	req2, err := http.NewRequest(http.MethodGet, server2.URL+"/demo/public/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp2.StatusCode)
	}
}

func TestPolicyConditionsAfterFutureDenied(t *testing.T) {
	future := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"after":"` + future + `"}}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "k", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, server.URL+"/demo/k", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
}

func TestPolicyConditionsSourceIPMultipleCIDR(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}],"conditions":{"source_ip":["10.0.0.0/8","192.168.0.0/16"]}}]}`
	server, handler, cleanup := newPolicyServer(t, policy)
	defer cleanup()
	handler.TrustedProxies = []string{"127.0.0.1/32"}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/demo?list-type=2", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Forwarded-For", "192.168.1.10")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list objects error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list objects status: %d", resp.StatusCode)
	}

	req2, err := http.NewRequest(http.MethodGet, server.URL+"/demo?list-type=2", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req2.Header.Set("X-Forwarded-For", "172.16.0.1")
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("list objects error: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("list objects status: %d", resp2.StatusCode)
	}
}
