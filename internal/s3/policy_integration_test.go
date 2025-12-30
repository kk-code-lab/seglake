package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func newPolicyHandler(t *testing.T, policy string) *Handler {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	if err := store.UpsertAPIKey(context.Background(), "ak", "sk", policy, true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	return &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			Region:               "us-east-1",
			SecretLookup:         store.LookupAPISecret,
			AllowUnsignedPayload: true,
		},
	}
}

func doRequest(t *testing.T, handler *Handler, req *http.Request) *http.Response {
	t.Helper()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w.Result()
}

func newTestRequest(method, path string, body io.Reader) *http.Request {
	return httptest.NewRequest(method, path, body)
}

func TestPolicyEnforcedOnRequests(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}]}]}`
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	getReq := newTestRequest(http.MethodGet, "/demo/public/ok", nil)
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	getResp := doRequest(t, handler, getReq)
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}

	putReq := newTestRequest(http.MethodPut, "/demo/public/new", bytes.NewReader([]byte("nope")))
	signRequestTest(putReq, "ak", "sk", "us-east-1")
	putResp := doRequest(t, handler, putReq)
	_ = putResp.Body.Close()
	if putResp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}
}

func TestPolicyDenyPrefixOverridesAllow(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]},{"effect":"deny","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "secret/x", "", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	getReq := newTestRequest(http.MethodGet, "/demo/secret/x", nil)
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, getReq)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
}

func TestPolicyMPUDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, policy)

	req := newTestRequest(http.MethodPost, "/demo/key?uploads", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("MPU initiate status: %d", resp.StatusCode)
	}
}

func TestPolicyCopyDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "src", "", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req := newTestRequest(http.MethodPut, "/demo/dst", nil)
	req.Header.Set("X-Amz-Copy-Source", "/demo/src")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("COPY status: %d", resp.StatusCode)
	}
}

func TestPolicyMetaDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, policy)

	req := newTestRequest(http.MethodGet, "/v1/meta/stats", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("meta stats status: %d", resp.StatusCode)
	}
}

func TestPolicyListBucketsDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, policy)

	req := newTestRequest(http.MethodGet, "/", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("list buckets status: %d", resp.StatusCode)
	}
}

func TestPolicyListObjectsDenied(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, policy)

	req := newTestRequest(http.MethodGet, "/demo?list-type=2", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("list objects status: %d", resp.StatusCode)
	}
}

func TestListBucketsHonorsAllowlist(t *testing.T) {
	handler := newPolicyHandler(t, "rw")

	ctx := context.Background()
	if _, _, err := handler.Engine.PutObject(ctx, "xsxs-terraform", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed xsxs-terraform: %v", err)
	}
	if _, _, err := handler.Engine.PutObject(ctx, "foo", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed foo: %v", err)
	}
	if err := handler.Meta.AllowBucketForKey(ctx, "ak", "xsxs-terraform"); err != nil {
		t.Fatalf("AllowBucketForKey: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list buckets status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<Name>xsxs-terraform</Name>")) {
		t.Fatalf("expected xsxs-terraform in list")
	}
	if bytes.Contains(body, []byte("<Name>foo</Name>")) {
		t.Fatalf("unexpected foo in list")
	}
}

func TestPolicyConditionsHeadersEnforced(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"headers":{"x-tenant":"alpha"}}}]}`
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/demo/public/ok", nil)
	req.Header.Set("X-Tenant", "alpha")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}

	req2 := newTestRequest(http.MethodGet, "/demo/public/ok", nil)
	req2.Header.Set("X-Tenant", "beta")
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2 := doRequest(t, handler, req2)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp2.StatusCode)
	}
}

func TestGetBucketPolicy(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, "rw")

	ctx := context.Background()
	if _, _, err := handler.Engine.PutObject(ctx, "demo", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}
	if err := handler.Meta.SetBucketPolicy(ctx, "demo", policy); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/demo?policy", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get bucket policy status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	var want map[string]any
	if err := json.Unmarshal([]byte(policy), &want); err != nil {
		t.Fatalf("Unmarshal policy: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected policy body")
	}
}

func TestGetBucketPolicyMissing(t *testing.T) {
	handler := newPolicyHandler(t, "rw")

	ctx := context.Background()
	if _, _, err := handler.Engine.PutObject(ctx, "demo", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/demo?policy", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get bucket policy status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<Code>NoSuchBucketPolicy</Code>")) {
		t.Fatalf("expected NoSuchBucketPolicy")
	}
}

func TestPutDeleteBucketPolicy(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}]}]}`
	handler := newPolicyHandler(t, "rw")

	ctx := context.Background()
	if _, _, err := handler.Engine.PutObject(ctx, "demo", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	putReq := newTestRequest(http.MethodPut, "/demo?policy", bytes.NewReader([]byte(policy)))
	signRequestTest(putReq, "ak", "sk", "us-east-1")
	putResp := doRequest(t, handler, putReq)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("put bucket policy status: %d", putResp.StatusCode)
	}

	getReq := newTestRequest(http.MethodGet, "/demo?policy", nil)
	signRequestTest(getReq, "ak", "sk", "us-east-1")
	getResp := doRequest(t, handler, getReq)
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get bucket policy status: %d", getResp.StatusCode)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	var want map[string]any
	if err := json.Unmarshal([]byte(policy), &want); err != nil {
		t.Fatalf("Unmarshal policy: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected policy body")
	}

	delReq := newTestRequest(http.MethodDelete, "/demo?policy", nil)
	signRequestTest(delReq, "ak", "sk", "us-east-1")
	delResp := doRequest(t, handler, delReq)
	defer func() { _ = delResp.Body.Close() }()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete bucket policy status: %d", delResp.StatusCode)
	}

	missingReq := newTestRequest(http.MethodGet, "/demo?policy", nil)
	signRequestTest(missingReq, "ak", "sk", "us-east-1")
	missingResp := doRequest(t, handler, missingReq)
	defer func() { _ = missingResp.Body.Close() }()
	if missingResp.StatusCode != http.StatusNotFound {
		t.Fatalf("get bucket policy status: %d", missingResp.StatusCode)
	}
}

func TestPutBucketPolicyInvalid(t *testing.T) {
	handler := newPolicyHandler(t, "rw")

	ctx := context.Background()
	if _, _, err := handler.Engine.PutObject(ctx, "demo", "obj", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	putReq := newTestRequest(http.MethodPut, "/demo?policy", bytes.NewReader([]byte("not-json")))
	signRequestTest(putReq, "ak", "sk", "us-east-1")
	putResp := doRequest(t, handler, putReq)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("put bucket policy status: %d", putResp.StatusCode)
	}
	body, err := io.ReadAll(putResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<Code>InvalidArgument</Code>")) {
		t.Fatalf("expected InvalidArgument")
	}
}

func TestPolicyConditionsTimeWindowEnforced(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"after":"1970-01-01T00:00:00Z","before":"2999-01-01T00:00:00Z"}}]}`
	if pol, err := ParsePolicy(policy); err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	} else if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "public/ok", &PolicyContext{Now: time.Now().UTC()}); !allowed {
		t.Fatalf("expected policy to allow in time window")
	}
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/demo/public/ok", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}

	policy2 := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"public/"}],"conditions":{"before":"1970-01-01T00:00:00Z"}}]}`
	handler2 := newPolicyHandler(t, policy2)

	if _, _, err := handler2.Engine.PutObject(context.Background(), "demo", "public/ok", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}
	req2 := newTestRequest(http.MethodGet, "/demo/public/ok", nil)
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2 := doRequest(t, handler2, req2)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp2.StatusCode)
	}
}

func TestPolicyConditionsAfterFutureDenied(t *testing.T) {
	future := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"after":"` + future + `"}}]}`
	handler := newPolicyHandler(t, policy)

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "k", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}
	req := newTestRequest(http.MethodGet, "/demo/k", nil)
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
}

func TestPolicyConditionsSourceIPMultipleCIDR(t *testing.T) {
	policy := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}],"conditions":{"source_ip":["10.0.0.0/8","192.168.0.0/16"]}}]}`
	handler := newPolicyHandler(t, policy)
	handler.TrustedProxies = []string{"127.0.0.1/32"}

	if _, _, err := handler.Engine.PutObject(context.Background(), "demo", "k", "", bytes.NewReader([]byte("ok"))); err != nil {
		t.Fatalf("PutObject seed: %v", err)
	}

	req := newTestRequest(http.MethodGet, "/demo?list-type=2", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "192.168.1.10")
	signRequestTest(req, "ak", "sk", "us-east-1")
	resp := doRequest(t, handler, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list objects status: %d", resp.StatusCode)
	}

	req2 := newTestRequest(http.MethodGet, "/demo?list-type=2", nil)
	req2.RemoteAddr = "127.0.0.1:1234"
	req2.Header.Set("X-Forwarded-For", "172.16.0.1")
	signRequestTest(req2, "ak", "sk", "us-east-1")
	resp2 := doRequest(t, handler, req2)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("list objects status: %d", resp2.StatusCode)
	}
}
