package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceModeBlocksWrites(t *testing.T) {
	h := newTestHandler(t)
	putObject(t, h, "bucket", "key", "data")

	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		header map[string]string
	}{
		{name: "put_object", method: http.MethodPut, path: "/bucket/other", body: "x"},
		{name: "delete_object", method: http.MethodDelete, path: "/bucket/key"},
		{name: "create_bucket", method: http.MethodPut, path: "/newbucket"},
		{name: "delete_bucket", method: http.MethodDelete, path: "/bucket"},
		{
			name:   "copy_object",
			method: http.MethodPut,
			path:   "/bucket/copied",
			header: map[string]string{"X-Amz-Copy-Source": "/bucket/key"},
		},
		{
			name:   "bucket_policy_put",
			method: http.MethodPut,
			path:   "/bucket?policy",
			body:   `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"bucket"}]}]}`,
		},
		{name: "bucket_policy_delete", method: http.MethodDelete, path: "/bucket?policy"},
		{name: "mpu_initiate", method: http.MethodPost, path: "/bucket/key?uploads"},
		{name: "mpu_upload_part", method: http.MethodPut, path: "/bucket/key?uploadId=upload1&partNumber=1", body: "part"},
		{name: "mpu_complete", method: http.MethodPost, path: "/bucket/key?uploadId=upload1", body: "<CompleteMultipartUpload/>"},
		{name: "mpu_abort", method: http.MethodDelete, path: "/bucket/key?uploadId=upload1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status: %d", w.Code)
			}
		})
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET status: %d", getW.Code)
	}
}

func TestMaintenanceLoopTransitions(t *testing.T) {
	h := newTestHandler(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.RunMaintenanceLoop(ctx, 10*time.Millisecond)

	if _, err := h.Meta.SetMaintenanceState(context.Background(), "entering"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	deadline := time.After(500 * time.Millisecond)
	for {
		state, err := h.Meta.MaintenanceState(context.Background())
		if err != nil {
			t.Fatalf("MaintenanceState: %v", err)
		}
		if state.State == "quiesced" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("maintenance did not reach quiesced, got %q", state.State)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if _, err := h.Meta.SetMaintenanceState(context.Background(), "exiting"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	deadline = time.After(500 * time.Millisecond)
	for {
		state, err := h.Meta.MaintenanceState(context.Background())
		if err != nil {
			t.Fatalf("MaintenanceState: %v", err)
		}
		if state.State == "off" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("maintenance did not reach off, got %q", state.State)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestOpsRunRequiresQuiesced(t *testing.T) {
	h := newTestHandler(t)
	reqBody := `{"mode":"status"}`

	req := httptest.NewRequest(http.MethodPost, "/v1/ops/run", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected maintenance check, got %d", w.Code)
	}

	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/ops/run", strings.NewReader(reqBody))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func TestOpsRunGCRunInQuiesced(t *testing.T) {
	h := newTestHandler(t)
	if err := h.Meta.UpsertAPIKey(context.Background(), "ops", "secret", "ops", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	h.Auth = &AuthConfig{
		Region:               "us-east-1",
		AllowUnsignedPayload: true,
		SecretLookup:         h.Meta.LookupAPISecret,
	}
	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	reqBody := `{"mode":"gc-run","gc_force":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ops/run", strings.NewReader(reqBody))
	req = presignRequest(t, h, req, "ops", "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func TestOpsRunDeniedForRWPolicy(t *testing.T) {
	h := newTestHandler(t)
	if err := h.Meta.UpsertAPIKey(context.Background(), "rw", "secret", "rw", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	h.Auth = &AuthConfig{
		Region:               "us-east-1",
		AllowUnsignedPayload: true,
		SecretLookup:         h.Meta.LookupAPISecret,
	}
	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	reqBody := `{"mode":"status"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ops/run", strings.NewReader(reqBody))
	req = presignRequest(t, h, req, "rw", "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d", w.Code)
	}
}

func TestOpsRunAllowedForServerKey(t *testing.T) {
	h := newTestHandler(t)
	if err := h.Meta.UpsertAPIKey(context.Background(), "other", "secret", "rw", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	h.Auth = &AuthConfig{
		OpsAccessKey:         "ops",
		OpsSecretKey:         "opssecret",
		Region:               "us-east-1",
		AllowUnsignedPayload: true,
	}
	if _, err := h.Meta.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	reqBody := `{"mode":"status"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ops/run", strings.NewReader(reqBody))
	req = presignRequest(t, h, req, "ops", "opssecret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", w.Code)
	}
}

func presignRequest(t *testing.T, h *Handler, r *http.Request, accessKey, secretKey string) *http.Request {
	t.Helper()
	if h == nil || h.Auth == nil {
		t.Fatalf("handler auth not configured")
	}
	reqURL := url.URL{
		Scheme: "http",
		Host:   "localhost",
		Path:   r.URL.Path,
	}
	if r.URL.RawQuery != "" {
		reqURL.RawQuery = r.URL.RawQuery
	}
	auth := &AuthConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    h.Auth.Region,
	}
	signed, err := auth.Presign(http.MethodPost, reqURL.String(), 5*time.Minute)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	parsed, err := url.Parse(signed)
	if err != nil {
		t.Fatalf("parse presigned: %v", err)
	}
	r.Host = reqURL.Host
	r.URL.Host = reqURL.Host
	r.URL.Scheme = reqURL.Scheme
	r.URL.RawQuery = parsed.RawQuery
	r.Header.Set("Authorization", "")
	return r
}
