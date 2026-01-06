package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
