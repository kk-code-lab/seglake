package s3

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteErrorWithResourceOverridesStatus(t *testing.T) {
	w := httptest.NewRecorder()
	writeErrorWithResource(w, http.StatusOK, "NoSuchKey", "", "req", "/bucket/key")
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestWriteErrorWithResourceDefaultMessage(t *testing.T) {
	w := httptest.NewRecorder()
	writeErrorWithResource(w, http.StatusOK, "SlowDown", "", "req", "/bucket/key")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
	if got := w.Body.String(); got == "" || !containsString(got, "SlowDown") {
		t.Fatalf("expected SlowDown in body: %s", got)
	}
}

func TestWriteErrorWithResourceSetsRequestHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "", "req-123", "/bucket/key")
	if got := w.Header().Get("x-amz-request-id"); got != "req-123" {
		t.Fatalf("expected x-amz-request-id to be set, got %q", got)
	}
	if got := w.Header().Get("x-amz-id-2"); got == "" {
		t.Fatalf("expected x-amz-id-2 to be set")
	}
}

func containsString(s, sub string) bool {
	return strings.Contains(s, sub)
}
