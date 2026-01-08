package admin

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoggingMiddlewareRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	logger := log.Default()
	oldOut := logger.Writer()
	logger.SetOutput(&buf)
	defer logger.SetOutput(oldOut)

	payload := `{"action":"create","access_key":"a","secret_key":"secret","policy":"rw"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader(payload))
	req.Header.Set(TokenHeader(), "token")

	w := httptest.NewRecorder()
	LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}), nil).ServeHTTP(w, req)

	logOutput := buf.String()
	if strings.Contains(logOutput, "secret") {
		t.Fatalf("expected secret to be redacted, got log: %s", logOutput)
	}
}
