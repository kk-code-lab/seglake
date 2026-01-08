package admin

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/kk-code-lab/seglake/internal/clock"
)

func LoggingMiddleware(next http.Handler, clk clock.Clock) http.Handler {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			next.ServeHTTP(w, r)
			return
		}
		start := clk.Now()
		body, summary := readAndSummarizeBody(r)
		if body != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		reqID := newRequestID()
		rw := &responseWithReqID{ResponseWriter: w, reqID: reqID, status: http.StatusOK}
		next.ServeHTTP(rw, r.WithContext(withRequestID(r.Context(), reqID)))
		end := clk.Now()
		log.Printf("admin method=%s path=%s status=%d dur_ms=%d req_id=%s %s", r.Method, r.URL.Path, rw.status, end.Sub(start).Milliseconds(), reqID, summary)
	})
}

func readAndSummarizeBody(r *http.Request) ([]byte, string) {
	if r == nil || r.Body == nil {
		return nil, "summary=none"
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		return nil, "summary=read_error"
	}
	if len(data) == 0 {
		return data, "summary=empty"
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return data, "summary=non_json"
	}
	redactPayload(payload)
	parts := make([]string, 0, 4)
	if action, ok := payloadString(payload, "action"); ok {
		parts = append(parts, "action="+action)
	}
	if mode, ok := payloadString(payload, "mode"); ok {
		parts = append(parts, "mode="+mode)
	}
	if bucket, ok := payloadString(payload, "bucket"); ok {
		parts = append(parts, "bucket="+bucket)
	}
	if remote, ok := payloadString(payload, "remote"); ok {
		parts = append(parts, "remote="+remote)
	}
	if len(parts) == 0 {
		return data, "summary=json"
	}
	return data, "summary=" + strings.Join(parts, ",")
}

func redactPayload(payload map[string]any) {
	if payload == nil {
		return
	}
	if _, ok := payload["secret_key"]; ok {
		payload["secret_key"] = "REDACTED"
	}
	if _, ok := payload["token"]; ok {
		payload["token"] = "REDACTED"
	}
}

func payloadString(payload map[string]any, key string) (string, bool) {
	val, ok := payload[key]
	if !ok || val == nil {
		return "", false
	}
	switch v := val.(type) {
	case string:
		if v == "" {
			return "", false
		}
		return v, true
	default:
		return "", false
	}
}
