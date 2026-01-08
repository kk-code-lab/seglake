package s3

import (
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/kk-code-lab/seglake/internal/clock"
)

// LoggingMiddleware logs request method/path/status/latency with request-id.
func LoggingMiddleware(next http.Handler, clk clock.Clock) http.Handler {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := clk.Now()
		lw := &loggingWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(lw, r)
		end := clk.Now()
		reqID := w.Header().Get("x-amz-request-id")
		log.Printf("method=%s path=%s status=%d dur_ms=%d req_id=%s", r.Method, redactURL(r.URL), lw.status, end.Sub(start).Milliseconds(), reqID)
	})
}

type loggingWriter struct {
	http.ResponseWriter
	status int
}

func (w *loggingWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	if u.RawQuery == "" {
		return u.Path
	}
	q := u.Query()
	for _, key := range []string{
		"X-Amz-Signature",
		"X-Amz-Credential",
		"X-Amz-Security-Token",
		"X-Amz-SignedHeaders",
	} {
		if q.Has(key) {
			q.Set(key, "REDACTED")
		}
	}
	u2 := *u
	u2.RawQuery = q.Encode()
	return u2.Path + "?" + u2.RawQuery
}
