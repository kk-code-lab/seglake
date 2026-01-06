package admin

import "net/http"

type responseWithReqID struct {
	http.ResponseWriter
	reqID  string
	status int
}

func (w *responseWithReqID) WriteHeader(code int) {
	w.status = code
	if w.reqID != "" {
		w.Header().Set("X-Request-Id", w.reqID)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWithReqID) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
		if w.reqID != "" {
			w.Header().Set("X-Request-Id", w.reqID)
		}
	}
	return w.ResponseWriter.Write(p)
}
