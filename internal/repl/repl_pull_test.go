package repl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
)

func newMetadataOnlyReplServer(t *testing.T, entry meta.OplogEntry) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var manifestCalls int32
	var chunkCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			resp := replOplogResponse{
				Entries: []meta.OplogEntry{entry},
				LastHLC: entry.HLCTS,
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/manifest":
			atomic.AddInt32(&manifestCalls, 1)
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/chunk":
			atomic.AddInt32(&chunkCalls, 1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, &manifestCalls, &chunkCalls
}
