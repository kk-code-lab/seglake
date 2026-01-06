//go:build e2e

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestAdminSocketOpsAndKeys(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	handler := &Handler{
		DataDir:   dir,
		Meta:      store,
		Engine:    eng,
		AuthToken: "admin-token",
	}

	socketPath := filepath.Join(dir, "admin.sock")
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.Remove(socketPath)
	})
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
	}

	req, _ := http.NewRequest(http.MethodPost, "http://unix/admin/ops/run", bytes.NewReader([]byte(`{"mode":"status"}`)))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("admin request: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden without token, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	if _, err := store.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	req, _ = http.NewRequest(http.MethodPost, "http://unix/admin/ops/run", bytes.NewReader([]byte(`{"mode":"status"}`)))
	req.Header.Set(TokenHeader(), "admin-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("admin ops: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ops status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPost, "http://unix/admin/keys", bytes.NewReader([]byte(`{"action":"create","access_key":"ops","secret_key":"secret","policy":"ops","enabled":true}`)))
	req.Header.Set(TokenHeader(), "admin-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("admin keys create: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("keys create status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPost, "http://unix/admin/keys", bytes.NewReader([]byte(`{"action":"list"}`)))
	req.Header.Set(TokenHeader(), "admin-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("admin keys list: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("keys list status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestAdminSocketReplPullPush(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	handler := &Handler{
		DataDir:   dir,
		Meta:      store,
		Engine:    eng,
		AuthToken: "admin-token",
	}

	socketPath := filepath.Join(dir, "admin.sock")
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.Remove(socketPath)
	})
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	var getCalls int
	var postCalls int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/replication/oplog":
			getCalls++
			resp := map[string]any{
				"entries": []meta.OplogEntry{{
					SiteID:    "site-a",
					HLCTS:     "0000000000000000002-0000000001",
					OpType:    "put",
					Bucket:    "bucket",
					Key:       "key",
					VersionID: "v1",
				}},
				"last_hlc": "0000000000000000002-0000000001",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/replication/oplog":
			postCalls++
			var req struct {
				Entries []meta.OplogEntry `json:"entries"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]any{
				"applied": len(req.Entries),
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(remote.Close)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
	}

	pullBody := []byte(`{"remote":"` + remote.URL + `","fetch_data":false}`)
	req, _ := http.NewRequest(http.MethodPost, "http://unix/admin/repl/pull", bytes.NewReader(pullBody))
	req.Header.Set(TokenHeader(), "admin-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("admin repl pull: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repl pull status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	if getCalls == 0 || postCalls == 0 {
		t.Fatalf("expected repl pull to hit remote, get=%d post=%d", getCalls, postCalls)
	}

	if err := store.UpsertAPIKey(context.Background(), "local", "secret", "rw", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	pushBody := []byte(`{"remote":"` + remote.URL + `"}`)
	req, _ = http.NewRequest(http.MethodPost, "http://unix/admin/repl/push", bytes.NewReader(pushBody))
	req.Header.Set(TokenHeader(), "admin-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("admin repl push: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repl push status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	if postCalls < 2 {
		t.Fatalf("expected repl push to post, postCalls=%d", postCalls)
	}
}
