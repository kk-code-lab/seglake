//go:build e2e

package s3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestE2EMaintenanceOpsFlow(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	if err := store.UpsertAPIKey(context.Background(), "ops", "opsecret", "ops", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey ops: %v", err)
	}
	if err := store.UpsertAPIKey(context.Background(), "rw", "rwsecret", "rw", true, 0); err != nil {
		t.Fatalf("UpsertAPIKey rw: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			SecretLookup:         store.LookupAPISecret,
			MaxSkew:              5 * time.Minute,
		},
		DataDir: filepath.Join(dir),
	}
	maintCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handler.RunMaintenanceLoop(maintCtx, 10*time.Millisecond)

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/obj"
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(putReq, "rw", "rwsecret", "us-east-1")
	if resp, err := http.DefaultClient.Do(putReq); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status=%d err=%v", resp.StatusCode, err)
	}

	if _, err := store.SetMaintenanceState(context.Background(), "entering"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		state, err := store.MaintenanceState(context.Background())
		if err != nil {
			t.Fatalf("MaintenanceState: %v", err)
		}
		if state.State == "quiesced" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("maintenance did not reach quiesced")
		}
		time.Sleep(10 * time.Millisecond)
	}

	blockedReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/blocked", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("NewRequest blocked: %v", err)
	}
	signRequest(blockedReq, "rw", "rwsecret", "us-east-1")
	if resp, err := http.DefaultClient.Do(blockedReq); err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("blocked PUT status=%d err=%v", resp.StatusCode, err)
	}

	opsReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/ops/run", bytes.NewReader([]byte(`{"mode":"gc-run","gc_force":true}`)))
	if err != nil {
		t.Fatalf("NewRequest ops: %v", err)
	}
	signRequest(opsReq, "ops", "opsecret", "us-east-1")
	if resp, err := http.DefaultClient.Do(opsReq); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ops run status=%d err=%v", resp.StatusCode, err)
	}

	if _, err := store.SetMaintenanceState(context.Background(), "exiting"); err != nil {
		t.Fatalf("SetMaintenanceState exiting: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		state, err := store.MaintenanceState(context.Background())
		if err != nil {
			t.Fatalf("MaintenanceState: %v", err)
		}
		if state.State == "off" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("maintenance did not reach off")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
