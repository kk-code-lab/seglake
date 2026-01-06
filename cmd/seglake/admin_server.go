package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/admin"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
)

func startAdminServer(ctx context.Context, dataDir, addr string, store *meta.Store, eng *engine.Engine, writeInflight func() int64) (*http.Server, net.Listener, string, string, error) {
	socketPath := defaultAdminSocketPath(dataDir)
	tokenPath := defaultAdminTokenPath(dataDir)
	token, err := writeAdminToken(tokenPath)
	if err != nil {
		return nil, nil, "", "", err
	}
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, "", "", err
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		_ = os.Remove(tokenPath)
		return nil, nil, "", "", err
	}
	_ = os.Chmod(socketPath, 0o600)
	handler := &admin.Handler{
		DataDir:       dataDir,
		Addr:          addr,
		Meta:          store,
		Engine:        eng,
		AuthToken:     token,
		WriteInflight: writeInflight,
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = server.Serve(ln)
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return server, ln, socketPath, tokenPath, nil
}

func writeAdminToken(path string) (string, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	return token, nil
}
