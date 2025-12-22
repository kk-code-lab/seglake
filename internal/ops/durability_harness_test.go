//go:build durability

package ops

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDurabilityAfterCrash(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	dataDir := t.TempDir()
	bin := filepath.Join(t.TempDir(), "seglake")

	build := exec.Command("go", "build", "-o", bin, "./cmd/seglake")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, string(out))
	}

	addr, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	host := "http://" + addr

	cmd := startServer(t, bin, dataDir, addr)
	if err := waitForStats(host, 2*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("server did not start: %v", err)
	}

	putURL := host + "/demo/durable.txt"
	resp, err := http.Post(putURL, "application/octet-stream", bytes.NewReader([]byte("durable")))
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("PUT error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = cmd.Process.Kill()
		t.Fatalf("PUT status: %d", resp.StatusCode)
	}

	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()

	cmd = startServer(t, bin, dataDir, addr)
	if err := waitForStats(host, 2*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("server did not restart: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	getResp, err := http.Get(putURL)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(getResp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if buf.String() != "durable" {
		t.Fatalf("body mismatch: %q", buf.String())
	}
}

func pickFreePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr, nil
}

func startServer(t *testing.T, bin, dataDir, addr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin,
		"-addr", addr,
		"-data-dir", dataDir,
		"-sync-interval", "5ms",
		"-sync-bytes", "1048576",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	return cmd
}

func waitForStats(host string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := host + "/v1/meta/stats"
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for stats")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
