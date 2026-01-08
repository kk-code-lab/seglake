package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/kk-code-lab/seglake/internal/admin"
)

const (
	adminSocketName = ".seglake-admin.sock"
	adminTokenName  = ".seglake-admin.token"
)

type adminClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func adminClientIfRunning(dataDir string) (*adminClient, bool, error) {
	if dataDir == "" {
		return nil, false, nil
	}
	status, err := readHeartbeatStatus(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !status.Fresh {
		return nil, false, nil
	}
	socketPath := status.Data.AdminSocket
	if socketPath == "" {
		socketPath = filepath.Join(dataDir, adminSocketName)
	}
	tokenPath := status.Data.AdminTokenPath
	if tokenPath == "" {
		tokenPath = filepath.Join(dataDir, adminTokenName)
	}
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, false, err
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, false, fmt.Errorf("admin token missing")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   0,
	}
	return &adminClient{
		baseURL:    "http://unix",
		token:      token,
		httpClient: client,
	}, true, nil
}

func (c *adminClient) postJSON(path string, reqPayload any, respPayload any) error {
	if c == nil {
		return fmt.Errorf("admin client not initialized")
	}
	body, err := json.Marshal(reqPayload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(admin.TokenHeader(), c.token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		var errPayload map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		if msg := errPayload["error"]; msg != "" {
			return fmt.Errorf("admin error: %s", msg)
		}
		return fmt.Errorf("admin request failed: %s", resp.Status)
	}
	if respPayload == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(respPayload)
}

func defaultAdminSocketPath(dataDir string) string {
	return filepath.Join(dataDir, adminSocketName)
}

func defaultAdminTokenPath(dataDir string) string {
	return filepath.Join(dataDir, adminTokenName)
}
