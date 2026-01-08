package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/meta"
)

const (
	lockFilename         = ".seglake.lock"
	heartbeatInterval    = 5 * time.Second
	heartbeatStaleAfter  = 15 * time.Second
	heartbeatFileMode    = 0o644
	heartbeatTempSuffix  = ".tmp"
	heartbeatPromptLabel = "Continue anyway? [y/N] "
)

type serverHeartbeat struct {
	PID            int       `json:"pid"`
	Addr           string    `json:"addr"`
	AdminSocket    string    `json:"admin_socket"`
	AdminTokenPath string    `json:"admin_token_path"`
	StartedAt      time.Time `json:"started_at"`
	HeartbeatAt    time.Time `json:"heartbeat_at"`
	Version        string    `json:"version"`
	Commit         string    `json:"commit"`
}

type heartbeatStatus struct {
	Path    string
	ModTime time.Time
	Fresh   bool
	Data    serverHeartbeat
	HasData bool
}

type serverLock struct {
	path           string
	started        time.Time
	addr           string
	adminSocket    string
	adminTokenPath string
	stop           chan struct{}
	done           chan struct{}
	interval       time.Duration
}

func lockPath(dataDir string) string {
	return filepath.Join(dataDir, lockFilename)
}

func readHeartbeatStatus(dataDir string) (heartbeatStatus, error) {
	path := lockPath(dataDir)
	info, err := os.Stat(path)
	if err != nil {
		return heartbeatStatus{}, err
	}
	status := heartbeatStatus{
		Path:    path,
		ModTime: info.ModTime(),
	}
	status.Fresh = time.Since(info.ModTime()) <= heartbeatStaleAfter
	file, err := os.Open(path)
	if err == nil {
		defer func() { _ = file.Close() }()
		if err := json.NewDecoder(file).Decode(&status.Data); err == nil {
			status.HasData = true
		}
	}
	return status, nil
}

func acquireServerLock(dataDir, addr, adminSocketPath, adminTokenPath string) (*serverLock, error) {
	if err := ensureDataDir(dataDir); err != nil {
		return nil, err
	}
	path := lockPath(dataDir)
	if status, err := readHeartbeatStatus(dataDir); err == nil {
		if status.Fresh {
			return nil, formatLockConflict(status)
		}
		_ = os.Remove(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	lockFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, heartbeatFileMode)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			if status, err := readHeartbeatStatus(dataDir); err == nil && status.Fresh {
				return nil, formatLockConflict(status)
			}
		}
		return nil, err
	}
	_ = lockFile.Close()
	lock := &serverLock{
		path:           path,
		started:        time.Now().UTC(),
		addr:           addr,
		adminSocket:    adminSocketPath,
		adminTokenPath: adminTokenPath,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		interval:       heartbeatInterval,
	}
	if err := lock.writeHeartbeat(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	go lock.loop()
	return lock, nil
}

func (l *serverLock) loop() {
	ticker := time.NewTicker(l.interval)
	defer func() {
		ticker.Stop()
		close(l.done)
	}()
	for {
		select {
		case <-ticker.C:
			_ = l.writeHeartbeat()
		case <-l.stop:
			return
		}
	}
}

func (l *serverLock) writeHeartbeat() error {
	if l == nil || l.path == "" {
		return nil
	}
	payload := serverHeartbeat{
		PID:            os.Getpid(),
		Addr:           l.addr,
		AdminSocket:    l.adminSocket,
		AdminTokenPath: l.adminTokenPath,
		StartedAt:      l.started,
		HeartbeatAt:    time.Now().UTC(),
		Version:        app.Version,
		Commit:         app.BuildCommit,
	}
	data, err := json.Marshal(&payload)
	if err != nil {
		return err
	}
	tempPath := l.path + heartbeatTempSuffix
	if err := os.WriteFile(tempPath, data, heartbeatFileMode); err != nil {
		return err
	}
	return os.Rename(tempPath, l.path)
}

func (l *serverLock) Release() {
	if l == nil {
		return
	}
	close(l.stop)
	<-l.done
	_ = os.Remove(l.path)
}

func formatLockConflict(status heartbeatStatus) error {
	if status.HasData {
		return fmt.Errorf("server appears to be running (pid=%d addr=%s heartbeat=%s)",
			status.Data.PID,
			status.Data.Addr,
			status.ModTime.UTC().Format(time.RFC3339),
		)
	}
	return fmt.Errorf("server appears to be running (lock updated %s)",
		status.ModTime.UTC().Format(time.RFC3339),
	)
}

func isUnsafeLiveMode(mode string) bool {
	switch mode {
	case "rebuild-index", "gc-run", "gc-rewrite", "gc-rewrite-run", "mpu-gc-run", "repl-pull", "repl-push", "repl-bootstrap", "db-integrity-check", "db-reindex":
		return true
	default:
		return false
	}
}

func confirmLiveMode(dataDir, mode string, assumeYes bool) error {
	if dataDir == "" || mode == "" {
		return nil
	}
	status, err := readHeartbeatStatus(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !status.Fresh {
		return nil
	}
	if !isUnsafeLiveMode(mode) {
		return nil
	}
	if hasAdminChannel(dataDir, status) {
		return nil
	}
	if allowsUnsafeInMaintenance(mode) {
		if state, err := maintenanceState(dataDir); err == nil && state == "quiesced" {
			return nil
		}
	}
	if assumeYes {
		if status.HasData {
			fmt.Fprintf(os.Stderr, "seglake: detected running server for %s (pid=%d addr=%s); proceeding due to -yes\n", dataDir, status.Data.PID, status.Data.Addr)
		} else {
			fmt.Fprintf(os.Stderr, "seglake: detected running server for %s; proceeding due to -yes\n", dataDir)
		}
		return nil
	}
	if !isTerminal(os.Stdin) {
		return fmt.Errorf("server appears to be running for %s; stop it or re-run with -yes", dataDir)
	}
	if status.HasData {
		fmt.Fprintf(os.Stderr, "seglake: detected running server for %s (pid=%d addr=%s last_heartbeat=%s)\n",
			dataDir, status.Data.PID, status.Data.Addr, status.ModTime.UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintf(os.Stderr, "seglake: detected running server for %s (lock updated %s)\n",
			dataDir, status.ModTime.UTC().Format(time.RFC3339))
	}
	fmt.Fprint(os.Stderr, heartbeatPromptLabel)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	switch strings.TrimSpace(strings.ToLower(line)) {
	case "y", "yes":
		return nil
	default:
		return ErrAbortedByUser
	}
}

func hasAdminChannel(dataDir string, status heartbeatStatus) bool {
	if !status.Fresh {
		return false
	}
	socketPath := ""
	tokenPath := ""
	if status.HasData {
		socketPath = status.Data.AdminSocket
		tokenPath = status.Data.AdminTokenPath
	}
	if socketPath == "" {
		socketPath = defaultAdminSocketPath(dataDir)
	}
	if tokenPath == "" {
		tokenPath = defaultAdminTokenPath(dataDir)
	}
	if _, err := os.Stat(socketPath); err != nil {
		return false
	}
	if _, err := os.Stat(tokenPath); err != nil {
		return false
	}
	return true
}

func maintenanceState(dataDir string) (string, error) {
	store, err := meta.Open(filepath.Join(dataDir, "meta.db"))
	if err != nil {
		return "", err
	}
	defer func() { _ = store.Close() }()
	state, err := store.MaintenanceState(context.Background())
	if err != nil {
		return "", err
	}
	return state.State, nil
}

func allowsUnsafeInMaintenance(mode string) bool {
	switch mode {
	case "gc-run", "gc-rewrite", "gc-rewrite-run", "mpu-gc-run":
		return true
	default:
		return false
	}
}

func isTerminal(file *os.File) bool {
	if file == nil {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
