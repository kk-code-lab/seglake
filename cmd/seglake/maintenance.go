package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

type maintenanceOptions struct {
	dataDir string
	action  string
	jsonOut bool
	noWait  bool
}

func newMaintenanceFlagSet() (*flag.FlagSet, *maintenanceOptions) {
	fs := flag.NewFlagSet("maintenance", flag.ContinueOnError)
	opts := &maintenanceOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.action, "maintenance-action", "status", "Maintenance action: status|enable|disable")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output maintenance status as JSON")
	fs.BoolVar(&opts.noWait, "maintenance-no-wait", false, "Do not wait for quiesced/off when enabling/disabling maintenance")
	return fs, opts
}

func runMaintenance(opts *maintenanceOptions) error {
	if opts == nil {
		return fmt.Errorf("maintenance options required")
	}
	if err := requireDataDir(opts.dataDir); err != nil {
		return err
	}
	store, err := meta.Open(filepath.Join(opts.dataDir, "meta.db"))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	action := strings.ToLower(strings.TrimSpace(opts.action))
	var state meta.MaintenanceState
	running := false
	addr := ""
	if status, err := readHeartbeatStatus(opts.dataDir); err == nil && status.Fresh {
		running = true
		addr = status.Data.Addr
	}
	switch action {
	case "status", "get", "show":
		state, err = store.MaintenanceState(context.Background())
	case "enable", "on", "true":
		if running {
			state, err = store.SetMaintenanceState(context.Background(), "entering")
		} else {
			state, err = store.SetMaintenanceState(context.Background(), "quiesced")
		}
	case "disable", "off", "false":
		if running {
			state, err = store.SetMaintenanceState(context.Background(), "exiting")
		} else {
			state, err = store.SetMaintenanceState(context.Background(), "off")
		}
	default:
		return fmt.Errorf("maintenance action must be status|enable|disable")
	}
	if err != nil {
		return err
	}
	if running && !opts.noWait {
		switch action {
		case "enable":
			state, err = waitForMaintenanceQuiesced(store)
			if err != nil {
				return err
			}
		case "disable":
			state, err = waitForMaintenanceOff(store)
			if err != nil {
				return err
			}
		}
	}
	writeInflight := int64(0)
	statsState := ""
	statsUpdatedAt := ""
	if running {
		if stats, err := maintenanceStats(addr); err == nil {
			statsState = stats.MaintenanceState
			statsUpdatedAt = stats.MaintenanceUpdatedAt
			writeInflight = stats.WriteInflight
		}
	}
	if opts.jsonOut {
		payload := map[string]any{
			"maintenance":          state.State,
			"maintenance_updated":  state.UpdatedAt,
			"running":              running,
			"addr":                 addr,
			"write_inflight":       writeInflight,
			"server_state":         statsState,
			"server_state_updated": statsUpdatedAt,
		}
		return writeJSON(payload)
	}
	if state.UpdatedAt != "" {
		fmt.Printf("maintenance=%s updated_at=%s running=%t addr=%s write_inflight=%d server_state=%s server_state_updated=%s\n",
			state.State, state.UpdatedAt, running, addr, writeInflight, statsState, statsUpdatedAt)
		return nil
	}
	fmt.Printf("maintenance=%s running=%t addr=%s write_inflight=%d server_state=%s server_state_updated=%s\n",
		state.State, running, addr, writeInflight, statsState, statsUpdatedAt)
	return nil
}

func waitForMaintenanceQuiesced(store *meta.Store) (meta.MaintenanceState, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := store.MaintenanceState(context.Background())
		if err != nil {
			return state, err
		}
		switch state.State {
		case "quiesced":
			return state, nil
		case "entering":
			<-ticker.C
			continue
		default:
			return state, fmt.Errorf("maintenance enable interrupted (state=%s)", state.State)
		}
	}
}

func waitForMaintenanceOff(store *meta.Store) (meta.MaintenanceState, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := store.MaintenanceState(context.Background())
		if err != nil {
			return state, err
		}
		switch state.State {
		case "off":
			return state, nil
		case "exiting":
			<-ticker.C
			continue
		default:
			return state, fmt.Errorf("maintenance disable interrupted (state=%s)", state.State)
		}
	}
}

type maintenanceStatsResponse struct {
	MaintenanceState     string `json:"maintenance_state"`
	MaintenanceUpdatedAt string `json:"maintenance_updated_at"`
	WriteInflight        int64  `json:"write_inflight"`
}

func maintenanceStats(addr string) (maintenanceStatsResponse, error) {
	if addr == "" {
		return maintenanceStatsResponse{}, fmt.Errorf("missing server addr")
	}
	url := normalizeStatsURL(addr)
	access := envOrDefault("SEGLAKE_ACCESS_KEY", "")
	secret := envOrDefault("SEGLAKE_SECRET_KEY", "")
	region := envOrDefault("SEGLAKE_REGION", "us-east-1")
	if access != "" || secret != "" {
		auth := &s3.AuthConfig{AccessKey: access, SecretKey: secret, Region: region}
		signed, err := auth.Presign(http.MethodGet, url, 5*time.Minute)
		if err != nil {
			return maintenanceStatsResponse{}, err
		}
		url = signed
	}
	resp, err := http.Get(url)
	if err != nil {
		return maintenanceStatsResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return maintenanceStatsResponse{}, fmt.Errorf("stats request failed: %s", resp.Status)
	}
	var out maintenanceStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return maintenanceStatsResponse{}, err
	}
	return out, nil
}

func normalizeStatsURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		if strings.HasPrefix(addr, ":") {
			addr = "http://127.0.0.1" + addr
		} else {
			addr = "http://" + addr
		}
	}
	addr = strings.TrimSuffix(addr, "/")
	return addr + "/v1/meta/stats"
}
