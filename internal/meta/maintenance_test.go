package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMaintenanceStateLifecycle(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	state, err := store.MaintenanceState(context.Background())
	if err != nil {
		t.Fatalf("MaintenanceState: %v", err)
	}
	if state.State != maintenanceStateOff {
		t.Fatalf("expected off, got %q", state.State)
	}

	state, err = store.SetMaintenanceState(context.Background(), "entering")
	if err != nil {
		t.Fatalf("SetMaintenanceState entering: %v", err)
	}
	if state.State != maintenanceStateEntering {
		t.Fatalf("expected entering, got %q", state.State)
	}

	state, err = store.SetMaintenanceState(context.Background(), "quiesced")
	if err != nil {
		t.Fatalf("SetMaintenanceState quiesced: %v", err)
	}
	if state.State != maintenanceStateQuiesced {
		t.Fatalf("expected quiesced, got %q", state.State)
	}

	state, err = store.SetMaintenanceState(context.Background(), "exiting")
	if err != nil {
		t.Fatalf("SetMaintenanceState exiting: %v", err)
	}
	if state.State != maintenanceStateExiting {
		t.Fatalf("expected exiting, got %q", state.State)
	}
}
