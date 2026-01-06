package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
)

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
