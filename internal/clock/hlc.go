package clock

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HLC implements a simple hybrid logical clock for ordering local events.
type HLC struct {
	mu           sync.Mutex
	lastPhysical int64
	logical      uint32
}

// New returns a new HLC instance.
func New() *HLC {
	return &HLC{}
}

// Next returns the next HLC timestamp string (lexicographically sortable).
func (h *HLC) Next() string {
	now := time.Now().UTC().UnixNano()
	h.mu.Lock()
	if now > h.lastPhysical {
		h.lastPhysical = now
		h.logical = 0
	} else {
		h.logical++
	}
	physical := h.lastPhysical
	logical := h.logical
	h.mu.Unlock()
	return format(physical, logical)
}

// Update advances the clock if the provided timestamp is ahead.
func (h *HLC) Update(ts string) bool {
	physical, logical, ok := parse(ts)
	if !ok {
		return false
	}
	h.mu.Lock()
	updated := false
	switch {
	case physical > h.lastPhysical:
		h.lastPhysical = physical
		h.logical = logical
		updated = true
	case physical == h.lastPhysical && logical > h.logical:
		h.logical = logical
		updated = true
	}
	h.mu.Unlock()
	return updated
}

func parse(ts string) (int64, uint32, bool) {
	parts := strings.SplitN(ts, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	physical, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	logical64, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return physical, uint32(logical64), true
}

func format(physical int64, logical uint32) string {
	return fmt.Sprintf("%019d-%010d", physical, logical)
}
