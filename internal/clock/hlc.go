package clock

import (
	"fmt"
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

func format(physical int64, logical uint32) string {
	return fmt.Sprintf("%019d-%010d", physical, logical)
}
