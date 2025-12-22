package s3

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects minimal in-memory stats for /v1/meta/stats.
type Metrics struct {
	requestsMu sync.Mutex
	requests   map[string]map[string]int64
	inflightMu sync.Mutex
	inflight   map[string]int64
	latencyMu  sync.Mutex
	latency    map[string]*latencyWindow

	bytesIn  atomic.Int64
	bytesOut atomic.Int64
}

type latencyWindow struct {
	mu     sync.Mutex
	values []int64
	idx    int
	filled bool
}

// LatencyStats captures p50/p95/p99 in milliseconds.
type LatencyStats struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	N   int     `json:"n"`
}

// NewMetrics creates a Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		requests: make(map[string]map[string]int64),
		inflight: make(map[string]int64),
		latency:  make(map[string]*latencyWindow),
	}
}

func (m *Metrics) InflightInc(op string) {
	if m == nil {
		return
	}
	m.inflightMu.Lock()
	m.inflight[op]++
	m.inflightMu.Unlock()
}

func (m *Metrics) InflightDec(op string) {
	if m == nil {
		return
	}
	m.inflightMu.Lock()
	if m.inflight[op] > 0 {
		m.inflight[op]--
	}
	m.inflightMu.Unlock()
}

func (m *Metrics) AddBytesIn(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesIn.Add(n)
}

func (m *Metrics) AddBytesOut(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesOut.Add(n)
}

func (m *Metrics) Record(op string, status int, dur time.Duration) {
	if m == nil {
		return
	}
	if op == "" {
		op = "unknown"
	}
	class := "0xx"
	if status > 0 {
		class = string([]byte{byte('0' + status/100), 'x', 'x'})
	}
	m.requestsMu.Lock()
	bucket := m.requests[op]
	if bucket == nil {
		bucket = make(map[string]int64)
		m.requests[op] = bucket
	}
	bucket[class]++
	m.requestsMu.Unlock()

	m.latencyMu.Lock()
	window := m.latency[op]
	if window == nil {
		window = newLatencyWindow(1024)
		m.latency[op] = window
	}
	m.latencyMu.Unlock()
	window.add(dur)
}

func (m *Metrics) Snapshot() (requests map[string]map[string]int64, inflight map[string]int64, bytesIn, bytesOut int64, latency map[string]LatencyStats) {
	if m == nil {
		return nil, nil, 0, 0, nil
	}
	requests = make(map[string]map[string]int64)
	m.requestsMu.Lock()
	for op, byClass := range m.requests {
		copyClass := make(map[string]int64, len(byClass))
		for class, v := range byClass {
			copyClass[class] = v
		}
		requests[op] = copyClass
	}
	m.requestsMu.Unlock()

	inflight = make(map[string]int64)
	m.inflightMu.Lock()
	for op, v := range m.inflight {
		inflight[op] = v
	}
	m.inflightMu.Unlock()

	bytesIn = m.bytesIn.Load()
	bytesOut = m.bytesOut.Load()

	latency = make(map[string]LatencyStats)
	m.latencyMu.Lock()
	for op, window := range m.latency {
		latency[op] = window.snapshot()
	}
	m.latencyMu.Unlock()
	return requests, inflight, bytesIn, bytesOut, latency
}

func newLatencyWindow(size int) *latencyWindow {
	if size <= 0 {
		size = 128
	}
	return &latencyWindow{values: make([]int64, size)}
}

func (w *latencyWindow) add(d time.Duration) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.values[w.idx] = d.Milliseconds()
	w.idx++
	if w.idx >= len(w.values) {
		w.idx = 0
		w.filled = true
	}
	w.mu.Unlock()
}

func (w *latencyWindow) snapshot() LatencyStats {
	if w == nil {
		return LatencyStats{}
	}
	w.mu.Lock()
	n := len(w.values)
	if !w.filled {
		n = w.idx
	}
	if n == 0 {
		w.mu.Unlock()
		return LatencyStats{}
	}
	sample := make([]int64, n)
	copy(sample, w.values[:n])
	w.mu.Unlock()

	sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
	return LatencyStats{
		P50: percentile(sample, 0.50),
		P95: percentile(sample, 0.95),
		P99: percentile(sample, 0.99),
		N:   n,
	}
}

func percentile(values []int64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return float64(values[0])
	}
	if p >= 1 {
		return float64(values[len(values)-1])
	}
	pos := p * float64(len(values)-1)
	idx := int(pos)
	frac := pos - float64(idx)
	if idx+1 >= len(values) {
		return float64(values[idx])
	}
	return float64(values[idx])*(1-frac) + float64(values[idx+1])*frac
}
