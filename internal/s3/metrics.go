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

	bucketMu       sync.Mutex
	bucketRequests map[string]map[string]int64
	bucketLatency  map[string]*latencyWindow

	keyMu       sync.Mutex
	keyRequests map[string]map[string]int64
	keyLatency  map[string]*latencyWindow

	bytesIn  atomic.Int64
	bytesOut atomic.Int64
	replayDetected atomic.Int64
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
		requests:       make(map[string]map[string]int64),
		inflight:       make(map[string]int64),
		latency:        make(map[string]*latencyWindow),
		bucketRequests: make(map[string]map[string]int64),
		bucketLatency:  make(map[string]*latencyWindow),
		keyRequests:    make(map[string]map[string]int64),
		keyLatency:     make(map[string]*latencyWindow),
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

func (m *Metrics) IncReplayDetected() {
	if m == nil {
		return
	}
	m.replayDetected.Add(1)
}

func (m *Metrics) Record(op string, status int, dur time.Duration, bucketName, key string) {
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
	byClass := m.requests[op]
	if byClass == nil {
		byClass = make(map[string]int64)
		m.requests[op] = byClass
	}
	byClass[class]++
	m.requestsMu.Unlock()

	m.latencyMu.Lock()
	window := m.latency[op]
	if window == nil {
		window = newLatencyWindow(1024)
		m.latency[op] = window
	}
	m.latencyMu.Unlock()
	window.add(dur)

	if bucketName != "" {
		m.bucketMu.Lock()
		updateClassMap(m.bucketRequests, bucketName, class, 100)
		window = ensureLatencyWindow(m.bucketLatency, bucketName, 100)
		m.bucketMu.Unlock()
		if window != nil {
			window.add(dur)
		}
		if key != "" {
			keyLabel := bucketName + "/" + key
			m.keyMu.Lock()
			updateClassMap(m.keyRequests, keyLabel, class, 1000)
			window = ensureLatencyWindow(m.keyLatency, keyLabel, 1000)
			m.keyMu.Unlock()
			if window != nil {
				window.add(dur)
			}
		}
	}
}

func (m *Metrics) Snapshot() (requests map[string]map[string]int64, inflight map[string]int64, bytesIn, bytesOut int64, replayDetected int64, latency map[string]LatencyStats, bucketReqs map[string]map[string]int64, bucketLatency map[string]LatencyStats, keyReqs map[string]map[string]int64, keyLatency map[string]LatencyStats) {
	if m == nil {
		return nil, nil, 0, 0, 0, nil, nil, nil, nil, nil
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
	replayDetected = m.replayDetected.Load()

	latency = make(map[string]LatencyStats)
	m.latencyMu.Lock()
	for op, window := range m.latency {
		latency[op] = window.snapshot()
	}
	m.latencyMu.Unlock()

	bucketReqs = make(map[string]map[string]int64)
	m.bucketMu.Lock()
	for name, byClass := range m.bucketRequests {
		copyClass := make(map[string]int64, len(byClass))
		for class, v := range byClass {
			copyClass[class] = v
		}
		bucketReqs[name] = copyClass
	}
	bucketLatency = make(map[string]LatencyStats)
	for name, window := range m.bucketLatency {
		bucketLatency[name] = window.snapshot()
	}
	m.bucketMu.Unlock()

	keyReqs = make(map[string]map[string]int64)
	m.keyMu.Lock()
	for name, byClass := range m.keyRequests {
		copyClass := make(map[string]int64, len(byClass))
		for class, v := range byClass {
			copyClass[class] = v
		}
		keyReqs[name] = copyClass
	}
	keyLatency = make(map[string]LatencyStats)
	for name, window := range m.keyLatency {
		keyLatency[name] = window.snapshot()
	}
	m.keyMu.Unlock()

	return requests, inflight, bytesIn, bytesOut, replayDetected, latency, bucketReqs, bucketLatency, keyReqs, keyLatency
}

func updateClassMap(target map[string]map[string]int64, key, class string, limit int) {
	if key == "" {
		return
	}
	entry := target[key]
	if entry == nil {
		if limit > 0 && len(target) >= limit {
			return
		}
		entry = make(map[string]int64)
		target[key] = entry
	}
	entry[class]++
}

func ensureLatencyWindow(target map[string]*latencyWindow, key string, limit int) *latencyWindow {
	if key == "" {
		return nil
	}
	window := target[key]
	if window == nil {
		if limit > 0 && len(target) >= limit {
			return nil
		}
		window = newLatencyWindow(1024)
		target[key] = window
	}
	return window
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
