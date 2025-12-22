package s3

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// AuthLimiter rate-limits failed auth attempts per IP and per access key.
type AuthLimiter struct {
	perIP   *tokenBucket
	perKey  *tokenBucket
	cleanup time.Duration
}

// NewAuthLimiter creates a limiter with defaults suitable for MVP.
func NewAuthLimiter() *AuthLimiter {
	return &AuthLimiter{
		perIP:   newTokenBucket(5, 5, 1*time.Second),
		perKey:  newTokenBucket(5, 5, 1*time.Second),
		cleanup: 10 * time.Minute,
	}
}

func (l *AuthLimiter) Allow(ip, key string) bool {
	if l == nil {
		return true
	}
	now := time.Now()
	if ip != "" && !l.perIP.allow(ip, now) {
		return false
	}
	if key != "" && !l.perKey.allow(key, now) {
		return false
	}
	return true
}

func (l *AuthLimiter) ObserveFailure(ip, key string) {
	if l == nil {
		return
	}
	now := time.Now()
	if ip != "" {
		l.perIP.consume(ip, now)
	}
	if key != "" {
		l.perKey.consume(key, now)
	}
}

func (l *AuthLimiter) Cleanup() {
	if l == nil {
		return
	}
	cutoff := time.Now().Add(-l.cleanup)
	l.perIP.cleanup(cutoff)
	l.perKey.cleanup(cutoff)
}

func clientIP(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return remoteAddr
}

func extractAccessKey(r *http.Request) string {
	if r == nil {
		return ""
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		params := parseAuthParams(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "))
		cred := params["Credential"]
		if cred != "" {
			parts := strings.SplitN(cred, "/", 2)
			return parts[0]
		}
	}
	if strings.HasPrefix(auth, "AWS ") {
		parts := strings.SplitN(strings.TrimPrefix(auth, "AWS "), ":", 2)
		if len(parts) > 0 {
			return parts[0]
		}
	}
	query := r.URL.Query()
	if query.Get("X-Amz-Algorithm") != "" {
		cred := query.Get("X-Amz-Credential")
		if cred != "" {
			parts := strings.SplitN(cred, "/", 2)
			return parts[0]
		}
	}
	return ""
}

type tokenBucket struct {
	mu       sync.Mutex
	buckets  map[string]*bucketState
	rate     float64
	burst    float64
	interval time.Duration
}

type bucketState struct {
	tokens float64
	last   time.Time
}

func newTokenBucket(rate, burst int, interval time.Duration) *tokenBucket {
	if rate <= 0 {
		rate = 5
	}
	if burst <= 0 {
		burst = rate
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &tokenBucket{
		buckets:  make(map[string]*bucketState),
		rate:     float64(rate),
		burst:    float64(burst),
		interval: interval,
	}
}

func (t *tokenBucket) allow(key string, now time.Time) bool {
	if t == nil || key == "" {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.stateFor(key, now)
	return state.tokens >= 1
}

func (t *tokenBucket) consume(key string, now time.Time) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.stateFor(key, now)
	if state.tokens >= 1 {
		state.tokens -= 1
	} else {
		state.tokens = 0
	}
}

func (t *tokenBucket) stateFor(key string, now time.Time) *bucketState {
	state := t.buckets[key]
	if state == nil {
		state = &bucketState{tokens: t.burst, last: now}
		t.buckets[key] = state
		return state
	}
	elapsed := now.Sub(state.last)
	if elapsed > 0 {
		state.tokens += (elapsed.Seconds() / t.interval.Seconds()) * t.rate
		if state.tokens > t.burst {
			state.tokens = t.burst
		}
		state.last = now
	}
	return state
}

func (t *tokenBucket) cleanup(cutoff time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for key, state := range t.buckets {
		if state.last.Before(cutoff) {
			delete(t.buckets, key)
		}
	}
}
