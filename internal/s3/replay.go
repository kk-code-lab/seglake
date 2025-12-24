package s3

import (
	"net/http"
	"sync"
	"time"
)

type replayCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]time.Time
}

func newReplayCache(ttl time.Duration) *replayCache {
	return &replayCache{
		ttl:     ttl,
		entries: make(map[string]time.Time),
	}
}

func (c *replayCache) allow(key string, now time.Time) bool {
	if key == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, t := range c.entries {
		if now.Sub(t) > c.ttl {
			delete(c.entries, k)
		}
	}
	if ts, ok := c.entries[key]; ok {
		if now.Sub(ts) <= c.ttl {
			return false
		}
	}
	c.entries[key] = now
	return true
}

func replayKey(r *http.Request) string {
	if r == nil {
		return ""
	}
	if sig := r.URL.Query().Get("X-Amz-Signature"); sig != "" {
		return "q:" + sig + "|" + r.Method + "|" + r.URL.Path + "|" + r.URL.RawQuery
	}
	auth := r.Header.Get("Authorization")
	amzDate := r.Header.Get("X-Amz-Date")
	if auth == "" || amzDate == "" {
		return ""
	}
	return "h:" + auth + "|" + amzDate + "|" + r.Method + "|" + r.URL.Path
}
