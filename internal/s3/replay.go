package s3

import (
	"container/list"
	"net/http"
	"sync"
	"time"
)

const defaultReplayMaxEntries = 10000

type replayEntry struct {
	key string
	ts  time.Time
}

type replayCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	order   *list.List
	entries map[string]*list.Element
}

func newReplayCache(ttl time.Duration, maxEntries int) *replayCache {
	if maxEntries <= 0 {
		maxEntries = defaultReplayMaxEntries
	}
	return &replayCache{
		ttl:     ttl,
		max:     maxEntries,
		order:   list.New(),
		entries: make(map[string]*list.Element),
	}
}

func (c *replayCache) allow(key string, now time.Time) bool {
	if key == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictExpired(now)
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(replayEntry)
		if now.Sub(entry.ts) <= c.ttl {
			return false
		}
		c.order.Remove(elem)
		delete(c.entries, key)
	}
	c.add(key, now)
	return true
}

func (c *replayCache) evictExpired(now time.Time) {
	for {
		front := c.order.Front()
		if front == nil {
			return
		}
		entry := front.Value.(replayEntry)
		if now.Sub(entry.ts) <= c.ttl {
			return
		}
		c.order.Remove(front)
		delete(c.entries, entry.key)
	}
}

func (c *replayCache) add(key string, now time.Time) {
	elem := c.order.PushBack(replayEntry{key: key, ts: now})
	c.entries[key] = elem
	for len(c.entries) > c.max {
		front := c.order.Front()
		if front == nil {
			return
		}
		entry := front.Value.(replayEntry)
		c.order.Remove(front)
		delete(c.entries, entry.key)
	}
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
