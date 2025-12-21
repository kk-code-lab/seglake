package s3

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestParseRangeVariants(t *testing.T) {
	tests := []struct {
		header string
		size   int64
		ok     bool
		start  int64
		length int64
	}{
		{header: "bytes=0-4", size: 10, ok: true, start: 0, length: 5},
		{header: "bytes=5-", size: 10, ok: true, start: 5, length: 5},
		{header: "bytes=-3", size: 10, ok: true, start: 7, length: 3},
		{header: "bytes=0-100", size: 10, ok: true, start: 0, length: 10},
		{header: "bytes=9-8", size: 10, ok: false},
		{header: "bytes=10-11", size: 10, ok: false},
		{header: "bytes=0-0,1-2", size: 10, ok: false},
		{header: "bytes=", size: 10, ok: false},
		{header: "bytes=0-4", size: -1, ok: false},
	}
	for _, tt := range tests {
		start, length, ok := parseRange(tt.header, tt.size)
		if ok != tt.ok {
			t.Fatalf("parseRange(%q) ok=%v want %v", tt.header, ok, tt.ok)
		}
		if ok {
			if start != tt.start || length != tt.length {
				t.Fatalf("parseRange(%q) got start=%d len=%d", tt.header, start, length)
			}
		}
	}
}

func TestRangeHeadReturnsPartialContent(t *testing.T) {
	h := newTestHandler(t)
	putObject(t, h, "bucket", "key", "abcdefghij")

	req := httptest.NewRequest(http.MethodHead, "/bucket/key", nil)
	req.Header.Set("Range", "bytes=1-3")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: %d", w.Code)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 1-3/10" {
		t.Fatalf("content-range: %q", got)
	}
	if got := w.Header().Get("Content-Length"); got != "3" {
		t.Fatalf("content-length: %q", got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("expected empty body for HEAD")
	}
}

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "data")),
		MetaStore: store,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return &Handler{Engine: eng, Meta: store}
}

func putObject(t *testing.T, h *Handler, bucket, key, body string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status: %d", w.Code)
	}
}
