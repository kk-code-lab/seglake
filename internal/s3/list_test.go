package s3

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestListV2DelimiterUnit(t *testing.T) {
	handler := newListTestHandler(t)

	put := func(key string) {
		req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader(key))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("PUT status: %d", w.Code)
		}
	}

	put("a/one.txt")
	put("a/two.txt")
	put("b/three.txt")

	req := httptest.NewRequest("GET", "/bucket?list-type=2&prefix=a&delimiter=/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>") {
		t.Fatalf("expected common prefix")
	}
	if strings.Contains(body, "<Key>a/one.txt</Key>") {
		t.Fatalf("expected keys grouped by delimiter")
	}
}

func TestListV2DelimiterEmptyPrefix(t *testing.T) {
	handler := newListTestHandler(t)

	put := func(key string) {
		req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader(key))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("PUT status: %d", w.Code)
		}
	}

	put("foo/bar")
	put("baz.txt")

	req := httptest.NewRequest("GET", "/bucket?list-type=2&delimiter=/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<CommonPrefixes><Prefix>foo/</Prefix></CommonPrefixes>") {
		t.Fatalf("expected common prefix")
	}
	if !strings.Contains(body, "<Key>baz.txt</Key>") {
		t.Fatalf("expected non-delimited key")
	}
	if strings.Contains(body, "<Key>foo/bar</Key>") {
		t.Fatalf("expected keys grouped by delimiter")
	}
}

func TestListV2DelimiterMaxKeysTruncates(t *testing.T) {
	handler := newListTestHandler(t)

	put := func(key string) {
		req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader(key))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("PUT status: %d", w.Code)
		}
	}

	put("a/one.txt")
	put("b/two.txt")

	req := httptest.NewRequest("GET", "/bucket?list-type=2&delimiter=/&max-keys=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Fatalf("expected truncated response")
	}
	if !strings.Contains(body, "<NextContinuationToken>") {
		t.Fatalf("expected continuation token")
	}
	if !strings.Contains(body, "<KeyCount>1</KeyCount>") {
		t.Fatalf("expected key count 1")
	}
	if !strings.Contains(body, "<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>") &&
		!strings.Contains(body, "<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>") {
		t.Fatalf("expected a common prefix")
	}
	if strings.Contains(body, "<Contents>") {
		t.Fatalf("expected no contents")
	}
}

func TestListV2AcceptsTrailingSlashBucketPath(t *testing.T) {
	handler := newListTestHandler(t)

	create := httptest.NewRequest("PUT", "/bucket", nil)
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, create)
	if createW.Code != 200 {
		t.Fatalf("PUT status: %d", createW.Code)
	}

	req := httptest.NewRequest("GET", "/bucket/?list-type=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST status: %d", w.Code)
	}
}

func TestListV2MissingBucket(t *testing.T) {
	handler := newListTestHandler(t)

	req := httptest.NewRequest("GET", "/missing?list-type=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("LIST status: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "<Code>NoSuchBucket</Code>") {
		t.Fatalf("expected NoSuchBucket")
	}
}

func TestGetBucketLocationMissingBucket(t *testing.T) {
	handler := newListTestHandler(t)

	req := httptest.NewRequest("GET", "/missing?location", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("GET status: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "<Code>NoSuchBucket</Code>") {
		t.Fatalf("expected NoSuchBucket")
	}
}

func TestHeadBucket(t *testing.T) {
	handler := newListTestHandler(t)

	req := httptest.NewRequest("HEAD", "/missing", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("HEAD status: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "<Code>NoSuchBucket</Code>") {
		t.Fatalf("expected NoSuchBucket")
	}

	create := httptest.NewRequest("PUT", "/demo", nil)
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, create)
	if createW.Code != 200 {
		t.Fatalf("PUT status: %d", createW.Code)
	}

	okReq := httptest.NewRequest("HEAD", "/demo", nil)
	okW := httptest.NewRecorder()
	handler.ServeHTTP(okW, okReq)
	if okW.Code != 200 {
		t.Fatalf("HEAD ok status: %d", okW.Code)
	}
}

func TestCreateBucket(t *testing.T) {
	handler := newListTestHandler(t)

	req := httptest.NewRequest("PUT", "/demo/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT status: %d", w.Code)
	}

	exists, err := handler.Meta.BucketExists(req.Context(), "demo")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Fatalf("expected bucket to exist")
	}
}

func newListTestHandler(t *testing.T) *Handler {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(dir + "/meta.db")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(dir + "/objects"),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	return &Handler{
		Engine: eng,
		Meta:   store,
	}
}
