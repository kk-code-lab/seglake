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
	dir := t.TempDir()
	store, err := meta.Open(dir + "/meta.db")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(dir + "/objects"),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

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

func TestListV2AcceptsTrailingSlashBucketPath(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(dir + "/meta.db")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(dir + "/objects"),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest("GET", "/bucket/?list-type=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST status: %d", w.Code)
	}
}

func TestCreateBucket(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(dir + "/meta.db")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(dir + "/objects"),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	req := httptest.NewRequest("PUT", "/demo/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT status: %d", w.Code)
	}

	exists, err := store.BucketExists(req.Context(), "demo")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Fatalf("expected bucket to exist")
	}
}
