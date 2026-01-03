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

	listPutObject(t, handler, "a/one.txt")
	listPutObject(t, handler, "a/two.txt")
	listPutObject(t, handler, "b/three.txt")

	body := listAndReadBody(t, handler, "/bucket?list-type=2&prefix=a&delimiter=/", "LIST")
	assertCommonPrefixGrouped(t, body, "a/")
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

func TestCreateBucketWithVersioningHeader(t *testing.T) {
	handler := newListTestHandler(t)

	req := httptest.NewRequest("PUT", "/demo", nil)
	req.Header.Set("x-seglake-versioning", "unversioned")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT status: %d", w.Code)
	}
	state, err := handler.Meta.GetBucketVersioningState(req.Context(), "demo")
	if err != nil {
		t.Fatalf("GetBucketVersioningState: %v", err)
	}
	if state != meta.BucketVersioningDisabled {
		t.Fatalf("expected disabled, got %s", state)
	}
}

func TestPutGetBucketVersioning(t *testing.T) {
	handler := newListTestHandler(t)

	create := httptest.NewRequest("PUT", "/demo", nil)
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, create)
	if createW.Code != 200 {
		t.Fatalf("PUT status: %d", createW.Code)
	}

	putBody := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>`
	put := httptest.NewRequest("PUT", "/demo?versioning", strings.NewReader(putBody))
	put.Header.Set("Content-Type", "application/xml")
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, put)
	if putW.Code != 200 {
		t.Fatalf("PUT versioning status: %d", putW.Code)
	}

	get := httptest.NewRequest("GET", "/demo?versioning", nil)
	getW := httptest.NewRecorder()
	handler.ServeHTTP(getW, get)
	if getW.Code != 200 {
		t.Fatalf("GET versioning status: %d", getW.Code)
	}
	if !strings.Contains(getW.Body.String(), "<Status>Suspended</Status>") {
		t.Fatalf("expected Suspended status")
	}
}

func TestListVersionsIncludesDeleteMarker(t *testing.T) {
	handler := newListTestHandler(t)

	put := func(key string) {
		req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader("data"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("PUT status: %d", w.Code)
		}
	}

	put("demo.txt")
	put("demo.txt")

	del := httptest.NewRequest("DELETE", "/bucket/demo.txt", nil)
	delW := httptest.NewRecorder()
	handler.ServeHTTP(delW, del)
	if delW.Code != 204 {
		t.Fatalf("DELETE status: %d", delW.Code)
	}

	req := httptest.NewRequest("GET", "/bucket?versions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST versions status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<DeleteMarker>") {
		t.Fatalf("expected delete marker entry")
	}
	if !strings.Contains(body, "<Version>") {
		t.Fatalf("expected version entry")
	}
}

func TestListVersionsNullVersionID(t *testing.T) {
	handler := newListTestHandler(t)

	create := httptest.NewRequest("PUT", "/bucket", nil)
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, create)
	if createW.Code != 200 {
		t.Fatalf("PUT bucket status: %d", createW.Code)
	}

	putBody := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>`
	setReq := httptest.NewRequest("PUT", "/bucket?versioning", strings.NewReader(putBody))
	setW := httptest.NewRecorder()
	handler.ServeHTTP(setW, setReq)
	if setW.Code != 200 {
		t.Fatalf("PUT versioning status: %d", setW.Code)
	}

	put := httptest.NewRequest("PUT", "/bucket/null.txt", strings.NewReader("data"))
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, put)
	if putW.Code != 200 {
		t.Fatalf("PUT status: %d", putW.Code)
	}

	req := httptest.NewRequest("GET", "/bucket?versions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST versions status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<VersionId>null</VersionId>") {
		t.Fatalf("expected null version id")
	}
}

func TestListVersionsDelimiter(t *testing.T) {
	handler := newListTestHandler(t)

	listPutObject(t, handler, "a/one.txt")
	listPutObject(t, handler, "a/two.txt")
	listPutObject(t, handler, "b.txt")

	body := listAndReadBody(t, handler, "/bucket?versions&prefix=a&delimiter=/", "LIST versions")
	assertCommonPrefixGrouped(t, body, "a/")
}

func listPutObject(t *testing.T, handler *Handler, key string) {
	t.Helper()
	req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader(key))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT status: %d", w.Code)
	}
}

func listAndReadBody(t *testing.T, handler *Handler, path, label string) string {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("%s status: %d", label, w.Code)
	}
	return w.Body.String()
}

func assertCommonPrefixGrouped(t *testing.T, body, prefix string) {
	t.Helper()
	if !strings.Contains(body, "<CommonPrefixes><Prefix>"+prefix+"</Prefix></CommonPrefixes>") {
		t.Fatalf("expected common prefix")
	}
	if strings.Contains(body, "<Key>"+prefix+"one.txt</Key>") {
		t.Fatalf("expected keys grouped by delimiter")
	}
}

func TestListVersionsTruncates(t *testing.T) {
	handler := newListTestHandler(t)

	put := func(key string) {
		req := httptest.NewRequest("PUT", "/bucket/"+key, strings.NewReader(key))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("PUT status: %d", w.Code)
		}
	}

	put("a.txt")
	put("b.txt")

	req := httptest.NewRequest("GET", "/bucket?versions&max-keys=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("LIST versions status: %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Fatalf("expected truncated response")
	}
	if !strings.Contains(body, "<NextKeyMarker>") {
		t.Fatalf("expected next key marker")
	}
	if !strings.Contains(body, "<NextVersionIdMarker>") {
		t.Fatalf("expected next version id marker")
	}
}

func TestListVersionsUnversionedReturnsEmpty(t *testing.T) {
	handler := newListTestHandler(t)

	create := httptest.NewRequest("PUT", "/bucket", nil)
	create.Header.Set("x-seglake-versioning", "unversioned")
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, create)
	if createW.Code != 200 {
		t.Fatalf("PUT bucket status: %d", createW.Code)
	}

	put := httptest.NewRequest("PUT", "/bucket/key.txt", strings.NewReader("data"))
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, put)
	if putW.Code != 200 {
		t.Fatalf("PUT status: %d", putW.Code)
	}

	body := listAndReadBody(t, handler, "/bucket?versions", "LIST versions")
	if strings.Contains(body, "<Version>") || strings.Contains(body, "<DeleteMarker>") {
		t.Fatalf("expected empty version listing")
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
