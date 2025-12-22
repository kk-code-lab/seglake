package s3

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestVersionedGetAndDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/key"

	put := func(body []byte) string {
		req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
		versionID := resp.Header.Get("x-amz-version-id")
		if versionID == "" {
			t.Fatalf("missing version id")
		}
		return versionID
	}

	v1 := put([]byte("first"))
	v2 := put([]byte("second"))

	getReq, err := http.NewRequest(http.MethodGet, putURL+"?versionId="+v1, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	if got := getResp.Header.Get("x-amz-version-id"); got != v1 {
		t.Fatalf("expected version id %s, got %s", v1, got)
	}
	gotBody, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(gotBody) != "first" {
		t.Fatalf("unexpected body: %s", string(gotBody))
	}

	delReq, err := http.NewRequest(http.MethodDelete, putURL+"?versionId="+v2, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", delResp.StatusCode)
	}
	if got := delResp.Header.Get("x-amz-version-id"); got != v2 {
		t.Fatalf("expected delete version id %s, got %s", v2, got)
	}

	getLatest, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	latestResp, err := http.DefaultClient.Do(getLatest)
	if err != nil {
		t.Fatalf("GET latest error: %v", err)
	}
	defer func() { _ = latestResp.Body.Close() }()
	if latestResp.StatusCode != http.StatusOK {
		t.Fatalf("GET latest status: %d", latestResp.StatusCode)
	}
	latestBody, err := io.ReadAll(latestResp.Body)
	if err != nil {
		t.Fatalf("ReadAll latest: %v", err)
	}
	if string(latestBody) != "first" {
		t.Fatalf("unexpected latest body: %s", string(latestBody))
	}
}
