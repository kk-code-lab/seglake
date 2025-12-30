//go:build e2e

package s3

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestS3E2EListV2(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

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
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a/one.txt")
	put("a/two.txt")
	put("b/three.txt")

	listURL := server.URL + "/bucket?list-type=2&prefix=a&delimiter=/&max-keys=10"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("List status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>")) {
		t.Fatalf("expected common prefix")
	}
	if bytes.Contains(body, []byte("<Key>a/one.txt</Key>")) {
		t.Fatalf("expected keys under prefix to be grouped by delimiter")
	}
}

func TestS3E2EListV2Continuation(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

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
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&max-keys=1"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) {
		t.Fatalf("expected truncated response")
	}
	tokenStart := bytes.Index(body, []byte("<NextContinuationToken>"))
	if tokenStart < 0 {
		t.Fatalf("missing token")
	}
	tokenStart += len("<NextContinuationToken>")
	tokenEnd := bytes.Index(body[tokenStart:], []byte("</NextContinuationToken>"))
	if tokenEnd < 0 {
		t.Fatalf("missing token end")
	}
	token := string(body[tokenStart : tokenStart+tokenEnd])

	listURL2 := server.URL + "/bucket?list-type=2&max-keys=2&continuation-token=" + url.QueryEscape(token)
	listReq2, err := http.NewRequest(http.MethodGet, listURL2, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq2, "test", "testsecret", "us-east-1")
	resp2, err := http.DefaultClient.Do(listReq2)
	if err != nil {
		t.Fatalf("List2 error: %v", err)
	}
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body2, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("unexpected first key repeated")
	}
	if !bytes.Contains(body2, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("expected next key")
	}
}

func TestS3E2EListV2StartAfter(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

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
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&start-after=a.txt"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("start-after should exclude a.txt")
	}
	if !bytes.Contains(body, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("expected b.txt")
	}
}

func TestS3E2EListV2RawContinuationToken(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

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
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&continuation-token=b.txt"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("continuation token should skip a.txt")
	}
	if bytes.Contains(body, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("continuation token should skip b.txt")
	}
	if !bytes.Contains(body, []byte("<Key>c.txt</Key>")) {
		t.Fatalf("expected c.txt")
	}
}

func TestS3E2EListBuckets(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

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
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/obj"
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(putReq, "test", "testsecret", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("List status: %d", listResp.StatusCode)
	}
	body, err := io.ReadAll(listResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<Name>bucket</Name>")) {
		t.Fatalf("expected bucket name in list")
	}
}
