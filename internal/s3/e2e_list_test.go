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

func TestS3E2EListObjectVersions(t *testing.T) {
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
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/"+key, bytes.NewReader([]byte(key)))
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

	del := func(key string) {
		req, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket/"+key, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE status: %d", resp.StatusCode)
		}
	}

	put("demo.txt")
	put("demo.txt")
	del("demo.txt")

	listURL := server.URL + "/bucket?versions&max-keys=1"
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
	if !bytes.Contains(body, []byte("<DeleteMarker>")) && !bytes.Contains(body, []byte("<Version>")) {
		t.Fatalf("expected versions or delete markers")
	}

	nextKey := extractXMLTag(string(body), "NextKeyMarker")
	nextVersion := extractXMLTag(string(body), "NextVersionIdMarker")
	if nextKey == "" || nextVersion == "" {
		t.Fatalf("expected next markers")
	}

	listURL2 := server.URL + "/bucket?versions&max-keys=2&key-marker=" + url.QueryEscape(nextKey) + "&version-id-marker=" + url.QueryEscape(nextVersion)
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
	if !bytes.Contains(body2, []byte("<DeleteMarker>")) && !bytes.Contains(body2, []byte("<Version>")) {
		t.Fatalf("expected versions or delete markers on page 2")
	}
}

func TestS3E2EListObjectVersionsNull(t *testing.T) {
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

	createReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(createReq, "test", "testsecret", "us-east-1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("Create bucket error: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("Create bucket status: %d", createResp.StatusCode)
	}

	putBody := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>`
	setReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket?versioning", bytes.NewReader([]byte(putBody)))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(setReq, "test", "testsecret", "us-east-1")
	setResp, err := http.DefaultClient.Do(setReq)
	if err != nil {
		t.Fatalf("Set versioning error: %v", err)
	}
	setResp.Body.Close()
	if setResp.StatusCode != http.StatusOK {
		t.Fatalf("Set versioning status: %d", setResp.StatusCode)
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/null.txt", bytes.NewReader([]byte("data")))
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

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?versions", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<VersionId>null</VersionId>")) {
		t.Fatalf("expected null version id")
	}
}

func TestS3E2EListObjectVersionsDelimiter(t *testing.T) {
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
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/"+key, bytes.NewReader([]byte(key)))
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
	put("b.txt")

	listURL := server.URL + "/bucket?versions&prefix=a&delimiter=/"
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
	if !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>")) {
		t.Fatalf("expected common prefix")
	}
	if bytes.Contains(body, []byte("<Key>a/one.txt</Key>")) {
		t.Fatalf("expected keys grouped by delimiter")
	}
}

func TestS3E2EListObjectVersionsEncodingType(t *testing.T) {
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
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/"+key, bytes.NewReader([]byte(key)))
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

	put("space key.txt")

	listURL := server.URL + "/bucket?versions&encoding-type=url"
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
	if !bytes.Contains(body, []byte("<EncodingType>url</EncodingType>")) {
		t.Fatalf("expected encoding type url")
	}
	if !bytes.Contains(body, []byte("<Key>space%20key.txt</Key>")) {
		t.Fatalf("expected encoded key")
	}
}

func TestS3E2EListObjectVersionsMarkersSameKey(t *testing.T) {
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

	put := func(payload string) {
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/demo.txt", bytes.NewReader([]byte(payload)))
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

	put("v1")
	put("v2")
	put("v3")

	listURL := server.URL + "/bucket?versions&max-keys=1"
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
	firstVersion := extractXMLTag(string(body), "VersionId")
	if firstVersion == "" {
		t.Fatalf("expected version id")
	}
	nextKey := extractXMLTag(string(body), "NextKeyMarker")
	nextVersion := extractXMLTag(string(body), "NextVersionIdMarker")
	if nextKey == "" || nextVersion == "" {
		t.Fatalf("expected next markers")
	}

	listURL2 := server.URL + "/bucket?versions&max-keys=2&key-marker=" + url.QueryEscape(nextKey) + "&version-id-marker=" + url.QueryEscape(nextVersion)
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
	if !bytes.Contains(body2, []byte("<VersionId>")) {
		t.Fatalf("expected more versions")
	}
	if bytes.Contains(body2, []byte("<VersionId>"+firstVersion+"</VersionId>")) {
		t.Fatalf("expected marker to exclude first version")
	}
}

func TestS3E2EListObjectVersionsInvalidEncodingType(t *testing.T) {
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

	createReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(createReq, "test", "testsecret", "us-east-1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("Create bucket error: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("Create bucket status: %d", createResp.StatusCode)
	}

	listURL := server.URL + "/bucket?versions&encoding-type=bad"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestS3E2EListObjectVersionsUnversionedEmpty(t *testing.T) {
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

	createReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	createReq.Header.Set("x-seglake-versioning", "unversioned")
	signRequest(createReq, "test", "testsecret", "us-east-1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("Create bucket error: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("Create bucket status: %d", createResp.StatusCode)
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/key.txt", bytes.NewReader([]byte("data")))
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

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?versions", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body, []byte("<Version>")) || bytes.Contains(body, []byte("<DeleteMarker>")) {
		t.Fatalf("expected empty version listing")
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
