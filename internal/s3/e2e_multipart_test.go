//go:build e2e

package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestS3E2EMultipart(t *testing.T) {
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

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/key?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	body, _ := io.ReadAll(initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}
	var initResult initiateMultipartResult
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("init decode: %v", err)
	}

	putPart := func(n int, data string) string {
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/key?partNumber="+strconv.Itoa(n)+"&uploadId="+initResult.UploadID, strings.NewReader(data))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT part error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT part status: %d", resp.StatusCode)
		}
		return resp.Header.Get("ETag")
	}

	etag1 := putPart(1, strings.Repeat("a", 5<<20))
	etag2 := putPart(2, "tail")

	completeBody := `<CompleteMultipartUpload>` +
		`<Part><PartNumber>1</PartNumber><ETag>` + etag1 + `</ETag></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>` + etag2 + `</ETag></Part>` +
		`</CompleteMultipartUpload>`
	completeReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/key?uploadId="+initResult.UploadID, strings.NewReader(completeBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(completeReq, "test", "testsecret", "us-east-1")
	completeResp, err := http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatalf("complete error: %v", err)
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status: %d", completeResp.StatusCode)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list uploads error: %v", err)
	}
	listBody, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list uploads status: %d", listResp.StatusCode)
	}
	if bytes.Contains(listBody, []byte(initResult.UploadID)) {
		t.Fatalf("completed upload should not be listed")
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !strings.HasPrefix(string(got), strings.Repeat("a", 5<<20)) {
		t.Fatalf("GET body prefix mismatch")
	}
}

func TestS3E2EMultipartRejectsOversizedPart(t *testing.T) {
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

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/large?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	body, _ := io.ReadAll(initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}
	var initResult initiateMultipartResult
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("init decode: %v", err)
	}

	putURL := server.URL + "/bucket/large?partNumber=1&uploadId=" + initResult.UploadID
	payloadHash := "STREAMING-UNSIGNED-PAYLOAD"
	chunkBody := strings.NewReader("0\r\n\r\n")
	req, err := http.NewRequest(http.MethodPut, putURL, io.NopCloser(chunkBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(maxPartSize+1, 10))
	req.ContentLength = int64(len("0\r\n\r\n"))
	signRequestWithPayloadHashAndTime(req, "test", "testsecret", "us-east-1", payloadHash, time.Now().UTC())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT part error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("EntityTooLarge")) {
		t.Fatalf("expected EntityTooLarge")
	}
}

func TestS3E2EMultipartRejectsPartNumberTooLarge(t *testing.T) {
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

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/parts?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	body, _ := io.ReadAll(initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}
	var initResult initiateMultipartResult
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("init decode: %v", err)
	}

	req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/parts?partNumber=10001&uploadId="+initResult.UploadID, strings.NewReader("x"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithPayloadHashAndTime(req, "test", "testsecret", "us-east-1", "UNSIGNED-PAYLOAD", time.Now().UTC())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT part error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("InvalidArgument")) {
		t.Fatalf("expected InvalidArgument")
	}
}

func TestS3E2EListMultipartUploads(t *testing.T) {
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

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/a?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<ListMultipartUploadsResult>")) {
		t.Fatalf("expected xml response")
	}
}

func TestS3E2EListMultipartUploadsPagination(t *testing.T) {
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

	init := func(key string) {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/"+key+"?uploads", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("init error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("init status: %d", resp.StatusCode)
		}
	}

	init("a")
	init("b")

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads&max-uploads=1", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) {
		t.Fatalf("expected truncated response")
	}
	nextKey := extractXMLTag(string(body), "NextKeyMarker")
	nextUpload := extractXMLTag(string(body), "NextUploadIdMarker")
	if nextKey == "" || nextUpload == "" {
		t.Fatalf("missing next markers: key=%q upload=%q", nextKey, nextUpload)
	}

	listReq, err = http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads&max-uploads=1&key-marker="+nextKey+"&upload-id-marker="+nextUpload, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err = http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ = io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<Key>b</Key>")) {
		t.Fatalf("expected second key in page")
	}
	if bytes.Contains(body, []byte("<Key>a</Key>")) {
		t.Fatalf("unexpected first key in page")
	}
}

func TestS3E2EListMultipartUploadsDelimiterPrefix(t *testing.T) {
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

	init := func(key string) {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/"+key+"?uploads", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("init error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("init status: %d", resp.StatusCode)
		}
	}

	init("logs/2025/a")
	init("logs/2024/b")
	init("logs/file")
	init("other")

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads&prefix=logs/&delimiter=/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<Prefix>logs/</Prefix>")) {
		t.Fatalf("expected prefix in response")
	}
	if !bytes.Contains(body, []byte("<Delimiter>/</Delimiter>")) {
		t.Fatalf("expected delimiter in response")
	}
	if !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>logs/2024/</Prefix></CommonPrefixes>")) {
		t.Fatalf("expected logs/2024/ common prefix")
	}
	if !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>logs/2025/</Prefix></CommonPrefixes>")) {
		t.Fatalf("expected logs/2025/ common prefix")
	}
	if !bytes.Contains(body, []byte("<Key>logs/file</Key>")) {
		t.Fatalf("expected logs/file upload entry")
	}
	if bytes.Contains(body, []byte("<Key>logs/2024/b</Key>")) || bytes.Contains(body, []byte("<Key>logs/2025/a</Key>")) {
		t.Fatalf("unexpected nested upload entry")
	}
	if bytes.Contains(body, []byte("<Key>other</Key>")) {
		t.Fatalf("unexpected upload outside prefix")
	}
}

func TestS3E2EListMultipartUploadsDelimiterMarkers(t *testing.T) {
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

	init := func(key string) {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/"+key+"?uploads", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("init error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("init status: %d", resp.StatusCode)
		}
	}

	init("logs/2024/a")
	init("logs/file")
	init("logs/z")

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads&prefix=logs/&delimiter=/&max-uploads=1", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) {
		t.Fatalf("expected truncated response")
	}
	nextKey := extractXMLTag(string(body), "NextKeyMarker")
	nextUpload := extractXMLTag(string(body), "NextUploadIdMarker")
	if nextKey == "" || nextUpload == "" {
		t.Fatalf("missing next markers: key=%q upload=%q", nextKey, nextUpload)
	}

	listReq, err = http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads&prefix=logs/&delimiter=/&max-uploads=1&key-marker="+nextKey+"&upload-id-marker="+nextUpload, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err = http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ = io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<Key>logs/file</Key>")) {
		t.Fatalf("expected logs/file in second page")
	}
}
