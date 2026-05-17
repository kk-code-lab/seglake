package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestMultipartFlowUnit(t *testing.T) {
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

	initReq := httptest.NewRequest("POST", "/bucket/key?uploads", nil)
	initReq.Header.Set("Content-Type", "text/plain")
	initW := httptest.NewRecorder()
	handler.ServeHTTP(initW, initReq)
	if initW.Code != http.StatusOK {
		t.Fatalf("init status: %d", initW.Code)
	}
	var initResp initiateMultipartResult
	if err := xml.NewDecoder(strings.NewReader(initW.Body.String())).Decode(&initResp); err != nil {
		t.Fatalf("init decode: %v", err)
	}
	if initResp.UploadID == "" {
		t.Fatalf("missing upload id")
	}

	partReq := httptest.NewRequest("PUT", "/bucket/key?partNumber=1&uploadId="+initResp.UploadID, strings.NewReader("part1"))
	partW := httptest.NewRecorder()
	handler.ServeHTTP(partW, partReq)
	if partW.Code != http.StatusOK {
		t.Fatalf("part status: %d", partW.Code)
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + partW.Result().Header.Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	completeReq := httptest.NewRequest("POST", "/bucket/key?uploadId="+initResp.UploadID, strings.NewReader(completeBody))
	completeW := httptest.NewRecorder()
	handler.ServeHTTP(completeW, completeReq)
	if completeW.Code != http.StatusOK {
		t.Fatalf("complete status: %d", completeW.Code)
	}

	getReq := httptest.NewRequest("GET", "/bucket/key", nil)
	getW := httptest.NewRecorder()
	handler.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("get status: %d", getW.Code)
	}
	if got := getW.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("content-type mismatch: %q", got)
	}
	if !bytes.Equal(getW.Body.Bytes(), []byte("part1")) {
		t.Fatalf("get mismatch")
	}
}

func TestMultipartSSES3UsesEncryptedPartManifests(t *testing.T) {
	h := newTestHandler(t)

	initReq := httptest.NewRequest(http.MethodPost, "/bucket/encrypted-mpu?uploads", nil)
	initReq.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	initW := httptest.NewRecorder()
	h.ServeHTTP(initW, initReq)
	if initW.Code != http.StatusOK {
		t.Fatalf("init status: %d body=%s", initW.Code, initW.Body.String())
	}
	if got := initW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected init SSE header, got %q", got)
	}
	var initResp initiateMultipartResult
	if err := xml.Unmarshal(initW.Body.Bytes(), &initResp); err != nil {
		t.Fatalf("unmarshal init: %v", err)
	}

	partBodies := []string{strings.Repeat("a", int(minPartSize)), "tail"}
	etags := make([]string, 0, len(partBodies))
	for i, body := range partBodies {
		partReq := httptest.NewRequest(http.MethodPut, "/bucket/encrypted-mpu?partNumber="+intToString(int64(i+1))+"&uploadId="+initResp.UploadID, strings.NewReader(body))
		partW := httptest.NewRecorder()
		h.ServeHTTP(partW, partReq)
		if partW.Code != http.StatusOK {
			t.Fatalf("part %d status: %d body=%s", i+1, partW.Code, partW.Body.String())
		}
		if got := partW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
			t.Fatalf("expected part SSE header, got %q", got)
		}
		etags = append(etags, partW.Header().Get("ETag"))
	}

	parts, err := h.Meta.ListMultipartParts(context.Background(), initResp.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	for _, part := range parts {
		man, err := h.Engine.GetManifest(context.Background(), part.VersionID)
		if err != nil {
			t.Fatalf("GetManifest part: %v", err)
		}
		if !man.Encrypted() {
			t.Fatalf("expected encrypted part manifest")
		}
	}

	completeBody := `<CompleteMultipartUpload>` +
		`<Part><PartNumber>1</PartNumber><ETag>` + etags[0] + `</ETag></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>` + etags[1] + `</ETag></Part>` +
		`</CompleteMultipartUpload>`
	completeReq := httptest.NewRequest(http.MethodPost, "/bucket/encrypted-mpu?uploadId="+initResp.UploadID, strings.NewReader(completeBody))
	completeW := httptest.NewRecorder()
	h.ServeHTTP(completeW, completeReq)
	if completeW.Code != http.StatusOK {
		t.Fatalf("complete status: %d body=%s", completeW.Code, completeW.Body.String())
	}
	if got := completeW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected complete SSE header, got %q", got)
	}
	versionID := completeW.Header().Get("x-amz-version-id")
	if versionID == "" {
		t.Fatalf("expected version id")
	}
	finalMan, err := h.Engine.GetManifest(context.Background(), versionID)
	if err != nil {
		t.Fatalf("GetManifest final: %v", err)
	}
	if !finalMan.Encrypted() || len(finalMan.Encryption.Keys) < 2 {
		t.Fatalf("expected multi-key encrypted final manifest: %+v", finalMan.Encryption)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bucket/encrypted-mpu", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET status: %d body len=%d", getW.Code, getW.Body.Len())
	}
	if getW.Body.String() != partBodies[0]+partBodies[1] {
		t.Fatalf("GET body mismatch")
	}
}

func TestMultipartRejectsOversizedPart(t *testing.T) {
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

	initReq := httptest.NewRequest("POST", "/bucket/key?uploads", nil)
	initReq.Header.Set("Content-Type", "text/plain")
	initW := httptest.NewRecorder()
	handler.ServeHTTP(initW, initReq)
	if initW.Code != http.StatusOK {
		t.Fatalf("init status: %d", initW.Code)
	}
	var initResp initiateMultipartResult
	if err := xml.NewDecoder(strings.NewReader(initW.Body.String())).Decode(&initResp); err != nil {
		t.Fatalf("init decode: %v", err)
	}
	if initResp.UploadID == "" {
		t.Fatalf("missing upload id")
	}

	partReq := httptest.NewRequest("PUT", "/bucket/key?partNumber=1&uploadId="+initResp.UploadID, strings.NewReader("x"))
	partReq.ContentLength = maxPartSize + 1
	partW := httptest.NewRecorder()
	handler.ServeHTTP(partW, partReq)
	if partW.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected entity too large, got %d", partW.Code)
	}
	if !strings.Contains(partW.Body.String(), "EntityTooLarge") {
		t.Fatalf("expected entity too large error code")
	}
}

func TestParsePartNumberLimit(t *testing.T) {
	if _, ok := parsePartNumber("10001"); ok {
		t.Fatalf("expected part number to be rejected")
	}
	if got, ok := parsePartNumber("10000"); !ok || got != 10000 {
		t.Fatalf("expected max part number to be accepted")
	}
}

func TestListMultipartUploadsDelimiterEmptyPrefix(t *testing.T) {
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

	createReq := httptest.NewRequest("PUT", "/bucket", nil)
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, createReq)
	if createW.Code != http.StatusOK {
		t.Fatalf("create bucket status: %d", createW.Code)
	}

	init := func(key string) {
		req := httptest.NewRequest("POST", "/bucket/"+key+"?uploads", nil)
		req.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("init status: %d", w.Code)
		}
	}

	init("foo/bar")
	init("baz.txt")

	listReq := httptest.NewRequest("GET", "/bucket?uploads&delimiter=/", nil)
	listW := httptest.NewRecorder()
	handler.ServeHTTP(listW, listReq)
	if listW.Code != http.StatusOK {
		t.Fatalf("list uploads status: %d", listW.Code)
	}
	body := listW.Body.String()
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
