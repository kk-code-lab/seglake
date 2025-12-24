package s3

import (
	"bytes"
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
