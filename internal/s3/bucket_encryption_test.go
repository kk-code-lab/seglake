package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const bucketEncryptionBody = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`

func TestPutGetDeleteBucketEncryption(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "demo")

	getMissing := httptest.NewRequest(http.MethodGet, "/demo?encryption", nil)
	getMissingW := httptest.NewRecorder()
	h.ServeHTTP(getMissingW, getMissing)
	if getMissingW.Code != http.StatusNotFound {
		t.Fatalf("missing GET status: %d body=%s", getMissingW.Code, getMissingW.Body.String())
	}
	if !strings.Contains(getMissingW.Body.String(), "ServerSideEncryptionConfigurationNotFoundError") {
		t.Fatalf("expected missing encryption config error, got %s", getMissingW.Body.String())
	}

	put := httptest.NewRequest(http.MethodPut, "/demo?encryption", strings.NewReader(bucketEncryptionBody))
	put.Header.Set("Content-Type", "application/xml")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT encryption status: %d body=%s", putW.Code, putW.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/demo?encryption", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET encryption status: %d body=%s", getW.Code, getW.Body.String())
	}
	var cfg serverSideEncryptionConfiguration
	if err := xml.Unmarshal(getW.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("decode GET encryption: %v", err)
	}
	if len(cfg.Rules) != 1 || cfg.Rules[0].ApplyByDefault.SSEAlgorithm != "AES256" {
		t.Fatalf("unexpected encryption config: %+v", cfg)
	}

	del := httptest.NewRequest(http.MethodDelete, "/demo?encryption", nil)
	delW := httptest.NewRecorder()
	h.ServeHTTP(delW, del)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("DELETE encryption status: %d body=%s", delW.Code, delW.Body.String())
	}
	getAfterDelete := httptest.NewRequest(http.MethodGet, "/demo?encryption", nil)
	getAfterDeleteW := httptest.NewRecorder()
	h.ServeHTTP(getAfterDeleteW, getAfterDelete)
	if getAfterDeleteW.Code != http.StatusNotFound {
		t.Fatalf("GET after delete status: %d", getAfterDeleteW.Code)
	}
}

func TestBucketEncryptionVirtualHostedStyle(t *testing.T) {
	h := newTestHandler(t)
	h.VirtualHosted = true
	if err := h.Meta.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	put := httptest.NewRequest(http.MethodPut, "http://localhost/?encryption", strings.NewReader(bucketEncryptionBody))
	put.Host = "demo.localhost"
	put.Header.Set("Content-Type", "application/xml")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("virtual hosted PUT encryption status: %d body=%s", putW.Code, putW.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "http://localhost/?encryption", nil)
	get.Host = "demo.localhost"
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("virtual hosted GET encryption status: %d body=%s", getW.Code, getW.Body.String())
	}
	if !strings.Contains(getW.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>") {
		t.Fatalf("expected AES256 config, got %s", getW.Body.String())
	}

	del := httptest.NewRequest(http.MethodDelete, "http://localhost/?encryption", nil)
	del.Host = "demo.localhost"
	delW := httptest.NewRecorder()
	h.ServeHTTP(delW, del)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("virtual hosted DELETE encryption status: %d body=%s", delW.Code, delW.Body.String())
	}
}

func TestPutBucketEncryptionRejectsUnsupportedConfigs(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "demo")

	cases := []struct {
		name string
		body string
		want int
	}{
		{
			name: "invalid xml",
			body: "<ServerSideEncryptionConfiguration>",
			want: http.StatusBadRequest,
		},
		{
			name: "missing rule",
			body: `<ServerSideEncryptionConfiguration/>`,
			want: http.StatusBadRequest,
		},
		{
			name: "unsupported algorithm",
			body: `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES128</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			want: http.StatusBadRequest,
		},
		{
			name: "kms algorithm",
			body: `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			want: http.StatusNotImplemented,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/demo?encryption", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/xml")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("expected %d, got %d body=%s", tc.want, w.Code, w.Body.String())
			}
		})
	}
}

func TestBucketDefaultEncryptionAffectsPutCopyAndDelete(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	putBucketEncryption(t, h, "bucket")

	put := httptest.NewRequest(http.MethodPut, "/bucket/default-encrypted", strings.NewReader("secret"))
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT status: %d body=%s", putW.Code, putW.Body.String())
	}
	if got := putW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected default encrypted PUT header, got %q", got)
	}
	head := httptest.NewRequest(http.MethodHead, "/bucket/default-encrypted", nil)
	headW := httptest.NewRecorder()
	h.ServeHTTP(headW, head)
	if headW.Code != http.StatusOK {
		t.Fatalf("HEAD status: %d", headW.Code)
	}
	if got := headW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected encrypted object HEAD header, got %q", got)
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/bucket/default-copy", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/bucket/default-encrypted")
	copyW := httptest.NewRecorder()
	h.ServeHTTP(copyW, copyReq)
	if copyW.Code != http.StatusOK {
		t.Fatalf("copy status: %d body=%s", copyW.Code, copyW.Body.String())
	}
	if got := copyW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected default encrypted copy header, got %q", got)
	}

	badPut := httptest.NewRequest(http.MethodPut, "/bucket/bad", strings.NewReader("x"))
	badPut.Header.Set("X-Amz-Server-Side-Encryption", "AES128")
	badPutW := httptest.NewRecorder()
	h.ServeHTTP(badPutW, badPut)
	if badPutW.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid explicit header to fail, got %d", badPutW.Code)
	}

	del := httptest.NewRequest(http.MethodDelete, "/bucket?encryption", nil)
	delW := httptest.NewRecorder()
	h.ServeHTTP(delW, del)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("delete encryption status: %d body=%s", delW.Code, delW.Body.String())
	}
	plainPut := httptest.NewRequest(http.MethodPut, "/bucket/plain-after-delete", strings.NewReader("plain"))
	plainPutW := httptest.NewRecorder()
	h.ServeHTTP(plainPutW, plainPut)
	if plainPutW.Code != http.StatusOK {
		t.Fatalf("plain PUT status: %d body=%s", plainPutW.Code, plainPutW.Body.String())
	}
	if got := plainPutW.Header().Get("x-amz-server-side-encryption"); got != "" {
		t.Fatalf("expected plaintext PUT after deleting default, got %q", got)
	}
}

func TestBucketDefaultEncryptionAffectsMultipartInitiate(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	putBucketEncryption(t, h, "bucket")

	initReq := httptest.NewRequest(http.MethodPost, "/bucket/default-mpu?uploads", nil)
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

	partReq := httptest.NewRequest(http.MethodPut, "/bucket/default-mpu?partNumber=1&uploadId="+initResp.UploadID, strings.NewReader("tail"))
	partW := httptest.NewRecorder()
	h.ServeHTTP(partW, partReq)
	if partW.Code != http.StatusOK {
		t.Fatalf("part status: %d body=%s", partW.Code, partW.Body.String())
	}
	if got := partW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected part SSE header, got %q", got)
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + partW.Header().Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	completeReq := httptest.NewRequest(http.MethodPost, "/bucket/default-mpu?uploadId="+initResp.UploadID, strings.NewReader(completeBody))
	completeW := httptest.NewRecorder()
	h.ServeHTTP(completeW, completeReq)
	if completeW.Code != http.StatusOK {
		t.Fatalf("complete status: %d body=%s", completeW.Code, completeW.Body.String())
	}
	if got := completeW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected complete SSE header, got %q", got)
	}
	getReq := httptest.NewRequest(http.MethodGet, "/bucket/default-mpu", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK || getW.Body.String() != "tail" {
		t.Fatalf("GET status/body: %d %q", getW.Code, getW.Body.String())
	}
}

func TestBucketDefaultEncryptionFailsWhenSSES3Disabled(t *testing.T) {
	h := newTestHandlerWithoutSSE(t)
	createBucket(t, h, "bucket")
	if err := h.Meta.SetBucketEncryption(context.Background(), "bucket", "SSE-S3", "AES256"); err != nil {
		t.Fatalf("SetBucketEncryption: %v", err)
	}

	put := httptest.NewRequest(http.MethodPut, "/bucket/encrypted", strings.NewReader("secret"))
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", putW.Code, putW.Body.String())
	}
	if !strings.Contains(putW.Body.String(), "InvalidRequest") {
		t.Fatalf("expected InvalidRequest, got %s", putW.Body.String())
	}
}

func createBucket(t *testing.T, h *Handler, bucket string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create bucket %s: status=%d body=%s", bucket, w.Code, w.Body.String())
	}
}

func putBucketEncryption(t *testing.T, h *Handler, bucket string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?encryption", strings.NewReader(bucketEncryptionBody))
	req.Header.Set("Content-Type", "application/xml")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("put bucket encryption %s: status=%d body=%s", bucket, w.Code, w.Body.String())
	}
}
