package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const objectTaggingBody = `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>project</Key><Value>alpha</Value></Tag><Tag><Key>env</Key><Value>dev</Value></Tag></TagSet></Tagging>`

func TestObjectTaggingXMLAndHeaderValidation(t *testing.T) {
	t.Parallel()
	xmlReq := httptest.NewRequest(http.MethodPut, "/bucket/key?tagging", strings.NewReader(objectTaggingBody))
	tags, err := parseObjectTaggingXML(xmlReq)
	if err != nil {
		t.Fatalf("parseObjectTaggingXML: %v", err)
	}
	if len(tags) != 2 || tags[0].Key != "env" || tags[1].Key != "project" {
		t.Fatalf("unexpected xml tags: %+v", tags)
	}
	headerTags, err := parseTaggingHeader("project=alpha+one&empty=&space=a%20b")
	if err != nil {
		t.Fatalf("parseTaggingHeader: %v", err)
	}
	if len(headerTags) != 3 {
		t.Fatalf("expected 3 header tags, got %+v", headerTags)
	}
	cases := []string{
		"=value",
		"dup=1&dup=2",
		"k0=v&k1=v&k2=v&k3=v&k4=v&k5=v&k6=v&k7=v&k8=v&k9=v&k10=v",
		strings.Repeat("x", 129) + "=v",
		"k=" + strings.Repeat("x", 257),
	}
	for _, raw := range cases {
		if _, err := parseTaggingHeader(raw); err == nil {
			t.Fatalf("expected invalid tag header %q", raw)
		}
	}
	if _, err := parseTaggingHeader(strings.Repeat("\U0001F600", 65) + "=v"); err == nil {
		t.Fatalf("expected UTF-16 key length limit to reject surrogate pairs")
	}
}

func TestPutGetDeleteObjectTagging(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	putTaggedObject(t, h, "/bucket/key", "data", nil)

	put := httptest.NewRequest(http.MethodPut, "/bucket/key?tagging", strings.NewReader(objectTaggingBody))
	put.Header.Set("Content-Type", "application/xml")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT tagging status: %d body=%s", putW.Code, putW.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET tagging status: %d body=%s", getW.Code, getW.Body.String())
	}
	var got objectTaggingXML
	if err := xml.Unmarshal(getW.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode tags: %v", err)
	}
	if len(got.TagSet.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %+v body=%s", got.TagSet.Tags, getW.Body.String())
	}

	head := httptest.NewRequest(http.MethodHead, "/bucket/key", nil)
	headW := httptest.NewRecorder()
	h.ServeHTTP(headW, head)
	if headW.Code != http.StatusOK {
		t.Fatalf("HEAD status: %d", headW.Code)
	}
	if got := headW.Header().Get("x-amz-tagging-count"); got != "2" {
		t.Fatalf("expected tagging count 2, got %q", got)
	}

	del := httptest.NewRequest(http.MethodDelete, "/bucket/key?tagging", nil)
	delW := httptest.NewRecorder()
	h.ServeHTTP(delW, del)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("DELETE tagging status: %d body=%s", delW.Code, delW.Body.String())
	}
	getAfterDelete := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging", nil)
	getAfterDeleteW := httptest.NewRecorder()
	h.ServeHTTP(getAfterDeleteW, getAfterDelete)
	if getAfterDeleteW.Code != http.StatusOK {
		t.Fatalf("GET after delete status: %d body=%s", getAfterDeleteW.Code, getAfterDeleteW.Body.String())
	}
	if strings.Contains(getAfterDeleteW.Body.String(), "<Tag>") {
		t.Fatalf("expected empty tag set, got %s", getAfterDeleteW.Body.String())
	}
}

func TestObjectTaggingVersionIDTargetsSpecificVersion(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	v1 := putTaggedObject(t, h, "/bucket/key", "v1", map[string]string{"x-amz-tagging": "version=one"})
	v2 := putTaggedObject(t, h, "/bucket/key", "v2", map[string]string{"x-amz-tagging": "version=two"})

	put := httptest.NewRequest(http.MethodPut, "/bucket/key?tagging&versionId="+v1, strings.NewReader(`<Tagging><TagSet><Tag><Key>version</Key><Value>updated</Value></Tag></TagSet></Tagging>`))
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT version tagging status: %d body=%s", putW.Code, putW.Body.String())
	}

	getV1 := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging&versionId="+v1, nil)
	getV1W := httptest.NewRecorder()
	h.ServeHTTP(getV1W, getV1)
	if !strings.Contains(getV1W.Body.String(), "<Value>updated</Value>") {
		t.Fatalf("expected updated v1 tags, got %s", getV1W.Body.String())
	}
	getV2 := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging&versionId="+v2, nil)
	getV2W := httptest.NewRecorder()
	h.ServeHTTP(getV2W, getV2)
	if !strings.Contains(getV2W.Body.String(), "<Value>two</Value>") {
		t.Fatalf("expected original v2 tags, got %s", getV2W.Body.String())
	}
}

func TestPutObjectTaggingHeaderAndInvalidHeaderAtomicity(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	putTaggedObject(t, h, "/bucket/tagged", "data", map[string]string{"x-amz-tagging": "project=alpha&empty="})
	get := httptest.NewRequest(http.MethodGet, "/bucket/tagged?tagging", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK || !strings.Contains(getW.Body.String(), "<Key>project</Key>") || !strings.Contains(getW.Body.String(), "<Key>empty</Key>") {
		t.Fatalf("expected stored header tags, status=%d body=%s", getW.Code, getW.Body.String())
	}

	bad := httptest.NewRequest(http.MethodPut, "/bucket/bad", strings.NewReader("data"))
	bad.Header.Set("x-amz-tagging", "=value")
	badW := httptest.NewRecorder()
	h.ServeHTTP(badW, bad)
	if badW.Code != http.StatusBadRequest {
		t.Fatalf("invalid tagged PUT status: %d body=%s", badW.Code, badW.Body.String())
	}
	missing := httptest.NewRequest(http.MethodGet, "/bucket/bad", nil)
	missingW := httptest.NewRecorder()
	h.ServeHTTP(missingW, missing)
	if missingW.Code != http.StatusNotFound {
		t.Fatalf("invalid tagged PUT left visible object: status=%d body=%s", missingW.Code, missingW.Body.String())
	}
}

func TestCopyObjectTaggingDirectives(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "bucket")
	putTaggedObject(t, h, "/bucket/source", "data", map[string]string{"x-amz-tagging": "project=alpha"})

	copyReq := httptest.NewRequest(http.MethodPut, "/bucket/copy", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/bucket/source")
	copyW := httptest.NewRecorder()
	h.ServeHTTP(copyW, copyReq)
	if copyW.Code != http.StatusOK {
		t.Fatalf("COPY status: %d body=%s", copyW.Code, copyW.Body.String())
	}
	getCopy := httptest.NewRequest(http.MethodGet, "/bucket/copy?tagging", nil)
	getCopyW := httptest.NewRecorder()
	h.ServeHTTP(getCopyW, getCopy)
	if !strings.Contains(getCopyW.Body.String(), "<Value>alpha</Value>") {
		t.Fatalf("expected copied tags, got %s", getCopyW.Body.String())
	}

	replaceReq := httptest.NewRequest(http.MethodPut, "/bucket/replaced", nil)
	replaceReq.Header.Set("X-Amz-Copy-Source", "/bucket/source")
	replaceReq.Header.Set("x-amz-tagging-directive", "REPLACE")
	replaceReq.Header.Set("x-amz-tagging", "project=beta")
	replaceW := httptest.NewRecorder()
	h.ServeHTTP(replaceW, replaceReq)
	if replaceW.Code != http.StatusOK {
		t.Fatalf("COPY REPLACE status: %d body=%s", replaceW.Code, replaceW.Body.String())
	}
	getReplace := httptest.NewRequest(http.MethodGet, "/bucket/replaced?tagging", nil)
	getReplaceW := httptest.NewRecorder()
	h.ServeHTTP(getReplaceW, getReplace)
	if !strings.Contains(getReplaceW.Body.String(), "<Value>beta</Value>") || strings.Contains(getReplaceW.Body.String(), "<Value>alpha</Value>") {
		t.Fatalf("expected replaced tags, got %s", getReplaceW.Body.String())
	}

	badReq := httptest.NewRequest(http.MethodPut, "/bucket/bad-copy", nil)
	badReq.Header.Set("X-Amz-Copy-Source", "/bucket/source")
	badReq.Header.Set("x-amz-tagging-directive", "INVALID")
	badW := httptest.NewRecorder()
	h.ServeHTTP(badW, badReq)
	if badW.Code != http.StatusBadRequest {
		t.Fatalf("bad directive status: %d body=%s", badW.Code, badW.Body.String())
	}
}

func TestObjectTaggingPolicyActions(t *testing.T) {
	t.Parallel()
	pol, err := ParsePolicy(`{"version":"v1","statements":[{"effect":"allow","actions":["GetObjectTagging","PutObjectTagging","DeleteObjectTagging"],"resources":[{"bucket":"bucket","prefix":"a/"}]}]}`)
	if err != nil {
		t.Fatalf("ParsePolicy native: %v", err)
	}
	for _, action := range []string{policyActionGetObjectTagging, policyActionPutObjectTagging, policyActionDeleteObjectTagging} {
		if allowed, denied := pol.DecisionWithContext(action, "bucket", "a/key", nil); !allowed || denied {
			t.Fatalf("expected action %s allowed", action)
		}
	}
	awsPol, err := ParsePolicy(`{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":["s3:GetObjectVersionTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":"arn:aws:s3:::bucket/a/*"}}`)
	if err != nil {
		t.Fatalf("ParsePolicy AWS: %v", err)
	}
	if allowed, denied := awsPol.DecisionWithContext(policyActionGetObjectTagging, "bucket", "a/key", nil); !allowed || denied {
		t.Fatalf("expected AWS alias get object tagging allowed")
	}
}

func putTaggedObject(t *testing.T, h *Handler, target, body string, headers map[string]string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, target, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT %s status=%d body=%s", target, w.Code, w.Body.String())
	}
	return w.Header().Get("x-amz-version-id")
}
