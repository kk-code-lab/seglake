package s3

import (
	"net/url"
	"strings"
	"testing"
)

func TestRedactURL(t *testing.T) {
	u, err := url.Parse("http://example.com/bucket/key?X-Amz-Signature=abc&X-Amz-Credential=cred&foo=bar")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := redactURL(u)
	if got == "" || got == u.Path {
		t.Fatalf("expected query in redacted url")
	}
	if !containsAll(got, []string{"X-Amz-Signature=REDACTED", "X-Amz-Credential=REDACTED", "foo=bar"}) {
		t.Fatalf("redacted url mismatch: %s", got)
	}
}

func containsAll(s string, parts []string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
