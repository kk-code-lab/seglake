package s3

import (
	"strings"
	"testing"
	"time"
)

func FuzzParsePolicy(f *testing.F) {
	// Future: add seed corpus with complex conditions (CIDR/time/headers).
	f.Add("rw", "getobject", "demo", "a.txt", "x-amz-meta-test", "ok")
	f.Add("ro", "putobject", "demo", "a.txt", "x-amz-meta-test", "ok")
	f.Add(`{"version":"v1","statements":[{"effect":"allow","actions":["getobject"],"resources":[{"bucket":"demo","prefix":"a/"}]}]}`, "getobject", "demo", "a/b.txt", "x-amz-meta-test", "ok")
	f.Fuzz(func(t *testing.T, raw string, action string, bucket string, key string, headerKey string, headerValue string) {
		pol, err := ParsePolicy(raw)
		if err != nil {
			return
		}
		_ = pol.Allows(action, bucket, key)
		_, _ = pol.Decision(action, bucket, key)
		ctx := &PolicyContext{
			Now:      time.Unix(0, 0).UTC(),
			SourceIP: "127.0.0.1",
			Headers: map[string]string{
				strings.ToLower(headerKey): headerValue,
			},
		}
		_, _ = pol.DecisionWithContext(action, bucket, key, ctx)
	})
}
