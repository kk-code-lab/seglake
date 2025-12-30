package s3

import (
	"testing"
	"time"
)

func TestParsePolicyShortHands(t *testing.T) {
	pol, err := ParsePolicy("rw")
	if err != nil {
		t.Fatalf("ParsePolicy rw: %v", err)
	}
	if !pol.Allows("PutObject", "b", "k") {
		t.Fatalf("expected rw to allow write")
	}
	pol, err = ParsePolicy("ro")
	if err != nil {
		t.Fatalf("ParsePolicy ro: %v", err)
	}
	if pol.Allows("PutObject", "b", "k") {
		t.Fatalf("expected ro to deny write")
	}
}

func TestParsePolicyJSON(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"p/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy json: %v", err)
	}
	if !pol.Allows("GetObject", "demo", "p/x") {
		t.Fatalf("expected allow for prefix")
	}
	if pol.Allows("GetObject", "demo", "q/x") {
		t.Fatalf("expected deny for non-prefix")
	}
}

func TestPolicyDenyOverrides(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"*"}]},{"effect":"deny","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy deny: %v", err)
	}
	if !pol.Allows("GetObject", "demo", "public/x") {
		t.Fatalf("expected allow for public")
	}
	if pol.Allows("GetObject", "demo", "secret/x") {
		t.Fatalf("expected deny for secret")
	}
}

func TestPolicyActionForRequest(t *testing.T) {
	op := policyActionForRequest("mpu_upload_part")
	if op != policyActionUploadPart {
		t.Fatalf("expected upload part action, got %q", op)
	}
}

func TestPolicyConditionsSourceIP(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"source_ip":["10.0.0.0/8"]}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), SourceIP: "10.1.1.1"}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow for source ip")
	}
	ctx.SourceIP = "192.168.1.1"
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny for non-matching source ip")
	}
}

func TestPolicyConditionsBeforeAfter(t *testing.T) {
	now := time.Now().UTC()
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"after":"` + now.Add(-time.Minute).Format(time.RFC3339) + `","before":"` + now.Add(time.Minute).Format(time.RFC3339) + `"}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: now}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow within time window")
	}
	ctx.Now = now.Add(2 * time.Minute)
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny outside time window")
	}
}

func TestPolicyConditionsHeaders(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"headers":{"x-tenant":"alpha"}}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), Headers: map[string]string{"x-tenant": "alpha"}}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow for header match")
	}
	ctx.Headers["x-tenant"] = "beta"
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny for header mismatch")
	}
}

func TestParsePolicyAWSBasic(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::demo/public/*"
    }
  ]
}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy aws: %v", err)
	}
	if !pol.Allows("GetObject", "demo", "public/x") {
		t.Fatalf("expected allow for aws prefix")
	}
	if pol.Allows("GetObject", "demo", "private/x") {
		t.Fatalf("expected deny for non-prefix")
	}
}

func TestParsePolicyAWSConditions(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::demo",
      "Condition": {
        "IpAddress": { "aws:SourceIp": ["10.0.0.0/8"] },
        "DateGreaterThan": { "aws:CurrentTime": "1970-01-01T00:00:00Z" },
        "DateLessThan": { "aws:CurrentTime": "2999-01-01T00:00:00Z" }
      }
    }
  ]
}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy aws: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), SourceIP: "10.1.1.1"}
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); !allowed {
		t.Fatalf("expected allow for aws conditions")
	}
}

func TestParsePolicyAWSUnsupportedAction(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "s3:PutBucketAcl",
    "Resource": "arn:aws:s3:::demo"
  }]
}`
	if _, err := ParsePolicy(raw); err == nil {
		t.Fatalf("expected unsupported action error")
	}
}

func TestParsePolicyAWSUnsupportedPrincipal(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::demo",
    "Principal": {"AWS": "arn:aws:iam::123456789012:user/bob"}
  }]
}`
	if _, err := ParsePolicy(raw); err == nil {
		t.Fatalf("expected unsupported principal error")
	}
}

func TestParsePolicyAWSUnsupportedCondition(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::demo",
    "Condition": {"StringEquals": {"s3:max-keys": "10"}}
  }]
}`
	if _, err := ParsePolicy(raw); err == nil {
		t.Fatalf("expected unsupported condition error")
	}
}

func TestPolicyConditionsPrefixDelimiterSecureTransport(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["ListBucket"],"resources":[{"bucket":"demo"}],"conditions":{"prefix":"public/","delimiter":"/","secure_transport":true}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), Prefix: "public/", Delimiter: "/", SecureTransport: true}
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); !allowed {
		t.Fatalf("expected allow for prefix/delimiter/secure transport")
	}
	ctx.Prefix = "private/"
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for prefix mismatch")
	}
	ctx.Prefix = "public/"
	ctx.Delimiter = "-"
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for delimiter mismatch")
	}
	ctx.Delimiter = "/"
	ctx.SecureTransport = false
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for insecure transport")
	}
}

func TestParsePolicyAWSConditionsPrefixAndSecureTransport(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::demo",
      "Condition": {
        "StringEquals": { "s3:prefix": "public/" },
        "Bool": { "aws:SecureTransport": "true" }
      }
    }
  ]
}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy aws: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), Prefix: "public/", SecureTransport: true}
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); !allowed {
		t.Fatalf("expected allow for aws prefix/secure transport")
	}
	ctx.Prefix = "private/"
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for aws prefix mismatch")
	}
	ctx.Prefix = "public/"
	ctx.SecureTransport = false
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for insecure transport")
	}
}

func TestParsePolicyAWSConditionsPrefixLike(t *testing.T) {
	raw := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::demo",
      "Condition": {
        "StringLike": { "s3:prefix": "public/*" }
      }
    }
  ]
}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy aws: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), Prefix: "public/data"}
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); !allowed {
		t.Fatalf("expected allow for prefix like")
	}
	ctx.Prefix = "private/data"
	if allowed, _ := pol.DecisionWithContext("ListBucket", "demo", "", ctx); allowed {
		t.Fatalf("expected deny for prefix like mismatch")
	}
}

func FuzzParsePolicyAWS(f *testing.F) {
	valid := `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": "arn:aws:s3:::demo/public/*"
  }]
}`
	invalid := `{ "Version": "2012-10-17", "Statement": [{"Effect": "Allow"}] }`
	f.Add(valid)
	f.Add(invalid)
	f.Add(`{"Statement": "nope"}`)
	f.Add(`{"Version": 1, "Statement": []}`)
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = ParsePolicy(input)
	})
}
