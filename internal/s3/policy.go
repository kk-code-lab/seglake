package s3

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"
)

type Policy struct {
	Version    string      `json:"version"`
	Statements []Statement `json:"statements"`
}

type Statement struct {
	Effect     string     `json:"effect"`
	Actions    []string   `json:"actions"`
	Resources  []Resource `json:"resources"`
	Conditions Conditions `json:"conditions,omitempty"`
}

type Resource struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix,omitempty"`
}

type Conditions struct {
	SourceIP []string          `json:"source_ip,omitempty"`
	Before   string            `json:"before,omitempty"`
	After    string            `json:"after,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type PolicyContext struct {
	Now      time.Time
	SourceIP string
	Headers  map[string]string
}

const (
	policyEffectAllow = "allow"
	policyEffectDeny  = "deny"
)

const (
	policyActionAll = "*"

	policyActionListBuckets           = "listbuckets"
	policyActionListBucket            = "listbucket"
	policyActionGetBucketLocation     = "getbucketlocation"
	policyActionGetObject             = "getobject"
	policyActionHeadObject            = "headobject"
	policyActionPutObject             = "putobject"
	policyActionDeleteObject          = "deleteobject"
	policyActionDeleteBucket          = "deletebucket"
	policyActionCopyObject            = "copyobject"
	policyActionCreateMultipartUpload = "createmultipartupload"
	policyActionUploadPart            = "uploadpart"
	policyActionCompleteMultipart     = "completemultipartupload"
	policyActionAbortMultipart        = "abortmultipartupload"
	policyActionListMultipartUploads  = "listmultipartuploads"
	policyActionListMultipartParts    = "listmultipartparts"
	policyActionGetMetaStats          = "getmetastats"
)

var validPolicyActions = map[string]struct{}{
	policyActionAll:                   {},
	policyActionListBuckets:           {},
	policyActionListBucket:            {},
	policyActionGetBucketLocation:     {},
	policyActionGetObject:             {},
	policyActionHeadObject:            {},
	policyActionPutObject:             {},
	policyActionDeleteObject:          {},
	policyActionDeleteBucket:          {},
	policyActionCopyObject:            {},
	policyActionCreateMultipartUpload: {},
	policyActionUploadPart:            {},
	policyActionCompleteMultipart:     {},
	policyActionAbortMultipart:        {},
	policyActionListMultipartUploads:  {},
	policyActionListMultipartParts:    {},
	policyActionGetMetaStats:          {},
}

// ParsePolicy parses a policy JSON string or accepts short-hands "rw" and "ro".
func ParsePolicy(raw string) (*Policy, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "rw") {
		return &Policy{Version: "v1", Statements: []Statement{{
			Effect:  policyEffectAllow,
			Actions: []string{policyActionAll},
			Resources: []Resource{{
				Bucket: "*",
			}},
		}}}, nil
	}
	if strings.EqualFold(trimmed, "ro") || strings.EqualFold(trimmed, "read-only") {
		return &Policy{Version: "v1", Statements: []Statement{{
			Effect: policyEffectAllow,
			Actions: []string{
				policyActionListBuckets,
				policyActionListBucket,
				policyActionGetBucketLocation,
				policyActionGetObject,
				policyActionHeadObject,
				policyActionListMultipartUploads,
				policyActionListMultipartParts,
				policyActionGetMetaStats,
			},
			Resources: []Resource{{
				Bucket: "*",
			}},
		}}}, nil
	}
	var pol Policy
	if err := json.Unmarshal([]byte(trimmed), &pol); err != nil {
		return nil, err
	}
	if err := pol.validate(); err != nil {
		return nil, err
	}
	return &pol, nil
}

func (p *Policy) validate() error {
	if p == nil {
		return errors.New("policy required")
	}
	if len(p.Statements) == 0 {
		return errors.New("policy requires statements")
	}
	for i := range p.Statements {
		stmt := &p.Statements[i]
		stmt.Effect = strings.ToLower(strings.TrimSpace(stmt.Effect))
		if stmt.Effect != policyEffectAllow && stmt.Effect != policyEffectDeny {
			return errors.New("policy statement effect must be allow or deny")
		}
		if len(stmt.Actions) == 0 {
			return errors.New("policy statement requires actions")
		}
		for j := range stmt.Actions {
			a := normalizeAction(stmt.Actions[j])
			if _, ok := validPolicyActions[a]; !ok {
				return errors.New("policy statement has invalid action")
			}
			stmt.Actions[j] = a
		}
		if len(stmt.Resources) == 0 {
			return errors.New("policy statement requires resources")
		}
		for j := range stmt.Resources {
			res := &stmt.Resources[j]
			res.Bucket = strings.TrimSpace(res.Bucket)
			res.Prefix = strings.TrimSpace(res.Prefix)
			if res.Bucket == "" {
				return errors.New("policy resource bucket required")
			}
		}
		if err := stmt.Conditions.validate(); err != nil {
			return err
		}
	}
	return nil
}

// Allows returns whether the policy permits the action for a bucket/key pair.
func (p *Policy) Allows(action, bucket, key string) bool {
	if p == nil {
		return false
	}
	action = normalizeAction(action)
	bucket = strings.TrimSpace(bucket)
	key = strings.TrimSpace(key)
	allowed := false
	for _, stmt := range p.Statements {
		if !stmt.matches(action, bucket, key, nil) {
			continue
		}
		if stmt.Effect == policyEffectDeny {
			return false
		}
		allowed = true
	}
	return allowed
}

// Decision returns (allowed, denied) for the policy.
func (p *Policy) Decision(action, bucket, key string) (bool, bool) {
	if p == nil {
		return false, false
	}
	action = normalizeAction(action)
	bucket = strings.TrimSpace(bucket)
	key = strings.TrimSpace(key)
	allowed := false
	denied := false
	for _, stmt := range p.Statements {
		if !stmt.matches(action, bucket, key, nil) {
			continue
		}
		if stmt.Effect == policyEffectDeny {
			denied = true
		} else {
			allowed = true
		}
	}
	return allowed, denied
}

// DecisionWithContext evaluates policy with request context (ip/time/headers).
func (p *Policy) DecisionWithContext(action, bucket, key string, ctx *PolicyContext) (bool, bool) {
	if p == nil {
		return false, false
	}
	action = normalizeAction(action)
	bucket = strings.TrimSpace(bucket)
	key = strings.TrimSpace(key)
	allowed := false
	denied := false
	for _, stmt := range p.Statements {
		if !stmt.matches(action, bucket, key, ctx) {
			continue
		}
		if stmt.Effect == policyEffectDeny {
			denied = true
		} else {
			allowed = true
		}
	}
	return allowed, denied
}

func (s *Statement) matches(action, bucket, key string, ctx *PolicyContext) bool {
	if s == nil {
		return false
	}
	if !actionsMatch(s.Actions, action) {
		return false
	}
	if !s.Conditions.match(ctx) {
		return false
	}
	for _, res := range s.Resources {
		if res.matches(bucket, key) {
			return true
		}
	}
	return false
}

func actionsMatch(actions []string, action string) bool {
	for _, a := range actions {
		if a == policyActionAll || a == action {
			return true
		}
	}
	return false
}

func (r Resource) matches(bucket, key string) bool {
	if r.Bucket != "*" && r.Bucket != bucket {
		return false
	}
	if r.Prefix == "" {
		return true
	}
	return strings.HasPrefix(key, r.Prefix)
}

func (c *Conditions) validate() error {
	if c == nil {
		return nil
	}
	if c.Before != "" {
		if _, err := time.Parse(time.RFC3339, c.Before); err != nil {
			return errors.New("policy condition before must be RFC3339")
		}
	}
	if c.After != "" {
		if _, err := time.Parse(time.RFC3339, c.After); err != nil {
			return errors.New("policy condition after must be RFC3339")
		}
	}
	for _, cidr := range c.SourceIP {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return errors.New("policy condition source_ip must be CIDR")
		}
	}
	for k := range c.Headers {
		if strings.TrimSpace(k) == "" {
			return errors.New("policy condition headers require keys")
		}
	}
	return nil
}

func (c *Conditions) match(ctx *PolicyContext) bool {
	if c == nil {
		return true
	}
	if ctx == nil {
		return len(c.SourceIP) == 0 && c.Before == "" && c.After == "" && len(c.Headers) == 0
	}
	if len(c.SourceIP) > 0 {
		ip := net.ParseIP(ctx.SourceIP)
		if ip == nil {
			return false
		}
		ok := false
		for _, cidr := range c.SourceIP {
			_, network, err := net.ParseCIDR(cidr)
			if err != nil {
				continue
			}
			if network.Contains(ip) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if c.Before != "" {
		before, err := time.Parse(time.RFC3339, c.Before)
		if err != nil || !ctx.Now.Before(before) {
			return false
		}
	}
	if c.After != "" {
		after, err := time.Parse(time.RFC3339, c.After)
		if err != nil || !ctx.Now.After(after) {
			return false
		}
	}
	if len(c.Headers) > 0 {
		for k, v := range c.Headers {
			got := ""
			if ctx.Headers != nil {
				got = ctx.Headers[strings.ToLower(k)]
			}
			if got != v {
				return false
			}
		}
	}
	return true
}

func policyActionForRequest(op string) string {
	switch op {
	case "meta_stats":
		return policyActionGetMetaStats
	case "list_buckets":
		return policyActionListBuckets
	case "list_v1", "list_v2":
		return policyActionListBucket
	case "get":
		return policyActionGetObject
	case "head":
		return policyActionHeadObject
	case "put":
		return policyActionPutObject
	case "delete":
		return policyActionDeleteObject
	case "delete_bucket":
		return policyActionDeleteBucket
	case "copy":
		return policyActionCopyObject
	case "mpu_initiate":
		return policyActionCreateMultipartUpload
	case "mpu_upload_part":
		return policyActionUploadPart
	case "mpu_complete":
		return policyActionCompleteMultipart
	case "mpu_abort":
		return policyActionAbortMultipart
	case "mpu_list_uploads":
		return policyActionListMultipartUploads
	case "mpu_list_parts":
		return policyActionListMultipartParts
	default:
		return ""
	}
}

func normalizeAction(action string) string {
	action = strings.ToLower(strings.TrimSpace(action))
	action = strings.TrimPrefix(action, "s3:")
	return action
}
