package s3

import (
	"encoding/json"
	"errors"
	"strings"
)

type Policy struct {
	Version    string      `json:"version"`
	Statements []Statement `json:"statements"`
}

type Statement struct {
	Effect    string     `json:"effect"`
	Actions   []string   `json:"actions"`
	Resources []Resource `json:"resources"`
}

type Resource struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix,omitempty"`
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
		if !stmt.matches(action, bucket, key) {
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
		if !stmt.matches(action, bucket, key) {
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

func (s *Statement) matches(action, bucket, key string) bool {
	if s == nil {
		return false
	}
	if !actionsMatch(s.Actions, action) {
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
