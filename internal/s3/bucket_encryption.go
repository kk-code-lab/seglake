package s3

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
)

const bucketEncryptionXMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type serverSideEncryptionConfiguration struct {
	XMLName xml.Name                   `xml:"ServerSideEncryptionConfiguration"`
	Xmlns   string                     `xml:"xmlns,attr,omitempty"`
	Rules   []serverSideEncryptionRule `xml:"Rule"`
}

type serverSideEncryptionRule struct {
	ApplyByDefault   serverSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault"`
	BucketKeyEnabled *bool                         `xml:"BucketKeyEnabled,omitempty"`
}

type serverSideEncryptionByDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty"`
}

func (h *Handler) handleGetBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	cfg, err := h.Meta.GetBucketEncryption(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "ServerSideEncryptionConfigurationNotFoundError", "bucket encryption configuration not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !supportedBucketEncryptionConfig(cfg.Mode, cfg.Algorithm) {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "unsupported bucket encryption configuration", requestID, r.URL.Path)
		return
	}
	alg := ssecrypto.ServerSideHeaderS3
	keyID := ""
	if cfg.Mode == meta.BucketEncryptionModeSSEKMS {
		alg = ssecrypto.ServerSideHeaderKMS
		keyID = cfg.KeyID
	}
	resp := serverSideEncryptionConfiguration{
		Xmlns: bucketEncryptionXMLNamespace,
		Rules: []serverSideEncryptionRule{{
			ApplyByDefault: serverSideEncryptionByDefault{SSEAlgorithm: alg, KMSMasterKeyID: keyID},
		}},
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handlePutBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	var req serverSideEncryptionConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid xml", requestID, r.URL.Path)
		return
	}
	if err := validateBucketEncryptionConfig(req); err != nil {
		if errors.Is(err, errBucketEncryptionUnsupportedFeature) {
			writeErrorWithResource(w, http.StatusNotImplemented, "NotImplemented", err.Error(), requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", err.Error(), requestID, r.URL.Path)
		return
	}
	rule := req.Rules[0]
	mode := meta.BucketEncryptionModeSSES3
	algorithm := meta.BucketEncryptionAlgorithmAES256
	keyID := ""
	if strings.EqualFold(strings.TrimSpace(rule.ApplyByDefault.SSEAlgorithm), ssecrypto.ServerSideHeaderKMS) {
		mode = meta.BucketEncryptionModeSSEKMS
		algorithm = meta.BucketEncryptionAlgorithmAWSKMS
		keyID = strings.TrimSpace(rule.ApplyByDefault.KMSMasterKeyID)
	}
	if err := h.Meta.SetBucketEncryptionWithKey(ctx, bucket, mode, algorithm, keyID); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	if err := h.Meta.DeleteBucketEncryption(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var errBucketEncryptionUnsupportedFeature = errors.New("unsupported bucket encryption feature")

func validateBucketEncryptionConfig(cfg serverSideEncryptionConfiguration) error {
	if len(cfg.Rules) != 1 {
		return fmt.Errorf("bucket encryption requires exactly one rule")
	}
	rule := cfg.Rules[0]
	alg := strings.TrimSpace(rule.ApplyByDefault.SSEAlgorithm)
	if rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled {
		return fmt.Errorf("%w: bucket keys are not implemented", errBucketEncryptionUnsupportedFeature)
	}
	if strings.EqualFold(alg, "aws:kms:dsse") {
		return fmt.Errorf("%w: DSSE-KMS is not implemented", errBucketEncryptionUnsupportedFeature)
	}
	if strings.EqualFold(alg, ssecrypto.ServerSideHeaderKMS) {
		return nil
	}
	if rule.ApplyByDefault.KMSMasterKeyID != "" {
		return fmt.Errorf("KMSMasterKeyID requires aws:kms")
	}
	if alg != ssecrypto.ServerSideHeaderS3 {
		return fmt.Errorf("unsupported bucket encryption algorithm")
	}
	return nil
}

type effectiveEncryptionMode int

const (
	effectiveEncryptionPlaintext effectiveEncryptionMode = iota
	effectiveEncryptionSSES3
	effectiveEncryptionSSEKMS
)

type effectiveEncryption struct {
	Mode     effectiveEncryptionMode
	KeyID    string
	Explicit bool
}

func (e effectiveEncryption) Encrypted() bool {
	return e.Mode == effectiveEncryptionSSES3 || e.Mode == effectiveEncryptionSSEKMS
}

func (e effectiveEncryption) SSES3() bool {
	return e.Mode == effectiveEncryptionSSES3
}

func (e effectiveEncryption) SSEKMS() bool {
	return e.Mode == effectiveEncryptionSSEKMS
}

func (e effectiveEncryption) HeaderValue() string {
	switch e.Mode {
	case effectiveEncryptionSSES3:
		return ssecrypto.ServerSideHeaderS3
	case effectiveEncryptionSSEKMS:
		return ssecrypto.ServerSideHeaderKMS
	default:
		return ""
	}
}

func (h *Handler) effectiveEncryptionForWrite(ctx context.Context, r *http.Request, bucket string) (effectiveEncryption, *requestError) {
	requested, reqErr := explicitEncryptionForWrite(r)
	if reqErr != nil || requested.SSES3() || (requested.SSEKMS() && requested.KeyID != "") {
		return requested, reqErr
	}
	if requested.SSEKMS() {
		keyID := ""
		if h != nil && h.Meta != nil && bucket != "" {
			cfg, err := h.Meta.GetBucketEncryption(ctx, bucket)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return effectiveEncryption{}, &requestError{status: http.StatusInternalServerError, code: "InternalError", message: err.Error()}
			}
			if err == nil && cfg.Mode == meta.BucketEncryptionModeSSEKMS && cfg.Algorithm == meta.BucketEncryptionAlgorithmAWSKMS {
				keyID = strings.TrimSpace(cfg.KeyID)
			}
		}
		if keyID == "" && h != nil && h.Engine != nil {
			keyID = h.Engine.SSEDefaultKeyID()
		}
		if keyID == "" {
			return effectiveEncryption{}, &requestError{status: http.StatusBadRequest, code: "InvalidRequest", message: "SSE-KMS key id could not be resolved"}
		}
		return effectiveEncryption{Mode: effectiveEncryptionSSEKMS, KeyID: keyID, Explicit: true}, nil
	}
	if h == nil || h.Meta == nil || bucket == "" {
		return effectiveEncryption{}, nil
	}
	cfg, err := h.Meta.GetBucketEncryption(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return effectiveEncryption{}, nil
		}
		return effectiveEncryption{}, &requestError{status: http.StatusInternalServerError, code: "InternalError", message: err.Error()}
	}
	if cfg.Mode == meta.BucketEncryptionModeSSES3 && cfg.Algorithm == meta.BucketEncryptionAlgorithmAES256 {
		return effectiveEncryption{Mode: effectiveEncryptionSSES3}, nil
	}
	if cfg.Mode == meta.BucketEncryptionModeSSEKMS && cfg.Algorithm == meta.BucketEncryptionAlgorithmAWSKMS {
		keyID := strings.TrimSpace(cfg.KeyID)
		if keyID == "" && h.Engine != nil {
			keyID = h.Engine.SSEDefaultKeyID()
		}
		if keyID == "" {
			return effectiveEncryption{}, &requestError{status: http.StatusBadRequest, code: "InvalidRequest", message: "SSE-KMS key id could not be resolved"}
		}
		return effectiveEncryption{Mode: effectiveEncryptionSSEKMS, KeyID: keyID}, nil
	}
	return effectiveEncryption{}, &requestError{status: http.StatusInternalServerError, code: "InternalError", message: "unsupported bucket encryption configuration"}
}

func explicitEncryptionForWrite(r *http.Request) (effectiveEncryption, *requestError) {
	value := strings.TrimSpace(r.Header.Get("X-Amz-Server-Side-Encryption"))
	kmsKeyID := strings.TrimSpace(r.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))
	contextHeader := strings.TrimSpace(r.Header.Get("X-Amz-Server-Side-Encryption-Context"))
	bucketKeyEnabled := strings.TrimSpace(r.Header.Get("X-Amz-Server-Side-Encryption-Bucket-Key-Enabled"))
	if contextHeader != "" {
		return effectiveEncryption{}, &requestError{status: http.StatusNotImplemented, code: "NotImplemented", message: "SSE-KMS encryption context is not implemented"}
	}
	if strings.EqualFold(bucketKeyEnabled, "true") {
		return effectiveEncryption{}, &requestError{status: http.StatusNotImplemented, code: "NotImplemented", message: "SSE-KMS bucket keys are not implemented"}
	}
	if value == "" {
		if kmsKeyID != "" || bucketKeyEnabled != "" {
			return effectiveEncryption{}, &requestError{status: http.StatusBadRequest, code: "InvalidArgument", message: "SSE-KMS parameters require aws:kms encryption"}
		}
		return effectiveEncryption{}, nil
	}
	switch {
	case value == ssecrypto.ServerSideHeaderS3:
		if kmsKeyID != "" || bucketKeyEnabled != "" {
			return effectiveEncryption{}, &requestError{status: http.StatusBadRequest, code: "InvalidArgument", message: "SSE-KMS parameters require aws:kms encryption"}
		}
		return effectiveEncryption{Mode: effectiveEncryptionSSES3, Explicit: true}, nil
	case strings.EqualFold(value, ssecrypto.ServerSideHeaderKMS):
		return effectiveEncryption{Mode: effectiveEncryptionSSEKMS, KeyID: kmsKeyID, Explicit: true}, nil
	case strings.EqualFold(value, "aws:kms:dsse"):
		return effectiveEncryption{}, &requestError{status: http.StatusNotImplemented, code: "NotImplemented", message: "DSSE-KMS is not implemented"}
	default:
		return effectiveEncryption{}, &requestError{status: http.StatusBadRequest, code: "InvalidArgument", message: "unsupported server-side encryption"}
	}
}

func supportedBucketEncryptionConfig(mode, algorithm string) bool {
	return (mode == meta.BucketEncryptionModeSSES3 && algorithm == meta.BucketEncryptionAlgorithmAES256) ||
		(mode == meta.BucketEncryptionModeSSEKMS && algorithm == meta.BucketEncryptionAlgorithmAWSKMS)
}
