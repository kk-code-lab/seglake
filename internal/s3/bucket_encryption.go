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
	if cfg.Mode != meta.BucketEncryptionModeSSES3 || cfg.Algorithm != meta.BucketEncryptionAlgorithmAES256 {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "unsupported bucket encryption configuration", requestID, r.URL.Path)
		return
	}
	resp := serverSideEncryptionConfiguration{
		Xmlns: bucketEncryptionXMLNamespace,
		Rules: []serverSideEncryptionRule{{
			ApplyByDefault: serverSideEncryptionByDefault{SSEAlgorithm: ssecrypto.ServerSideHeaderS3},
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
		if errors.Is(err, errBucketEncryptionKMSUnsupported) {
			writeErrorWithResource(w, http.StatusNotImplemented, "NotImplemented", "SSE-KMS bucket encryption is not supported", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", err.Error(), requestID, r.URL.Path)
		return
	}
	if err := h.Meta.SetBucketEncryption(ctx, bucket, meta.BucketEncryptionModeSSES3, meta.BucketEncryptionAlgorithmAES256); err != nil {
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

var errBucketEncryptionKMSUnsupported = errors.New("bucket encryption KMS unsupported")

func validateBucketEncryptionConfig(cfg serverSideEncryptionConfiguration) error {
	if len(cfg.Rules) != 1 {
		return fmt.Errorf("bucket encryption requires exactly one rule")
	}
	rule := cfg.Rules[0]
	alg := strings.TrimSpace(rule.ApplyByDefault.SSEAlgorithm)
	if rule.ApplyByDefault.KMSMasterKeyID != "" || isKMSAlgorithm(alg) {
		return errBucketEncryptionKMSUnsupported
	}
	if rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled {
		return errBucketEncryptionKMSUnsupported
	}
	if alg != ssecrypto.ServerSideHeaderS3 {
		return fmt.Errorf("unsupported bucket encryption algorithm")
	}
	return nil
}

func isKMSAlgorithm(alg string) bool {
	return strings.EqualFold(alg, "aws:kms") || strings.EqualFold(alg, "aws:kms:dsse")
}

func (h *Handler) effectiveSSES3ForWrite(ctx context.Context, r *http.Request, bucket string) (bool, *requestError) {
	requested, reqErr := sseS3Requested(r)
	if reqErr != nil || requested {
		return requested, reqErr
	}
	if h == nil || h.Meta == nil || bucket == "" {
		return false, nil
	}
	cfg, err := h.Meta.GetBucketEncryption(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, &requestError{status: http.StatusInternalServerError, code: "InternalError", message: err.Error()}
	}
	if cfg.Mode == meta.BucketEncryptionModeSSES3 && cfg.Algorithm == meta.BucketEncryptionAlgorithmAES256 {
		return true, nil
	}
	return false, &requestError{status: http.StatusInternalServerError, code: "InternalError", message: "unsupported bucket encryption configuration"}
}
