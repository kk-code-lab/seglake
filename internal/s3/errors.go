package s3

import (
	"encoding/xml"
	"net/http"
)

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
	HostID    string   `xml:"HostId"`
}

func writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	writeErrorWithResource(w, status, code, message, requestID, "")
}

func writeErrorWithResource(w http.ResponseWriter, status int, code, message, requestID, resource string) {
	if code != "" {
		if mapped, ok := statusByCode[code]; ok {
			status = mapped
		}
		if message == "" {
			if def, ok := defaultMessageByCode[code]; ok {
				message = def
			}
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	resp := errorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: requestID,
		HostID:    hostID(),
	}
	_ = xml.NewEncoder(w).Encode(resp)
}

var statusByCode = map[string]int{
	"AccessDenied":              http.StatusForbidden,
	"BucketNotEmpty":            http.StatusConflict,
	"InternalError":             http.StatusInternalServerError,
	"InvalidArgument":           http.StatusBadRequest,
	"InvalidBucketName":         http.StatusBadRequest,
	"InvalidDigest":             http.StatusBadRequest,
	"InvalidPart":               http.StatusBadRequest,
	"InvalidRange":              http.StatusRequestedRangeNotSatisfiable,
	"InvalidRequest":            http.StatusBadRequest,
	"InvalidURI":                http.StatusBadRequest,
	"MethodNotAllowed":          http.StatusMethodNotAllowed,
	"NoSuchBucket":              http.StatusNotFound,
	"NoSuchKey":                 http.StatusNotFound,
	"NoSuchUpload":              http.StatusNotFound,
	"PreconditionFailed":        http.StatusPreconditionFailed,
	"RequestTimeTooSkewed":      http.StatusForbidden,
	"SignatureDoesNotMatch":     http.StatusForbidden,
	"SlowDown":                  http.StatusServiceUnavailable,
	"XAmzContentSHA256Mismatch": http.StatusBadRequest,
}

var defaultMessageByCode = map[string]string{
	"AccessDenied":              "access denied",
	"BucketNotEmpty":            "bucket not empty",
	"InternalError":             "internal error",
	"InvalidArgument":           "invalid argument",
	"InvalidBucketName":         "invalid bucket name",
	"InvalidDigest":             "invalid digest",
	"InvalidPart":               "invalid part",
	"InvalidRange":              "invalid range",
	"InvalidRequest":            "invalid request",
	"InvalidURI":                "invalid uri",
	"MethodNotAllowed":          "the specified method is not allowed against this resource",
	"NoSuchBucket":              "bucket not found",
	"NoSuchKey":                 "key not found",
	"NoSuchUpload":              "upload not found",
	"PreconditionFailed":        "precondition failed",
	"RequestTimeTooSkewed":      "request time too skewed",
	"SignatureDoesNotMatch":     "signature mismatch",
	"SlowDown":                  "slow down",
	"XAmzContentSHA256Mismatch": "payload hash mismatch",
}
