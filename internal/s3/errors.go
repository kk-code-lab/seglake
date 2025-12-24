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
	if requestID != "" && w.Header().Get("x-amz-request-id") == "" {
		w.Header().Set("x-amz-request-id", requestID)
	}
	if w.Header().Get("x-amz-id-2") == "" {
		w.Header().Set("x-amz-id-2", hostID())
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
	"AccessDenied":                 http.StatusForbidden,
	"AuthorizationHeaderMalformed": http.StatusBadRequest,
	"BadDigest":                    http.StatusBadRequest,
	"BucketNotEmpty":               http.StatusConflict,
	"EntityTooLarge":               http.StatusRequestEntityTooLarge,
	"InternalError":                http.StatusInternalServerError,
	"InvalidArgument":              http.StatusBadRequest,
	"InvalidBucketName":            http.StatusBadRequest,
	"InvalidDigest":                http.StatusBadRequest,
	"InvalidPart":                  http.StatusBadRequest,
	"InvalidRange":                 http.StatusRequestedRangeNotSatisfiable,
	"InvalidRequest":               http.StatusBadRequest,
	"InvalidURI":                   http.StatusBadRequest,
	"MissingContentLength":         http.StatusLengthRequired,
	"MethodNotAllowed":             http.StatusMethodNotAllowed,
	"NoSuchBucket":                 http.StatusNotFound,
	"NoSuchKey":                    http.StatusNotFound,
	"NoSuchUpload":                 http.StatusNotFound,
	"NoSuchVersion":                http.StatusNotFound,
	"PreconditionFailed":           http.StatusPreconditionFailed,
	"RequestTimeTooSkewed":         http.StatusForbidden,
	"SignatureDoesNotMatch":        http.StatusForbidden,
	"SlowDown":                     http.StatusServiceUnavailable,
	"XAmzContentSHA256Mismatch":    http.StatusBadRequest,
}

var defaultMessageByCode = map[string]string{
	"AccessDenied":                 "access denied",
	"AuthorizationHeaderMalformed": "authorization header malformed",
	"BadDigest":                    "bad digest",
	"BucketNotEmpty":               "bucket not empty",
	"EntityTooLarge":               "entity too large",
	"InternalError":                "internal error",
	"InvalidArgument":              "invalid argument",
	"InvalidBucketName":            "invalid bucket name",
	"InvalidDigest":                "invalid digest",
	"InvalidPart":                  "invalid part",
	"InvalidRange":                 "invalid range",
	"InvalidRequest":               "invalid request",
	"InvalidURI":                   "invalid uri",
	"MissingContentLength":         "missing content length",
	"MethodNotAllowed":             "the specified method is not allowed against this resource",
	"NoSuchBucket":                 "bucket not found",
	"NoSuchKey":                    "key not found",
	"NoSuchUpload":                 "upload not found",
	"NoSuchVersion":                "version not found",
	"PreconditionFailed":           "precondition failed",
	"RequestTimeTooSkewed":         "request time too skewed",
	"SignatureDoesNotMatch":        "signature mismatch",
	"SlowDown":                     "slow down",
	"XAmzContentSHA256Mismatch":    "payload hash mismatch",
}
