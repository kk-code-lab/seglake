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
