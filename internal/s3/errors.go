package s3

import (
	"encoding/xml"
	"net/http"
)

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestID string   `xml:"RequestId"`
	HostID    string   `xml:"HostId"`
}

func writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	resp := errorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
		HostID:    "seglake",
	}
	_ = xml.NewEncoder(w).Encode(resp)
}
