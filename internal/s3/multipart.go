package s3

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type initiateMultipartResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type completeMultipartRequest struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []completePartItem `xml:"Part"`
}

type completePartItem struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type listPartsResult struct {
	XMLName  xml.Name          `xml:"ListPartsResult"`
	Bucket   string            `xml:"Bucket"`
	Key      string            `xml:"Key"`
	UploadID string            `xml:"UploadId"`
	Parts    []listPartContent `xml:"Part"`
}

type listPartContent struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

func (h *Handler) handleInitiateMultipart(ctx context.Context, w http.ResponseWriter, bucket, key, requestID string) {
	uploadID := newRequestID() + newRequestID()
	if err := h.Meta.CreateMultipartUpload(ctx, bucket, key, uploadID); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	resp := initiateMultipartResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleUploadPart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	partNumber, ok := parsePartNumber(r.URL.Query().Get("partNumber"))
	if !ok {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid part number", requestID, r.URL.Path)
		return
	}
	if _, err := h.Meta.GetMultipartUpload(ctx, uploadID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	reader := io.Reader(r.Body)
	if hashHeader := r.Header.Get("X-Amz-Content-Sha256"); hashHeader != "" {
		expected, verify, err := parsePayloadHash(hashHeader)
		if err != nil {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid payload hash", requestID, r.URL.Path)
			return
		}
		if verify {
			reader = newPayloadHashReader(reader, expected)
		}
	}
	_, result, err := h.Engine.PutObject(ctx, "", "", reader)
	if err != nil {
		if errors.Is(err, errPayloadHashMismatch) {
			writeErrorWithResource(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload hash mismatch", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if err := h.Meta.PutMultipartPart(ctx, uploadID, partNumber, result.VersionID, result.ETag, result.Size); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	w.Header().Set("ETag", `"`+result.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleListParts(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	upload, err := h.Meta.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	parts, err := h.Meta.ListMultipartParts(ctx, uploadID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	out := make([]listPartContent, 0, len(parts))
	for _, part := range parts {
		out = append(out, listPartContent{
			PartNumber:   part.PartNumber,
			ETag:         `"` + part.ETag + `"`,
			Size:         part.Size,
			LastModified: formatLastModified(part.LastModified),
		})
	}
	resp := listPartsResult{
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadID: uploadID,
		Parts:    out,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleCompleteMultipart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	upload, err := h.Meta.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	var req completeMultipartRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid xml", requestID, r.URL.Path)
		return
	}
	if len(req.Parts) == 0 {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "no parts", requestID, r.URL.Path)
		return
	}
	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	parts, err := h.Meta.ListMultipartParts(ctx, uploadID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	partMap := make(map[int]meta.MultipartPart, len(parts))
	for _, p := range parts {
		partMap[p.PartNumber] = p
	}

	ordered := make([]meta.MultipartPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		part, ok := partMap[p.PartNumber]
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", "missing part", requestID, r.URL.Path)
			return
		}
		if normalizeETag(p.ETag) != part.ETag {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", "etag mismatch", requestID, r.URL.Path)
			return
		}
		ordered = append(ordered, part)
	}
	if err := validatePartSizes(ordered); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", err.Error(), requestID, r.URL.Path)
		return
	}

	pr, pw := io.Pipe()
	go func() {
		for _, part := range ordered {
			reader, _, err := h.Engine.Get(ctx, part.VersionID)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_, err = io.Copy(pw, reader)
			_ = reader.Close()
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
		_ = pw.Close()
	}()

	_, result, err := h.Engine.PutObject(ctx, upload.Bucket, upload.Key, pr)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	if h.Meta != nil {
		if err := h.Meta.CompleteMultipartUpload(ctx, uploadID); err != nil {
			writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
			return
		}
	}

	multiETag := multipartETag(ordered)
	resp := completeMultipartResult{
		Bucket: upload.Bucket,
		Key:    upload.Key,
		ETag:   `"` + multiETag + `"`,
	}
	w.Header().Set("ETag", `"`+multiETag+`"`)
	if result != nil {
		_ = result
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleAbortMultipart(ctx context.Context, w http.ResponseWriter, uploadID string, requestID string) {
	if err := h.Meta.AbortMultipartUpload(ctx, uploadID); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parsePartNumber(raw string) (int, bool) {
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func normalizeETag(etag string) string {
	etag = strings.TrimSpace(etag)
	etag = strings.TrimPrefix(etag, "\"")
	etag = strings.TrimSuffix(etag, "\"")
	return etag
}

func validatePartSizes(parts []meta.MultipartPart) error {
	const minPartSize = 5 << 20
	if len(parts) == 0 {
		return errors.New("no parts")
	}
	for i := 0; i < len(parts)-1; i++ {
		if parts[i].Size < minPartSize {
			return errors.New("part too small")
		}
	}
	return nil
}

func multipartETag(parts []meta.MultipartPart) string {
	h := md5.New()
	for _, part := range parts {
		sum, err := hex.DecodeString(part.ETag)
		if err != nil {
			continue
		}
		h.Write(sum)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}
