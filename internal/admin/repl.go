package admin

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/repl"
)

func (h *Handler) handleReplPull(w http.ResponseWriter, r *http.Request) {
	var req ReplPullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	interval := time.Duration(req.IntervalNanos)
	backoffMax := time.Duration(req.BackoffMaxNanos)
	retryTimeout := time.Duration(req.RetryTimeoutNanos)
	err := repl.RunPull(req.Remote, req.Since, req.Limit, req.FetchData, req.Watch, interval, backoffMax, retryTimeout, req.AccessKey, req.SecretKey, req.Region, h.Meta, h.Engine)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeAdminJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleReplPush(w http.ResponseWriter, r *http.Request) {
	var req ReplPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	interval := time.Duration(req.IntervalNanos)
	backoffMax := time.Duration(req.BackoffMaxNanos)
	err := repl.RunPush(req.Remote, req.Since, req.Limit, req.Watch, interval, backoffMax, req.AccessKey, req.SecretKey, req.Region, h.Meta)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeAdminJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleReplBootstrap(w http.ResponseWriter, r *http.Request) {
	var req ReplBootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	dataDir := h.DataDir
	if dataDir == "" {
		layout := h.Engine.Layout()
		dataDir = filepath.Dir(layout.Root)
	}
	err := repl.RunBootstrap(req.Remote, req.AccessKey, req.SecretKey, req.Region, dataDir, req.Force)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeAdminJSON(w, map[string]string{"status": "ok"})
}
