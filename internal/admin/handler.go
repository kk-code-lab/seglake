package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

const tokenHeader = "X-Seglake-Admin-Token"

type Handler struct {
	DataDir       string
	Addr          string
	Meta          *meta.Store
	Engine        *engine.Engine
	AuthToken     string
	WriteInflight func() int64
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.Meta == nil || h.Engine == nil {
		writeAdminError(w, http.StatusInternalServerError, "storage not initialized")
		return
	}
	if !h.authorize(r) {
		writeAdminError(w, http.StatusForbidden, "admin token required")
		return
	}
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	switch r.URL.Path {
	case "/admin/ops/run":
		h.handleOpsRun(w, r)
	case "/admin/keys":
		h.handleKeys(w, r)
	case "/admin/bucket-policy":
		h.handleBucketPolicy(w, r)
	case "/admin/buckets":
		h.handleBuckets(w, r)
	case "/admin/maintenance":
		h.handleMaintenance(w, r)
	case "/admin/repl/pull":
		h.handleReplPull(w, r)
	case "/admin/repl/push":
		h.handleReplPush(w, r)
	case "/admin/repl/bootstrap":
		h.handleReplBootstrap(w, r)
	default:
		writeAdminError(w, http.StatusNotFound, "unknown admin endpoint")
	}
}

func (h *Handler) authorize(r *http.Request) bool {
	if h.AuthToken == "" {
		return false
	}
	token := strings.TrimSpace(r.Header.Get(tokenHeader))
	return token != "" && token == h.AuthToken
}

func (h *Handler) handleOpsRun(w http.ResponseWriter, r *http.Request) {
	var req OpsRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if !isOpsMode(req.Mode) {
		writeAdminError(w, http.StatusBadRequest, "unknown ops mode")
		return
	}
	if requiresQuiescedOps(req.Mode) {
		state, err := h.Meta.MaintenanceState(context.Background())
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if state.State != "quiesced" {
			writeAdminError(w, http.StatusServiceUnavailable, "maintenance mode not quiesced")
			return
		}
	}
	dataDir := h.DataDir
	if dataDir == "" {
		layout := h.Engine.Layout()
		dataDir = filepath.Dir(layout.Root)
	}
	metaPath := filepath.Join(dataDir, "meta.db")
	if req.RebuildMeta != "" {
		metaPath = req.RebuildMeta
	}
	layout := fs.NewLayout(filepath.Join(dataDir, "objects"))
	gcGuard := ops.GCGuardrails{
		WarnCandidates:     req.GCWarnSegments,
		WarnReclaimedBytes: req.GCWarnReclaim,
		MaxCandidates:      req.GCMaxSegments,
		MaxReclaimedBytes:  req.GCMaxReclaim,
	}
	mpuGuard := ops.MPUGCGuardrails{
		WarnUploads:        req.MPUWarnUploads,
		WarnReclaimedBytes: req.MPUWarnReclaim,
		MaxUploads:         req.MPUMaxUploads,
		MaxReclaimedBytes:  req.MPUMaxReclaim,
	}
	gcMinAge := time.Duration(req.GCMinAgeNanos)
	mpuTTL := time.Duration(req.MPUTTLNanos)
	report, err := runOpsRequest(req.Mode, layout, metaPath, req.SnapshotDir, req.ReplCompareDir, req.FsckAllManifests, req.ScrubAllManifests, gcMinAge, req.GCForce, req.GCLiveThreshold, req.GCRewritePlanFile, req.GCRewriteFromPlan, req.GCRewriteBps, req.GCPauseFile, mpuTTL, req.MPUForce, gcGuard, mpuGuard, req.DBReindexTable)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeAdminJSON(w, report)
}

func (h *Handler) handleKeys(w http.ResponseWriter, r *http.Request) {
	var req KeysRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "list":
		keys, err := h.Meta.ListAPIKeys(context.Background())
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if keys == nil {
			keys = []meta.APIKey{}
		}
		writeAdminJSON(w, keys)
	case "create":
		if req.AccessKey == "" || req.SecretKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key and secret_key required")
			return
		}
		if err := parsePolicy(req.Policy); err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		if err := h.Meta.UpsertAPIKey(context.Background(), req.AccessKey, req.SecretKey, req.Policy, enabled, req.Inflight); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "allow-bucket":
		if req.AccessKey == "" || req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key and bucket required")
			return
		}
		if err := h.Meta.AllowBucketForKey(context.Background(), req.AccessKey, req.Bucket); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "disallow-bucket":
		if req.AccessKey == "" || req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key and bucket required")
			return
		}
		if err := h.Meta.DisallowBucketForKey(context.Background(), req.AccessKey, req.Bucket); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "list-buckets":
		if req.AccessKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key required")
			return
		}
		buckets, err := h.Meta.ListAllowedBuckets(context.Background(), req.AccessKey)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if buckets == nil {
			buckets = []string{}
		}
		writeAdminJSON(w, buckets)
	case "list-buckets-all":
		keyBuckets, err := h.Meta.ListAllKeyBuckets(context.Background())
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if keyBuckets == nil {
			keyBuckets = map[string][]string{}
		}
		writeAdminJSON(w, keyBuckets)
	case "enable":
		if req.AccessKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key required")
			return
		}
		if err := h.Meta.SetAPIKeyEnabled(context.Background(), req.AccessKey, true); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "disable":
		if req.AccessKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key required")
			return
		}
		if err := h.Meta.SetAPIKeyEnabled(context.Background(), req.AccessKey, false); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "delete":
		if req.AccessKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key required")
			return
		}
		if err := h.Meta.DeleteAPIKey(context.Background(), req.AccessKey); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "set-policy":
		if req.AccessKey == "" {
			writeAdminError(w, http.StatusBadRequest, "access_key required")
			return
		}
		if err := parsePolicy(req.Policy); err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.Meta.UpdateAPIKeyPolicy(context.Background(), req.AccessKey, req.Policy); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	default:
		writeAdminError(w, http.StatusBadRequest, "unknown keys action")
	}
}

func (h *Handler) handleBucketPolicy(w http.ResponseWriter, r *http.Request) {
	var req BucketPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "get":
		if req.Bucket == "" {
			policies, err := h.Meta.ListBucketPolicies(context.Background())
			if err != nil {
				writeAdminError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if policies == nil {
				policies = map[string]string{}
			}
			writeAdminJSON(w, policies)
			return
		}
		value, err := h.Meta.GetBucketPolicy(context.Background(), req.Bucket)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeAdminJSON(w, map[string]any{"policy": nil})
				return
			}
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"policy": value})
	case "set":
		if req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "bucket required")
			return
		}
		if req.Policy == "" {
			writeAdminError(w, http.StatusBadRequest, "policy required")
			return
		}
		if err := parsePolicy(req.Policy); err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.Meta.SetBucketPolicy(context.Background(), req.Bucket, req.Policy); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "delete":
		if req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "bucket required")
			return
		}
		if err := h.Meta.DeleteBucketPolicy(context.Background(), req.Bucket); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	default:
		writeAdminError(w, http.StatusBadRequest, "unknown bucket-policy action")
	}
}

func (h *Handler) handleBuckets(w http.ResponseWriter, r *http.Request) {
	var req BucketsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "list":
		buckets, err := h.Meta.ListBuckets(context.Background())
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if buckets == nil {
			buckets = []string{}
		}
		writeAdminJSON(w, buckets)
	case "create":
		if req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "bucket required")
			return
		}
		if err := validateBucketName(req.Bucket); err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(req.Versioning) == "" {
			if err := h.Meta.CreateBucket(context.Background(), req.Bucket); err != nil {
				writeAdminError(w, http.StatusInternalServerError, err.Error())
				return
			}
		} else {
			if err := h.Meta.CreateBucketWithVersioning(context.Background(), req.Bucket, req.Versioning); err != nil {
				writeAdminError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "delete":
		if req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "bucket required")
			return
		}
		exists, err := h.Meta.BucketExists(context.Background(), req.Bucket)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if exists {
			if req.Force {
				if err := deleteBucketObjects(context.Background(), h.Meta, req.Bucket); err != nil {
					writeAdminError(w, http.StatusInternalServerError, err.Error())
					return
				}
			} else {
				hasObjects, err := h.Meta.BucketHasObjects(context.Background(), req.Bucket)
				if err != nil {
					writeAdminError(w, http.StatusInternalServerError, err.Error())
					return
				}
				if hasObjects {
					writeAdminError(w, http.StatusBadRequest, "bucket not empty")
					return
				}
			}
			if err := h.Meta.DeleteBucket(context.Background(), req.Bucket); err != nil {
				writeAdminError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		writeAdminJSON(w, map[string]string{"status": "ok"})
	case "exists":
		if req.Bucket == "" {
			writeAdminError(w, http.StatusBadRequest, "bucket required")
			return
		}
		exists, err := h.Meta.BucketExists(context.Background(), req.Bucket)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeAdminJSON(w, map[string]bool{"exists": exists})
	default:
		writeAdminError(w, http.StatusBadRequest, "unknown bucket action")
	}
}

func (h *Handler) handleMaintenance(w http.ResponseWriter, r *http.Request) {
	var req MaintenanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	var state meta.MaintenanceState
	var err error
	switch action {
	case "status", "get", "show":
		state, err = h.Meta.MaintenanceState(context.Background())
	case "enable", "on", "true":
		state, err = h.Meta.SetMaintenanceState(context.Background(), "entering")
	case "disable", "off", "false":
		state, err = h.Meta.SetMaintenanceState(context.Background(), "exiting")
	default:
		writeAdminError(w, http.StatusBadRequest, "maintenance action must be status|enable|disable")
		return
	}
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !req.NoWait {
		switch action {
		case "enable":
			state, err = waitForMaintenanceQuiesced(h.Meta)
		case "disable":
			state, err = waitForMaintenanceOff(h.Meta)
		}
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeInflight := int64(0)
	if h.WriteInflight != nil {
		writeInflight = h.WriteInflight()
	}
	resp := MaintenanceResponse{
		Maintenance:          state.State,
		MaintenanceUpdated:   state.UpdatedAt,
		Running:              true,
		Addr:                 h.Addr,
		WriteInflight:        writeInflight,
		ServerState:          state.State,
		ServerStateUpdatedAt: state.UpdatedAt,
	}
	writeAdminJSON(w, resp)
}

func writeAdminJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAdminError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
