package api

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/pvmbg-mock/internal/volcanic"
)

const (
	TokenPrefix      = "pvmbg_v1."
	HeaderAdminToken = "X-PVMBG-Admin-Token"

	OutageModeError   = "error"
	OutageModeTimeout = "timeout"
)

type Config struct {
	Service       string
	Token         string
	AdminToken    string
	DelayMin      time.Duration
	DelayMax      time.Duration
	OutageMode    string
	SchemaVersion int
}

type handler struct {
	store *volcanic.Store
	cfg   Config
	log   *slog.Logger

	schemaVersion atomic.Int64
	outage        atomic.Bool
}

func New(store *volcanic.Store, cfg Config, log *slog.Logger) http.Handler {
	h := &handler{store: store, cfg: cfg, log: log}
	h.schemaVersion.Store(int64(cfg.SchemaVersion))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /volcanic-reports", h.volcanicReports)
	mux.HandleFunc("POST /admin/schema-version", h.setSchemaVersion)
	mux.HandleFunc("POST /admin/outage", h.setOutage)
	return mux
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"service":        h.cfg.Service,
		"outage":         h.outage.Load(),
		"schema_version": h.schemaVersion.Load(),
	})
}

func (h *handler) volcanicReports(w http.ResponseWriter, r *http.Request) {
	if !h.available(w, r) {
		return
	}
	if !h.authorized(w, r) {
		return
	}
	if !h.pause(r) {
		return
	}

	since, ok := parseSince(w, r)
	if !ok {
		return
	}

	schemaVersion := int(h.schemaVersion.Load())
	reports := h.store.ReportsSince(since)

	out := make([]volcanic.ReportResponse, 0, len(reports))
	for _, report := range reports {
		out = append(out, report.Response(schemaVersion))
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (h *handler) available(w http.ResponseWriter, r *http.Request) bool {
	if !h.outage.Load() {
		return true
	}

	h.log.WarnContext(r.Context(), "request dropped by outage",
		"mode", h.cfg.OutageMode, "path", r.URL.Path)

	if h.cfg.OutageMode == OutageModeTimeout {
		<-r.Context().Done()
		return false
	}

	httpx.Error(w, http.StatusServiceUnavailable, "service_unavailable",
		"PVMBG is currently unavailable")
	return false
}

func (h *handler) authorized(w http.ResponseWriter, r *http.Request) bool {
	raw := r.Header.Get("Authorization")
	if raw == "" {
		h.reject(w, r, http.StatusUnauthorized, "missing_token", "header Authorization is required")
		return false
	}

	token, found := strings.CutPrefix(raw, "Bearer ")
	if !found {
		h.reject(w, r, http.StatusUnauthorized, "invalid_auth_scheme", "Authorization must use the Bearer scheme")
		return false
	}
	if !strings.HasPrefix(token, TokenPrefix) {
		h.reject(w, r, http.StatusForbidden, "malformed_token", "token is not a PVMBG credential")
		return false
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(h.cfg.Token)) != 1 {
		h.reject(w, r, http.StatusForbidden, "invalid_token", "credential is not valid for PVMBG")
		return false
	}
	return true
}

func (h *handler) adminAuthorized(w http.ResponseWriter, r *http.Request) bool {
	token := r.Header.Get(HeaderAdminToken)
	if token == "" {
		h.reject(w, r, http.StatusUnauthorized, "missing_admin_token", "header "+HeaderAdminToken+" is required")
		return false
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(h.cfg.AdminToken)) != 1 {
		h.reject(w, r, http.StatusForbidden, "invalid_admin_token", "admin credential is not valid")
		return false
	}
	return true
}

func (h *handler) reject(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	h.log.WarnContext(r.Context(), "request rejected", "reason", code, "path", r.URL.Path)
	httpx.Error(w, status, code, message)
}

func (h *handler) pause(r *http.Request) bool {
	d := h.cfg.DelayMin
	if spread := h.cfg.DelayMax - h.cfg.DelayMin; spread > 0 {
		d += time.Duration(rand.Int64N(int64(spread)))
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-r.Context().Done():
		return false
	}
}

func (h *handler) setSchemaVersion(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuthorized(w, r) {
		return
	}

	var body struct {
		Version *int `json:"version"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Version == nil || (*body.Version != volcanic.SchemaLegacy && *body.Version != volcanic.SchemaConfidence) {
		httpx.Error(w, http.StatusBadRequest, "invalid_version",
			"version must be 1 (without confidence_level) or 2 (with confidence_level)")
		return
	}

	previous := h.schemaVersion.Swap(int64(*body.Version))
	h.log.InfoContext(r.Context(), "schema version changed",
		"previous", previous, "current", *body.Version)

	httpx.JSON(w, http.StatusOK, map[string]any{"schema_version": *body.Version})
}

func (h *handler) setOutage(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuthorized(w, r) {
		return
	}

	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Enabled == nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_enabled", "enabled must be true or false")
		return
	}

	h.outage.Store(*body.Enabled)
	h.log.InfoContext(r.Context(), "outage toggled", "enabled", *body.Enabled, "mode", h.cfg.OutageMode)

	httpx.JSON(w, http.StatusOK, map[string]any{
		"outage": *body.Enabled,
		"mode":   h.cfg.OutageMode,
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(dst); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_body", "request body must be valid JSON")
		return false
	}
	return true
}

func parseSince(w http.ResponseWriter, r *http.Request) (time.Time, bool) {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		return time.Time{}, true
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_since",
			"since must be an RFC 3339 timestamp, for example 2026-09-23T08:00:00Z")
		return time.Time{}, false
	}
	return t.UTC(), true
}
