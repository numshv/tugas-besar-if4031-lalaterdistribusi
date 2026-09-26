package api

import (
	"crypto/subtle"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/bmkg-mock/internal/seismic"
)

const HeaderAPIKey = "X-BMKG-Key"

type Config struct {
	Service  string
	APIKey   string
	DelayMin time.Duration
	DelayMax time.Duration
}

type handler struct {
	store *seismic.Store
	cfg   Config
	log   *slog.Logger
}

func New(store *seismic.Store, cfg Config, log *slog.Logger) http.Handler {
	h := &handler{store: store, cfg: cfg, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(cfg.Service))
	mux.Handle("GET /seismic-events", h.guard(h.seismicEvents))
	mux.Handle("GET /tsunami-warnings", h.guard(h.tsunamiWarnings))
	return mux
}

func (h *handler) guard(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.authorized(w, r) {
			return
		}
		if !h.pause(r) {
			return
		}
		next(w, r)
	})
}

func (h *handler) authorized(w http.ResponseWriter, r *http.Request) bool {
	key := r.Header.Get(HeaderAPIKey)
	if key == "" {
		h.log.WarnContext(r.Context(), "request rejected", "reason", "missing api key", "path", r.URL.Path)
		httpx.Error(w, http.StatusUnauthorized, "missing_api_key",
			"header "+HeaderAPIKey+" is required")
		return false
	}
	if subtle.ConstantTimeCompare([]byte(key), []byte(h.cfg.APIKey)) != 1 {
		h.log.WarnContext(r.Context(), "request rejected", "reason", "invalid api key", "path", r.URL.Path)
		httpx.Error(w, http.StatusForbidden, "invalid_api_key",
			"credential is not valid for BMKG")
		return false
	}
	return true
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

func (h *handler) seismicEvents(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	httpx.JSON(w, http.StatusOK, h.store.EventsSince(since))
}

func (h *handler) tsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	httpx.JSON(w, http.StatusOK, h.store.WarningsSince(since))
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
