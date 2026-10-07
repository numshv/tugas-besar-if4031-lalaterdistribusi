package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/poller"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type Handler struct {
	store  store.Store
	poller *poller.Poller
	log    *slog.Logger
}

func New(s store.Store, p *poller.Poller, log *slog.Logger, maxConcurrency int) http.Handler {
	h := &Handler{store: s, poller: p, log: log}

	limited := http.NewServeMux()
	limited.HandleFunc("GET /internal/hazards", h.listHazards)
	limited.HandleFunc("GET /internal/status", h.status)

	root := http.NewServeMux()
	root.HandleFunc("GET /health", httpx.Health("aggregator"))
	root.Handle("/", limitConcurrency(maxConcurrency)(limited))

	return root
}

func (h *Handler) listHazards(w http.ResponseWriter, r *http.Request) {
	typ := hazard.Type(r.URL.Query().Get("type"))
	since := parseSinceOrZero(r)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	events, err := h.store.ListByType(ctx, typ, since)
	if err != nil {
		h.log.ErrorContext(r.Context(), "list hazards failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "store_error", "failed to read hazard events")
		return
	}
	httpx.JSON(w, http.StatusOK, events)
}

// status dipakai Client API untuk memutuskan apakah harus menandai respons
// vulkanik sebagai stale (P2.3): kalau pvmbg.healthy == false, Client API
// menghitung stale_since dari last_success di sini.
func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"bmkg":  h.poller.BMKGStatus(),
		"pvmbg": h.poller.PVMBGStatus(),
	})
}

func parseSinceOrZero(r *http.Request) time.Time {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func limitConcurrency(max int) func(http.Handler) http.Handler {
	if max <= 0 {
		max = 64
	}
	sem := make(chan struct{}, max)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				next.ServeHTTP(w, r)
			default:
				httpx.Error(w, http.StatusTooManyRequests, "too_many_requests",
					"aggregator internal API is at capacity")
			}
		})
	}
}
