package analytics

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

//go:embed static
var staticFiles embed.FS

const cacheTTL = 5 * time.Second

type cached struct {
	body    []byte
	expires time.Time
}

type Handler struct {
	source           Source
	includeRetracted bool
	log              *slog.Logger
	now              func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	group singleflight.Group
}

func NewHandler(source Source, includeRetracted bool, log *slog.Logger) *Handler {
	return &Handler{source: source, includeRetracted: includeRetracted, log: log, now: time.Now, cache: map[string]cached{}}
}

func (h *Handler) Routes(requireAuth func(http.Handler) http.Handler) http.Handler {
	static, _ := fs.Sub(staticFiles, "static")
	mux := http.NewServeMux()
	mux.Handle("GET /analytics/static/", http.StripPrefix("/analytics/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /analytics", func(w http.ResponseWriter, r *http.Request) {
		page, _ := fs.ReadFile(static, "index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(page)
	})
	mux.Handle("GET /analytics/data", requireAuth(http.HandlerFunc(h.data)))
	return mux
}

func (h *Handler) data(w http.ResponseWriter, r *http.Request) {
	q, err := ParseQuery(r.URL.Query(), h.now())
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	if body, ok := h.lookup(q.CacheKey); ok {
		writeBody(w, body)
		return
	}
	v, err, _ := h.group.Do(q.CacheKey, func() (any, error) {
		if body, ok := h.lookup(q.CacheKey); ok {
			return body, nil
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 12*time.Second)
		defer cancel()
		res, err := h.source.Analytics(ctx, q, h.includeRetracted)
		if err != nil {
			return nil, err
		}
		res.AsOf = h.now().UTC().Truncate(time.Second)
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(res); err != nil {
			return nil, err
		}
		h.mu.Lock()
		h.cache[q.CacheKey] = cached{body: buf.Bytes(), expires: h.now().Add(cacheTTL)}
		h.mu.Unlock()
		return buf.Bytes(), nil
	})
	if err != nil {
		h.log.ErrorContext(r.Context(), "analytics query failed", "error", err)
		httpx.Error(w, http.StatusServiceUnavailable, "store_unavailable", "penyimpanan sedang tidak dapat dijangkau")
		return
	}
	writeBody(w, v.([]byte))
}

func (h *Handler) lookup(key string) ([]byte, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.cache[key]
	if !ok || h.now().After(c.expires) {
		return nil, false
	}
	return c.body, true
}

func writeBody(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
