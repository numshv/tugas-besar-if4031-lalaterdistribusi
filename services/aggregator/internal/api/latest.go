package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

const (
	DefaultLatestLimit = 50
	latestCacheTTL     = time.Second
)

type LatestResponse struct {
	Items            []hazard.Event `json:"items"`
	NextCursor       *string        `json:"next_cursor"`
	Partial          bool           `json:"partial"`
	UnavailableUnits []string       `json:"unavailable_units"`
}

type cachedPage struct {
	body    []byte
	expires time.Time
}

type latestCache struct {
	mu    sync.Mutex
	pages map[string]cachedPage
	group singleflight.Group
	now   func() time.Time
}

func newLatestCache() *latestCache {
	return &latestCache{pages: map[string]cachedPage{}, now: time.Now}
}

func (c *latestCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pages[key]
	if !ok || c.now().After(p.expires) {
		return nil, false
	}
	return p.body, true
}

func (c *latestCache) put(key string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages[key] = cachedPage{body: body, expires: c.now().Add(latestCacheTTL)}
}

func (h *Handler) listLatest(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query["type"]) > 1 || len(query["limit"]) > 1 || len(query["cursor"]) > 1 {
		httpx.Error(w, http.StatusBadRequest, "invalid_pagination", "type, limit and cursor may only be specified once")
		return
	}
	typ := hazard.Type(query.Get("type"))
	if typ != "" && !typ.Known() {
		httpx.Error(w, http.StatusBadRequest, "invalid_type", "unknown hazard type")
		return
	}
	limit := DefaultLatestLimit
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > store.MaxLatestLimit {
			httpx.Error(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and "+strconv.Itoa(store.MaxLatestLimit))
			return
		}
		limit = n
	}
	cursor := query.Get("cursor")
	q := store.LatestQuery{Type: typ, Limit: limit, Cursor: cursor}

	if cursor != "" {
		body, err := h.renderLatest(r.Context(), q)
		h.writeLatest(w, r, body, err)
		return
	}

	key := string(typ) + "|" + strconv.Itoa(limit)
	if body, ok := h.latest.get(key); ok {
		writeJSONBytes(w, body)
		return
	}
	v, err, _ := h.latest.group.Do(key, func() (any, error) {
		if body, ok := h.latest.get(key); ok {
			return body, nil
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 3*time.Second)
		defer cancel()
		body, err := h.renderLatest(ctx, q)
		if err == nil {
			h.latest.put(key, body)
		}
		return body, err
	})
	body, _ := v.([]byte)
	h.writeLatest(w, r, body, err)
}

func (h *Handler) renderLatest(ctx context.Context, q store.LatestQuery) ([]byte, error) {
	page, err := h.store.ListLatest(ctx, q)
	if err != nil {
		return nil, err
	}
	resp := LatestResponse{Items: page.Items, Partial: page.Partial, UnavailableUnits: page.UnavailableUnits}
	if resp.Items == nil {
		resp.Items = []hazard.Event{}
	}
	if resp.UnavailableUnits == nil {
		resp.UnavailableUnits = []string{}
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *Handler) writeLatest(w http.ResponseWriter, r *http.Request, body []byte, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidCursor):
		httpx.Error(w, http.StatusBadRequest, "invalid_cursor", "cursor is invalid")
	case errors.Is(err, store.ErrInvalidQuery):
		httpx.Error(w, http.StatusBadRequest, "invalid_query", err.Error())
	case err != nil:
		h.log.ErrorContext(r.Context(), "list latest hazards failed", "error", err)
		httpx.Error(w, http.StatusServiceUnavailable, "store_unavailable", "hazard store is unreachable")
	default:
		writeJSONBytes(w, body)
	}
}

func writeJSONBytes(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
