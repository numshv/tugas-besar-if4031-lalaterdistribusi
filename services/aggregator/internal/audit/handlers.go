package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/storemon"
)

type Members interface {
	ShardMembers(replicaSet string) []storemon.UnitStatus
}

type Config struct {
	Tokens           []string
	IncludeRetracted bool
	PartialMode      store.PartialMode
	MaxConcurrent    int
}

type API struct {
	store   store.Store
	locator store.Locator
	members Members
	cfg     Config
	log     *slog.Logger
	now     func() time.Time
	mux     *http.ServeMux
	wrap    func(http.Handler) http.Handler
}

func New(s store.Store, locator store.Locator, members Members, cfg Config, log *slog.Logger) *API {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 4
	}
	auth, limit := RequireToken(cfg.Tokens), LimitConcurrency(cfg.MaxConcurrent)
	a := &API{
		store: s, locator: locator, members: members, cfg: cfg, log: log, now: time.Now,
		mux:  http.NewServeMux(),
		wrap: func(h http.Handler) http.Handler { return auth(limit(h)) },
	}
	a.Register("GET /internal/audit/events", a.events)
	a.Register("GET /internal/audit/checksum", a.checksum)
	a.Register("GET /internal/audit/location", a.location)
	return a
}

func (a *API) Register(pattern string, h http.HandlerFunc) {
	a.mux.Handle(pattern, a.wrap(h))
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

func (a *API) storeFailed(r *http.Request, w http.ResponseWriter, op string, err error) {
	a.log.ErrorContext(r.Context(), "audit store read failed", "op", op, "error", err)
	WriteError(w, http.StatusServiceUnavailable, "store_unavailable", "penyimpanan sedang tidak dapat dijangkau")
}

type eventsResponse struct {
	Items            []hazard.Event `json:"items"`
	NextCursor       *string        `json:"next_cursor"`
	Partial          bool           `json:"partial"`
	UnavailableUnits []string       `json:"unavailable_units"`
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	p, ok := ParseParams(w, r)
	if !ok {
		return
	}
	page, err := a.store.ListLatest(r.Context(), store.LatestQuery{
		Source: p.Source, Since: p.Since, Ascending: true, Limit: p.Limit, Cursor: p.Cursor,
		IncludeRetracted: a.cfg.IncludeRetracted,
	})
	switch {
	case errors.Is(err, store.ErrInvalidCursor):
		WriteError(w, http.StatusBadRequest, "invalid_cursor", "cursor tidak valid untuk parameter ini")
		return
	case errors.Is(err, store.ErrInvalidQuery):
		WriteError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	case err != nil:
		a.storeFailed(r, w, "events", err)
		return
	}
	items := page.Items
	if items == nil {
		items = []hazard.Event{}
	}
	httpx.JSON(w, http.StatusOK, eventsResponse{
		Items: items, NextCursor: nullable(page.NextCursor),
		Partial: page.Partial, UnavailableUnits: units(page.UnavailableUnits),
	})
}

type checksumResponse struct {
	Source           *string  `json:"source"`
	Count            int      `json:"count"`
	Checksum         string   `json:"checksum"`
	Algorithm        string   `json:"algorithm"`
	AsOf             string   `json:"as_of"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

func Checksum(ids []string) string {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}

func (a *API) checksum(w http.ResponseWriter, r *http.Request) {
	if !single(w, r, "source") {
		return
	}
	source, ok := ParseSource(w, r.URL.Query().Get("source"))
	if !ok {
		return
	}
	asOf := a.now().UTC().Truncate(time.Second)
	refs, err := a.store.SourceRefIDs(r.Context(), source, a.cfg.IncludeRetracted)
	if err != nil {
		a.storeFailed(r, w, "checksum", err)
		return
	}
	httpx.JSON(w, http.StatusOK, checksumResponse{
		Source: nullable(string(source)), Count: len(refs.IDs), Checksum: Checksum(refs.IDs),
		Algorithm: "sha256", AsOf: asOf.Format(time.RFC3339),
		Partial: refs.Partial, UnavailableUnits: units(refs.UnavailableUnits),
	})
}

type location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type locationResponse struct {
	HazardID  string     `json:"hazard_id"`
	Locations []location `json:"locations"`
	Partial   bool       `json:"partial"`
}

func (a *API) location(w http.ResponseWriter, r *http.Request) {
	if !single(w, r, "hazard_id") {
		return
	}
	id := r.URL.Query().Get("hazard_id")
	if id == "" {
		WriteError(w, http.StatusBadRequest, "missing_hazard_id", "parameter hazard_id wajib diisi")
		return
	}
	resp := locationResponse{HazardID: id, Locations: []location{}}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if _, err := a.store.Get(ctx, id); errors.Is(err, store.ErrNotFound) {
		WriteError(w, http.StatusNotFound, "not_found", "hazard_id tidak ditemukan")
		return
	} else if err != nil {
		a.log.WarnContext(r.Context(), "location: hazard unreachable", "hazard_id", id, "error", err)
		resp.Partial = true
		httpx.JSON(w, http.StatusOK, resp)
		return
	}

	shard, err := a.locator.ShardOf(ctx, id)
	if err != nil {
		a.storeFailed(r, w, "location", err)
		return
	}
	var down, up int
	if a.members != nil {
		for _, m := range a.members.ShardMembers(shard) {
			if !m.Up {
				down++
				continue
			}
			up++
			role := "replica"
			if m.Primary {
				role = "primary"
			}
			resp.Locations = append(resp.Locations, location{Node: m.Node, Unit: m.Name, Role: role})
		}
	}
	sort.Slice(resp.Locations, func(i, j int) bool {
		if resp.Locations[i].Role != resp.Locations[j].Role {
			return resp.Locations[i].Role == "primary"
		}
		return resp.Locations[i].Unit < resp.Locations[j].Unit
	})
	if a.cfg.PartialMode == store.PartialData {
		resp.Partial = up == 0
	} else {
		resp.Partial = down > 0 || up == 0
	}
	httpx.JSON(w, http.StatusOK, resp)
}
