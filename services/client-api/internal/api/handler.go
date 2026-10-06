package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

type Handler struct {
	aggregatorBaseURL string
	httpClient        *http.Client
	log               *slog.Logger
}

func NewHandler(aggregatorBaseURL string, log *slog.Logger) *Handler {
	return &Handler{
		aggregatorBaseURL: aggregatorBaseURL,
		httpClient:        &http.Client{Timeout: 3 * time.Second},
		log:               log,
	}
}

type aggregatorStatus struct {
	PVMBG struct {
		Healthy     bool      `json:"healthy"`
		LastSuccess time.Time `json:"last_success"`
	} `json:"pvmbg"`
}

func (h *Handler) ListHazardsHandler(w http.ResponseWriter, r *http.Request) {
	scope, _ := r.Context().Value(scopeKey).(string)
	if scope == "" {
		httpx.Error(w, http.StatusUnauthorized, "missing_scope", "token has no valid scope")
		return
	}

	query := r.URL.Query()
	hazardType := query.Get("type")
	if len(query["type"]) != 1 || (hazardType != string(hazard.TypeSeismic) && hazardType != string(hazard.TypeVolcanic)) {
		httpx.Error(w, http.StatusBadRequest, "invalid_type", "type must be SEISMIC or VOLCANIC")
		return
	}
	if scope == "media" && requestsRawFields(query["fields"]) {
		httpx.Error(w, http.StatusForbidden, "forbidden_fields", "media scope cannot request raw fields")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	events, err := h.fetchHazards(ctx, hazardType, query.Get("since"))
	if err != nil {
		h.log.ErrorContext(r.Context(), "fetch hazards failed", "error", err)
		httpx.Error(w, http.StatusBadGateway, "aggregator_unreachable", "failed to reach aggregator")
		return
	}

	status, err := h.fetchStatus(ctx)
	staleSince := ""
	if err == nil && !status.PVMBG.Healthy {
		staleSince = status.PVMBG.LastSuccess.UTC().Format(time.RFC3339)
	}

	filtered := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		filtered = append(filtered, filterByScope(ev, scope, staleSince))
	}

	httpx.JSON(w, http.StatusOK, filtered)
}

func (h *Handler) fetchHazards(ctx context.Context, hazardType, since string) ([]hazard.Event, error) {
	query := make(url.Values)
	query.Set("type", hazardType)
	if since != "" {
		query.Set("since", since)
	}
	endpoint := h.aggregatorBaseURL + "/internal/hazards?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var events []hazard.Event
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, err
	}
	return events, nil
}

func requestsRawFields(requested []string) bool {
	rawFields := map[string]struct{}{
		"source_ref_id": {},
		"latitude":      {},
		"longitude":     {},
		"attributes":    {},
	}
	for _, value := range requested {
		for _, field := range strings.Split(value, ",") {
			if _, ok := rawFields[strings.TrimSpace(field)]; ok {
				return true
			}
		}
	}
	return false
}

func (h *Handler) fetchStatus(ctx context.Context) (*aggregatorStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.aggregatorBaseURL+"/internal/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status aggregatorStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

func filterByScope(ev hazard.Event, scope string, staleSince string) map[string]any {
	out := map[string]any{
		"hazard_id":   ev.HazardID,
		"source":      ev.Source,
		"hazard_type": ev.HazardType,
		"severity":    ev.Severity,
		"area_name":   ev.AreaName,
		"occurred_at": ev.OccurredAt,
		"ingested_at": ev.IngestedAt,
	}

	if staleSince != "" && ev.HazardType == hazard.TypeVolcanic {
		out["stale_since"] = staleSince
	}

	if scope == "media" {
		return out
	}

	out["source_ref_id"] = ev.SourceRefID
	out["latitude"] = ev.Latitude
	out["longitude"] = ev.Longitude
	out["attributes"] = ev.Attributes

	return out
}
