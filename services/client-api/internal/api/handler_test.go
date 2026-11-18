package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const testHazardEvent = `[{"hazard_id":"HZ-PVMBG-1","source":"PVMBG","source_ref_id":"1","hazard_type":"VOLCANIC","severity":"WASPADA","area_name":"Test","latitude":1.25,"longitude":2.5,"occurred_at":"2026-01-01T00:00:00Z","ingested_at":"2026-01-01T00:00:01Z","attributes":{"raw":"value"}}]`

func newTestHandler(t *testing.T, aggregator http.HandlerFunc) *Handler {
	t.Helper()
	server := httptest.NewServer(aggregator)
	t.Cleanup(server.Close)
	return NewHandler(server.URL, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func requestHazards(handler *Handler, scope, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	request = request.WithContext(context.WithValue(request.Context(), scopeKey, scope))
	response := httptest.NewRecorder()
	handler.ListHazardsHandler(response, request)
	return response
}

func TestListHazardsValidatesAndForwardsType(t *testing.T) {
	var forwardedTypes []string
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/hazards" {
			forwardedTypes = append(forwardedTypes, r.URL.Query().Get("type"))
			_, _ = io.WriteString(w, `[]`)
			return
		}
		_, _ = io.WriteString(w, `{"pvmbg":{"healthy":true}}`)
	})

	for _, hazardType := range []string{"SEISMIC", "VOLCANIC"} {
		t.Run(hazardType, func(t *testing.T) {
			target := "/hazards?" + url.Values{"type": []string{hazardType}}.Encode()
			response := requestHazards(handler, "field", target)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
			}
		})
	}

	if len(forwardedTypes) != 2 || forwardedTypes[0] != "SEISMIC" || forwardedTypes[1] != "VOLCANIC" {
		t.Fatalf("forwarded types = %v, want [SEISMIC VOLCANIC]", forwardedTypes)
	}
}

func TestListHazardsRejectsInvalidTypeBeforeAggregatorCall(t *testing.T) {
	var calls int
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = io.WriteString(w, `[]`)
	})

	tests := []struct {
		name   string
		target string
	}{
		{name: "empty", target: "/hazards?type="},
		{name: "unsupported", target: "/hazards?type=TSUNAMI"},
		{name: "wrong case", target: "/hazards?type=seismic"},
		{name: "duplicate", target: "/hazards?type=SEISMIC&type=VOLCANIC"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := requestHazards(handler, "field", test.target)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("aggregator calls = %d, want 0", calls)
	}
}

func TestListHazardsWithoutTypeReturnsLatestAcrossTypes(t *testing.T) {
	var path, limit, cursor string
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/status" {
			_, _ = io.WriteString(w, `{"pvmbg":{"healthy":true}}`)
			return
		}
		path, limit, cursor = r.URL.Path, r.URL.Query().Get("limit"), r.URL.Query().Get("cursor")
		_, _ = io.WriteString(w, `{"items":`+testHazardEvent+`,"next_cursor":"opaque-next","partial":true,"unavailable_units":["shard-1-b","shard-2-c"]}`)
	})

	response := requestHazards(handler, "media", "/hazards?limit=50&cursor=opaque-prev")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body)
	}
	if path != "/internal/hazards/latest" || limit != "50" || cursor != "opaque-prev" {
		t.Fatalf("forwarded %s limit=%s cursor=%s", path, limit, cursor)
	}
	if got := response.Header().Get("X-Next-Cursor"); got != "opaque-next" {
		t.Fatalf("X-Next-Cursor = %q", got)
	}
	if response.Header().Get(PartialHeader) != "true" || response.Header().Get(UnavailableUnitsHeader) != "shard-1-b,shard-2-c" {
		t.Fatalf("partial headers = %v", response.Header())
	}
	body := response.Body.String()
	if !strings.HasPrefix(body, "[") || strings.Contains(body, "source_ref_id") {
		t.Fatalf("body must stay an array filtered by scope: %s", body)
	}
}

func TestListHazardsLatestForwardsBadCursorAs400(t *testing.T) {
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/hazards/latest" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_cursor","message":"cursor is invalid"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"pvmbg":{"healthy":true}}`)
	})
	response := requestHazards(handler, "field", "/hazards?cursor=garbage")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "invalid_cursor") {
		t.Fatalf("status = %d body = %s", response.Code, response.Body)
	}
}

func TestListHazardsRejectsRawFieldsForMedia(t *testing.T) {
	var calls int
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = io.WriteString(w, `[]`)
	})

	for _, fields := range []string{"latitude,longitude", "source_ref_id", "attributes"} {
		t.Run(fields, func(t *testing.T) {
			query := url.Values{"type": []string{"SEISMIC"}, "fields": []string{fields}}
			response := requestHazards(handler, "media", "/hazards?"+query.Encode())
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("aggregator calls = %d, want 0", calls)
	}
}

func TestListHazardsAllowsRawFieldsForFieldAndInternal(t *testing.T) {
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/hazards" {
			_, _ = io.WriteString(w, testHazardEvent)
			return
		}
		_, _ = io.WriteString(w, `{"pvmbg":{"healthy":true}}`)
	})
	query := url.Values{"type": []string{"VOLCANIC"}, "fields": []string{"latitude,longitude"}}

	for _, scope := range []string{"field", "internal"} {
		t.Run(scope, func(t *testing.T) {
			response := requestHazards(handler, scope, "/hazards?"+query.Encode())
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
			}
			body := response.Body.String()
			for _, field := range []string{`"latitude":1.25`, `"longitude":2.5`, `"source_ref_id":"1"`, `"attributes":{"raw":"value"}`} {
				if !contains(body, field) {
					t.Errorf("response does not contain %s: %s", field, body)
				}
			}
		})
	}
}

func TestListVolcanicHazardsIncludesStaleSinceWhenPVMBGUnhealthy(t *testing.T) {
	const lastSuccess = "2026-01-02T03:04:05Z"
	handler := newTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/hazards" {
			_, _ = io.WriteString(w, testHazardEvent)
			return
		}
		_, _ = io.WriteString(w, `{"pvmbg":{"healthy":false,"last_success":"`+lastSuccess+`"}}`)
	})

	response := requestHazards(handler, "field", "/hazards?type=VOLCANIC")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
	}
	if !contains(response.Body.String(), `"stale_since":"`+lastSuccess+`"`) {
		t.Fatalf("response does not contain stale_since %s: %s", lastSuccess, response.Body)
	}
}

func contains(value, substring string) bool {
	return strings.Contains(value, substring)
}
