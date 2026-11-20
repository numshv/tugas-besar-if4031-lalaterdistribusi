package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

func asScope(scope string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/hazards", nil)
	return r.WithContext(context.WithValue(r.Context(), scopeKey, scope))
}

func TestMediaIsShedWhileFieldStillServed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	h := ShedMedia(1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("hold") != "" {
			entered <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))

	busy := asScope("media")
	busy.URL.RawQuery = "hold=1"
	go h.ServeHTTP(httptest.NewRecorder(), busy)
	<-entered
	defer close(release)

	media := httptest.NewRecorder()
	h.ServeHTTP(media, asScope("media"))
	if media.Code != http.StatusTooManyRequests || media.Header().Get("Retry-After") == "" {
		t.Fatalf("media while full = %d (Retry-After %q), want 429 with Retry-After", media.Code, media.Header().Get("Retry-After"))
	}
	field := httptest.NewRecorder()
	h.ServeHTTP(field, asScope("field"))
	if field.Code != http.StatusOK {
		t.Fatalf("field while media is full = %d, want 200", field.Code)
	}
}

func TestCoastalReadingIsMarkedStale(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	ev := hazard.Event{HazardType: hazard.TypeCoastal, Attributes: map[string]any{"received_at": now.Add(-time.Minute).Format(time.RFC3339Nano)}}

	out := map[string]any{}
	markStaleCoastal(out, ev, now)
	if out["stale"] != true || out["as_of"] == nil {
		t.Fatalf("out = %v, want stale with as_of", out)
	}

	fresh := map[string]any{}
	ev.Attributes["received_at"] = now.Add(-5 * time.Second).Format(time.RFC3339Nano)
	markStaleCoastal(fresh, ev, now)
	if _, ok := fresh["stale"]; ok {
		t.Fatalf("fresh reading marked stale: %v", fresh)
	}
}
