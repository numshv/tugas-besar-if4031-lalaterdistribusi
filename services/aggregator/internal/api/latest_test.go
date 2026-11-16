package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type countingStore struct {
	store.Store
	calls atomic.Int32
	delay time.Duration
}

func (c *countingStore) ListLatest(ctx context.Context, q store.LatestQuery) (store.LatestPage, error) {
	c.calls.Add(1)
	time.Sleep(c.delay)
	return c.Store.ListLatest(ctx, q)
}

func newLatestHandler(t *testing.T, s store.Store) (*Handler, *latestCache) {
	t.Helper()
	h := &Handler{store: s, log: slog.New(slog.NewTextHandler(io.Discard, nil)), latest: newLatestCache()}
	return h, h.latest
}

func get(h *Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.listLatest(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func seedLatest(t *testing.T, s store.Store, n int) {
	t.Helper()
	for i := range n {
		ref := string(rune('a' + i))
		e := hazard.Event{HazardID: hazard.NewID(hazard.SourceSatelit, ref), Source: hazard.SourceSatelit, SourceRefID: ref, HazardType: hazard.TypeWildfire, Version: 1}
		if _, _, err := s.Upsert(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLatestFirstPageIsCachedBriefly(t *testing.T) {
	mem := store.NewMemory()
	seedLatest(t, mem, 3)
	cs := &countingStore{Store: mem}
	h, cache := newLatestHandler(t, cs)
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }

	for range 3 {
		if rec := get(h, "/internal/hazards/latest?limit=2"); rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
	if n := cs.calls.Load(); n != 1 {
		t.Fatalf("store calls within TTL = %d, want 1", n)
	}
	now = now.Add(1500 * time.Millisecond)
	get(h, "/internal/hazards/latest?limit=2")
	if n := cs.calls.Load(); n != 2 {
		t.Fatalf("store calls after TTL = %d, want 2", n)
	}
	get(h, "/internal/hazards/latest?limit=3")
	if n := cs.calls.Load(); n != 3 {
		t.Fatalf("a different limit must not share the cache entry, calls = %d", n)
	}
}

func TestLatestSingleflightCollapsesConcurrentMisses(t *testing.T) {
	mem := store.NewMemory()
	seedLatest(t, mem, 2)
	cs := &countingStore{Store: mem, delay: 50 * time.Millisecond}
	h, _ := newLatestHandler(t, cs)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if rec := get(h, "/internal/hazards/latest"); rec.Code != http.StatusOK {
				t.Errorf("status %d", rec.Code)
			}
		})
	}
	wg.Wait()
	if n := cs.calls.Load(); n != 1 {
		t.Fatalf("50 concurrent cold requests hit the store %d times, want 1", n)
	}
}

func TestLatestPagesWithCursorAndHidesRetracted(t *testing.T) {
	tick := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	mem := store.NewMemory().WithClock(func() time.Time { tick = tick.Add(time.Millisecond); return tick })
	seedLatest(t, mem, 3)
	if _, _, err := mem.Retract(context.Background(), hazard.SourceSatelit, "b", 2); err != nil {
		t.Fatal(err)
	}
	h, _ := newLatestHandler(t, mem)

	rec := get(h, "/internal/hazards/latest?limit=1")
	var first LatestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil || first.NextCursor == nil || len(first.Items) != 1 {
		t.Fatalf("first page: %s", rec.Body)
	}
	rec = get(h, "/internal/hazards/latest?limit=1&cursor="+*first.NextCursor)
	var second LatestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &second); err != nil || second.NextCursor != nil || len(second.Items) != 1 {
		t.Fatalf("second page must be the last (retracted b hidden): %s", rec.Body)
	}
	if first.Items[0].SourceRefID != "c" || second.Items[0].SourceRefID != "a" {
		t.Fatalf("order = %s,%s; want newest first c,a", first.Items[0].SourceRefID, second.Items[0].SourceRefID)
	}
	if second.UnavailableUnits == nil {
		t.Fatal("unavailable_units must be [] not null")
	}
}

func TestLatestValidation(t *testing.T) {
	h, _ := newLatestHandler(t, store.NewMemory())
	for target, want := range map[string]int{
		"/internal/hazards/latest?type=FLOOD":     http.StatusBadRequest,
		"/internal/hazards/latest?limit=0":        http.StatusBadRequest,
		"/internal/hazards/latest?cursor=garbage": http.StatusBadRequest,
		"/internal/hazards/latest?type=WILDFIRE":  http.StatusOK,
	} {
		if rec := get(h, target); rec.Code != want {
			t.Fatalf("%s: status %d, want %d (%s)", target, rec.Code, want, rec.Body)
		}
	}
}
