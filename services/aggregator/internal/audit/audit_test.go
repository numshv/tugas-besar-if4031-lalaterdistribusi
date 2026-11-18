package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/storemon"
)

const token = "test-token"

type fakeLocator struct{ shard string }

func (f fakeLocator) ShardOf(context.Context, string) (string, error) { return f.shard, nil }

type fakeMembers struct{ members []storemon.UnitStatus }

func (f fakeMembers) ShardMembers(rs string) []storemon.UnitStatus {
	var out []storemon.UnitStatus
	for _, m := range f.members {
		if m.ReplicaSet == rs {
			out = append(out, m)
		}
	}
	return out
}

func member(unit, rs, node string, up, primary bool) storemon.UnitStatus {
	return storemon.UnitStatus{Unit: storemon.Unit{Name: unit, ReplicaSet: rs, Data: true}, Node: node, Up: up, Primary: primary}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Second)
	return c.t
}

func seed(t *testing.T, s store.Store, source hazard.Source, refs ...string) {
	t.Helper()
	for _, ref := range refs {
		e := hazard.Event{HazardID: hazard.NewID(source, ref), Source: source, SourceRefID: ref, HazardType: hazard.TypeSeismic, Version: 1}
		if _, _, err := s.Upsert(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
}

func newAPI(t *testing.T, cfg Config, members Members) (*API, *store.Memory) {
	t.Helper()
	c := &clock{t: time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)}
	s := store.NewMemory().WithClock(c.now)
	if cfg.Tokens == nil {
		cfg.Tokens = []string{token}
	}
	a := New(s, fakeLocator{shard: "shard-2"}, members, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.now = func() time.Time { return time.Date(2026, 11, 12, 3, 0, 0, 500, time.UTC) }
	return a, s
}

func call(t *testing.T, h http.Handler, target, bearer string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: response is not JSON: %s", target, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	return rec.Code, body
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func TestAuth(t *testing.T) {
	a, _ := newAPI(t, Config{Tokens: []string{"old", token}}, nil)
	for _, bearer := range []string{"", "wrong", token + "x"} {
		code, body := call(t, a, "/internal/audit/checksum", bearer)
		if code != http.StatusUnauthorized || errorCode(body) != "unauthorized" {
			t.Fatalf("bearer %q: %d %v", bearer, code, body)
		}
		if msg := body["error"].(map[string]any)["message"]; msg != "token tidak valid" {
			t.Fatalf("message = %v", msg)
		}
	}
	if code, _ := call(t, a, "/internal/audit/checksum", "old"); code != http.StatusOK {
		t.Fatalf("second configured token must work, got %d", code)
	}

	disabled, _ := newAPI(t, Config{Tokens: []string{}}, nil)
	if code, body := call(t, disabled, "/internal/audit/events", token); code != http.StatusServiceUnavailable || errorCode(body) != "audit_not_configured" {
		t.Fatalf("no tokens configured: %d %v", code, body)
	}
}

func TestParameterValidation(t *testing.T) {
	a, _ := newAPI(t, Config{}, nil)
	cases := map[string]string{
		"/internal/audit/events?source=NASA":               "invalid_source",
		"/internal/audit/events?limit=0":                   "invalid_limit",
		"/internal/audit/events?limit=1001":                "invalid_limit",
		"/internal/audit/events?limit=abc":                 "invalid_limit",
		"/internal/audit/events?since=yesterday":           "invalid_since",
		"/internal/audit/events?cursor=garbage":            "invalid_cursor",
		"/internal/audit/events?limit=1&limit=2":           "duplicate_parameter",
		"/internal/audit/checksum?source=bmkg":             "invalid_source",
		"/internal/audit/location":                         "missing_hazard_id",
		"/internal/audit/location?hazard_id=a&hazard_id=b": "duplicate_parameter",
	}
	for target, want := range cases {
		code, body := call(t, a, target, token)
		if code != http.StatusBadRequest || errorCode(body) != want {
			t.Fatalf("%s: %d %v, want 400 %s", target, code, body, want)
		}
	}
}

func TestEventsPagesInIngestOrder(t *testing.T) {
	a, s := newAPI(t, Config{}, nil)
	seed(t, s, hazard.SourceBMKG, "e1", "e2", "e3", "e4", "e5")
	seed(t, s, hazard.SourcePVMBG, "v1")

	var refs []string
	target := "/internal/audit/events?source=BMKG&limit=2"
	for range 10 {
		code, body := call(t, a, target, token)
		if code != http.StatusOK || body["partial"] != false {
			t.Fatalf("%d %v", code, body)
		}
		if u, ok := body["unavailable_units"].([]any); !ok || len(u) != 0 {
			t.Fatalf("unavailable_units must be an empty array, got %#v", body["unavailable_units"])
		}
		for _, it := range body["items"].([]any) {
			item := it.(map[string]any)
			for _, field := range []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "ingested_at", "attributes"} {
				if _, ok := item[field]; !ok {
					t.Fatalf("item misses field %s: %v", field, item)
				}
			}
			if _, leaked := item["version"]; leaked {
				t.Fatal("version must not be exposed")
			}
			refs = append(refs, item["source_ref_id"].(string))
		}
		next, isNull := body["next_cursor"], body["next_cursor"] == nil
		if isNull {
			break
		}
		target = "/internal/audit/events?source=BMKG&limit=2&cursor=" + next.(string)
	}
	if strings.Join(refs, ",") != "e1,e2,e3,e4,e5" {
		t.Fatalf("refs = %v", refs)
	}

	code, body := call(t, a, "/internal/audit/events?since=2026-11-12T02:00:04Z", token)
	if code != http.StatusOK || len(body["items"].([]any)) != 3 {
		t.Fatalf("since must filter ingested_at >=: %d %v", code, body)
	}
}

func TestChecksumVectors(t *testing.T) {
	cases := []struct {
		ids  []string
		want string
	}{
		{nil, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{[]string{"b", "a"}, "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78"},
		{[]string{"é", "a", "B"}, "9af5c29a0ca9f2ee95639b500d576ce9e951fad2a5e341679fc50becdd37605d"},
		{[]string{"fire-1"}, "4ef66436ed51851e3fb6efb518b153d7f1bd28b945ccce2ab3d2513c3c385aef"},
	}
	for _, c := range cases {
		if got := Checksum(c.ids); got != c.want {
			t.Fatalf("Checksum(%q) = %s, want %s", c.ids, got, c.want)
		}
	}
}

func TestChecksumEndpoint(t *testing.T) {
	a, s := newAPI(t, Config{}, nil)
	seed(t, s, hazard.SourceBMKG, "b", "a")
	seed(t, s, hazard.SourcePVMBG, "zzz")
	if _, _, err := s.Retract(context.Background(), hazard.SourceBMKG, "gone", 2); err != nil {
		t.Fatal(err)
	}

	code, body := call(t, a, "/internal/audit/checksum?source=BMKG", token)
	if code != http.StatusOK {
		t.Fatalf("%d %v", code, body)
	}
	want := map[string]any{
		"source": "BMKG", "count": float64(2), "algorithm": "sha256", "as_of": "2026-11-12T03:00:00Z", "partial": false,
		"checksum": "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78",
	}
	for k, v := range want {
		if body[k] != v {
			t.Fatalf("%s = %v, want %v (body %v)", k, body[k], v, body)
		}
	}

	_, all := call(t, a, "/internal/audit/checksum", token)
	if all["source"] != nil || all["count"] != float64(3) {
		t.Fatalf("no source must cover every source with source=null: %v", all)
	}

	withRetracted, _ := newAPI(t, Config{IncludeRetracted: true}, nil)
	seed(t, withRetracted.store, hazard.SourceBMKG, "a")
	if _, _, err := withRetracted.store.Retract(context.Background(), hazard.SourceBMKG, "b", 9); err != nil {
		t.Fatal(err)
	}
	_, inc := call(t, withRetracted, "/internal/audit/checksum?source=BMKG", token)
	if inc["count"] != float64(2) {
		t.Fatalf("AUDIT_INCLUDE_RETRACTED=true must count retracted events: %v", inc)
	}
}

func TestLocation(t *testing.T) {
	members := fakeMembers{members: []storemon.UnitStatus{
		member("shard-2-c", "shard-2", "node-c", true, true),
		member("shard-2-a", "shard-2", "node-a", true, false),
		member("shard-2-d", "shard-2", "node-d", true, false),
		member("shard-0-a", "shard-0", "node-a", true, true),
	}}
	a, s := newAPI(t, Config{}, members)
	seed(t, s, hazard.SourceBMKG, "loc")
	id := hazard.NewID(hazard.SourceBMKG, "loc")

	code, body := call(t, a, "/internal/audit/location?hazard_id="+id, token)
	if code != http.StatusOK || body["hazard_id"] != id || body["partial"] != false {
		t.Fatalf("%d %v", code, body)
	}
	locs := body["locations"].([]any)
	got := []string{}
	for _, l := range locs {
		m := l.(map[string]any)
		got = append(got, fmt.Sprintf("%s/%s/%s", m["node"], m["unit"], m["role"]))
	}
	if strings.Join(got, ",") != "node-c/shard-2-c/primary,node-a/shard-2-a/replica,node-d/shard-2-d/replica" {
		t.Fatalf("locations = %v", got)
	}

	if code, body := call(t, a, "/internal/audit/location?hazard_id=missing", token); code != http.StatusNotFound || errorCode(body) != "not_found" {
		t.Fatalf("missing: %d %v", code, body)
	}

	members.members[1].Up = false
	degraded, s2 := newAPI(t, Config{}, members)
	seed(t, s2, hazard.SourceBMKG, "loc")
	_, body = call(t, degraded, "/internal/audit/location?hazard_id="+id, token)
	if body["partial"] != true || len(body["locations"].([]any)) != 2 {
		t.Fatalf("a dead copy must be dropped and flagged: %v", body)
	}
}

type unreachableStore struct{ store.Store }

func (unreachableStore) Get(context.Context, string) (hazard.Event, error) {
	return hazard.Event{}, errors.New("shard unreachable")
}

func (unreachableStore) ListLatest(context.Context, store.LatestQuery) (store.LatestPage, error) {
	return store.LatestPage{}, errors.New("mongos timeout")
}

func TestStorageFailuresNeverReturn500(t *testing.T) {
	a := New(unreachableStore{Store: store.NewMemory()}, fakeLocator{}, nil, Config{Tokens: []string{token}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	code, body := call(t, a, "/internal/audit/location?hazard_id=x", token)
	if code != http.StatusOK || body["partial"] != true || len(body["locations"].([]any)) != 0 {
		t.Fatalf("unreachable shard: %d %v", code, body)
	}
	code, body = call(t, a, "/internal/audit/events", token)
	if code != http.StatusServiceUnavailable || errorCode(body) != "store_unavailable" {
		t.Fatalf("events with store down: %d %v", code, body)
	}
}

type slowStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}
}

func (s slowStore) SourceRefIDs(ctx context.Context, src hazard.Source, inc bool) (store.RefIDs, error) {
	close(s.entered)
	<-s.release
	return s.Store.SourceRefIDs(ctx, src, inc)
}

func TestConcurrencyLimitReturns429(t *testing.T) {
	slow := slowStore{Store: store.NewMemory(), entered: make(chan struct{}), release: make(chan struct{})}
	a := New(slow, fakeLocator{}, nil, Config{Tokens: []string{token}, MaxConcurrent: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan int)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/internal/audit/checksum", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		a.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	select {
	case <-slow.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request never reached the store")
	}
	code, body := call(t, a, "/internal/audit/location?hazard_id=x", token)
	close(slow.release)
	if first := <-done; first != http.StatusOK {
		t.Fatalf("first request status = %d", first)
	}
	if code != http.StatusTooManyRequests || errorCode(body) != "too_many_requests" {
		t.Fatalf("request over the limit: %d %v", code, body)
	}
}
