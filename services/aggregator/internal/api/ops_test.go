package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
)

type fakeReplayer struct{ calls int }

type fakeRetractor struct{ sources []hazard.Source }

func (f *fakeRetractor) RetractSource(_ context.Context, source hazard.Source) (int, error) {
	f.sources = append(f.sources, source)
	return 3, nil
}

func (f *fakeReplayer) Replay(_ context.Context, source string) (pipeline.ReplayResult, error) {
	f.calls++
	return pipeline.ReplayResult{Source: source}, nil
}

func newOpsServer(t *testing.T) (*httptest.Server, *rsa.PrivateKey, *dlq.MemoryStore) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	q := dlq.NewMemoryStore()
	base := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	for i, status := range []dlq.Status{dlq.StatusParked, dlq.StatusRetrying, dlq.StatusReplaying, dlq.StatusRecovered} {
		_, _ = q.Put(context.Background(), dlq.Message{
			ID: "msg-" + string(rune('a'+i)), Source: "PVMBG", Kind: dlq.KindRecord, Payload: []byte(`{"x":1}`),
			ReceivedAt: base.Add(time.Duration(i) * time.Minute), Reason: "schema_incompatible: test",
			Attempts: 1, Status: status,
		})
	}
	ops := &Ops{
		DLQ: q, Replayer: &fakeReplayer{}, Retractor: &fakeRetractor{}, AuditTokens: []string{"audit-secret"},
		Verifier: &jwtauth.Verifier{Keys: jwtauth.NewStaticKeyProvider(&key.PublicKey), Issuer: "bnpb-auth", Audience: "client-api"},
	}
	srv := httptest.NewServer(New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), 8, ops))
	t.Cleanup(srv.Close)
	return srv, key, q
}

func token(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "bnpb-auth", "aud": "client-api", "sub": "tester", "type": "access", "scope": scope,
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func call(t *testing.T, method, url, bearer string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestUnprocessedOnlyForInternalOps(t *testing.T) {
	srv, key, _ := newOpsServer(t)
	url := srv.URL + "/internal/ops/unprocessed"
	cases := map[string]struct {
		bearer string
		want   int
	}{
		"no token":       {"", 401},
		"wrong token":    {"nope", 401},
		"audit token":    {"audit-secret", 200},
		"internal scope": {token(t, key, "internal"), 200},
		"media scope":    {token(t, key, "media"), 403},
		"field scope":    {token(t, key, "field"), 403},
	}
	for name, c := range cases {
		code, body := call(t, http.MethodGet, url, c.bearer)
		if code != c.want {
			t.Errorf("%s: HTTP %d, want %d", name, code, c.want)
		}
		if code >= 400 {
			if e, _ := body["error"].(map[string]any); e["code"] == nil || e["message"] == nil {
				t.Errorf("%s: error body %v does not follow Lampiran A", name, body)
			}
		}
	}
	if code, _ := call(t, http.MethodPost, srv.URL+"/internal/ops/replay?source=PVMBG", token(t, key, "media")); code != 403 {
		t.Errorf("media replay: HTTP %d, want 403", code)
	}
	if code, _ := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed/msg-a", token(t, key, "field")); code != 403 {
		t.Errorf("field inspect: HTTP %d, want 403", code)
	}
}

func TestUnprocessedContractAndPaging(t *testing.T) {
	srv, _, _ := newOpsServer(t)
	code, page1 := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed?limit=2", "audit-secret")
	if code != 200 {
		t.Fatalf("HTTP %d", code)
	}
	items := page1["items"].([]any)
	if len(items) != 2 || page1["next_cursor"] == nil {
		t.Fatalf("page 1 = %v", page1)
	}
	first := items[0].(map[string]any)
	for _, f := range []string{"message_id", "source", "received_at", "reason", "retryable", "attempts", "status"} {
		if _, ok := first[f]; !ok {
			t.Errorf("item lacks %q", f)
		}
	}
	if first["received_at"] != "2026-11-17T08:00:00Z" {
		t.Errorf("received_at = %v", first["received_at"])
	}

	_, page2 := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed?limit=2&cursor="+page1["next_cursor"].(string), "audit-secret")
	items2 := page2["items"].([]any)
	if len(items2) != 1 || page2["next_cursor"] != nil {
		t.Fatalf("page 2 = %v (recovered must be hidden, cursor null on last page)", page2)
	}
	if s := items2[0].(map[string]any)["status"]; s != "parked" {
		t.Errorf("replaying reported as %v, want parked", s)
	}
	if code, _ := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed?limit=5000", "audit-secret"); code != 400 {
		t.Errorf("limit 5000: HTTP %d, want 400", code)
	}
}

func TestInspectShowsPayload(t *testing.T) {
	srv, key, _ := newOpsServer(t)
	code, body := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed/msg-a", token(t, key, "internal"))
	if code != 200 || body["payload"] != `{"x":1}` {
		t.Fatalf("HTTP %d, body %v", code, body)
	}
	if code, _ := call(t, http.MethodGet, srv.URL+"/internal/ops/unprocessed/missing", "audit-secret"); code != 404 {
		t.Errorf("missing message: HTTP %d", code)
	}
}

func TestRetractSourceOnlyForInternalOps(t *testing.T) {
	srv, key, _ := newOpsServer(t)
	cases := []struct {
		name, query, bearer string
		want                int
	}{
		{"media", "?source=SATELIT", token(t, key, "media"), 403},
		{"no token", "?source=SATELIT", "", 401},
		{"unknown source", "?source=NOPE", token(t, key, "internal"), 400},
		{"internal", "?source=SATELIT", token(t, key, "internal"), 200},
	}
	for _, c := range cases {
		code, body := call(t, http.MethodPost, srv.URL+"/internal/ops/retract"+c.query, c.bearer)
		if code != c.want {
			t.Errorf("%s: HTTP %d, want %d (%v)", c.name, code, c.want, body)
		}
		if code == 200 && body["retracted"] != float64(3) {
			t.Errorf("%s: body %v", c.name, body)
		}
	}
}
