package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/fieldreport"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
)

type fakeProducer struct {
	reports []fieldreport.Report
	fail    bool
}

func (p *fakeProducer) Produce(_ context.Context, r fieldreport.Report) error {
	if p.fail {
		return errors.New("kafka unavailable")
	}
	p.reports = append(p.reports, r)
	return nil
}

type fakeStatus map[string]map[string]any

func (f fakeStatus) Lookup(_ context.Context, id string) (map[string]any, bool, error) {
	d, ok := f[id]
	return d, ok, nil
}

const body = `{"hazard_type":"VOLCANIC","latitude":-7.54,"longitude":110.44,
	"observed_at":"2026-11-17T08:00:00Z","description":"abu vulkanik turun di desa"}`

func setup(t *testing.T) (*httptest.Server, *fakeProducer, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProducer{}
	h := &Handler{
		Producer: p,
		Status:   fakeStatus{"done-0001": {"report_id": "done-0001", "deliveries": float64(2)}},
		Verifier: &jwtauth.Verifier{Keys: jwtauth.NewStaticKeyProvider(&key.PublicKey), Issuer: "bnpb-auth", Audience: "client-api"},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)), ProduceTimeout: time.Second, Now: time.Now,
	}
	srv := httptest.NewServer(h.Routes())
	t.Cleanup(srv.Close)
	return srv, p, key
}

func token(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "bnpb-auth", "aud": "client-api", "sub": "tim-" + scope, "type": "access", "scope": scope,
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func post(t *testing.T, url, bearer, idemKey, b string) (int, map[string]any, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/field-reports", strings.NewReader(b))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if idemKey != "" {
		req.Header.Set(HeaderIdempotencyKey, idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

func TestAcceptedOnlyAfterProduce(t *testing.T) {
	srv, p, key := setup(t)
	code, out, hdr := post(t, srv.URL, token(t, key, "field"), "report-0001", body)
	if code != http.StatusAccepted || out["report_id"] != "report-0001" || hdr.Get("Location") != "/field-reports/report-0001" {
		t.Fatalf("HTTP %d %v", code, out)
	}
	if len(p.reports) != 1 || p.reports[0].Reporter != "tim-field" || p.reports[0].ReportID != "report-0001" {
		t.Fatalf("produced %+v", p.reports)
	}

	p.fail = true
	code, _, hdr = post(t, srv.URL, token(t, key, "field"), "report-0002", body)
	if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") == "" {
		t.Errorf("kafka down: HTTP %d, want 503 with Retry-After", code)
	}
}

func TestRejections(t *testing.T) {
	srv, p, key := setup(t)
	field := token(t, key, "field")
	cases := map[string]struct {
		bearer, key, body string
		want              int
	}{
		"no token":          {"", "report-0001", body, 401},
		"media scope":       {token(t, key, "media"), "report-0001", body, 403},
		"no key":            {field, "", body, 400},
		"short key":         {field, "abc", body, 400},
		"bad hazard":        {field, "report-0001", strings.Replace(body, "VOLCANIC", "METEOR", 1), 400},
		"no latitude":       {field, "report-0001", strings.Replace(body, `"latitude":-7.54,`, "", 1), 400},
		"unknown field":     {field, "report-0001", strings.Replace(body, `{`, `{"x":1,`, 1), 400},
		"not json":          {field, "report-0001", "{", 400},
		"too large":         {field, "report-0001", `{"description":"` + strings.Repeat("x", 20000) + `"}`, 413},
		"internal scope ok": {token(t, key, "internal"), "report-0009", body, 202},
	}
	for name, c := range cases {
		if code, _, _ := post(t, srv.URL, c.bearer, c.key, c.body); code != c.want {
			t.Errorf("%s: HTTP %d, want %d", name, code, c.want)
		}
	}
	if len(p.reports) != 1 {
		t.Errorf("produced %d reports, want only the valid one", len(p.reports))
	}
}

func TestStatus(t *testing.T) {
	srv, _, key := setup(t)
	get := func(id string) map[string]any {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/field-reports/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+token(t, key, "field"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
	if s := get("done-0001")["status"]; s != "processed" {
		t.Errorf("stored report status = %v", s)
	}
	if s := get("wait-0001")["status"]; s != "pending" {
		t.Errorf("unknown report status = %v", s)
	}
}

func TestRefusedStatusLookupIsNotCountedAsAReport(t *testing.T) {
	h := &Handler{Verifier: nil}
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	before := testutil.ToFloat64(received.WithLabelValues("unauthorized"))
	resp, err := http.Get(srv.URL + "/field-reports/report-0001")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}
	if after := testutil.ToFloat64(received.WithLabelValues("unauthorized")); after != before {
		t.Fatalf("a GET was counted as a refused report (%v -> %v)", before, after)
	}
	resp, _ = http.Post(srv.URL+"/field-reports", "application/json", nil)
	resp.Body.Close()
	if after := testutil.ToFloat64(received.WithLabelValues("unauthorized")); after != before+1 {
		t.Fatalf("a refused POST was not counted (%v -> %v)", before, after)
	}
}
