package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/pvmbg-mock/internal/volcanic"
)

const (
	testToken = TokenPrefix + "test"
	testAdmin = "admin-test"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	h := New(volcanic.NewStore(5), Config{
		Service: "pvmbg-mock", Token: testToken, AdminToken: testAdmin, SchemaVersion: volcanic.SchemaLegacy,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func admin(t *testing.T, srv *httptest.Server, path, body string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	req.Header.Set(HeaderAdminToken, testAdmin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func reports(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /volcanic-reports: HTTP %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	return b
}

func TestSchemaVersions(t *testing.T) {
	srv := newServer(t)
	cases := []struct {
		version    int
		present    []string
		absent     []string
		levelIsNum bool
	}{
		{1, []string{"alert_level", "reported_at", "ash_column_height_m"}, []string{"confidence_level", "level"}, false},
		{2, []string{"alert_level", "confidence_level"}, []string{"level"}, false},
		{3, []string{"level", "reported_at"}, []string{"alert_level"}, false},
		{4, []string{"alert_level", "reported_at"}, []string{"level"}, true},
		{5, []string{"alert_level"}, []string{"reported_at"}, false},
		{6, []string{"alert_level", "reported_at"}, []string{"ash_column_height_m"}, false},
	}
	for _, c := range cases {
		if code := admin(t, srv, "/admin/schema-version", `{"version":`+string(rune('0'+c.version))+`}`); code != 200 {
			t.Fatalf("set version %d: HTTP %d", c.version, code)
		}
		var got []map[string]any
		if err := json.Unmarshal(reports(t, srv), &got); err != nil || len(got) == 0 {
			t.Fatalf("version %d: %d reports, err %v", c.version, len(got), err)
		}
		for _, f := range c.present {
			if _, ok := got[0][f]; !ok {
				t.Errorf("version %d: field %q missing", c.version, f)
			}
		}
		for _, f := range c.absent {
			if _, ok := got[0][f]; ok {
				t.Errorf("version %d: field %q present", c.version, f)
			}
		}
		if _, isNum := got[0]["alert_level"].(float64); c.levelIsNum != isNum {
			t.Errorf("version %d: alert_level numeric = %v, want %v", c.version, isNum, c.levelIsNum)
		}
	}
	if code := admin(t, srv, "/admin/schema-version", `{"version":7}`); code != 400 {
		t.Errorf("version 7 accepted: HTTP %d", code)
	}
}

func TestCorrupt(t *testing.T) {
	srv := newServer(t)
	checks := map[string]func([]byte) bool{
		CorruptTruncated:   func(b []byte) bool { return !json.Valid(b) },
		CorruptDeep:        func(b []byte) bool { return strings.HasPrefix(string(b), strings.Repeat("[", 1000)) },
		CorruptInvalidUTF8: func(b []byte) bool { return !utf8.Valid(b) },
		CorruptOversize:    func(b []byte) bool { return len(b) > corruptOversizeBytes && json.Valid(b) },
	}
	for kind, broken := range checks {
		if code := admin(t, srv, "/admin/corrupt", `{"kind":"`+kind+`","count":2}`); code != 200 {
			t.Fatalf("schedule %s: HTTP %d", kind, code)
		}
		for i := range 2 {
			if !broken(reports(t, srv)) {
				t.Errorf("%s response %d is not broken as expected", kind, i)
			}
		}
		if b := reports(t, srv); !json.Valid(b) {
			t.Errorf("%s: response after count ran out is still broken", kind)
		}
	}
	if code := admin(t, srv, "/admin/corrupt", `{"kind":"nope"}`); code != 400 {
		t.Errorf("unknown kind accepted: HTTP %d", code)
	}
	admin(t, srv, "/admin/corrupt", `{"kind":"truncated","count":5}`)
	admin(t, srv, "/admin/corrupt", `{"count":0}`)
	if b := reports(t, srv); !json.Valid(b) {
		t.Error("count 0 did not cancel pending corruption")
	}
}
