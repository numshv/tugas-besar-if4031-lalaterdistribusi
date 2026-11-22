package api

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/processor"
)

type fakeSource struct{ late []fire.LateDetection }

func (f fakeSource) Status() processor.Status                { return processor.Status{CaughtUp: true} }
func (f fakeSource) Fires() ([]hazard.UpsertMessage, string) { return nil, "d" }
func (f fakeSource) Late() []fire.LateDetection              { return f.late }

func TestInternalEndpointsNeedInternalScope(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	v := &jwtauth.Verifier{Keys: jwtauth.NewStaticKeyProvider(&key.PublicKey), Issuer: "bnpb-auth", Audience: "client-api"}
	srv := httptest.NewServer(Routes(fakeSource{}, jwtauth.RequireInternal(v, []string{"audit-secret"}, "no")))
	defer srv.Close()

	sign := func(scope string) string {
		s, _ := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": "bnpb-auth", "aud": "client-api", "sub": "t", "type": "access", "scope": scope,
			"exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString(key)
		return s
	}
	cases := map[string]struct {
		bearer string
		want   int
	}{
		"none":     {"", 401},
		"audit":    {"audit-secret", 200},
		"internal": {sign("internal"), 200},
		"media":    {sign("media"), 403},
		"field":    {sign("field"), 403},
	}
	for name, c := range cases {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/internal/wildfire/stats", nil)
		if c.bearer != "" {
			req.Header.Set("Authorization", "Bearer "+c.bearer)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: HTTP %d, want %d", name, resp.StatusCode, c.want)
		}
	}
	if resp, _ := http.Get(srv.URL + "/health"); resp.StatusCode != 200 {
		t.Errorf("health: HTTP %d", resp.StatusCode)
	}
}

func TestLatePaging(t *testing.T) {
	var late []fire.LateDetection
	for i := range 5 {
		late = append(late, fire.LateDetection{Offset: int64(10 + i), Key: "k"})
	}
	srv := httptest.NewServer(Routes(fakeSource{late: late}, func(h http.Handler) http.Handler { return h }))
	defer srv.Close()

	var page struct {
		Total int                  `json:"total"`
		Items []fire.LateDetection `json:"items"`
		Next  *int64               `json:"next_after_offset"`
	}
	get := func(q string) {
		resp, err := http.Get(srv.URL + "/internal/wildfire/late" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		page.Next = nil
		_ = json.NewDecoder(resp.Body).Decode(&page)
	}
	get("?limit=2")
	if page.Total != 5 || len(page.Items) != 2 || page.Next == nil || *page.Next != 11 {
		t.Fatalf("page 1 %+v", page)
	}
	get("?limit=2&after_offset=13")
	if len(page.Items) != 1 || page.Items[0].Offset != 14 || page.Next != nil {
		t.Fatalf("last page %+v", page)
	}
}
