package jwtauth

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func signed(t *testing.T, key *rsa.PrivateKey, scope, typ string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "bnpb-auth", "aud": "client-api", "sub": "viewer", "scope": scope, "type": typ,
		"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(),
	})
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRequireScopes(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v := &Verifier{Keys: NewStaticKeyProvider(&key.PublicKey), Issuer: "bnpb-auth", Audience: "client-api"}
	h := RequireScopes(v, []string{ScopeAnalytics, ScopeInternal}, "nope")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	call := func(auth string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	cases := map[string]int{
		"Bearer " + signed(t, key, ScopeAnalytics, "access"):  http.StatusNoContent,
		"Bearer " + signed(t, key, ScopeInternal, "access"):   http.StatusNoContent,
		"Bearer " + signed(t, key, ScopeMedia, "access"):      http.StatusForbidden,
		"Bearer " + signed(t, key, ScopeField, "access"):      http.StatusForbidden,
		"Bearer " + signed(t, key, ScopeAnalytics, "refresh"): http.StatusUnauthorized,
		"Bearer garbage": http.StatusUnauthorized,
		"":               http.StatusUnauthorized,
	}
	for auth, want := range cases {
		if got := call(auth); got != want {
			t.Fatalf("auth %.30q: status %d, want %d", auth, got, want)
		}
	}

	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	if got := call("Bearer " + signed(t, other, ScopeAnalytics, "access")); got != http.StatusUnauthorized {
		t.Fatalf("token signed by another key: %d", got)
	}
	unconfigured := RequireScopes(nil, []string{ScopeAnalytics}, "nope")(http.NotFoundHandler())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer x")
	unconfigured.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no verifier must be 503, got %d", rec.Code)
	}
}
