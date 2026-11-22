package api

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
)

func TestRequireAuthClockLeeway(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	h := RequireAuth(&KeyProvider{publicKey: &key.PublicKey}, "bnpb-auth", "client-api")(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	call := func(expiredAgo time.Duration) int {
		now := time.Now()
		tok, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": "bnpb-auth", "aud": "client-api", "sub": "field-basarnas", "scope": "field", "type": "access",
			"iat": now.Add(-expiredAgo - time.Minute).Unix(), "exp": now.Add(-expiredAgo).Unix(),
		}).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "/hazards", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if got := call(jwtauth.ClockLeeway - 10*time.Second); got != http.StatusNoContent {
		t.Fatalf("expired inside leeway: status %d, want 204", got)
	}
	if got := call(jwtauth.ClockLeeway + 10*time.Second); got != http.StatusUnauthorized {
		t.Fatalf("expired beyond leeway: status %d, want 401", got)
	}
}
