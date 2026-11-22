package jwtauth

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyToleratesClockSkewWithinLeeway(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v := Verifier{Keys: NewStaticKeyProvider(&key.PublicKey), Issuer: "bnpb-auth", Audience: "client-api"}
	mint := func(expiredAgo time.Duration) string {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": "bnpb-auth", "aud": "client-api", "sub": "ops", "scope": ScopeInternal, "type": "access",
			"iat": now.Add(-expiredAgo - time.Minute).Unix(), "exp": now.Add(-expiredAgo).Unix(),
		})
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if _, _, err := v.Verify(mint(ClockLeeway - 10*time.Second)); err != nil {
		t.Fatalf("token expired inside the leeway must pass, got %v", err)
	}
	if _, _, err := v.Verify(mint(ClockLeeway + 10*time.Second)); err == nil {
		t.Fatal("token expired beyond the leeway must be rejected")
	}
}
