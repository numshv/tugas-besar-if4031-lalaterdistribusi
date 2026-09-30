package api

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

type contextKey string

const scopeKey contextKey = "scope"

func RequireAuth(publicKey *rsa.PublicKey) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				httpx.Error(w, http.StatusUnauthorized, "missing_token", "Authorization header is required")
				return
			}

			token, ok := strings.CutPrefix(authHeader, "Bearer ")
			if !ok {
				httpx.Error(w, http.StatusUnauthorized, "invalid_token_format", "Authorization header must use Bearer scheme")
				return
			}

			claims := jwt.MapClaims{}
			parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return publicKey, nil
			})
			if err != nil || !parsed.Valid {
				httpx.Error(w, http.StatusUnauthorized, "invalid_token", "token is invalid or expired")
				return
			}

			scope, _ := claims["scope"].(string)
			ctx := context.WithValue(r.Context(), scopeKey, scope)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
