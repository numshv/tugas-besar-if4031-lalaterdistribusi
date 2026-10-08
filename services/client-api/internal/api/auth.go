package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

type contextKey string

const scopeKey contextKey = "scope"

func RequireAuth(keys *KeyProvider, issuer, audience string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				httpx.Error(w, http.StatusUnauthorized, "missing_token", "Authorization header is required")
				return
			}

			authParts := strings.Fields(authHeader)
			if len(authParts) != 2 || !strings.EqualFold(authParts[0], "Bearer") {
				httpx.Error(w, http.StatusUnauthorized, "invalid_token_format", "Authorization header must use Bearer scheme")
				return
			}
			token := authParts[1]

			claims := jwt.MapClaims{}
			parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
				if t.Method != jwt.SigningMethodRS256 {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				pub := keys.Get()
				if pub == nil {
					return nil, fmt.Errorf("public key not yet available")
				}
				return pub, nil
			}, jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}), jwt.WithIssuer(issuer), jwt.WithAudience(audience), jwt.WithExpirationRequired())
			if err != nil || !parsed.Valid {
				httpx.Error(w, http.StatusUnauthorized, "invalid_token", "token is invalid or expired")
				return
			}

			scope, _ := claims["scope"].(string)
			subject, _ := claims["sub"].(string)
			if claims["type"] != "access" || subject == "" || !validScope(scope) {
				httpx.Error(w, http.StatusUnauthorized, "invalid_token", "token is invalid or expired")
				return
			}
			ctx := context.WithValue(r.Context(), scopeKey, scope)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func validScope(scope string) bool {
	switch scope {
	case "media", "field", "internal":
		return true
	default:
		return false
	}
}
