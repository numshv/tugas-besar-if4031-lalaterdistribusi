package jwtauth

import (
	"crypto/subtle"
	"net/http"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

func RequireInternal(v *Verifier, auditTokens []string, forbidden string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if header == "" {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization header is required")
				return
			}
			token, ok := BearerToken(header)
			if !ok {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization must use the Bearer scheme")
				return
			}
			if validToken(auditTokens, token) {
				next.ServeHTTP(w, r)
				return
			}
			if v == nil {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
				return
			}
			scope, _, err := v.Verify(token)
			if err != nil {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
				return
			}
			if scope != ScopeInternal {
				httpx.Error(w, http.StatusForbidden, "forbidden", forbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func validToken(tokens []string, presented string) bool {
	match := 0
	for _, t := range tokens {
		match |= subtle.ConstantTimeCompare([]byte(t), []byte(presented))
	}
	return match == 1
}

func RequireScopes(v *Verifier, allowed []string, forbidden string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := BearerToken(r.Header.Get("Authorization"))
			if !ok {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization must be a Bearer token")
				return
			}
			if v == nil {
				httpx.Error(w, http.StatusServiceUnavailable, "auth_not_configured", "token verification is not configured")
				return
			}
			scope, _, err := v.Verify(token)
			if err != nil {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
				return
			}
			for _, s := range allowed {
				if s == scope {
					next.ServeHTTP(w, r)
					return
				}
			}
			httpx.Error(w, http.StatusForbidden, "forbidden", forbidden)
		})
	}
}
