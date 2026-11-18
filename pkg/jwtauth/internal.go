package jwtauth

import (
	"crypto/subtle"
	"net/http"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

// RequireInternal lets a request through when it carries one of the
// assistants' audit tokens or a JWT with scope "internal". Media and Field
// tokens get 403 with forbidden as the message.
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
