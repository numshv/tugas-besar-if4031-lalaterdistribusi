package audit

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

func WriteError(w http.ResponseWriter, status int, code, message string) {
	httpx.Error(w, status, code, message)
}

func ParseTokens(raw string) []string {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func RequireToken(tokens []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(tokens) == 0 {
				WriteError(w, http.StatusServiceUnavailable, "audit_not_configured", "audit token belum dikonfigurasi")
				return
			}
			presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || !validToken(tokens, presented) {
				WriteError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
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

func LimitConcurrency(max int) func(http.Handler) http.Handler {
	sem := make(chan struct{}, max)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				next.ServeHTTP(w, r)
			default:
				WriteError(w, http.StatusTooManyRequests, "too_many_requests", "audit API sedang penuh, coba lagi")
			}
		})
	}
}

type Params struct {
	Source hazard.Source
	Since  time.Time
	Limit  int
	Cursor string
}

func single(w http.ResponseWriter, r *http.Request, names ...string) bool {
	q := r.URL.Query()
	for _, n := range names {
		if len(q[n]) > 1 {
			WriteError(w, http.StatusBadRequest, "duplicate_parameter", "parameter "+n+" hanya boleh muncul sekali")
			return false
		}
	}
	return true
}

func ParseSource(w http.ResponseWriter, raw string) (hazard.Source, bool) {
	if raw == "" {
		return "", true
	}
	s := hazard.Source(raw)
	if !s.Known() {
		WriteError(w, http.StatusBadRequest, "invalid_source", "source tidak dikenal: "+raw)
		return "", false
	}
	return s, true
}

func ParseParams(w http.ResponseWriter, r *http.Request) (Params, bool) {
	if !single(w, r, "source", "since", "limit", "cursor", "hazard_id") {
		return Params{}, false
	}
	q := r.URL.Query()
	var p Params
	var ok bool
	if p.Source, ok = ParseSource(w, q.Get("source")); !ok {
		return Params{}, false
	}
	if raw := q.Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, "invalid_since", "since harus ISO 8601, contoh 2026-11-12T00:00:00Z")
			return Params{}, false
		}
		p.Since = t.UTC()
	}
	p.Limit = DefaultLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > MaxLimit {
			WriteError(w, http.StatusBadRequest, "invalid_limit", "limit harus 1 sampai "+strconv.Itoa(MaxLimit))
			return Params{}, false
		}
		p.Limit = n
	}
	p.Cursor = q.Get("cursor")
	return p, true
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func units(u []string) []string {
	if u == nil {
		return []string{}
	}
	return u
}
