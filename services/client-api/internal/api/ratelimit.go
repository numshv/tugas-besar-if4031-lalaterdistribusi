package api

import (
	"net/http"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

func LimitConcurrency(max int) func(http.Handler) http.Handler {
	if max <= 0 {
		max = 64
	}
	sem := make(chan struct{}, max)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				next.ServeHTTP(w, r)
			default:
				httpx.Error(w, http.StatusTooManyRequests, "too_many_requests",
					"client-api is at capacity")
			}
		})
	}
}