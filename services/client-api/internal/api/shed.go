package api

import (
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

var shedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "client_api_shed_total",
	Help: "Requests refused with 429 to keep capacity for higher-priority clients, by scope.",
}, []string{"scope"})

// ShedMedia keeps capacity for Tim Lapangan and Internal Ops: Media may hold at most
// maxMedia requests at a time, and anything beyond that gets 429 at once instead of queueing.
func ShedMedia(maxMedia int) func(http.Handler) http.Handler {
	slots := make(chan struct{}, max(maxMedia, 1))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if scope, _ := r.Context().Value(scopeKey).(string); scope != "media" {
				next.ServeHTTP(w, r)
				return
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
				next.ServeHTTP(w, r)
			default:
				shedTotal.WithLabelValues("media").Inc()
				w.Header().Set("Retry-After", "1")
				httpx.Error(w, http.StatusTooManyRequests, "too_many_requests", "media capacity reached, retry later")
			}
		})
	}
}

var coastalStaleAfter = func() time.Duration {
	if n, err := strconv.Atoi(os.Getenv("COASTAL_STALE_AFTER_SECONDS")); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 30 * time.Second
}()

// markStaleCoastal flags a coastal reading that has not been refreshed recently, e.g.
// while the sensor breaker is open: the client gets the last value at once, marked stale.
func markStaleCoastal(out map[string]any, ev hazard.Event, now time.Time) {
	if ev.HazardType != hazard.TypeCoastal {
		return
	}
	raw, _ := ev.Attributes["received_at"].(string)
	receivedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return
	}
	if now.Sub(receivedAt) > coastalStaleAfter {
		out["stale"] = true
		out["as_of"] = receivedAt.UTC().Format(time.RFC3339)
	}
}
