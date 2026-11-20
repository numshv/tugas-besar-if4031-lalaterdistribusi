package httpx

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddlewareServesMetricsLabelledByRoutePattern(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := Middleware(slog.New(slog.NewTextHandler(io.Discard, nil)))(mux)

	for _, path := range []string{"/items/1", "/items/2", "/nope"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		`http_server_requests_total{code="204",method="GET",route="GET /items/{id}"} 2`,
		`http_server_requests_total{code="404",method="GET",route="unmatched"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if strings.Contains(body, `route="/items/1"`) {
		t.Error("route label must be the pattern, not the raw path")
	}
}
