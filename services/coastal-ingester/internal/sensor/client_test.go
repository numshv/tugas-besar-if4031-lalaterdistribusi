package sensor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

func newClient(t *testing.T, h http.HandlerFunc) (*Client, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); h(w, r) }))
	t.Cleanup(srv.Close)
	c := New(Config{BaseURL: srv.URL, Token: "t", RequestTimeout: time.Second, MaxRetries: 2,
		BreakerFailures: 3, BreakerDelay: time.Hour, MaxConcurrency: 2, StuckAfter: 50 * time.Millisecond,
		StaleAfter: 15 * time.Minute}, srv.Client())
	return c, &hits
}

func TestOpenBreakerStopsHammeringASickSensor(t *testing.T) {
	c, hits := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })

	var lastErr error
	for range 20 {
		_, lastErr = c.Observations(context.Background(), "", 10)
	}
	if !errors.Is(lastErr, circuitbreaker.ErrOpen) {
		t.Fatalf("last error = %v, want the breaker open", lastErr)
	}
	if n := hits.Load(); n > 3 {
		t.Fatalf("sensor received %d requests for 20 polls, want at most the 3 that opened the breaker", n)
	}
}

func TestClientErrorsAreNotRetried(t *testing.T) {
	c, hits := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	if _, err := c.Observations(context.Background(), "", 10); err == nil {
		t.Fatal("want an error")
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("401 was retried: %d requests", n)
	}
}

func TestFrozenGeneratedAtIsStale(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"generated_at":"2026-11-12T00:00:00Z","items":[]}`))
	})
	if _, err := c.Observations(context.Background(), "", 10); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := c.Observations(context.Background(), "", 10); !errors.Is(err, ErrStale) {
		t.Fatalf("err = %v, want ErrStale for a snapshot that stopped moving", err)
	}
}

func TestExpiredDataIsStaleOnlyWhenCaughtUp(t *testing.T) {
	old := `{"generated_at":"2026-11-12T05:00:00Z","items":[{"observed_at":"2026-11-12T01:00:00Z"},{"observed_at":"2026-11-12T01:00:15Z"}]}`
	c, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(old)) })

	if _, err := c.Observations(context.Background(), "", 2); err != nil {
		t.Fatalf("a full page is backlog, got %v", err)
	}
	if _, err := c.Observations(context.Background(), "", 10); !errors.Is(err, ErrStale) {
		t.Fatalf("err = %v, want ErrStale for a caught-up page hours behind the sensor clock", err)
	}
}

func TestSensorClockGoingBackIsNotStale(t *testing.T) {
	gen := []string{"2026-11-12T05:00:00Z", "2026-11-12T00:00:05Z"}
	var n atomic.Int64
	c, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"generated_at":"` + gen[min(int(n.Add(1))-1, 1)] + `","items":[]}`))
	})
	c.Observations(context.Background(), "", 10)
	time.Sleep(80 * time.Millisecond)
	if _, err := c.Observations(context.Background(), "", 10); err != nil {
		t.Fatalf("a restarted sensor clock was treated as stale: %v", err)
	}
}
