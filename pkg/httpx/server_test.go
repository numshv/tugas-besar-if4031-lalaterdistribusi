package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestLimitConcurrency(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := LimitConcurrency(1, "svc is at capacity")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-release
	}))

	done := make(chan int)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		done <- rec.Code
	}()
	<-entered

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), `"code":"too_many_requests"`) ||
		!strings.Contains(rec.Body.String(), "svc is at capacity") {
		t.Fatalf("over limit: %d %s", rec.Code, rec.Body)
	}
	close(release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("first request = %d", code)
	}

	rec = httptest.NewRecorder()
	release = make(chan struct{})
	close(release)
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("slot must be released after a request, got %d", rec.Code)
	}
}

func TestLimitConcurrencyDefaultsTo64(t *testing.T) {
	var mu sync.Mutex
	inside, peak := 0, 0
	gate := make(chan struct{})
	h := LimitConcurrency(0, "full")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inside++
		peak = max(peak, inside)
		mu.Unlock()
		<-gate
		mu.Lock()
		inside--
		mu.Unlock()
	}))
	var wg sync.WaitGroup
	rejected := make(chan int, 80)
	for range 80 {
		wg.Go(func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code == http.StatusTooManyRequests {
				rejected <- 1
			}
		})
	}
	for {
		mu.Lock()
		n := inside
		mu.Unlock()
		if n+len(rejected) == 80 {
			break
		}
	}
	close(gate)
	wg.Wait()
	if peak != 64 || len(rejected) != 16 {
		t.Fatalf("peak = %d, rejected = %d; want 64 and 16", peak, len(rejected))
	}
}
