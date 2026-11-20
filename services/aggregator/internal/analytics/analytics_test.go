package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

func TestCategory(t *testing.T) {
	cases := []struct {
		typ   hazard.Type
		attrs map[string]any
		want  string
	}{
		{hazard.TypeSeismic, map[string]any{"potential_tsunami": true}, CategoryTsunami},
		{hazard.TypeSeismic, map[string]any{"potential_tsunami": false}, CategoryGempa},
		{hazard.TypeSeismic, nil, CategoryGempa},
		{hazard.TypeSeismic, map[string]any{"potential_tsunami": "true"}, CategoryGempa},
		{hazard.TypeVolcanic, map[string]any{"potential_tsunami": true}, CategoryVolcanic},
		{hazard.TypeWildfire, nil, CategoryWildfire},
		{hazard.TypeCoastal, nil, CategoryCoastal},
	}
	for _, c := range cases {
		if got := Category(c.typ, c.attrs); got != c.want {
			t.Fatalf("Category(%s, %v) = %s, want %s", c.typ, c.attrs, got, c.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	at := time.Date(2026, 11, 18, 21, 44, 5, 0, time.UTC)
	want := map[string]time.Time{
		BucketHour:  time.Date(2026, 11, 18, 21, 0, 0, 0, time.UTC),
		BucketDay:   time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		BucketWeek:  time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC),
		BucketMonth: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
	}
	for bucket, w := range want {
		if got := Truncate(at, bucket); !got.Equal(w) {
			t.Fatalf("Truncate(%s) = %v, want %v", bucket, got, w)
		}
	}
	sunday := time.Date(2026, 11, 22, 23, 0, 0, 0, time.UTC)
	if got := Truncate(sunday, BucketWeek); !got.Equal(want[BucketWeek]) {
		t.Fatalf("Sunday belongs to the week starting Monday 16th, got %v", got)
	}
	wib := time.Date(2026, 11, 19, 5, 0, 0, 0, time.FixedZone("WIB", 7*3600))
	if got := Truncate(wib, BucketDay); !got.Equal(want[BucketDay]) {
		t.Fatalf("buckets are UTC: 05:00 WIB on the 19th is the 18th UTC, got %v", got)
	}
}

func TestParseQuery(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	q, err := ParseQuery(url.Values{}, now)
	if err != nil || !q.From.Equal(now.Add(-30*24*time.Hour)) || q.ResolveBucket() != BucketDay {
		t.Fatalf("default = %+v err=%v", q, err)
	}
	q, _ = ParseQuery(url.Values{"range": {"24h"}}, now)
	if q.ResolveBucket() != BucketHour || q.CacheKey != "rel|24h|" {
		t.Fatalf("24h = %+v", q)
	}
	q, _ = ParseQuery(url.Values{"range": {"all"}, "bucket": {"month"}}, now)
	if !q.AllTime || q.Bucket != BucketMonth {
		t.Fatalf("all = %+v", q)
	}
	q, err = ParseQuery(url.Values{"from": {"2026-01-01T00:00:00Z"}, "to": {"2026-02-01T00:00:00Z"}}, now)
	if err != nil || q.ResolveBucket() != BucketDay || !strings.HasPrefix(q.CacheKey, "abs|") {
		t.Fatalf("custom = %+v err=%v", q, err)
	}
	for _, bad := range []url.Values{
		{"range": {"2w"}}, {"bucket": {"year"}}, {"from": {"x"}, "to": {"2026-02-01T00:00:00Z"}},
		{"from": {"2026-02-01T00:00:00Z"}, "to": {"2026-01-01T00:00:00Z"}},
	} {
		if _, err := ParseQuery(bad, now); !errors.Is(err, ErrBadQuery) {
			t.Fatalf("%v must be rejected, err=%v", bad, err)
		}
	}
}

func TestAccumulatorBuildsDenseAlignedSeries(t *testing.T) {
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	q := Query{From: from, To: from.Add(3 * 24 * time.Hour), Bucket: BucketDay}
	var acc Accumulator
	acc.Add(Point{Category: CategoryGempa, Severity: "WASPADA", AreaName: "Selat Sunda", At: from.Add(2 * time.Hour)})
	acc.Add(Point{Category: CategoryGempa, Severity: "SIAGA", AreaName: "Selat Sunda", At: from.Add(50 * time.Hour)})
	acc.Add(Point{Category: CategoryWildfire, Severity: "AWAS", AreaName: "Kalimantan Tengah", At: from.Add(26 * time.Hour)})
	r := acc.Result(q)
	if len(r.Buckets) != 4 || r.Total != 3 {
		t.Fatalf("buckets=%d total=%d", len(r.Buckets), r.Total)
	}
	if got := r.Series[CategoryGempa]; got[0] != 1 || got[1] != 0 || got[2] != 1 {
		t.Fatalf("gempa series = %v", got)
	}
	if got := r.Series[CategoryWildfire]; got[1] != 1 {
		t.Fatalf("wildfire series = %v", got)
	}
	for _, c := range Categories {
		if len(r.Series[c]) != len(r.Buckets) {
			t.Fatalf("series %s must align with buckets", c)
		}
	}
	if r.BySeverity["NORMAL"] != 0 || r.BySeverity["AWAS"] != 1 || len(r.BySeverity) != 4 {
		t.Fatalf("severity = %v", r.BySeverity)
	}
	if r.ByArea[0].AreaName != "Selat Sunda" || r.ByArea[0].Count != 2 || r.ByCategory[CategoryGempa] != 2 {
		t.Fatalf("area = %v category = %v", r.ByArea, r.ByCategory)
	}
}

type fakeSource struct {
	calls atomic.Int32
	delay time.Duration
	err   error
}

func (f *fakeSource) Analytics(_ context.Context, q Query, _ bool) (Result, error) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	if f.err != nil {
		return Result{}, f.err
	}
	var acc Accumulator
	acc.Add(Point{Category: CategoryGempa, Severity: "NORMAL", AreaName: "X", At: q.To.Add(-time.Hour)})
	r := acc.Result(q)
	r.Method = "fake"
	return r, nil
}

func passthrough(next http.Handler) http.Handler { return next }

func serve(h http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestHandlerCachesAndCollapses(t *testing.T) {
	src := &fakeSource{delay: 30 * time.Millisecond}
	h := NewHandler(src, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return now }
	routes := h.Routes(passthrough)

	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			if rec := serve(routes, "/analytics/data?range=7d"); rec.Code != http.StatusOK {
				t.Errorf("status %d", rec.Code)
			}
		})
	}
	wg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("30 concurrent requests hit the store %d times, want 1", n)
	}
	rec := serve(routes, "/analytics/data?range=7d")
	var r Result
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil || r.Total != 1 || r.Method != "fake" || !r.AsOf.Equal(now) {
		t.Fatalf("cached body = %s", rec.Body)
	}
	now = now.Add(6 * time.Second)
	serve(routes, "/analytics/data?range=7d")
	if n := src.calls.Load(); n != 2 {
		t.Fatalf("after the 5 s TTL the store must be asked again, calls = %d", n)
	}
	serve(routes, "/analytics/data?range=30d")
	if n := src.calls.Load(); n != 3 {
		t.Fatalf("another range must not share the cache, calls = %d", n)
	}
}

func TestHandlerErrorsAndPage(t *testing.T) {
	h := NewHandler(&fakeSource{err: errors.New("mongos down")}, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	routes := h.Routes(passthrough)
	if rec := serve(routes, "/analytics/data?range=2w"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad range: %d", rec.Code)
	}
	if rec := serve(routes, "/analytics/data"); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "store_unavailable") {
		t.Fatalf("store down must be 503, got %d %s", rec.Code, rec.Body)
	}
	page := serve(routes, "/analytics")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "/analytics/static/chart.umd.js") {
		t.Fatalf("page: %d", page.Code)
	}
	for _, asset := range []string{"/analytics/static/app.js", "/analytics/static/chart.umd.js"} {
		if rec := serve(routes, asset); rec.Code != http.StatusOK || rec.Body.Len() < 1000 {
			t.Fatalf("%s: %d (%d bytes)", asset, rec.Code, rec.Body.Len())
		}
	}

	denied := h.Routes(func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) })
	})
	if rec := serve(denied, "/analytics/data"); rec.Code != http.StatusForbidden {
		t.Fatalf("data must go through the auth middleware, got %d", rec.Code)
	}
	if rec := serve(denied, "/analytics"); rec.Code != http.StatusOK {
		t.Fatalf("the page itself is public, got %d", rec.Code)
	}
}
