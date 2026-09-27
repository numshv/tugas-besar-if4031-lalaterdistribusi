package poller

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

type Status struct {
	Healthy     bool      `json:"healthy"`
	LastSuccess time.Time `json:"last_success"`
	LastError   string    `json:"last_error,omitempty"`
}

type sourceStatus struct{ v atomic.Value }

func newSourceStatus() *sourceStatus {
	s := &sourceStatus{}
	s.v.Store(Status{})
	return s
}
func (s *sourceStatus) get() Status { return s.v.Load().(Status) }
func (s *sourceStatus) markOK() {
	s.v.Store(Status{Healthy: true, LastSuccess: time.Now().UTC()})
}
func (s *sourceStatus) markErr(err error) {
	prev := s.get()
	s.v.Store(Status{Healthy: false, LastSuccess: prev.LastSuccess, LastError: err.Error()})
}

// sourceClient membungkus base URL + header auth per instansi. Kredensial
// BMKG dan PVMBG memang harus beda header/format (lihat Problem 3), jadi
// tidak digabung jadi satu client generik.
type SourceClient struct {
	HTTP    *httpx.Client
	BaseURL string
	Header  http.Header
}

func (c SourceClient) getJSON(ctx context.Context, path string, since time.Time, out any) error {
	url := c.BaseURL + path
	if !since.IsZero() {
		url += "?since=" + since.UTC().Format(time.RFC3339)
	}
	return c.HTTP.GetJSON(ctx, url, c.Header, out)
}

type Poller struct {
	log      *slog.Logger
	interval time.Duration
	timeout  time.Duration

	bmkg  SourceClient
	pvmbg SourceClient

	bmkgStatus  *sourceStatus
	pvmbgStatus *sourceStatus

	onSeismic  func(ctx context.Context, events, warnings []map[string]any) error
	onVolcanic func(ctx context.Context, reports []map[string]any) error

	mu          sync.Mutex
	bmkgCursor  time.Time
	pvmbgCursor time.Time
}

func New(
	log *slog.Logger, interval, timeout time.Duration, bmkg, pvmbg SourceClient,
	onSeismic func(context.Context, []map[string]any, []map[string]any) error,
	onVolcanic func(context.Context, []map[string]any) error,
) *Poller {
	return &Poller{
		log: log, interval: interval, timeout: timeout,
		bmkg: bmkg, pvmbg: pvmbg,
		bmkgStatus: newSourceStatus(), pvmbgStatus: newSourceStatus(),
		onSeismic: onSeismic, onVolcanic: onVolcanic,
	}
}

func (p *Poller) BMKGStatus() Status  { return p.bmkgStatus.get() }
func (p *Poller) PVMBGStatus() Status { return p.pvmbgStatus.get() }

// Run dipanggil lewat `go p.Run(ctx)` dari main, bukan blocking start-up.
// Dua goroutine di dalamnya berjalan sampai ctx dibatalkan (graceful
// shutdown lewat signal.NotifyContext di main.go).
func (p *Poller) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.loop(ctx, p.pollBMKGOnce) }()
	go func() { defer wg.Done(); p.loop(ctx, p.pollPVMBGOnce) }()
	wg.Wait()
}

func (p *Poller) loop(ctx context.Context, poll func(context.Context)) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollBMKGOnce(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	p.mu.Lock()
	since := p.bmkgCursor
	p.mu.Unlock()

	var events, warnings []map[string]any
	var eventsErr, warningsErr error

	// dua endpoint BMKG diambil paralel murni untuk kecepatan (dua-duanya
	// sama-sama cepat), bukan untuk mencegah blocking — pencegahan blocking
	// yang sesungguhnya ada di pemisahan goroutine BMKG vs PVMBG di atas.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		eventsErr = p.bmkg.getJSON(callCtx, "/seismic-events", since, &events)
	}()
	go func() {
		defer wg.Done()
		warningsErr = p.bmkg.getJSON(callCtx, "/tsunami-warnings", since, &warnings)
	}()
	wg.Wait()

	if eventsErr != nil {
		p.bmkgStatus.markErr(eventsErr)
		p.log.WarnContext(ctx, "bmkg seismic poll failed", "error", eventsErr)
		return
	}
	if warningsErr != nil {
		p.bmkgStatus.markErr(warningsErr)
		p.log.WarnContext(ctx, "bmkg tsunami poll failed", "error", warningsErr)
		return
	}

	if err := p.onSeismic(ctx, events, warnings); err != nil {
		p.bmkgStatus.markErr(err)
		p.log.ErrorContext(ctx, "process seismic batch failed", "error", err)
		return
	}

	p.bmkgStatus.markOK()
	p.mu.Lock()
	p.bmkgCursor = time.Now().UTC().Add(-2 * time.Second)
	p.mu.Unlock()
}

func (p *Poller) pollPVMBGOnce(ctx context.Context) {
	// context timeout ini adalah pertahanan utama P2.3: kalau PVMBG di mode
	// outage "timeout" (menggantung tanpa respons), goroutine ini tetap
	// dipaksa berhenti menunggu setelah p.timeout, TIDAK PERNAH menahan
	// goroutine BMKG di atas karena keduanya berjalan independen.
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	p.mu.Lock()
	since := p.pvmbgCursor
	p.mu.Unlock()

	var reports []map[string]any
	if err := p.pvmbg.getJSON(callCtx, "/volcanic-reports", since, &reports); err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.WarnContext(ctx, "pvmbg poll failed", "error", err)
		return
	}

	if err := p.onVolcanic(ctx, reports); err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.ErrorContext(ctx, "process volcanic batch failed", "error", err)
		return
	}

	p.pvmbgStatus.markOK()
	p.mu.Lock()
	p.pvmbgCursor = time.Now().UTC().Add(-2 * time.Second)
	p.mu.Unlock()
}
