package poller

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
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

// processTimeout adalah batas waktu terpisah untuk proses simpan+publish
// untuk mencegah kegagalan publish "palsu" akibat deadline HTTP fetch yang sudah hampir habis.

const processTimeout = 10 * time.Second

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
	// correlationID dibuat sekali per siklus poll, dipakai di SEMUA log
	// supaya satu siklus poll bisa ditelusuri sebagai satu alur utuh.
	correlationID := correlation.New()
	fetchCtx, cancelFetch := context.WithTimeout(correlation.WithID(ctx, correlationID), p.timeout)
	defer cancelFetch()

	p.mu.Lock()
	since := p.bmkgCursor
	p.mu.Unlock()

	var events, warnings []map[string]any
	var eventsErr, warningsErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		eventsErr = p.bmkg.getJSON(fetchCtx, "/seismic-events", since, &events)
	}()
	go func() {
		defer wg.Done()
		warningsErr = p.bmkg.getJSON(fetchCtx, "/tsunami-warnings", since, &warnings)
	}()
	wg.Wait()

	if eventsErr != nil {
		p.bmkgStatus.markErr(eventsErr)
		p.log.WarnContext(fetchCtx, "bmkg seismic poll failed", "error", eventsErr)
		return
	}
	if warningsErr != nil {
		p.bmkgStatus.markErr(warningsErr)
		p.log.WarnContext(fetchCtx, "bmkg tsunami poll failed", "error", warningsErr)
		return
	}

	// context buat proses simpan+publish dipisah dari fetchCtx, pakai
	// deadline sendiri yang tidak terpotong oleh sisa waktu HTTP fetch tadi.
	// correlationID tetap sama supaya satu siklus poll tetap terlacak utuh.
	processCtx, cancelProcess := context.WithTimeout(correlation.WithID(context.Background(), correlationID), processTimeout)
	defer cancelProcess()

	if err := p.onSeismic(processCtx, events, warnings); err != nil {
		p.bmkgStatus.markErr(err)
		p.log.ErrorContext(processCtx, "process seismic batch failed", "error", err)
		return
	}

	p.bmkgStatus.markOK()
	p.mu.Lock()
	p.bmkgCursor = time.Now().UTC().Add(-2 * time.Second)
	p.mu.Unlock()
}

func (p *Poller) pollPVMBGOnce(ctx context.Context) {
	correlationID := correlation.New()
	fetchCtx, cancelFetch := context.WithTimeout(correlation.WithID(ctx, correlationID), p.timeout)
	defer cancelFetch()

	p.mu.Lock()
	since := p.pvmbgCursor
	p.mu.Unlock()

	var reports []map[string]any
	if err := p.pvmbg.getJSON(fetchCtx, "/volcanic-reports", since, &reports); err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.WarnContext(fetchCtx, "pvmbg poll failed", "error", err)
		return
	}

	// sama seperti BMKG, context proses terpisah dari context fetch.
	processCtx, cancelProcess := context.WithTimeout(correlation.WithID(context.Background(), correlationID), processTimeout)
	defer cancelProcess()

	if err := p.onVolcanic(processCtx, reports); err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.ErrorContext(processCtx, "process volcanic batch failed", "error", err)
		return
	}

	p.pvmbgStatus.markOK()
	p.mu.Lock()
	p.pvmbgCursor = time.Now().UTC().Add(-2 * time.Second)
	p.mu.Unlock()
}
