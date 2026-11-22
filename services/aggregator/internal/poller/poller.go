package poller

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/telemetry"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/guard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

var fencedWrites = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "poller_fenced_writes_rejected_total",
	Help: "Poll batches rejected because a newer leader epoch already wrote.",
}, []string{"source"})

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

// getRaw fetches path without decoding it, so the pipeline can check the body
// and keep it intact if it turns out to be broken.
func (c SourceClient) getRaw(ctx context.Context, path string, since time.Time) (pipeline.Response, error) {
	url := c.BaseURL + path
	if !since.IsZero() {
		url += "?since=" + since.UTC().Format(time.RFC3339)
	}
	body, err := c.HTTP.GetRaw(ctx, url, c.Header, guard.MaxBodyBytes)
	r := pipeline.Response{Path: path, Body: body, ReceivedAt: time.Now().UTC()}
	if errors.Is(err, httpx.ErrBodyTooLarge) {
		r.TooLarge, err = true, nil
	}
	return r, err
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

	onSeismic  func(ctx context.Context, events, warnings pipeline.Response) error
	onVolcanic func(ctx context.Context, reports pipeline.Response) error

	mu          sync.Mutex
	bmkgCursor  time.Time
	pvmbgCursor time.Time
}

func New(
	log *slog.Logger, interval, timeout time.Duration, bmkg, pvmbg SourceClient,
	onSeismic func(context.Context, pipeline.Response, pipeline.Response) error,
	onVolcanic func(context.Context, pipeline.Response) error,
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
	go func() { defer wg.Done(); p.loop(ctx, hazard.SourceBMKG) }()
	go func() { defer wg.Done(); p.loop(ctx, hazard.SourcePVMBG) }()
	wg.Wait()
}

func (p *Poller) loop(ctx context.Context, source hazard.Source) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		_ = p.pollOnce(ctx, source, nil)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Leadership is asked right before every poll; see pkg/lease.
type Leadership interface {
	Confirm(ctx context.Context) (epoch int64, ok bool)
}

// Checkpoints persists the poll cursor together with the fencing epoch.
type Checkpoints interface {
	LoadCheckpoint(ctx context.Context, source hazard.Source) (cursor string, epoch int64, err error)
	CommitPoll(ctx context.Context, source hazard.Source, epoch int64, cursor string, events []hazard.Event) ([]hazard.Event, error)
}

// Lead polls one source for a single leader term, until ctx ends or the term is over.
// It resumes from the stored cursor and raises the fence to epoch before the first
// request, so writes from any older term are rejected from then on.
func (p *Poller) Lead(ctx context.Context, source hazard.Source, epoch int64, lead Leadership, cps Checkpoints) {
	log := p.log.With("source", source, "epoch", epoch)
	cursor, _, err := cps.LoadCheckpoint(ctx, source)
	if err != nil {
		log.ErrorContext(ctx, "load checkpoint failed, stepping down", "error", err)
		return
	}
	if _, err := cps.CommitPoll(ctx, source, epoch, cursor, nil); err != nil {
		log.ErrorContext(ctx, "raise fence failed, stepping down", "error", err)
		return
	}
	p.setCursor(source, parseCursor(cursor))
	log.InfoContext(ctx, "polling as leader", "cursor", cursor)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		if got, ok := lead.Confirm(ctx); !ok || got != epoch {
			log.WarnContext(ctx, "lease no longer ours, stop polling before contacting upstream")
			return
		}
		fence := pipeline.Fence{Source: source, Epoch: epoch, Cursor: formatCursor(p.cursor(source))}
		err := p.pollOnce(ctx, source, &fence)
		if err == nil {
			_, err = cps.CommitPoll(ctx, source, epoch, formatCursor(p.cursor(source)), nil)
		}
		if errors.Is(err, store.ErrStaleEpoch) {
			fencedWrites.WithLabelValues(string(source)).Inc()
			log.WarnContext(ctx, "stale epoch, write rejected; a newer leader took over", "error", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, source hazard.Source, fence *pipeline.Fence) error {
	if source == hazard.SourceBMKG {
		return p.pollBMKGOnce(ctx, fence)
	}
	return p.pollPVMBGOnce(ctx, fence)
}

// cursorOverlap re-reads a window on every poll. Duplicates are dropped by the versioned
// store, so the overlap only costs bandwidth, while it keeps a node clock that runs up to
// this much ahead from skipping events.
const cursorOverlap = 2 * time.Minute

func (p *Poller) cursor(source hazard.Source) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if source == hazard.SourceBMKG {
		return p.bmkgCursor
	}
	return p.pvmbgCursor
}

func (p *Poller) setCursor(source hazard.Source, t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if source == hazard.SourceBMKG {
		p.bmkgCursor = t
	} else {
		p.pvmbgCursor = t
	}
}

func formatCursor(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseCursor(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// processContext detaches saving and publishing from the fetch deadline, keeping the
// poll's correlation ID and trace, and its fence when polling as leader.
func processContext(ctx context.Context, fence *pipeline.Fence) (context.Context, context.CancelFunc) {
	pctx := correlation.WithID(context.Background(), correlation.FromContext(ctx))
	pctx = trace.ContextWithSpan(pctx, trace.SpanFromContext(ctx))
	if fence != nil {
		pctx = pipeline.WithFence(pctx, *fence)
	}
	return context.WithTimeout(pctx, processTimeout)
}

func startPoll(ctx context.Context, source hazard.Source) (context.Context, trace.Span) {
	ctx = correlation.WithID(ctx, correlation.New())
	return telemetry.Tracer().Start(ctx, "poll "+string(source))
}

func (p *Poller) pollBMKGOnce(ctx context.Context, fence *pipeline.Fence) error {
	ctx, span := startPoll(ctx, hazard.SourceBMKG)
	defer span.End()
	fetchCtx, cancelFetch := context.WithTimeout(ctx, p.timeout)
	defer cancelFetch()

	since := p.cursor(hazard.SourceBMKG)
	started := time.Now()

	var events, warnings pipeline.Response
	var eventsErr, warningsErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		events, eventsErr = p.bmkg.getRaw(fetchCtx, pipeline.PathSeismic, since)
	}()
	go func() {
		defer wg.Done()
		warnings, warningsErr = p.bmkg.getRaw(fetchCtx, pipeline.PathTsunami, since)
	}()
	wg.Wait()

	if eventsErr != nil {
		p.bmkgStatus.markErr(eventsErr)
		p.log.WarnContext(fetchCtx, "bmkg seismic poll failed", "error", eventsErr)
		return eventsErr
	}
	if warningsErr != nil {
		p.bmkgStatus.markErr(warningsErr)
		p.log.WarnContext(fetchCtx, "bmkg tsunami poll failed", "error", warningsErr)
		return warningsErr
	}

	processCtx, cancelProcess := processContext(ctx, fence)
	defer cancelProcess()

	if err := p.onSeismic(processCtx, events, warnings); err != nil {
		p.bmkgStatus.markErr(err)
		p.log.ErrorContext(processCtx, "process seismic batch failed", "error", err)
		return err
	}

	p.bmkgStatus.markOK()
	p.setCursor(hazard.SourceBMKG, started.UTC().Add(-cursorOverlap))
	return nil
}

func (p *Poller) pollPVMBGOnce(ctx context.Context, fence *pipeline.Fence) error {
	ctx, span := startPoll(ctx, hazard.SourcePVMBG)
	defer span.End()
	fetchCtx, cancelFetch := context.WithTimeout(ctx, p.timeout)
	defer cancelFetch()

	since := p.cursor(hazard.SourcePVMBG)
	started := time.Now()

	reports, err := p.pvmbg.getRaw(fetchCtx, pipeline.PathVolcanic, since)
	if err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.WarnContext(fetchCtx, "pvmbg poll failed", "error", err)
		return err
	}

	processCtx, cancelProcess := processContext(ctx, fence)
	defer cancelProcess()

	if err := p.onVolcanic(processCtx, reports); err != nil {
		p.pvmbgStatus.markErr(err)
		p.log.ErrorContext(processCtx, "process volcanic batch failed", "error", err)
		return err
	}

	p.pvmbgStatus.markOK()
	p.setCursor(hazard.SourcePVMBG, started.UTC().Add(-cursorOverlap))
	return nil
}
