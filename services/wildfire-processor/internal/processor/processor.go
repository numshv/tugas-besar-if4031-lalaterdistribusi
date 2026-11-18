package processor

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/feed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
)

type Feed interface {
	Session(ctx context.Context) (satfeed.Session, error)
	Stream(ctx context.Context, from int64, fn func(satfeed.Event) error) error
}

type Publisher interface {
	Publish(ctx context.Context, emits []fire.Emit) error
}

type SnapshotStore interface {
	Load(ctx context.Context) (*fire.Snapshot, error)
	Save(ctx context.Context, s *fire.Snapshot) (int, error)
}

type Processor struct {
	Engine        *fire.Engine
	Feed          Feed
	Pub           Publisher
	Log           *slog.Logger
	FlushInterval time.Duration
	FlushEvents   int
	// RetryBase is the first wait after a failed connection or send; 1 s when zero.
	RetryBase time.Duration
	// Snapshots, when set, lets a restart resume from the last saved state
	// instead of replaying the feed from offset 0.
	Snapshots     SnapshotStore
	SnapshotEvery time.Duration

	mu         sync.Mutex
	started    time.Time
	catchUpTo  int64
	caughtUp   bool
	rebuildDur time.Duration
	latest     satfeed.Session
	flushNow   chan struct{}
	lastSnap   time.Time
	restoredAt int64
}

// Run blocks until ctx ends. Nothing is sent until the engine has replayed the
// feed up to the offset that was latest at startup; from then on changes go
// out every FlushInterval or FlushEvents events, whichever comes first.
func (p *Processor) Run(ctx context.Context) error {
	p.started = time.Now()
	p.flushNow = make(chan struct{}, 1)

	sess := p.waitSession(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	restored := p.restore(ctx, sess)
	p.mu.Lock()
	if restored != nil {
		p.Engine = restored
		p.restoredAt = restored.Next() - 1
	}
	p.latest, p.catchUpTo = sess, sess.LatestOffset
	p.lastSnap = time.Now()
	p.checkCaughtUp()
	p.mu.Unlock()
	p.Log.Info("catching up with the feed", "from", p.next(), "catch_up_to", sess.LatestOffset, "profile", sess.Profile, "speed", sess.Speed)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.flusher(ctx) }()
	go func() { defer wg.Done(); p.watchSession(ctx) }()
	p.consume(ctx)
	wg.Wait()
	return ctx.Err()
}

// restore loads the last snapshot if it belongs to this feed session and was
// taken with the same config; otherwise the engine starts from offset 0.
func (p *Processor) restore(ctx context.Context, sess satfeed.Session) *fire.Engine {
	if p.Snapshots == nil {
		return nil
	}
	start := time.Now()
	loadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	snap, err := p.Snapshots.Load(loadCtx)
	switch {
	case err != nil:
		p.Log.Warn("snapshot unavailable, replaying from offset 0", "error", err)
		return nil
	case snap == nil:
		p.Log.Info("no snapshot yet, replaying from offset 0")
		return nil
	case snap.Profile != sess.Profile || snap.Next-1 > sess.LatestOffset:
		p.Log.Warn("snapshot belongs to another feed session, replaying from offset 0",
			"snapshot_profile", snap.Profile, "snapshot_offset", snap.Next-1, "feed_latest", sess.LatestOffset)
		return nil
	}
	e, err := fire.Restore(p.Engine.Config(), snap)
	if err != nil {
		p.Log.Warn("snapshot not usable, replaying from offset 0", "error", err)
		return nil
	}
	restoredOffset.Set(float64(snap.Next - 1))
	p.Log.Info("restored snapshot", "offset", snap.Next-1, "taken_at", snap.TakenAt, "seconds", time.Since(start).Seconds())
	return e
}

func (p *Processor) waitSession(ctx context.Context) satfeed.Session {
	backoff := p.retryBase()
	for {
		s, err := p.Feed.Session(ctx)
		if err == nil {
			return s
		}
		p.Log.Warn("feed session unavailable", "error", err, "retry_in", backoff)
		if !sleep(ctx, backoff) {
			return satfeed.Session{}
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (p *Processor) retryBase() time.Duration {
	if p.RetryBase > 0 {
		return p.RetryBase
	}
	return time.Second
}

func (p *Processor) consume(ctx context.Context) {
	backoff := p.retryBase()
	sinceFlush := 0
	for ctx.Err() == nil {
		p.mu.Lock()
		from := p.Engine.Next()
		p.mu.Unlock()

		got := 0
		err := p.Feed.Stream(ctx, from, func(ev satfeed.Event) error {
			got++
			eventsTotal.WithLabelValues(ev.Type).Inc()
			p.mu.Lock()
			if err := p.Engine.Apply(ev, time.Now()); err != nil {
				p.Log.Warn("feed event rejected", "error", err)
			}
			processedOffset.Set(float64(p.Engine.Next() - 1))
			pending := p.Engine.PendingLen()
			p.checkCaughtUp()
			p.mu.Unlock()
			if sinceFlush++; sinceFlush >= p.FlushEvents || pending >= p.FlushEvents {
				sinceFlush = 0
				select {
				case p.flushNow <- struct{}{}:
				default:
				}
			}
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		reason := classify(err)
		reconnects.WithLabelValues(reason).Inc()
		if got > 0 {
			backoff = p.retryBase()
		}
		wait := time.Duration(0)
		switch reason {
		case "server_closed", "idle":
		case "unauthorized":
			wait = 30 * time.Second
		default:
			wait = backoff
			backoff = min(backoff*2, 30*time.Second)
		}
		p.Log.Info("feed reconnect", "reason", reason, "from", p.next(), "events", got, "wait", wait, "error", err)
		if !sleep(ctx, wait) {
			return
		}
	}
}

func classify(err error) string {
	var se *feed.StatusError
	switch {
	case err == nil:
		return "server_closed"
	case errors.Is(err, feed.ErrIdle):
		return "idle"
	case errors.As(err, &se) && se.Code == http.StatusTooManyRequests:
		return "too_many_connections"
	case errors.As(err, &se) && se.Code == http.StatusUnauthorized:
		return "unauthorized"
	case errors.As(err, &se) && se.Code == http.StatusServiceUnavailable:
		return "unavailable"
	case errors.As(err, &se):
		return "http_error"
	default:
		return "network"
	}
}

func (p *Processor) next() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Engine.Next()
}

// checkCaughtUp must be called with p.mu held.
func (p *Processor) checkCaughtUp() {
	if p.caughtUp || p.Engine.Next() <= p.catchUpTo {
		return
	}
	p.caughtUp = true
	p.rebuildDur = time.Since(p.started)
	caughtUp.Set(1)
	rebuildSeconds.Set(p.rebuildDur.Seconds())
	p.Log.Info("rebuild finished, sending changes from now on", "offset", p.Engine.Next()-1,
		"seconds", p.rebuildDur.Seconds(), "pending", p.Engine.PendingLen())
	select {
	case p.flushNow <- struct{}{}:
	default:
	}
}

func (p *Processor) flusher(ctx context.Context) {
	t := time.NewTicker(p.FlushInterval)
	defer t.Stop()
	// The gauges scan the whole state, so they are refreshed on a slow clock
	// rather than on every flush.
	gauges := time.NewTicker(2 * time.Second)
	defer gauges.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			p.flush(shutdown, true)
			cancel()
			return
		case <-gauges.C:
			p.updateGauges()
			continue
		case <-t.C:
		case <-p.flushNow:
		}
		p.flush(ctx, false)
	}
}

// flush sends everything pending and retries until Kafka accepts it, so a
// change is never dropped. While it retries the feed keeps being read and
// newer changes of the same fire replace older ones in the next batch.
//
// A snapshot is cut right after Drain, so it holds exactly what this batch
// leaves behind, and it is saved only once the batch is in Kafka.
func (p *Processor) flush(ctx context.Context, final bool) {
	p.mu.Lock()
	if !p.caughtUp {
		p.mu.Unlock()
		return
	}
	var batch []fire.Emit
	if p.Engine.PendingLen() > 0 {
		batch = p.Engine.Drain()
	}
	var snap *fire.Snapshot
	every := p.SnapshotEvery
	if every <= 0 {
		every = time.Minute
	}
	if p.Snapshots != nil && (final || time.Since(p.lastSnap) >= every) {
		p.lastSnap = time.Now()
		snap, _ = p.Engine.Snapshot(p.latest.Profile, p.lastSnap)
	}
	p.mu.Unlock()

	if len(batch) > 0 && !p.publish(ctx, batch) {
		return
	}
	if snap != nil {
		p.saveSnapshot(ctx, snap)
	}
}

func (p *Processor) saveSnapshot(ctx context.Context, snap *fire.Snapshot) {
	start := time.Now()
	n, err := p.Snapshots.Save(ctx, snap)
	if err != nil {
		snapshotErrors.Inc()
		p.Log.Warn("snapshot not saved; the next one is tried later", "error", err)
		return
	}
	snapshotOffset.Set(float64(snap.Next - 1))
	snapshotBytes.Set(float64(n))
	snapshotSeconds.Set(time.Since(start).Seconds())
	p.Log.Info("snapshot saved", "offset", snap.Next-1, "bytes", n, "seconds", time.Since(start).Seconds())
}

func (p *Processor) publish(ctx context.Context, batch []fire.Emit) bool {
	backoff := p.retryBase() / 5
	for {
		err := p.Pub.Publish(ctx, batch)
		if err == nil {
			break
		}
		publishErrors.Inc()
		p.Log.Warn("publish to hazard-upserts failed, retrying", "error", err, "messages", len(batch), "retry_in", backoff)
		if !sleep(ctx, backoff) {
			p.Log.Error("stopped with unsent changes; they are rebuilt from the feed on restart", "messages", len(batch))
			return false
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	now := time.Now()
	for _, em := range batch {
		published.WithLabelValues(em.Message.Op).Inc()
		emitDelay.Observe(now.Sub(em.ArrivedAt).Seconds())
	}
	return true
}

func (p *Processor) watchSession(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s, err := p.Feed.Session(ctx)
		if err != nil {
			continue
		}
		p.mu.Lock()
		if s.LatestOffset < p.latest.LatestOffset || s.Profile != p.latest.Profile {
			sessionResets.Inc()
			p.Log.Error("feed session was reset; restart with a higher VERSION_EPOCH so the new results replace the old ones",
				"old_profile", p.latest.Profile, "new_profile", s.Profile, "old_latest", p.latest.LatestOffset, "new_latest", s.LatestOffset)
		}
		p.latest = s
		p.mu.Unlock()
		latestOffset.Set(float64(s.LatestOffset))
	}
}

func (p *Processor) updateGauges() {
	p.mu.Lock()
	s := p.Engine.Summary()
	latest := p.latest
	p.mu.Unlock()

	lag := latest.LatestOffset - s.Offset
	lagEvents.Set(float64(max(lag, 0)))
	if !s.Watermark.IsZero() && !latest.VirtualNow.IsZero() {
		watermarkLag.Set(latest.VirtualNow.Sub(s.Watermark).Seconds())
		clockAhead.Set(s.MaxEventTime.Sub(latest.VirtualNow).Seconds())
	}
	detections.WithLabelValues("unique").Set(float64(s.Detections))
	detections.WithLabelValues("duplicate").Set(float64(s.Duplicates))
	detections.WithLabelValues("low_confidence").Set(float64(s.LowConfidence))
	detections.WithLabelValues("retracted").Set(float64(s.RetractedFeedRows))
	detections.WithLabelValues("late").Set(float64(s.Late))
	detections.WithLabelValues("single").Set(float64(s.SingleDetections))
	fires.WithLabelValues("open").Set(float64(s.FiresActive))
	fires.WithLabelValues("final").Set(float64(s.FiresFinal))
	fires.WithLabelValues("retracted").Set(float64(s.FiresRetracted))
	offsetGaps.Set(float64(s.Gaps))
	malformed.Set(float64(s.Malformed))
}

type Status struct {
	fire.Summary
	CaughtUp       bool    `json:"caught_up"`
	CatchUpTo      int64   `json:"catch_up_to"`
	RebuildSeconds float64 `json:"rebuild_seconds"`
	RestoredFrom   int64   `json:"restored_from_offset"`
	LatestOffset   int64   `json:"latest_offset"`
	LagEvents      int64   `json:"lag_events"`
	Pending        int     `json:"pending"`
	Profile        string  `json:"profile"`
	Speed          float64 `json:"speed"`
}

func (p *Processor) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.Engine.Summary()
	return Status{
		Summary: s, CaughtUp: p.caughtUp, CatchUpTo: p.catchUpTo, RebuildSeconds: p.rebuildDur.Seconds(), RestoredFrom: p.restoredAt,
		LatestOffset: p.latest.LatestOffset, LagEvents: max(p.latest.LatestOffset-s.Offset, 0),
		Pending: p.Engine.PendingLen(), Profile: p.latest.Profile, Speed: p.latest.Speed,
	}
}

func (p *Processor) Fires() ([]hazard.UpsertMessage, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f := p.Engine.Fires()
	return f, fire.Digest(f)
}

func (p *Processor) Late() []fire.LateDetection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Engine.Late()
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
