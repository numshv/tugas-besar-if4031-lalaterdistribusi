package poller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type leadership struct{ epoch atomic.Int64 }

func (l *leadership) Confirm(context.Context) (int64, bool) {
	e := l.epoch.Load()
	return e, e > 0
}

func newTestPoller(t *testing.T) (*Poller, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Write([]byte(`[]`))
	}))
	t.Cleanup(upstream.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	src := SourceClient{HTTP: httpx.NewClient("test", time.Second, log), BaseURL: upstream.URL}
	noop := func(context.Context, pipeline.Response) error { return nil }
	p := New(log, 20*time.Millisecond, time.Second, src, src,
		func(context.Context, pipeline.Response, pipeline.Response) error { return nil }, noop)
	return p, &hits
}

func leadFor(p *Poller, d time.Duration, epoch int64, lead Leadership, cps Checkpoints) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	p.Lead(ctx, hazard.SourcePVMBG, epoch, lead, cps)
}

func TestLeadPollsAndStoresCheckpointWhileLeader(t *testing.T) {
	p, hits := newTestPoller(t)
	cps := store.NewMemory()
	lead := &leadership{}
	lead.epoch.Store(3)

	leadFor(p, 150*time.Millisecond, 3, lead, cps)

	if hits.Load() == 0 {
		t.Fatal("leader never polled upstream")
	}
	cursor, epoch, _ := cps.LoadCheckpoint(context.Background(), hazard.SourcePVMBG)
	if cursor == "" || epoch != 3 {
		t.Fatalf("checkpoint = (%q, %d), want a cursor at epoch 3", cursor, epoch)
	}
}

func TestLeadStopsBeforeUpstreamWhenLeaseIsGone(t *testing.T) {
	p, hits := newTestPoller(t)
	lead := &leadership{} // Confirm fails: e.g. the pod woke up from a freeze

	leadFor(p, 150*time.Millisecond, 3, lead, store.NewMemory())

	if n := hits.Load(); n != 0 {
		t.Fatalf("a pod without the lease sent %d upstream requests", n)
	}
}

func TestLeadOfOlderTermIsFencedOut(t *testing.T) {
	p, hits := newTestPoller(t)
	cps := store.NewMemory()
	if _, err := cps.CommitPoll(context.Background(), hazard.SourcePVMBG, 5, "", nil); err != nil {
		t.Fatal(err)
	}
	lead := &leadership{}
	lead.epoch.Store(4) // still believes it leads, but epoch 5 already wrote

	leadFor(p, 150*time.Millisecond, 4, lead, cps)

	if n := hits.Load(); n != 0 {
		t.Fatalf("stale leader sent %d upstream requests", n)
	}
	if _, epoch, _ := cps.LoadCheckpoint(context.Background(), hazard.SourcePVMBG); epoch != 5 {
		t.Fatalf("fence moved back to %d", epoch)
	}
}
