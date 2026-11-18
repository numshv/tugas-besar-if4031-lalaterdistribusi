package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/feed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/snapshot"
)

// fakeFeed serves a fixed list of events, closes every connection after a
// few events like the real server, and sometimes answers 429.
type fakeFeed struct {
	events []satfeed.Event
	chunk  int
	mu     sync.Mutex
	r      *rand.Rand
	opened int
}

func (f *fakeFeed) Session(context.Context) (satfeed.Session, error) {
	return satfeed.Session{Profile: "practice", LatestOffset: int64(len(f.events) - 1)}, nil
}

func (f *fakeFeed) Stream(ctx context.Context, from int64, fn func(satfeed.Event) error) error {
	f.mu.Lock()
	f.opened++
	busy := f.r.Intn(5) == 0
	f.mu.Unlock()
	if busy {
		return &feed.StatusError{Code: 429, Body: "too_many_connections"}
	}
	if from >= int64(len(f.events)) {
		<-ctx.Done()
		return ctx.Err()
	}
	end := min(int(from)+f.chunk, len(f.events))
	for _, ev := range f.events[from:end] {
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

type store struct {
	mu   sync.Mutex
	docs map[string]hazard.UpsertMessage
	dead map[string]bool
	r    *rand.Rand
	sent int
}

func (s *store) Publish(_ context.Context, emits []fire.Emit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.r.Intn(4) == 0 {
		return errors.New("kafka: not enough replicas")
	}
	for _, em := range emits {
		m := em.Message
		if cur, ok := s.docs[m.SourceRefID]; ok && cur.Version >= m.Version {
			continue
		}
		s.sent++
		if m.Op == hazard.OpRetract {
			if cur, ok := s.docs[m.SourceRefID]; ok {
				cur.Version = m.Version
				s.docs[m.SourceRefID] = cur
				s.dead[m.SourceRefID] = true
			}
			continue
		}
		s.docs[m.SourceRefID] = m
		delete(s.dead, m.SourceRefID)
	}
	return nil
}

func (s *store) live() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for id, m := range s.docs {
		if !s.dead[id] {
			b, _ := json.Marshal(m.Attributes["detection_keys"])
			out[id] = string(b)
		}
	}
	return out
}

func syntheticEvents(seed int64, n int) []satfeed.Event {
	r := rand.New(rand.NewSource(seed))
	t0 := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	var evs []satfeed.Event
	var keys []string
	add := func(typ, body string) {
		off := len(evs)
		evs = append(evs, satfeed.Event{ID: fmt.Sprint(off), Type: typ, Data: []byte(fmt.Sprintf(`{"offset":%d,%s}`, off, body))})
	}
	for i := 0; i < n; i++ {
		at := t0.Add(time.Duration(i*3+r.Intn(120)) * time.Minute)
		lat, lon := -2+float64(r.Intn(6))*0.5+r.Float64()*0.01, 110+float64(r.Intn(6))*0.5+r.Float64()*0.01
		sat := []string{"N", "N20", "N21"}[r.Intn(3)]
		add(satfeed.TypeDetection, fmt.Sprintf(`"detection":{"satellite":%q,"acq_date":%q,"acq_time":%d,"latitude":%.5f,"longitude":%.5f,"confidence":"n","frp":3.5}`,
			sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon))
		keys = append(keys, fmt.Sprintf("%s|%s|%d|%.5f|%.5f", sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon))
		if r.Intn(8) == 0 {
			add(satfeed.TypeRetract, fmt.Sprintf(`"key":%q,"reason":"false_positive"`, keys[r.Intn(len(keys))]))
		}
	}
	return evs
}

func expected(events []satfeed.Event) map[string]string {
	e := fire.New(fire.DefaultConfig())
	for _, ev := range events {
		_ = e.Apply(ev, time.Now())
	}
	out := map[string]string{}
	for _, f := range e.Fires() {
		b, _ := json.Marshal(f.Attributes["detection_keys"])
		out[f.SourceRefID] = string(b)
	}
	return out
}

func newProcessor(f *fakeFeed, s *store) *Processor {
	return &Processor{
		Engine: fire.New(fire.DefaultConfig()), Feed: f, Pub: s,
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		FlushInterval: 5 * time.Millisecond, FlushEvents: 20, RetryBase: 2 * time.Millisecond,
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReconnectsAndKafkaErrorsLoseNothing(t *testing.T) {
	events := syntheticEvents(7, 600)
	want := expected(events)
	f := &fakeFeed{events: events, chunk: 37, r: rand.New(rand.NewSource(1))}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(2))}
	p := newProcessor(f, s)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), want) })
	cancel()
	<-done
	if st := p.Status(); st.Gaps != 0 || !st.CaughtUp || len(want) == 0 {
		t.Fatalf("status %+v, %d fires", st, len(want))
	}
}

func TestNothingIsSentBeforeTheRebuildCatchesUp(t *testing.T) {
	events := syntheticEvents(8, 300)
	f := &fakeFeed{events: events, chunk: 10, r: rand.New(rand.NewSource(3))}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(4))}
	p := newProcessor(f, s)
	p.FlushEvents = 1
	p.Pub = publishFunc(func(ctx context.Context, emits []fire.Emit) error {
		if st := p.Status(); !st.CaughtUp {
			t.Errorf("sent %d messages while rebuilding at offset %d", len(emits), st.Offset)
		}
		return s.Publish(ctx, emits)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(events)) })
	cancel()
	<-done
}

func TestRestartedProcessorConvergesToTheSameFires(t *testing.T) {
	events := syntheticEvents(9, 600)
	want := expected(events)
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(5))}

	half := &fakeFeed{events: events[:300], chunk: 23, r: rand.New(rand.NewSource(6))}
	ctx, cancel := context.WithCancel(context.Background())
	p1 := newProcessor(half, s)
	done := make(chan struct{})
	go func() { _ = p1.Run(ctx); close(done) }()
	waitFor(t, func() bool { return p1.Status().CaughtUp })
	cancel()
	<-done

	// The replacement rebuilds from offset 0 over the full feed.
	full := &fakeFeed{events: events, chunk: 23, r: rand.New(rand.NewSource(7))}
	ctx, cancel = context.WithCancel(context.Background())
	p2 := newProcessor(full, s)
	done = make(chan struct{})
	go func() { _ = p2.Run(ctx); close(done) }()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), want) })
	cancel()
	<-done
}

type memSnapshots struct {
	mu    sync.Mutex
	b     []byte
	saves int
}

func (m *memSnapshots) Save(_ context.Context, s *fire.Snapshot) (int, error) {
	b, err := snapshot.Encode(s)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.b, m.saves = b, m.saves+1
	return len(b), nil
}

func (m *memSnapshots) Load(context.Context) (*fire.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.b == nil {
		return nil, nil
	}
	return snapshot.Decode(m.b)
}

func TestRestartResumesFromSnapshot(t *testing.T) {
	events := syntheticEvents(11, 600)
	want := expected(events)
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(8))}
	snaps := &memSnapshots{}

	half := &fakeFeed{events: events[:350], chunk: 29, r: rand.New(rand.NewSource(9))}
	p1 := newProcessor(half, s)
	p1.Snapshots, p1.SnapshotEvery = snaps, 10*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p1.Run(ctx); close(done) }()
	waitFor(t, func() bool { snaps.mu.Lock(); defer snaps.mu.Unlock(); return snaps.saves >= 2 })
	cancel()
	<-done

	full := &fakeFeed{events: events, chunk: 29, r: rand.New(rand.NewSource(10))}
	p2 := newProcessor(full, s)
	p2.Snapshots, p2.SnapshotEvery = snaps, time.Hour
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() { _ = p2.Run(ctx); close(done) }()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), want) })
	st := p2.Status()
	cancel()
	<-done
	if st.RestoredFrom < 300 {
		t.Fatalf("restarted from offset %d, want the snapshot near 349", st.RestoredFrom)
	}
}

func TestSnapshotFromAnotherSessionIsIgnored(t *testing.T) {
	events := syntheticEvents(12, 200)
	snaps := &memSnapshots{}
	e := fire.New(fire.DefaultConfig())
	for _, ev := range events {
		_ = e.Apply(ev, time.Now())
	}
	e.Drain()
	old, _ := e.Snapshot("other-profile", time.Now())
	_, _ = snaps.Save(context.Background(), old)

	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(1))}
	p := newProcessor(&fakeFeed{events: events, chunk: 50, r: rand.New(rand.NewSource(2))}, s)
	p.Snapshots = snaps
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	waitFor(t, func() bool { return p.Status().CaughtUp })
	st := p.Status()
	cancel()
	<-done
	if st.RestoredFrom != 0 {
		t.Fatalf("used a snapshot from another session (offset %d)", st.RestoredFrom)
	}
}

type publishFunc func(ctx context.Context, emits []fire.Emit) error

func (f publishFunc) Publish(ctx context.Context, emits []fire.Emit) error { return f(ctx, emits) }
