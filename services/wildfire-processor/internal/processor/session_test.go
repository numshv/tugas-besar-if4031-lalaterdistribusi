package processor

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/feed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
)

// sessionFeed serves one feed session at a time and can switch to the next,
// the way POST /v1/exam/start is expected to replace the practice session.
type sessionFeed struct {
	mu       sync.Mutex
	sessions [][]satfeed.Event
	profiles []string
	cur      int
	chunk    int
	// blips makes /v1/session report a smaller latest_offset this many times.
	blips int
}

func (f *sessionFeed) current() ([]satfeed.Event, string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions[f.cur], f.profiles[f.cur], f.cur
}

func (f *sessionFeed) next() {
	f.mu.Lock()
	f.cur++
	f.mu.Unlock()
}

func (f *sessionFeed) Session(context.Context) (satfeed.Session, error) {
	evs, profile, _ := f.current()
	f.mu.Lock()
	defer f.mu.Unlock()
	latest := int64(len(evs) - 1)
	if f.blips > 0 {
		f.blips--
		latest = 3
	}
	return satfeed.Session{Profile: profile, LatestOffset: latest}, nil
}

func (f *sessionFeed) Stream(ctx context.Context, from int64, fn func(satfeed.Event) error) error {
	evs, _, cur := f.current()
	if from-1 > int64(len(evs)-1) {
		return &feed.StatusError{Code: 400, Body: "offset beyond latest"}
	}
	if from >= int64(len(evs)) {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
			if _, _, now := f.current(); now != cur {
				return nil
			}
		}
	}
	end := min(int(from)+f.chunk, len(evs))
	for _, ev := range evs[from:end] {
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

// fireEvents builds a session whose fires sit around (lat, lon).
func fireEvents(seed int64, n int, lat, lon float64) []satfeed.Event {
	r := rand.New(rand.NewSource(seed))
	t0 := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	var evs []satfeed.Event
	for i := 0; i < n; i++ {
		at := t0.Add(time.Duration(i*3) * time.Minute)
		la, lo := lat+float64(r.Intn(4))*0.5+r.Float64()*0.005, lon+float64(r.Intn(4))*0.5+r.Float64()*0.005
		evs = append(evs, satfeed.Event{ID: fmt.Sprint(i), Type: satfeed.TypeDetection, Data: []byte(fmt.Sprintf(
			`{"offset":%d,"detection":{"satellite":"N20","acq_date":%q,"acq_time":%d,"latitude":%.5f,"longitude":%.5f,"confidence":"n","frp":3}}`,
			i, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), la, lo))})
	}
	return evs
}

func newSessionProcessor(f *sessionFeed, s *store) *Processor {
	p := newProcessor(nil, s)
	p.Feed = f
	p.SessionEvery = 10 * time.Millisecond
	return p
}

func start(t *testing.T, p *Processor) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func TestNewSessionRetractsOldFiresAndWinsOnVersion(t *testing.T) {
	practice := fireEvents(1, 300, -2, 110)
	exam := fireEvents(2, 120, -6, 120) // other place, so no fire ID is shared
	f := &sessionFeed{sessions: [][]satfeed.Event{practice, exam}, profiles: []string{"practice", "exam"}, chunk: 40}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(3))}
	p := newSessionProcessor(f, s)
	stop := start(t, p)
	defer stop()

	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(practice)) })
	epochBefore := p.Status().Epoch
	time.Sleep(1100 * time.Millisecond) // the next epoch is a later second
	f.next()

	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(exam)) })
	st := p.Status()
	if st.Epoch <= epochBefore || st.Profile != "exam" || st.Gaps != 0 {
		t.Fatalf("after the switch: epoch %d (was %d), profile %q, gaps %d", st.Epoch, epochBefore, st.Profile, st.Gaps)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range expected(practice) {
		if !s.dead[id] {
			t.Fatalf("practice fire %s still live", id)
		}
	}
}

func TestSameProfileWithFewerEventsIsANewSession(t *testing.T) {
	first := fireEvents(4, 300, -2, 110)
	second := fireEvents(5, 60, -6, 120)
	f := &sessionFeed{sessions: [][]satfeed.Event{first, second}, profiles: []string{"practice", "practice"}, chunk: 50}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(6))}
	p := newSessionProcessor(f, s)
	stop := start(t, p)
	defer stop()

	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(first)) })
	time.Sleep(1100 * time.Millisecond)
	f.next()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(second)) })
}

func TestOneOddSessionAnswerDoesNotThrowFiresAway(t *testing.T) {
	evs := fireEvents(7, 200, -2, 110)
	f := &sessionFeed{sessions: [][]satfeed.Event{evs}, profiles: []string{"practice"}, chunk: 50}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(8))}
	p := newSessionProcessor(f, s)
	stop := start(t, p)
	defer stop()

	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(evs)) })
	epoch := p.Status().Epoch
	f.mu.Lock()
	f.blips = 1
	f.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	if st := p.Status(); st.Epoch != epoch || !reflect.DeepEqual(s.live(), expected(evs)) {
		t.Fatalf("a single odd answer replaced the session (epoch %d -> %d)", epoch, st.Epoch)
	}
}

func TestRestartInTheSameSessionKeepsTheEpoch(t *testing.T) {
	practice := fireEvents(9, 200, -2, 110)
	exam := fireEvents(10, 80, -6, 120)
	f := &sessionFeed{sessions: [][]satfeed.Event{practice, exam}, profiles: []string{"practice", "exam"}, chunk: 40}
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(11))}
	snaps := &memSnapshots{}

	p1 := newSessionProcessor(f, s)
	p1.Snapshots = snaps
	stop := start(t, p1)
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(practice)) })
	time.Sleep(1100 * time.Millisecond)
	f.next()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(exam)) })
	epoch := p1.Status().Epoch
	stop()

	time.Sleep(1100 * time.Millisecond) // a fresh epoch would now be a later second
	p2 := newSessionProcessor(f, s)
	p2.Snapshots = snaps
	stop = start(t, p2)
	defer stop()
	waitFor(t, func() bool { return p2.Status().CaughtUp })
	if got := p2.Status().Epoch; got != epoch {
		t.Fatalf("restart in the same session changed the epoch: %d -> %d", epoch, got)
	}
}

func TestSessionChangedWhileDownRetractsOldFires(t *testing.T) {
	practice := fireEvents(12, 200, -2, 110)
	exam := fireEvents(13, 80, -6, 120)
	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(14))}
	snaps := &memSnapshots{}

	f := &sessionFeed{sessions: [][]satfeed.Event{practice, exam}, profiles: []string{"practice", "exam"}, chunk: 40}
	p1 := newSessionProcessor(f, s)
	p1.Snapshots = snaps
	stop := start(t, p1)
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(practice)) })
	stop() // graceful stop saves a snapshot of the practice session

	f.next() // the exam starts while nothing is running
	p2 := newSessionProcessor(f, s)
	p2.Snapshots = snaps
	stop = start(t, p2)
	defer stop()
	waitFor(t, func() bool { return reflect.DeepEqual(s.live(), expected(exam)) })
}

func TestEpochStaysAboveTheLastSnapshot(t *testing.T) {
	evs := fireEvents(15, 50, -2, 110)
	f := &sessionFeed{sessions: [][]satfeed.Event{evs}, profiles: []string{"exam"}, chunk: 50}
	snaps := &memSnapshots{}
	// A snapshot from another session whose epoch is ahead of this node's clock,
	// as if the clock was moved back after it was taken.
	cfg := fire.DefaultConfig()
	cfg.VersionEpoch = time.Now().Unix() + 1_000_000
	old := fire.New(cfg)
	snap, _ := old.Snapshot("practice", time.Now())
	_, _ = snaps.Save(context.Background(), snap)

	s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(16))}
	p := newSessionProcessor(f, s)
	p.Snapshots = snaps
	stop := start(t, p)
	defer stop()
	waitFor(t, func() bool { return p.Status().CaughtUp })
	if got := p.Status().Epoch; got <= cfg.VersionEpoch {
		t.Fatalf("epoch %d is not above the snapshot's %d", got, cfg.VersionEpoch)
	}
}

func TestVersionEpochSettingOnlyRaises(t *testing.T) {
	evs := fireEvents(17, 50, -2, 110)
	for _, tc := range []struct {
		name       string
		configured int64
		wantRaise  bool
	}{{"lower is ignored", 1, false}, {"higher wins", time.Now().Unix() + 5_000_000, true}} {
		f := &sessionFeed{sessions: [][]satfeed.Event{evs}, profiles: []string{"practice"}, chunk: 50}
		snaps := &memSnapshots{}
		cfg := fire.DefaultConfig()
		cfg.VersionEpoch = time.Now().Unix()
		base := fire.New(cfg)
		snap, _ := base.Snapshot("practice", time.Now())
		_, _ = snaps.Save(context.Background(), snap)

		s := &store{docs: map[string]hazard.UpsertMessage{}, dead: map[string]bool{}, r: rand.New(rand.NewSource(18))}
		p := newSessionProcessor(f, s)
		p.Snapshots = snaps
		pcfg := fire.DefaultConfig()
		pcfg.VersionEpoch = tc.configured
		p.Engine = fire.New(pcfg)
		stop := start(t, p)
		waitFor(t, func() bool { return p.Status().CaughtUp })
		got := p.Status().Epoch
		stop()
		want := cfg.VersionEpoch
		if tc.wantRaise {
			want = tc.configured
		}
		if got != want {
			t.Fatalf("%s: epoch %d, want %d", tc.name, got, want)
		}
	}
}
