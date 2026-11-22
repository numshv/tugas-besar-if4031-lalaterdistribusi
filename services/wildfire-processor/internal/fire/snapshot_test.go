package fire

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func roundTrip(t *testing.T, s *Snapshot) *Snapshot {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		t.Fatal(err)
	}
	var out Snapshot
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func TestRestoredSnapshotContinuesLikeAFullReplay(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		f := randomFeed(seed, 400)
		r := rand.New(rand.NewSource(seed * 7))

		full := versionedStore{}
		a := New(DefaultConfig())
		for _, ev := range f.events {
			_ = a.Apply(ev, time.Now())
			if r.Intn(25) == 0 {
				full.apply(a.Drain())
			}
		}
		full.apply(a.Drain())

		cut := r.Intn(len(f.events))
		resumed := versionedStore{}
		b := New(DefaultConfig())
		for _, ev := range f.events[:cut] {
			_ = b.Apply(ev, time.Now())
			if r.Intn(25) == 0 {
				resumed.apply(b.Drain())
			}
		}
		resumed.apply(b.Drain())
		snap, err := b.Snapshot("practice", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		c, err := Restore(DefaultConfig(), roundTrip(t, snap))
		if err != nil {
			t.Fatal(err)
		}
		if c.Next() != int64(cut) {
			t.Fatalf("seed %d: resumes at %d, want %d", seed, c.Next(), cut)
		}
		for _, ev := range f.events[cut:] {
			_ = c.Apply(ev, time.Now())
			if r.Intn(25) == 0 {
				resumed.apply(c.Drain())
			}
		}
		resumed.apply(c.Drain())

		if !reflect.DeepEqual(full.live(), resumed.live()) {
			t.Fatalf("seed %d: stored fires differ after restoring at offset %d", seed, cut)
		}
		if Digest(a.Fires()) != Digest(c.Fires()) {
			t.Fatalf("seed %d: engine fires differ", seed)
		}
		sa, sc := a.Summary(), c.Summary()
		if sa != sc {
			t.Fatalf("seed %d: summary differs\nfull     %+v\nrestored %+v", seed, sa, sc)
		}
	}
}

func TestSnapshotRefusesPendingAndOtherConfig(t *testing.T) {
	f := randomFeed(3, 50)
	e := run(t, f)
	if _, err := e.Snapshot("p", time.Now()); err != ErrPending {
		t.Fatalf("want ErrPending, got %v", err)
	}
	e.Drain()
	s, err := e.Snapshot("p", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	other := DefaultConfig()
	other.AllowedLateness = time.Hour
	if _, err := Restore(other, s); err == nil {
		t.Fatal("a snapshot from another config must not be used")
	}
}

func TestEpochVersionsRetractAllAndRestore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.VersionEpoch = 1_763_000_000
	e := New(cfg)
	for _, ev := range randomFeed(5, 60).events {
		_ = e.Apply(ev, time.Now())
	}
	live := 0
	for _, m := range e.Drain() {
		if m.Message.Version <= EpochBase(cfg.VersionEpoch) {
			t.Fatalf("version %d is not inside epoch %d", m.Message.Version, cfg.VersionEpoch)
		}
	}
	for _, st := range e.state {
		if st.alive {
			live++
		}
	}
	s, err := e.Snapshot("practice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A restored engine keeps the snapshot's epoch, whatever the caller asks.
	other := DefaultConfig()
	other.VersionEpoch = 1_799_999_999
	r, err := Restore(other, roundTrip(t, s))
	if err != nil || r.Epoch() != cfg.VersionEpoch {
		t.Fatalf("restored epoch %d, err %v", r.Epoch(), err)
	}

	next := EpochBase(cfg.VersionEpoch + 10)
	out := e.RetractAll(next, time.Now())
	if len(out) != live || live == 0 {
		t.Fatalf("retracted %d of %d live fires", len(out), live)
	}
	for _, m := range out {
		if m.Message.Op != "retract" || m.Message.Version != next {
			t.Fatalf("unexpected %+v", m.Message)
		}
	}
	if len(e.Fires()) == 0 || len(e.RetractAll(next, time.Now())) != 0 {
		t.Fatal("a second RetractAll must send nothing")
	}
}
