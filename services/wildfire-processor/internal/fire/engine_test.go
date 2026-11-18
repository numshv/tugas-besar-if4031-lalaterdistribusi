package fire

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
)

var t0 = time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC)

type feed struct{ events []satfeed.Event }

func (f *feed) add(typ, data string) {
	off := len(f.events)
	data = strings.Replace(data, "{", fmt.Sprintf(`{"offset":%d,`, off), 1)
	f.events = append(f.events, satfeed.Event{ID: fmt.Sprint(off), Type: typ, Data: []byte(data)})
}

func detKey(sat string, at time.Time, lat, lon float64) string {
	return fmt.Sprintf("%s|%s|%d|%.5f|%.5f", sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon)
}

func (f *feed) detect(sat string, at time.Time, lat, lon float64, conf string, frp float64) string {
	f.add(satfeed.TypeDetection, fmt.Sprintf(
		`{"detection":{"satellite":%q,"acq_date":%q,"acq_time":%d,"latitude":%.5f,"longitude":%.5f,"confidence":%q,"frp":%.2f}}`,
		sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon, conf, frp))
	return detKey(sat, at, lat, lon)
}

func (f *feed) update(key, changes string) {
	f.add(satfeed.TypeUpdate, fmt.Sprintf(`{"key":%q,"changes":%s}`, key, changes))
}
func (f *feed) retract(key string) {
	f.add(satfeed.TypeRetract, fmt.Sprintf(`{"key":%q,"reason":"false_positive"}`, key))
}
func (f *feed) heartbeat(now time.Time) {
	f.add(satfeed.TypeHeartbeat, fmt.Sprintf(`{"virtual_now":%q}`, now.Format(time.RFC3339)))
}

func run(t *testing.T, f *feed) *Engine {
	t.Helper()
	e := New(DefaultConfig())
	for _, ev := range f.events {
		if err := e.Apply(ev, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

// ~0.5 km apart at the equator
const step = 0.0045

func TestClustersFollowTheDefinition(t *testing.T) {
	f := &feed{}
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0.Add(40*time.Minute), -2.0, 113.0+step, "h", 7) // other satellite, same fire
	f.detect("N", t0.Add(80*time.Minute), -2.0, 113.0+2*step, "n", 1) // chain: 1 km from the first
	f.detect("N", t0, -2.0, 113.0+3*step, "l", 50)                    // low confidence, ignored
	f.detect("N20", t0, -3.0, 114.0, "n", 5)                          // single, not a fire
	f.detect("N20", t0.Add(7*time.Hour), -3.0, 114.0+step, "n", 5)    // 7 h later: not connected
	e := run(t, f)

	fires := e.Fires()
	if len(fires) != 1 {
		t.Fatalf("want 1 fire, got %d: %+v", len(fires), fires)
	}
	fr := fires[0]
	if fr.Attributes["detection_count"] != 3 || fr.Severity != hazard.SeverityWaspada || fr.AreaName != "Kalimantan Tengah" {
		t.Fatalf("unexpected fire %+v", fr)
	}
	if !reflect.DeepEqual(fr.Attributes["satellites"], []string{"N", "N20", "N21"}) || fr.Attributes["max_confidence"] != "h" {
		t.Fatalf("attributes %+v", fr.Attributes)
	}
	s := e.Summary()
	if s.SingleDetections != 2 || s.LowConfidence != 1 {
		t.Fatalf("summary %+v", s)
	}
}

func TestDuplicateDetectionCountsOnce(t *testing.T) {
	f := &feed{}
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0, -2.0, 113.0+step, "n", 5)
	e := run(t, f)
	if fires := e.Fires(); len(fires) != 1 || fires[0].Attributes["detection_count"] != 2 || e.Counters().Duplicates != 1 {
		t.Fatalf("fires %+v counters %+v", fires, e.Counters())
	}
}

func TestShrinkMergeAndRetractKeepIDs(t *testing.T) {
	f := &feed{}
	a1 := f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0, -2.0, 113.0+step, "n", 5)
	f.detect("N20", t0, -2.0, 113.0+4*step, "n", 5)
	f.detect("N21", t0, -2.0, 113.0+5*step, "n", 5)
	e := New(DefaultConfig())
	apply := func(from int) []Emit {
		for _, ev := range f.events[from:] {
			if err := e.Apply(ev, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		return e.Drain()
	}
	first := apply(0)
	if len(first) != 2 {
		t.Fatalf("want two fires, got %+v", first)
	}
	older := e.fireIDOfKey(a1)

	// A detection between the two fires merges them; the older ID survives.
	n := len(f.events)
	f.detect("N", t0.Add(30*time.Minute), -2.0, 113.0+2.5*step, "n", 5)
	merged := apply(n)
	ops := map[string]string{}
	for _, m := range merged {
		ops[m.Message.SourceRefID] = m.Message.Op
	}
	if len(ops) != 2 || ops[older] != hazard.OpUpsert {
		t.Fatalf("merge emits %+v", ops)
	}

	// Retracting the bridge splits them again: the younger fire comes back
	// under its own ID with a newer version, so the aggregator revives it.
	n = len(f.events)
	f.retract(detKey("N", t0.Add(30*time.Minute), -2.0, 113.0+2.5*step))
	split := apply(n)
	if len(split) != 2 {
		t.Fatalf("split emits %+v", split)
	}
	for _, m := range split {
		if m.Message.Op != hazard.OpUpsert || m.Message.Version != int64(n+1) {
			t.Fatalf("split emit %+v", m.Message)
		}
	}

	// A confidence drop leaves one detection: the fire is retracted, not deleted.
	n = len(f.events)
	f.update(a1, `{"confidence":"l"}`)
	gone := apply(n)
	if len(gone) != 1 || gone[0].Message.Op != hazard.OpRetract || gone[0].Message.SourceRefID != older {
		t.Fatalf("shrink emits %+v", gone)
	}
}

func (e *Engine) fireIDOfKey(key string) string { return e.fireID(e.dets[key].comp) }

func TestLateDetectionIsCountedNotApplied(t *testing.T) {
	f := &feed{}
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0.Add(10*time.Hour), -5.0, 120.0, "n", 5) // watermark -> t0+4h
	late := f.detect("N21", t0.Add(10*time.Minute), -2.0, 113.0+step, "n", 5)
	e := run(t, f)
	if len(e.Fires()) != 0 {
		t.Fatal("a late detection must not form a fire")
	}
	if l := e.Late(); len(l) != 1 || l[0].Key != late || !l[0].Watermark.Equal(t0.Add(4*time.Hour)) {
		t.Fatalf("late list %+v", l)
	}
}

func TestAheadClockDoesNotPushWatermark(t *testing.T) {
	f := &feed{}
	f.heartbeat(t0.Add(time.Hour))
	f.detect("N", t0.Add(20*time.Hour), -5.0, 120.0, "n", 5) // clock far ahead of the feed
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0, -2.0, 113.0+step, "n", 5)
	e := run(t, f)
	if !e.Watermark().Equal(t0.Add(-5*time.Hour)) || e.Counters().Late != 0 || len(e.Fires()) != 1 {
		t.Fatalf("watermark %s counters %+v fires %d", e.Watermark(), e.Counters(), len(e.Fires()))
	}
}

func TestFireBecomesFinalWhenWatermarkPassesIt(t *testing.T) {
	f := &feed{}
	f.detect("N20", t0, -2.0, 113.0, "n", 5)
	f.detect("N21", t0, -2.0, 113.0+step, "n", 5)
	f.detect("N", t0.Add(13*time.Hour), -5.0, 120.0, "n", 5)
	e := run(t, f)
	if fr := e.Fires(); len(fr) != 1 || fr[0].Attributes["final"] != true {
		t.Fatalf("fires %+v", fr)
	}
}

// --- randomized: crash recovery and the brute-force reference ---

func randomFeed(seed int64, n int) *feed {
	r := rand.New(rand.NewSource(seed))
	type pending struct {
		at            time.Time
		arrive        time.Time
		sat           string
		lat, lon, frp float64
		conf          string
	}
	var dets []pending
	for len(dets) < n {
		lat, lon := -8+r.Float64()*13, 95+r.Float64()*45
		base := t0.Add(time.Duration(r.Intn(48*60)) * time.Minute)
		for k := 1 + r.Intn(8); k > 0; k-- {
			at := base.Add(time.Duration(r.Intn(400)) * time.Minute)
			delay := time.Duration(r.Intn(300)) * time.Minute
			if r.Intn(40) == 0 {
				delay += 8 * time.Hour // beyond the allowed lateness
			}
			conf := []string{"n", "n", "n", "h", "l"}[r.Intn(5)]
			dets = append(dets, pending{at, at.Add(delay), []string{"N", "N20", "N21"}[r.Intn(3)],
				lat + (r.Float64()-0.5)*0.015, lon + (r.Float64()-0.5)*0.015, r.Float64() * 20, conf})
		}
	}
	sort.SliceStable(dets, func(i, j int) bool { return dets[i].arrive.Before(dets[j].arrive) })

	f := &feed{}
	var keys []string
	lastBeat := time.Time{}
	for i, d := range dets {
		if d.arrive.Sub(lastBeat) > 30*time.Minute {
			f.heartbeat(d.arrive)
			lastBeat = d.arrive
		}
		keys = append(keys, f.detect(d.sat, d.at, d.lat, d.lon, d.conf, d.frp))
		if i%7 == 0 {
			f.detect(d.sat, d.at, d.lat, d.lon, d.conf, d.frp) // redelivered duplicate
		}
		if len(keys) > 5 && r.Intn(6) == 0 {
			k := keys[r.Intn(len(keys))]
			switch r.Intn(3) {
			case 0:
				f.retract(k)
			case 1:
				f.update(k, fmt.Sprintf(`{"confidence":%q}`, []string{"l", "n", "h"}[r.Intn(3)]))
			default:
				f.update(k, fmt.Sprintf(`{"frp":%.2f}`, r.Float64()*40))
			}
		}
	}
	return f
}

type stored struct {
	msg       hazard.UpsertMessage
	retracted bool
}

// versionedStore behaves like the aggregator: a message is applied only when
// its version is newer than the stored one.
type versionedStore map[string]stored

func (s versionedStore) apply(emits []Emit) {
	for _, em := range emits {
		m := em.Message
		cur, ok := s[m.SourceRefID]
		if ok && cur.msg.Version >= m.Version {
			continue
		}
		if m.Op == hazard.OpRetract {
			if ok {
				cur.retracted, cur.msg.Version = true, m.Version
				s[m.SourceRefID] = cur
			}
			continue
		}
		s[m.SourceRefID] = stored{msg: m}
	}
}

func (s versionedStore) live() map[string]stored {
	out := map[string]stored{}
	for id, v := range s {
		if !v.retracted {
			out[id] = v
		}
	}
	return out
}

func TestCrashRecoveryGivesTheSameResult(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		f := randomFeed(seed, 400)
		r := rand.New(rand.NewSource(seed * 100))

		clean := versionedStore{}
		e := New(DefaultConfig())
		for _, ev := range f.events {
			_ = e.Apply(ev, time.Now())
			if r.Intn(25) == 0 {
				clean.apply(e.Drain())
			}
		}
		clean.apply(e.Drain())

		// The processor crashes twice, losing whatever it had not sent, and
		// each new instance rebuilds from offset 0 before sending anything.
		crashed := versionedStore{}
		start := 0
		for _, crashAt := range []int{r.Intn(len(f.events)), len(f.events)} {
			e := New(DefaultConfig())
			for i, ev := range f.events[:crashAt] {
				_ = e.Apply(ev, time.Now())
				if i >= start && r.Intn(25) == 0 {
					crashed.apply(e.Drain())
				}
			}
			if crashAt == len(f.events) {
				crashed.apply(e.Drain())
			}
			start = crashAt
		}

		// A fire born and gone between two sends is never stored in one run
		// and stored-then-retracted in the other; both hide it, so compare
		// what is visible.
		if !reflect.DeepEqual(clean.live(), crashed.live()) {
			t.Fatalf("seed %d: results differ after crash\nclean   %d fires\ncrashed %d fires", seed, len(clean), len(crashed))
		}
		live := 0
		for id, s := range clean {
			if !s.retracted {
				live++
				if want := e.fireIDOfKeys(s.msg); want != id {
					t.Fatalf("seed %d: stored fire %s is not current", seed, id)
				}
			}
		}
		if live != len(e.Fires()) {
			t.Fatalf("seed %d: store has %d live fires, engine %d", seed, live, len(e.Fires()))
		}
	}
}

func (e *Engine) fireIDOfKeys(m hazard.UpsertMessage) string {
	keys := m.Attributes["detection_keys"].([]string)
	return e.fireIDOfKey(keys[0])
}

func TestMatchesBruteForceReference(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		f := randomFeed(seed, 500)
		e := run(t, f)

		got, want := e.Clusters(), e.ReferenceClusters()
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("seed %d: streaming %d fires, brute force %d", seed, len(got), len(want))
		}
	}
}
