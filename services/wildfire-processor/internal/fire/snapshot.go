package fire

import (
	"errors"
	"sort"
	"time"
)

// SnapshotFormat changes whenever the Snapshot layout does; an older snapshot
// is then ignored and the engine replays the feed from offset 0 instead.
const SnapshotFormat = 1

// Snapshot is the complete engine state after some feed offset. Restoring it
// and applying the rest of the feed gives exactly what a full replay gives:
// components are rebuilt from the active detections, and the connected
// components of a set of detections do not depend on the order they came in.
type Snapshot struct {
	Format       int
	Config       Config
	Next         int64
	MaxEventTime time.Time
	VirtualNow   time.Time
	Watermark    time.Time
	Counters     Counters
	Late         []LateDetection
	Detections   []SnapshotDetection
	Emitted      []SnapshotEmitted
	Profile      string
	TakenAt      time.Time
}

type SnapshotDetection struct {
	Key       string
	Satellite string
	At        time.Time
	Lat, Lon  float64
	Conf      string
	FRP       float64
	Offset    int64
	Retracted bool
	Late      bool
}

type SnapshotEmitted struct {
	ID    string
	Hash  string
	Alive bool
}

var ErrPending = errors.New("snapshot taken with unsent changes")

// Snapshot copies the state. It must be taken right after Drain, when nothing
// is pending, so that everything before Next has already been handed over.
func (e *Engine) Snapshot(profile string, now time.Time) (*Snapshot, error) {
	if len(e.pending) > 0 {
		return nil, ErrPending
	}
	s := &Snapshot{
		Format: SnapshotFormat, Config: e.cfg, Next: e.next,
		MaxEventTime: e.maxEventTime, VirtualNow: e.virtualNow, Watermark: e.watermark,
		Counters: e.counters, Late: append([]LateDetection(nil), e.late...),
		Detections: make([]SnapshotDetection, 0, len(e.dets)),
		Emitted:    make([]SnapshotEmitted, 0, len(e.state)),
		Profile:    profile, TakenAt: now,
	}
	for _, d := range e.dets {
		s.Detections = append(s.Detections, SnapshotDetection{
			Key: d.key, Satellite: d.satellite, At: d.at, Lat: d.lat, Lon: d.lon,
			Conf: d.conf, FRP: d.frp, Offset: d.offset, Retracted: d.retracted, Late: d.late,
		})
	}
	sort.Slice(s.Detections, func(i, j int) bool { return s.Detections[i].Offset < s.Detections[j].Offset })
	for id, st := range e.state {
		s.Emitted = append(s.Emitted, SnapshotEmitted{ID: id, Hash: st.hash, Alive: st.alive})
	}
	sort.Slice(s.Emitted, func(i, j int) bool { return s.Emitted[i].ID < s.Emitted[j].ID })
	return s, nil
}

func Restore(cfg Config, s *Snapshot) (*Engine, error) {
	if s.Format != SnapshotFormat {
		return nil, errors.New("snapshot format differs")
	}
	if s.Config != cfg {
		return nil, errors.New("snapshot was taken with a different config")
	}
	e := New(cfg)
	e.next, e.maxEventTime, e.virtualNow, e.watermark = s.Next, s.MaxEventTime, s.VirtualNow, s.Watermark
	e.counters = s.Counters
	e.late = append([]LateDetection(nil), s.Late...)
	for _, sd := range s.Detections {
		d := &detection{
			key: sd.Key, satellite: sd.Satellite, at: sd.At, lat: sd.Lat, lon: sd.Lon,
			conf: sd.Conf, frp: sd.FRP, offset: sd.Offset, retracted: sd.Retracted, late: sd.Late,
		}
		e.dets[d.key] = d
		if d.active() {
			e.addActive(d)
		}
	}
	for _, se := range s.Emitted {
		e.state[se.ID] = &emitted{hash: se.Hash, alive: se.Alive}
	}
	for c := range e.comps {
		if e.fireID(c) != "" && !e.final(c) {
			e.open[c] = struct{}{}
		}
	}
	clear(e.touched)
	clear(e.dirty)
	return e, nil
}
