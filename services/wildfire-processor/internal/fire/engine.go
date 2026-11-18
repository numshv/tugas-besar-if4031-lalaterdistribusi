// Package fire turns the Feed Satelit stream into wildfire HazardEvents.
//
// The engine is a pure function of the feed prefix it has applied: the same

package fire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
)

type Config struct {
	MaxDistanceKM   float64
	MaxGap          time.Duration
	AllowedLateness time.Duration
	MinDetections   int
	// VersionEpoch is bumped when the feed session is reset, so versions from
	// the new session are newer than everything stored from the old one.
	VersionEpoch int64
}

func DefaultConfig() Config {
	return Config{MaxDistanceKM: 1.0, MaxGap: 360 * time.Minute, AllowedLateness: 360 * time.Minute, MinDetections: 2}
}

type Emit struct {
	Message hazard.UpsertMessage
	// ArrivedAt is when the oldest feed event behind this message reached us.
	ArrivedAt time.Time
}

type LateDetection struct {
	Offset     int64     `json:"offset"`
	Key        string    `json:"key"`
	DetectedAt time.Time `json:"detected_at"`
	Watermark  time.Time `json:"watermark"`
}

type Counters struct {
	Events      int64 `json:"events"`
	Detections  int64 `json:"detections"`
	Duplicates  int64 `json:"duplicates"`
	Late        int64 `json:"late"`
	Updates     int64 `json:"updates"`
	Retracts    int64 `json:"retracts"`
	Heartbeats  int64 `json:"heartbeats"`
	UnknownRefs int64 `json:"unknown_refs"`
	Malformed   int64 `json:"malformed"`
	Gaps        int64 `json:"gaps"`
	Skipped     int64 `json:"skipped"`
}

type detection struct {
	key       string
	satellite string
	at        time.Time
	lat, lon  float64
	conf      string
	frp       float64
	offset    int64
	retracted bool
	late      bool
	comp      *component
}

func (d *detection) active() bool { return !d.retracted && !d.late && d.conf != "l" }

// component is a set of connected active detections; two or more make a fire.
type component struct {
	members map[*detection]struct{}
}

type cell struct{ y, x int }

type emitted struct {
	hash  string
	alive bool
}

type Engine struct {
	cfg Config

	next         int64
	dets         map[string]*detection
	grid         map[cell][]*detection
	comps        map[*component]struct{}
	open         map[*component]struct{}
	late         []LateDetection
	maxEventTime time.Time
	virtualNow   time.Time
	watermark    time.Time
	counters     Counters

	state   map[string]*emitted
	pending map[string]Emit

	cur        int64
	curArrived time.Time
	touched    map[string]struct{}
	dirty      map[*component]struct{}
}

func New(cfg Config) *Engine {
	return &Engine{
		cfg: cfg, dets: map[string]*detection{}, grid: map[cell][]*detection{},
		comps: map[*component]struct{}{}, open: map[*component]struct{}{},
		state: map[string]*emitted{}, pending: map[string]Emit{},
		touched: map[string]struct{}{}, dirty: map[*component]struct{}{},
	}
}

func (e *Engine) Next() int64             { return e.next }
func (e *Engine) Watermark() time.Time    { return e.watermark }
func (e *Engine) MaxEventTime() time.Time { return e.maxEventTime }
func (e *Engine) VirtualNow() time.Time   { return e.virtualNow }
func (e *Engine) Counters() Counters      { return e.counters }
func (e *Engine) PendingLen() int         { return len(e.pending) }

// Apply processes one feed event. A malformed event is counted and returns an
// error for logging; the engine stays consistent and the next event goes on.
func (e *Engine) Apply(ev satfeed.Event, arrived time.Time) error {
	off, err := strconv.ParseInt(ev.ID, 10, 64)
	if err != nil {
		e.counters.Malformed++
		return fmt.Errorf("event id %q is not an offset", ev.ID)
	}
	if off < e.next {
		e.counters.Skipped++
		return nil
	}
	if off > e.next {
		e.counters.Gaps += off - e.next
	}
	e.next = off + 1
	e.cur, e.curArrived = off, arrived
	e.counters.Events++
	defer e.settle()

	switch ev.Type {
	case satfeed.TypeDetection:
		err = e.detection(ev.Data)
	case satfeed.TypeUpdate:
		err = e.update(ev.Data)
	case satfeed.TypeRetract:
		err = e.retract(ev.Data)
	case satfeed.TypeHeartbeat:
		err = e.heartbeat(ev.Data)
	}
	if err != nil {
		e.counters.Malformed++
		return fmt.Errorf("offset %d %s: %w", off, ev.Type, err)
	}
	return nil
}

func (e *Engine) detection(data []byte) error {
	var dd satfeed.DetectionData
	if err := json.Unmarshal(data, &dd); err != nil {
		return err
	}
	raw := dd.Detection

	key := raw.Key()
	if _, seen := e.dets[key]; seen {
		e.counters.Duplicates++
		return nil
	}

	at, err := raw.Time()
	if err != nil {
		return err
	}
	lat, err1 := raw.Latitude.Float64()
	lon, err2 := raw.Longitude.Float64()
	if err1 != nil || err2 != nil || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return fmt.Errorf("bad coordinates %q,%q", raw.Latitude, raw.Longitude)
	}
	frp, _ := raw.FRP.Float64()

	d := &detection{key: key, satellite: raw.Satellite, at: at, lat: lat, lon: lon, conf: raw.Confidence, frp: frp, offset: e.cur}
	e.dets[key] = d
	e.counters.Detections++

	if !e.watermark.IsZero() && at.Before(e.watermark) {
		d.late = true
		e.counters.Late++
		e.late = append(e.late, LateDetection{Offset: e.cur, Key: key, DetectedAt: at, Watermark: e.watermark})
		return nil
	}

	if at.After(e.maxEventTime) {
		e.maxEventTime = at
	}
	if d.active() {
		e.addActive(d)
	}
	e.advanceWatermark()
	return nil
}

func (e *Engine) update(data []byte) error {
	var u satfeed.UpdateData
	if err := json.Unmarshal(data, &u); err != nil {
		return err
	}
	e.counters.Updates++
	d := e.dets[u.Key]
	if d == nil {
		e.counters.UnknownRefs++
		return nil
	}

	was := d.active()
	if raw, ok := u.Changes["confidence"]; ok {
		var c string
		if err := json.Unmarshal(raw, &c); err != nil {
			return fmt.Errorf("confidence: %w", err)
		}
		d.conf = c
	}
	if raw, ok := u.Changes["frp"]; ok {
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return fmt.Errorf("frp: %w", err)
		}
		d.frp = f
	}

	switch now := d.active(); {
	case was && !now:
		e.removeActive(d)
	case !was && now:
		e.addActive(d)
	case now:
		e.dirty[d.comp] = struct{}{}
	}
	return nil
}

func (e *Engine) retract(data []byte) error {
	var r satfeed.RetractData
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	e.counters.Retracts++
	d := e.dets[r.Key]
	if d == nil {
		e.counters.UnknownRefs++
		return nil
	}
	if d.retracted {
		return nil
	}
	was := d.active()
	d.retracted = true
	if was {
		e.removeActive(d)
	}
	return nil
}

func (e *Engine) heartbeat(data []byte) error {
	var hb struct {
		VirtualNow time.Time `json:"virtual_now"`
	}
	if err := json.Unmarshal(data, &hb); err != nil {
		return err
	}
	e.counters.Heartbeats++
	if hb.VirtualNow.After(e.virtualNow) {
		e.virtualNow = hb.VirtualNow
		e.advanceWatermark()
	}
	return nil
}

// advanceWatermark moves the watermark to the newest detection time minus the
// allowed lateness. The feed's virtual clock (from heartbeats, so it is part of
// the replayable log) caps it: a satellite whose clock runs ahead cannot push
// everyone else's detections into the late list.
func (e *Engine) advanceWatermark() {
	ref := e.maxEventTime
	if !e.virtualNow.IsZero() && e.virtualNow.Before(ref) {
		ref = e.virtualNow
	}
	if ref.IsZero() {
		return
	}
	w := ref.Add(-e.cfg.AllowedLateness)
	if !w.After(e.watermark) {
		return
	}
	e.watermark = w
	for c := range e.open {
		if e.final(c) {
			e.dirty[c] = struct{}{}
		}
	}
}

// final: no on-time detection can join once the watermark is past the last
// detection plus the connection window.
func (e *Engine) final(c *component) bool {
	var last time.Time
	for d := range c.members {
		if d.at.After(last) {
			last = d.at
		}
	}
	return e.watermark.After(last.Add(e.cfg.MaxGap))
}

const cellDeg = 0.01

func cellOf(lat, lon float64) cell {
	return cell{int(math.Floor(lat / cellDeg)), int(math.Floor(lon / cellDeg))}
}

// neighbors calls fn for every active detection connected to d. The grid only
// narrows the search; the exact distance and time checks decide.
func (e *Engine) neighbors(d *detection, fn func(*detection)) {
	c := cellOf(d.lat, d.lon)
	ry := int(math.Ceil(e.cfg.MaxDistanceKM / 110.5 / cellDeg))
	cos := math.Cos((math.Abs(d.lat) + 0.1) * math.Pi / 180)
	rx := int(math.Ceil(e.cfg.MaxDistanceKM / (111.32 * math.Max(cos, 0.01)) / cellDeg))
	for y := c.y - ry; y <= c.y+ry; y++ {
		for x := c.x - rx; x <= c.x+rx; x++ {
			for _, o := range e.grid[cell{y, x}] {
				if o == d {
					continue
				}
				gap := d.at.Sub(o.at)
				if gap < 0 {
					gap = -gap
				}
				if gap <= e.cfg.MaxGap && HaversineKM(d.lat, d.lon, o.lat, o.lon) <= e.cfg.MaxDistanceKM {
					fn(o)
				}
			}
		}
	}
}

func (e *Engine) gridAdd(d *detection) {
	c := cellOf(d.lat, d.lon)
	e.grid[c] = append(e.grid[c], d)
}

func (e *Engine) gridRemove(d *detection) {
	c := cellOf(d.lat, d.lon)
	list := e.grid[c]
	for i, o := range list {
		if o == d {
			e.grid[c] = append(list[:i:i], list[i+1:]...)
			break
		}
	}
	if len(e.grid[c]) == 0 {
		delete(e.grid, c)
	}
}

// touch remembers the fire ID a component has before it is changed, so settle
// can retract IDs that no longer exist afterwards.
func (e *Engine) touch(c *component) {
	if id := e.fireID(c); id != "" {
		e.touched[id] = struct{}{}
	}
	e.dirty[c] = struct{}{}
}

func (e *Engine) dropComp(c *component) {
	delete(e.comps, c)
	delete(e.dirty, c)
	delete(e.open, c)
}

// addActive merges every neighbouring component and d into one.
func (e *Engine) addActive(d *detection) {
	var targets []*component
	seen := map[*component]bool{}
	e.neighbors(d, func(o *detection) {
		if !seen[o.comp] {
			seen[o.comp] = true
			targets = append(targets, o.comp)
		}
	})
	e.gridAdd(d)

	if len(targets) == 0 {
		c := &component{members: map[*detection]struct{}{d: {}}}
		d.comp = c
		e.comps[c] = struct{}{}
		e.dirty[c] = struct{}{}
		return
	}

	big := targets[0]
	for _, c := range targets {
		e.touch(c)
		if len(c.members) > len(big.members) {
			big = c
		}
	}
	for _, c := range targets {
		if c == big {
			continue
		}
		for m := range c.members {
			m.comp = big
			big.members[m] = struct{}{}
		}
		e.dropComp(c)
	}
	d.comp = big
	big.members[d] = struct{}{}
	e.dirty[big] = struct{}{}
}

// removeActive takes d out of its component and splits what is left into the
// groups that are still connected (d may have been the bridge).
func (e *Engine) removeActive(d *detection) {
	c := d.comp
	e.touch(c)
	e.gridRemove(d)
	delete(c.members, d)
	d.comp = nil
	e.dropComp(c)

	rest := sortedMembers(c)
	for _, start := range rest {
		if start.comp != c {
			continue
		}
		nc := &component{members: map[*detection]struct{}{}}
		queue := []*detection{start}
		start.comp = nc
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			nc.members[cur] = struct{}{}
			e.neighbors(cur, func(o *detection) {
				if o.comp == c {
					o.comp = nc
					queue = append(queue, o)
				}
			})
		}
		e.comps[nc] = struct{}{}
		e.dirty[nc] = struct{}{}
	}
}

func sortedMembers(c *component) []*detection {
	out := make([]*detection, 0, len(c.members))
	for d := range c.members {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].offset < out[j].offset })
	return out
}

// fireID names a fire after the detection that reached us first. Late
// detections always arrive later, so they never rename an existing fire, and
// replaying the same feed gives the same name.
func (e *Engine) fireID(c *component) string {
	if len(c.members) < e.cfg.MinDetections {
		return ""
	}
	var first *detection
	for d := range c.members {
		if first == nil || d.offset < first.offset {
			first = d
		}
	}
	sum := sha256.Sum256([]byte(first.key))
	return "fire-" + hex.EncodeToString(sum[:8])
}

func (e *Engine) version() int64 { return e.cfg.VersionEpoch*1_000_000_000_000 + e.cur + 1 }

// settle runs after every event: upserts the fires whose content changed and
// retracts the IDs that existed before the event but not after it.
func (e *Engine) settle() {
	alive := map[string]bool{}

	for c := range e.dirty {
		if _, ok := e.comps[c]; !ok {
			continue
		}
		id := e.fireID(c)
		if id == "" {
			delete(e.open, c)
			continue
		}
		alive[id] = true
		msg, final := e.build(c, id)
		if final {
			delete(e.open, c)
		} else {
			e.open[c] = struct{}{}
		}
		h := contentHash(msg)
		if st := e.state[id]; st != nil && st.alive && st.hash == h {
			continue
		}
		e.state[id] = &emitted{hash: h, alive: true}
		e.queue(msg)
	}

	for id := range e.touched {
		if alive[id] {
			continue
		}
		if st := e.state[id]; st != nil && st.alive {
			st.alive = false
			e.queue(hazard.UpsertMessage{Op: hazard.OpRetract, Source: hazard.SourceSatelit, SourceRefID: id, Version: e.version()})
		}
	}

	clear(e.touched)
	clear(e.dirty)
}

// queue keeps only the newest message per fire, with the oldest arrival time.
func (e *Engine) queue(m hazard.UpsertMessage) {
	arrived := e.curArrived
	if prev, ok := e.pending[m.SourceRefID]; ok && !prev.ArrivedAt.IsZero() && prev.ArrivedAt.Before(arrived) {
		arrived = prev.ArrivedAt
	}
	e.pending[m.SourceRefID] = Emit{Message: m, ArrivedAt: arrived}
}

// Drain hands over the latest message of every fire that changed since the
// previous Drain, ordered by fire ID.
func (e *Engine) Drain() []Emit {
	out := make([]Emit, 0, len(e.pending))
	for _, m := range e.pending {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Message.SourceRefID < out[j].Message.SourceRefID })
	clear(e.pending)
	return out
}

var confRank = map[string]int{"l": 1, "n": 2, "h": 3}

// build maps a fire to a HazardEvent. Members are summed in offset order so a
// replay gives bit-identical floats.
func (e *Engine) build(c *component, id string) (hazard.UpsertMessage, bool) {
	members := sortedMembers(c)
	var latSum, lonSum, frp float64
	first, last := members[0].at, members[0].at
	sats := map[string]bool{}
	maxConf := ""
	keys := make([]string, 0, len(members))
	for _, d := range members {
		latSum += d.lat
		lonSum += d.lon
		frp += d.frp
		if d.at.Before(first) {
			first = d.at
		}
		if d.at.After(last) {
			last = d.at
		}
		sats[d.satellite] = true
		if confRank[d.conf] > confRank[maxConf] {
			maxConf = d.conf
		}
		keys = append(keys, d.key)
	}
	sort.Strings(keys)
	satList := make([]string, 0, len(sats))
	for s := range sats {
		satList = append(satList, s)
	}
	sort.Strings(satList)

	n := len(members)
	lat := round(latSum/float64(n), 5)
	lon := round(lonSum/float64(n), 5)
	frp = round(frp, 3)
	final := e.watermark.After(last.Add(e.cfg.MaxGap))

	return hazard.UpsertMessage{
		Op: hazard.OpUpsert, Source: hazard.SourceSatelit, SourceRefID: id, Version: e.version(),
		HazardType: hazard.TypeWildfire, Severity: Severity(n, frp),
		AreaName: NearestProvince(lat, lon), Latitude: lat, Longitude: lon, OccurredAt: first,
		Attributes: map[string]any{
			"detection_count":   n,
			"satellites":        satList,
			"max_confidence":    maxConf,
			"total_frp_mw":      frp,
			"detection_keys":    keys,
			"last_detection_at": last.Format(time.RFC3339),
			"final":             final,
		},
	}, final
}

func Severity(count int, totalFRP float64) hazard.Severity {
	switch {
	case count >= 20 || totalFRP >= 100:
		return hazard.SeverityAwas
	case count >= 6 || totalFRP >= 30:
		return hazard.SeveritySiaga
	default:
		return hazard.SeverityWaspada
	}
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

func contentHash(m hazard.UpsertMessage) string {
	m.Version = 0
	b, _ := json.Marshal(m)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Fires returns every current fire, ordered by ID.
func (e *Engine) Fires() []hazard.UpsertMessage {
	var out []hazard.UpsertMessage
	for c := range e.comps {
		if id := e.fireID(c); id != "" {
			m, _ := e.build(c, id)
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceRefID < out[j].SourceRefID })
	return out
}

// Digest fingerprints the current fires (content only), to compare two runs.
func Digest(fires []hazard.UpsertMessage) string {
	h := sha256.New()
	for _, f := range fires {
		h.Write([]byte(contentHash(f)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (e *Engine) Late() []LateDetection { return append([]LateDetection(nil), e.late...) }

type Summary struct {
	Counters
	Offset            int64     `json:"offset"`
	Watermark         time.Time `json:"watermark"`
	MaxEventTime      time.Time `json:"max_event_time"`
	VirtualNow        time.Time `json:"virtual_now"`
	FiresActive       int       `json:"fires_active"`
	FiresFinal        int       `json:"fires_final"`
	FiresRetracted    int       `json:"fires_retracted"`
	SingleDetections  int       `json:"single_detections"`
	LowConfidence     int       `json:"low_confidence"`
	RetractedFeedRows int       `json:"retracted_detections"`
}

// Summary scans the whole state; call it on a slow clock.
func (e *Engine) Summary() Summary {
	s := Summary{Counters: e.counters, Offset: e.next - 1, Watermark: e.watermark, MaxEventTime: e.maxEventTime, VirtualNow: e.virtualNow}
	for c := range e.comps {
		switch {
		case len(c.members) == 1:
			s.SingleDetections++
		case e.fireID(c) != "":
			if _, open := e.open[c]; open {
				s.FiresActive++
			} else {
				s.FiresFinal++
			}
		}
	}
	for _, st := range e.state {
		if !st.alive {
			s.FiresRetracted++
		}
	}
	for _, d := range e.dets {
		if d.retracted {
			s.RetractedFeedRows++
		} else if d.conf == "l" {
			s.LowConfidence++
		}
	}
	return s
}
