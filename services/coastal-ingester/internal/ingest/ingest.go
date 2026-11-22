// Package ingest turns Sensor Pantai observations into critical alerts, HazardEvent
// upserts, and set-aside records, in that order of priority.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/telemetry"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/coastal-ingester/internal/sensor"
)

// HeaderIngestedAt carries the wall-clock time this pod first read a critical
// observation, so the field receiver can measure delivery latency.
const HeaderIngestedAt = "X-Ingested-At"

var (
	polls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_polls_total", Help: "Polls of Sensor Pantai, by outcome.",
	}, []string{"outcome"})
	items = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_observations_total", Help: "Observations read, by result.",
	}, []string{"result"})
	rejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_rejected_total", Help: "Observations set aside, by reason class.",
	}, []string{"reason"})
	criticalPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sensor_critical_published_total", Help: "Critical alerts published.",
	})
	lastSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_last_success_timestamp_seconds", Help: "Unix time of the last good poll.",
	})
)

type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers map[string]string) error
}

type Source interface {
	Observations(ctx context.Context, cursor string, limit int) (*sensor.Page, error)
	Stations(ctx context.Context) ([]sensor.Station, error)
	BreakerState() string
}

type Config struct {
	CriticalTopic, UpsertsTopic, RejectedTopic string
	PollInterval                               time.Duration
	PageLimit                                  int
	MaxPagesPerPoll                            int // keeps reading while pages are full, to catch up
}

type Latest struct {
	Observation
	ReceivedAt time.Time `json:"received_at"`
}

type Status struct {
	Leading     bool              `json:"leading"`
	Breaker     string            `json:"breaker"`
	LastSuccess time.Time         `json:"last_success,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	Cursor      string            `json:"cursor,omitempty"`
	Stations    map[string]Latest `json:"stations"`
}

type Ingester struct {
	cfg Config
	src Source
	out Producer
	log *slog.Logger

	mu       sync.Mutex
	stations map[string]sensor.Station
	seen     *dedupe
	status   Status
	lastGen  time.Time
}

// sensorRestarted notices the sensor's own clock moving back. Its cursor then points
// past data that no longer exists, so reading must start over (duplicates are dropped).
func (in *Ingester) sensorRestarted(generated time.Time) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	restarted := !in.lastGen.IsZero() && generated.Before(in.lastGen) && in.status.Cursor != ""
	in.lastGen = generated
	return restarted
}

func New(cfg Config, src Source, out Producer, log *slog.Logger) *Ingester {
	return &Ingester{cfg: cfg, src: src, out: out, log: log, seen: newDedupe(50_000),
		status: Status{Stations: map[string]Latest{}}}
}

// Run polls until ctx ends. Only the leader calls it, so the sensor sees one client.
func (in *Ingester) Run(ctx context.Context) {
	in.setLeading(true)
	defer in.setLeading(false)
	ticker := time.NewTicker(in.cfg.PollInterval)
	defer ticker.Stop()
	for {
		in.PollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// PollOnce reads pages until the sensor has no full page left (or MaxPagesPerPoll),
// so a backlog is drained quickly instead of one page per tick.
func (in *Ingester) PollOnce(ctx context.Context) {
	for range max(in.cfg.MaxPagesPerPoll, 1) {
		if full := in.pollPage(ctx); !full || ctx.Err() != nil {
			return
		}
	}
}

func (in *Ingester) pollPage(ctx context.Context) (full bool) {
	ctx = correlation.WithID(ctx, correlation.New())
	ctx, span := telemetry.Tracer().Start(ctx, "poll SENSOR_PANTAI")
	defer span.End()

	if len(in.knownStations()) == 0 {
		in.refreshStations(ctx)
	}
	page, err := in.src.Observations(ctx, in.cursor(), in.cfg.PageLimit)
	if err != nil {
		polls.WithLabelValues(outcome(err)).Inc()
		in.fail(err)
		in.log.WarnContext(ctx, "sensor poll failed", "error", err, "breaker", in.src.BreakerState())
		return false
	}
	if in.sensorRestarted(page.GeneratedAt) {
		in.log.WarnContext(ctx, "sensor clock went back, it restarted; reading again from the start")
		in.mu.Lock()
		in.status.Cursor = ""
		in.mu.Unlock()
		return true
	}
	if err := in.process(ctx, page); err != nil {
		polls.WithLabelValues("publish_failed").Inc()
		in.fail(err)
		in.log.ErrorContext(ctx, "publishing observations failed, page will be read again", "error", err)
		return false
	}
	polls.WithLabelValues("ok").Inc()
	lastSuccess.SetToCurrentTime()
	in.mu.Lock()
	in.status.LastSuccess, in.status.LastError = page.ReceivedAt, ""
	if page.NextCursor != "" {
		in.status.Cursor = page.NextCursor
	}
	in.mu.Unlock()
	return len(page.Items) >= in.cfg.PageLimit
}

type record struct {
	raw    json.RawMessage
	obs    Observation
	reason string
}

// process sends every critical alert of the page before any normal work, so an alert
// never waits behind a page full of routine observations.
func (in *Ingester) process(ctx context.Context, page *sensor.Page) error {
	stations := in.knownStations()
	fresh := make([]record, 0, len(page.Items))
	inPage := make(map[string]bool, len(page.Items))
	for _, raw := range page.Items {
		o, reason := parse(raw, stations)
		if o.ID != "" && (inPage[o.ID] || in.seen.has(o.ID)) {
			items.WithLabelValues("duplicate").Inc()
			continue
		}
		inPage[o.ID] = true
		fresh = append(fresh, record{raw, o, reason})
	}

	for _, r := range fresh {
		if r.obs.criticalReadable() {
			if err := in.publishCritical(ctx, r.obs, r.reason == "", page.ReceivedAt); err != nil {
				return err
			}
		}
	}
	for _, r := range fresh {
		var err error
		if r.reason != "" {
			err = in.setAside(ctx, r.raw, r.reason, page.ReceivedAt)
		} else {
			err = in.publishUpsert(ctx, r.obs, stations[r.obs.StationID], page.ReceivedAt)
		}
		if err != nil {
			return err
		}
		if r.obs.ID != "" {
			in.seen.add(r.obs.ID)
		}
	}
	return nil
}

func (in *Ingester) publishCritical(ctx context.Context, o Observation, verified bool, receivedAt time.Time) error {
	body, _ := json.Marshal(struct {
		Observation
		Verified bool `json:"verified"`
	}{o, verified})
	err := in.out.Produce(ctx, in.cfg.CriticalTopic, []byte(o.StationID), body, map[string]string{
		HeaderIngestedAt:           receivedAt.Format(time.RFC3339Nano),
		hazard.HeaderCorrelationID: correlation.FromContext(ctx),
	})
	if err != nil {
		return err
	}
	criticalPublished.Inc()
	in.log.WarnContext(ctx, "critical observation forwarded", "observation_id", o.ID, "station_id", o.StationID, "verified", verified)
	return nil
}

func (in *Ingester) publishUpsert(ctx context.Context, o Observation, st sensor.Station, receivedAt time.Time) error {
	area := st.Province
	if area == "" {
		area = o.StationID
	}
	msg := hazard.UpsertMessage{
		Op: hazard.OpUpsert, Source: hazard.SourceSensorPantai, SourceRefID: o.StationID,
		Version: o.ObservedAt.UnixMilli(), HazardType: hazard.TypeCoastal, Severity: severity(o),
		AreaName: area, Latitude: st.Latitude, Longitude: st.Longitude, OccurredAt: o.ObservedAt,
		Attributes: map[string]any{
			"station_id": o.StationID, "station_name": st.Name, "observation_id": o.ID,
			"sea_level_anomaly_m": o.SeaLevel, "wave_height_m": o.Wave, "quality": o.Quality,
			"critical": o.Critical, "received_at": receivedAt.Format(time.RFC3339Nano),
		},
	}
	enc, err := hazard.EncodeUpsert(msg, hazard.UpsertMeta{CorrelationID: correlation.FromContext(ctx), ProducedAt: receivedAt})
	if err != nil {
		return in.setAside(ctx, nil, "invalid_upsert: "+err.Error(), receivedAt)
	}
	if err := in.out.Produce(ctx, in.cfg.UpsertsTopic, enc.Key, enc.Value, enc.Headers); err != nil {
		return err
	}
	items.WithLabelValues("stored").Inc()
	in.mu.Lock()
	if cur, ok := in.status.Stations[o.StationID]; !ok || o.ObservedAt.After(cur.ObservedAt) {
		in.status.Stations[o.StationID] = Latest{Observation: o, ReceivedAt: receivedAt}
	}
	in.mu.Unlock()
	return nil
}

// setAside keeps a record that breaks the base contract, whole and with its reason,
// instead of storing it as a HazardEvent.
func (in *Ingester) setAside(ctx context.Context, raw json.RawMessage, reason string, receivedAt time.Time) error {
	body, _ := json.Marshal(map[string]any{
		"source": hazard.SourceSensorPantai, "reason": reason,
		"received_at": receivedAt.Format(time.RFC3339Nano), "payload": raw,
	})
	if err := in.out.Produce(ctx, in.cfg.RejectedTopic, []byte(hazard.SourceSensorPantai), body, map[string]string{
		hazard.HeaderCorrelationID: correlation.FromContext(ctx),
	}); err != nil {
		return err
	}
	items.WithLabelValues("rejected").Inc()
	rejected.WithLabelValues(reasonClass(reason)).Inc()
	in.log.WarnContext(ctx, "observation set aside", "reason", reason)
	return nil
}

func (in *Ingester) refreshStations(ctx context.Context) {
	list, err := in.src.Stations(ctx)
	if err != nil {
		in.log.WarnContext(ctx, "station list unavailable", "error", err)
		return
	}
	m := make(map[string]sensor.Station, len(list))
	for _, s := range list {
		m[s.ID] = s
	}
	in.mu.Lock()
	in.stations = m
	in.mu.Unlock()
}

func (in *Ingester) Status() Status {
	in.mu.Lock()
	defer in.mu.Unlock()
	s := in.status
	s.Breaker = in.src.BreakerState()
	s.Stations = make(map[string]Latest, len(in.status.Stations))
	for k, v := range in.status.Stations {
		s.Stations[k] = v
	}
	return s
}

func (in *Ingester) knownStations() map[string]sensor.Station {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.stations
}

func (in *Ingester) cursor() string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.status.Cursor
}

func (in *Ingester) fail(err error) {
	in.mu.Lock()
	in.status.LastError = err.Error()
	in.mu.Unlock()
}

func (in *Ingester) setLeading(v bool) {
	in.mu.Lock()
	in.status.Leading = v
	in.mu.Unlock()
}

func outcome(err error) string {
	switch {
	case errors.Is(err, sensor.ErrStale):
		return "stale"
	case isBreakerOpen(err):
		return "breaker_open"
	default:
		return "error"
	}
}

func reasonClass(reason string) string {
	for i, c := range reason {
		if c == ':' {
			return reason[:i]
		}
	}
	return reason
}

type dedupe struct {
	mu    sync.Mutex
	set   map[string]struct{}
	order []string
	cap   int
}

func newDedupe(capacity int) *dedupe {
	return &dedupe{set: make(map[string]struct{}, capacity), cap: capacity}
}

func (d *dedupe) has(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.set[id]
	return ok
}

func (d *dedupe) add(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.set[id]; ok {
		return
	}
	if len(d.order) >= d.cap {
		delete(d.set, d.order[0])
		d.order = d.order[1:]
	}
	d.set[id] = struct{}{}
	d.order = append(d.order, id)
}
