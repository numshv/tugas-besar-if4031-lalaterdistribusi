package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/coastal-ingester/internal/sensor"
)

type sent struct {
	topic string
	value string
}

type fakeOut struct {
	sent []sent
	fail bool
}

func (f *fakeOut) Produce(_ context.Context, topic string, _, value []byte, _ map[string]string) error {
	if f.fail {
		return errors.New("kafka down")
	}
	f.sent = append(f.sent, sent{topic, string(value)})
	return nil
}

func (f *fakeOut) topics() []string {
	var out []string
	for _, s := range f.sent {
		out = append(out, s.topic)
	}
	return out
}

type fakeSensor struct{ page *sensor.Page }

func (f *fakeSensor) Observations(context.Context, string, int) (*sensor.Page, error) {
	return f.page, nil
}
func (f *fakeSensor) Stations(context.Context) ([]sensor.Station, error) {
	return []sensor.Station{{ID: "stn-padang-01", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.9, Longitude: 100.3}}, nil
}
func (f *fakeSensor) BreakerState() string { return "closed" }

var generated = time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)

func obs(id string, extra string) json.RawMessage {
	return json.RawMessage(`{"observation_id":"` + id + `","station_id":"stn-padang-01","observed_at":"2026-11-12T00:59:00Z",` +
		`"sea_level_anomaly_m":0.42,"wave_height_m":1.8,"quality":"ok","critical":false` + extra + `}`)
}

func newIngester(items ...json.RawMessage) (*Ingester, *fakeOut) {
	out := &fakeOut{}
	src := &fakeSensor{page: &sensor.Page{GeneratedAt: generated, Items: items, ReceivedAt: time.Now()}}
	cfg := Config{CriticalTopic: "critical", UpsertsTopic: "upserts", RejectedTopic: "rejected", PageLimit: 100}
	return New(cfg, src, out, slog.New(slog.NewTextHandler(io.Discard, nil))), out
}

func TestValidObservationBecomesCoastalUpsert(t *testing.T) {
	in, out := newIngester(obs("obs-1", ""))
	in.PollOnce(context.Background())

	if got := out.topics(); len(got) != 1 || got[0] != "upserts" {
		t.Fatalf("topics = %v, want [upserts]", got)
	}
	msg, err := hazard.DecodeUpsert([]byte(out.sent[0].value))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Source != hazard.SourceSensorPantai || msg.HazardType != hazard.TypeCoastal || msg.SourceRefID != "stn-padang-01" || msg.AreaName != "Sumatera Barat" {
		t.Fatalf("upsert = %+v", msg)
	}
	if st := in.Status(); st.Stations["stn-padang-01"].ID != "obs-1" || st.LastSuccess.IsZero() {
		t.Fatalf("status = %+v", st)
	}
}

func TestContractViolationIsSetAsideWithReason(t *testing.T) {
	broken := json.RawMessage(`{"observation_id":"obs-2","station_id":"stn-padang-01","observed_at":"2026-11-12T00:59:00Z","sea_level_anomaly_m":"0.4","wave_height_m":1.8,"quality":"ok","critical":false}`)
	in, out := newIngester(broken, obs("obs-3", `,"extra":1`))
	in.PollOnce(context.Background())

	if got := out.topics(); len(got) != 2 || got[0] != "rejected" || got[1] != "upserts" {
		t.Fatalf("topics = %v, want [rejected upserts]", got)
	}
	if !strings.Contains(out.sent[0].value, "wrong_type: sea_level_anomaly_m") || !strings.Contains(out.sent[0].value, `"obs-2"`) {
		t.Fatalf("set-aside record lacks reason or payload: %s", out.sent[0].value)
	}
}

func TestCriticalGoesFirstEvenWhenRecordIsBroken(t *testing.T) {
	crit := json.RawMessage(`{"observation_id":"obs-4","station_id":"stn-padang-01","observed_at":"2026-11-12T00:59:00Z","sea_level_anomaly_m":5.1,"wave_height_m":"tinggi","quality":"ok","critical":true}`)
	in, out := newIngester(crit)
	in.PollOnce(context.Background())

	if got := out.topics(); len(got) != 2 || got[0] != "critical" || got[1] != "rejected" {
		t.Fatalf("topics = %v, want [critical rejected]", got)
	}
	if !strings.Contains(out.sent[0].value, `"verified":false`) {
		t.Fatalf("unverified critical alert not flagged: %s", out.sent[0].value)
	}
}

func TestDuplicatesAreSkippedAndFailedPublishIsRetried(t *testing.T) {
	in, out := newIngester(obs("obs-5", ""), obs("obs-5", ""))
	out.fail = true
	in.PollOnce(context.Background())
	if len(out.sent) != 0 || in.Status().LastError == "" {
		t.Fatal("a failed publish must leave the page unprocessed")
	}

	out.fail = false
	in.PollOnce(context.Background())
	in.PollOnce(context.Background())
	if got := out.topics(); len(got) != 1 {
		t.Fatalf("topics = %v, want the observation once despite duplicates and repeats", got)
	}
}

type pagedSensor struct {
	fakeSensor
	pages []*sensor.Page
	calls int
}

func (p *pagedSensor) Observations(context.Context, string, int) (*sensor.Page, error) {
	page := p.pages[min(p.calls, len(p.pages)-1)]
	p.calls++
	return page, nil
}

func TestBacklogIsDrainedInOnePoll(t *testing.T) {
	full := &sensor.Page{GeneratedAt: generated, Items: []json.RawMessage{obs("a", ""), obs("b", "")}, NextCursor: "c1"}
	full2 := &sensor.Page{GeneratedAt: generated, Items: []json.RawMessage{obs("c", ""), obs("d", "")}, NextCursor: "c2"}
	tail := &sensor.Page{GeneratedAt: generated, Items: []json.RawMessage{obs("e", "")}, NextCursor: "c3"}
	src := &pagedSensor{pages: []*sensor.Page{full, full2, tail}}
	out := &fakeOut{}
	cfg := Config{CriticalTopic: "critical", UpsertsTopic: "upserts", RejectedTopic: "rejected", PageLimit: 2, MaxPagesPerPoll: 10}
	in := New(cfg, src, out, slog.New(slog.NewTextHandler(io.Discard, nil)))

	in.PollOnce(context.Background())

	if src.calls != 3 || len(out.sent) != 5 {
		t.Fatalf("one poll read %d pages and published %d, want 3 pages and 5 observations", src.calls, len(out.sent))
	}
	if in.Status().Cursor != "c3" {
		t.Fatalf("cursor = %q, want c3", in.Status().Cursor)
	}
}

func TestCriticalAlertsLeaveBeforeRoutineWork(t *testing.T) {
	crit := json.RawMessage(`{"observation_id":"obs-9","station_id":"stn-padang-01","observed_at":"2026-11-12T00:59:30Z","sea_level_anomaly_m":2.5,"wave_height_m":3,"quality":"ok","critical":true}`)
	in, out := newIngester(obs("obs-7", ""), obs("obs-8", ""), crit)
	in.PollOnce(context.Background())

	if got := out.topics(); len(got) != 4 || got[0] != "critical" {
		t.Fatalf("topics = %v, want the critical alert first", got)
	}
}

func TestSensorRestartResetsCursor(t *testing.T) {
	later := &sensor.Page{GeneratedAt: generated.Add(time.Hour), Items: []json.RawMessage{obs("x", "")}, NextCursor: "c9"}
	restarted := &sensor.Page{GeneratedAt: generated.Add(-time.Hour), NextCursor: ""}
	src := &pagedSensor{pages: []*sensor.Page{later, restarted}}
	cfg := Config{CriticalTopic: "critical", UpsertsTopic: "upserts", RejectedTopic: "rejected", PageLimit: 100, MaxPagesPerPoll: 1}
	in := New(cfg, src, &fakeOut{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	in.PollOnce(context.Background())
	if in.Status().Cursor != "c9" {
		t.Fatalf("cursor = %q, want c9", in.Status().Cursor)
	}
	in.PollOnce(context.Background())
	if c := in.Status().Cursor; c != "" {
		t.Fatalf("cursor = %q after the sensor clock went back, want a fresh start", c)
	}
}
