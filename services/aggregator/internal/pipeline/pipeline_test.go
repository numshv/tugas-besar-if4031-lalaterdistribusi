package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/schema"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type fakeStore struct {
	store.Store
	mu     sync.Mutex
	events map[string]hazard.Event
	writes int
	down   bool
}

func (s *fakeStore) Upsert(_ context.Context, e hazard.Event) (hazard.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return hazard.Event{}, false, errors.New("mongo down")
	}
	s.events[e.HazardID] = e
	s.writes++
	return e, true, nil
}

func (s *fakeStore) ListByType(context.Context, hazard.Type, time.Time, *hazard.Cursor, int) ([]hazard.Event, bool, error) {
	return nil, false, nil
}

type fakePub struct{ published int }

func (p *fakePub) Publish(context.Context, hazard.Event) error { p.published++; return nil }
func (p *fakePub) Close() error                                { return nil }

type resolver struct{}

func (resolver) Resolve(id string) (string, float64, float64, bool) {
	if id == "VOL-PANIC" {
		panic("resolver exploded")
	}
	return "Gunung " + id, -7, 110, true
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { c.t = c.t.Add(time.Millisecond); return c.t }

func newPipeline() (*Pipeline, *fakeStore, *dlq.MemoryStore, *clock) {
	st := &fakeStore{events: map[string]hazard.Event{}}
	q := dlq.NewMemoryStore()
	c := &clock{t: time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)}
	return &Pipeline{
		Store: st, Pub: &fakePub{}, DLQ: q, Rules: schema.Rules{}, Resolver: resolver{},
		Policy: mapper.PolicyPassthrough, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: c.now,
	}, st, q, c
}

func pvmbg(body string, at time.Time) Response {
	return Response{Path: PathVolcanic, Body: []byte(body), ReceivedAt: at}
}

const v1 = `{"report_id":"r1","volcano_id":"VOL-A","alert_level":"Siaga","reported_at":"2026-11-17T07:00:00Z"}`

func unprocessed(t *testing.T, q *dlq.MemoryStore) []dlq.Message {
	t.Helper()
	out, err := q.List(context.Background(), "", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestIncompatibleRecordIsParkedAndOthersStillStored(t *testing.T) {
	p, st, q, c := newPipeline()
	body := `[` + v1 + `,{"report_id":"r2","volcano_id":"VOL-A","level":"Awas","reported_at":"2026-11-17T07:01:00Z"}]`
	if err := p.IngestPVMBG(context.Background(), pvmbg(body, c.now())); err != nil {
		t.Fatal(err)
	}
	if len(st.events) != 1 {
		t.Fatalf("stored %d events, want 1 (r1)", len(st.events))
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 1 || msgs[0].Status != dlq.StatusParked || msgs[0].Retryable ||
		!strings.HasPrefix(msgs[0].Reason, "schema_incompatible") || msgs[0].RefHint != "r2" {
		t.Fatalf("unprocessed = %+v", msgs)
	}
	stored, _ := q.Get(context.Background(), msgs[0].ID)
	if !strings.Contains(string(stored.Payload), `"level":"Awas"`) {
		t.Errorf("payload not kept intact: %s", stored.Payload)
	}
}

func TestReplayAfterRulesRecoversWithoutDuplicates(t *testing.T) {
	p, st, q, c := newPipeline()
	ctx := context.Background()
	renamed := `[{"report_id":"r2","volcano_id":"VOL-A","level":"Awas","reported_at":"2026-11-17T07:01:00Z"}]`
	numeric := `[{"report_id":"r3","volcano_id":"VOL-A","alert_level":3,"reported_at":"2026-11-17T07:02:00Z"}]`
	missing := `[{"report_id":"r4","volcano_id":"VOL-A","alert_level":"Normal"}]`
	for _, b := range []string{renamed, numeric, missing, renamed} { // renamed twice: same message, stored once
		if err := p.IngestPVMBG(ctx, pvmbg(b, c.now())); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(unprocessed(t, q)); n != 3 {
		t.Fatalf("parked %d messages, want 3", n)
	}

	// Nothing changed yet: replay keeps them parked.
	res, err := p.Replay(ctx, "PVMBG")
	if err != nil || res.Recovered != 0 || res.Parked != 3 {
		t.Fatalf("replay before adapting = %+v, %v", res, err)
	}

	p.Rules, _ = schema.Parse([]byte(`{"PVMBG":{"rename":{"level":"alert_level"},
		"values":{"alert_level":{"3":"Siaga"}},"defaults":{"reported_at":"$received_at"}}}`))
	res, err = p.Replay(ctx, "PVMBG")
	if err != nil || res.Recovered != 3 || res.Parked != 0 {
		t.Fatalf("replay after adapting = %+v, %v", res, err)
	}
	if len(unprocessed(t, q)) != 0 {
		t.Error("recovered messages still listed as unprocessed")
	}
	if len(st.events) != 3 || st.events[hazard.NewID(hazard.SourcePVMBG, "r3")].Severity != hazard.SeveritySiaga {
		t.Fatalf("stored = %+v", st.events)
	}

	writes := st.writes
	if res, _ := p.Replay(ctx, "PVMBG"); res.Replayed != 0 || st.writes != writes {
		t.Errorf("second replay touched %d messages", res.Replayed)
	}
}

func TestMalformedResponseIsParkedWholeAndPollFails(t *testing.T) {
	p, _, q, c := newPipeline()
	ctx := context.Background()
	cases := map[string]Response{
		"truncated":    pvmbg(`[{"report_id":"r1","alert_le`, c.now()),
		"invalid utf8": pvmbg("[{\"report_id\":\"\xff\"}]", c.now()),
		"deep":         pvmbg(strings.Repeat("[", 64)+strings.Repeat("]", 64), c.now()),
		"oversize":     {Path: PathVolcanic, Body: []byte("[{"), TooLarge: true, ReceivedAt: c.now()},
	}
	for name, r := range cases {
		if err := p.IngestPVMBG(ctx, r); !errors.Is(err, ErrMalformedResponse) {
			t.Errorf("%s: err = %v, want ErrMalformedResponse", name, err)
		}
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 4 {
		t.Fatalf("parked %d responses, want 4", len(msgs))
	}
	for _, m := range msgs {
		if m.Kind != dlq.KindResponse || m.Status != dlq.StatusParked || m.Retryable || !strings.HasPrefix(m.Reason, "malformed:") {
			t.Errorf("message = %+v", m)
		}
	}
	// The next, healthy response is processed normally.
	if err := p.IngestPVMBG(ctx, pvmbg(`[`+v1+`]`, c.now())); err != nil {
		t.Fatal(err)
	}
}

func TestTransientFailureRetriesThenRecovers(t *testing.T) {
	p, st, q, c := newPipeline()
	ctx := context.Background()
	st.down = true
	if err := p.IngestPVMBG(ctx, pvmbg(`[`+v1+`]`, c.now())); err != nil {
		t.Fatal(err)
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 1 || msgs[0].Status != dlq.StatusRetrying || !msgs[0].Retryable {
		t.Fatalf("unprocessed = %+v", msgs)
	}

	c.t = c.t.Add(time.Hour) // past the backoff
	p.RetryDue(ctx, 10)
	if m, _ := q.Get(ctx, msgs[0].ID); m.Status != dlq.StatusRetrying || m.Attempts != 2 {
		t.Fatalf("after failed retry: %+v", m)
	}

	st.down = false
	c.t = c.t.Add(time.Hour)
	p.RetryDue(ctx, 10)
	if len(unprocessed(t, q)) != 0 || len(st.events) != 1 {
		t.Fatal("message did not recover after storage came back")
	}
}

func TestRetryingIsParkedAfterMaxAttempts(t *testing.T) {
	p, st, q, c := newPipeline()
	ctx := context.Background()
	st.down = true
	_ = p.IngestPVMBG(ctx, pvmbg(`[`+v1+`]`, c.now()))
	for range dlq.MaxAttempts + 2 {
		c.t = c.t.Add(time.Hour)
		p.RetryDue(ctx, 10)
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 1 || msgs[0].Status != dlq.StatusParked || !msgs[0].Retryable || msgs[0].Attempts != dlq.MaxAttempts {
		t.Fatalf("unprocessed = %+v", msgs)
	}
}

func TestReplayKeepsOrderPerSourceRef(t *testing.T) {
	p, st, q, c := newPipeline()
	ctx := context.Background()
	older := `[{"report_id":"r9","volcano_id":"VOL-A","level":"Waspada","reported_at":"2026-11-17T07:00:00Z"}]`
	newer := `[{"report_id":"r9","volcano_id":"VOL-A","alert_level":5,"reported_at":"2026-11-17T07:05:00Z"}]`
	_ = p.IngestPVMBG(ctx, pvmbg(older, c.now()))
	_ = p.IngestPVMBG(ctx, pvmbg(newer, c.now()))

	// Only the newer shape can be adapted; the older must not be overtaken.
	p.Rules, _ = schema.Parse([]byte(`{"PVMBG":{"values":{"alert_level":{"5":"Awas"}}}}`))
	res, err := p.Replay(ctx, "PVMBG")
	if err != nil || res.Recovered != 0 || res.Skipped != 1 || len(st.events) != 0 {
		t.Fatalf("replay = %+v, %v, stored %d", res, err, len(st.events))
	}
	if n := len(unprocessed(t, q)); n != 2 {
		t.Errorf("unprocessed = %d, want 2", n)
	}
}

func TestPanicInOneRecordIsParked(t *testing.T) {
	p, st, q, c := newPipeline()
	body := `[{"report_id":"r1","volcano_id":"VOL-PANIC","alert_level":"Siaga","reported_at":"2026-11-17T07:00:00Z"},` + strings.Replace(v1, "r1", "r2", 1) + `]`
	if err := p.IngestPVMBG(context.Background(), pvmbg(body, c.now())); err != nil {
		t.Fatal(err)
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 1 || !strings.HasPrefix(msgs[0].Reason, "internal_error") || len(st.events) != 1 {
		t.Fatalf("unprocessed = %+v, stored = %d", msgs, len(st.events))
	}
}

func TestBMKGKeepsWarningWithEvent(t *testing.T) {
	p, st, q, c := newPipeline()
	ctx := context.Background()
	at := c.now()
	events := Response{Path: PathSeismic, ReceivedAt: at, Body: []byte(`[
		{"event_id":"e1","magnitude":7.1,"occurred_at":"2026-11-17T07:00:00Z","region_name":"Aceh"},
		{"event_id":"e2","magnitude":"big","occurred_at":"2026-11-17T07:01:00Z"}]`)}
	warnings := Response{Path: PathTsunami, ReceivedAt: at, Body: []byte(`[{"related_event_id":"e1","threat_level":"Awas"}]`)}
	if err := p.IngestBMKG(ctx, events, warnings); err != nil {
		t.Fatal(err)
	}
	if ev := st.events[hazard.NewID(hazard.SourceBMKG, "e1")]; ev.Severity != hazard.SeverityAwas {
		t.Errorf("e1 severity = %s, want AWAS from its warning", ev.Severity)
	}
	msgs := unprocessed(t, q)
	if len(msgs) != 1 || msgs[0].RefHint != "e2" || msgs[0].Source != "BMKG" {
		t.Fatalf("unprocessed = %+v", msgs)
	}
}
