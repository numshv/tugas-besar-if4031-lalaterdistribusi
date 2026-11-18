package upserts

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []hazard.Event
	failures  int
}

func (p *fakePublisher) Publish(_ context.Context, e hazard.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("kafka unavailable")
	}
	p.published = append(p.published, e)
	return nil
}

func (p *fakePublisher) Close() error { return nil }

type flakyStore struct {
	store.Store
	mu       sync.Mutex
	failures int
}

func (s *flakyStore) Upsert(ctx context.Context, e hazard.Event) (hazard.Event, bool, error) {
	s.mu.Lock()
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		return hazard.Event{}, false, errors.New("no primary")
	}
	s.mu.Unlock()
	return s.Store.Upsert(ctx, e)
}

type recordingDLQ struct{ reasons []string }

func (d *recordingDLQ) Park(_ context.Context, _ kafka.Message, reason string, _ error) error {
	d.reasons = append(d.reasons, reason)
	return nil
}

func newConsumer(st store.Store) (*Consumer, *fakePublisher, *recordingDLQ) {
	pub, dlq := &fakePublisher{}, &recordingDLQ{}
	return &Consumer{
		Store: st, Publisher: pub, DeadLetter: dlq,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
	}, pub, dlq
}

func fire(op string, version int64) hazard.UpsertMessage {
	return hazard.UpsertMessage{
		Op: op, Source: hazard.SourceSatelit, SourceRefID: "fire-0a41c7", Version: version,
		HazardType: hazard.TypeWildfire, Severity: hazard.SeveritySiaga, AreaName: "Kalimantan Tengah",
		Latitude: -2.51, Longitude: 113.8, OccurredAt: time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC),
		Attributes: map[string]any{"detection_count": 6},
	}
}

func kmsg(t *testing.T, m hazard.UpsertMessage) kafka.Message {
	t.Helper()
	enc, err := hazard.EncodeUpsert(m, hazard.UpsertMeta{CorrelationID: "cid-1", ProducedAt: time.Now().Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	km := kafka.Message{Key: enc.Key, Value: enc.Value}
	for k, v := range enc.Headers {
		km.Headers = append(km.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return km
}

func TestAppliedUpsertIsStoredAndPublished(t *testing.T) {
	st := store.NewMemory()
	c, pub, dlq := newConsumer(st)
	if err := c.Handle(context.Background(), kmsg(t, fire(hazard.OpUpsert, 1))); err != nil {
		t.Fatal(err)
	}
	if len(pub.published) != 1 || len(dlq.reasons) != 0 {
		t.Fatalf("published=%d parked=%v", len(pub.published), dlq.reasons)
	}
	got := pub.published[0]
	if got.HazardID != hazard.NewID(hazard.SourceSatelit, "fire-0a41c7") || got.IngestedAt.IsZero() || got.Version != 1 {
		t.Fatalf("published event must be the stored one: %+v", got)
	}
}

func TestStaleIsSkippedAndRedeliveryIsRepublished(t *testing.T) {
	st := store.NewMemory()
	c, pub, _ := newConsumer(st)
	ctx := context.Background()
	for _, v := range []int64{2, 2, 1} {
		if err := c.Handle(ctx, kmsg(t, fire(hazard.OpUpsert, v))); err != nil {
			t.Fatal(err)
		}
	}
	if len(pub.published) != 2 {
		t.Fatalf("want first publish + redelivery republish, stale skipped; got %d", len(pub.published))
	}
	if pub.published[0].Version != 2 || pub.published[1].Version != 2 {
		t.Fatalf("versions published: %d, %d", pub.published[0].Version, pub.published[1].Version)
	}
}

func TestRetractUpdatesExistingEvent(t *testing.T) {
	st := store.NewMemory()
	c, pub, _ := newConsumer(st)
	ctx := context.Background()
	if err := c.Handle(ctx, kmsg(t, fire(hazard.OpUpsert, 1))); err != nil {
		t.Fatal(err)
	}
	retract := hazard.UpsertMessage{Op: hazard.OpRetract, Source: hazard.SourceSatelit, SourceRefID: "fire-0a41c7", Version: 2}
	if err := c.Handle(ctx, kmsg(t, retract)); err != nil {
		t.Fatal(err)
	}
	last := pub.published[len(pub.published)-1]
	if !last.Retracted || last.AreaName != "Kalimantan Tengah" {
		t.Fatalf("retract must keep the fire and mark it: %+v", last)
	}
	if h := hazard.EventHeaders(last); h[hazard.HeaderOp] != hazard.OpRetract || h[hazard.HeaderVersion] != "2" {
		t.Fatalf("headers = %v", h)
	}
	page, _ := st.ListLatest(ctx, store.LatestQuery{Limit: 10, IncludeRetracted: true})
	if len(page.Items) != 1 {
		t.Fatalf("retract must not create a new event, got %d", len(page.Items))
	}
}

func TestPoisonMessagesAreParked(t *testing.T) {
	c, pub, dlq := newConsumer(store.NewMemory())
	bad := []kafka.Message{
		{Value: []byte(`{"op":"upsert","source":"SATELIT"`)},
		{Value: []byte{0xff, 0xfe, '{', '}'}},
		{Value: []byte(`{"op":"delete","source":"SATELIT","source_ref_id":"x","version":1}`)},
		kmsg(t, fire(hazard.OpUpsert, 1)),
	}
	bad[3].Value = []byte(`{"op":"upsert","source":"SATELIT","source_ref_id":"x","version":0}`)
	for _, m := range bad {
		if err := c.Handle(context.Background(), m); err != nil {
			t.Fatalf("poison message must not stop the consumer: %v", err)
		}
	}
	want := []string{"invalid_json", "invalid_utf8", "unknown_op", "bad_version"}
	if len(dlq.reasons) != len(want) {
		t.Fatalf("parked = %v, want %v", dlq.reasons, want)
	}
	for i := range want {
		if dlq.reasons[i] != want[i] {
			t.Fatalf("parked = %v, want %v", dlq.reasons, want)
		}
	}
	if len(pub.published) != 0 {
		t.Fatal("poison messages must not be published")
	}
}

func TestStoreAndPublishFailuresAreRetried(t *testing.T) {
	st := &flakyStore{Store: store.NewMemory(), failures: 3}
	c, pub, _ := newConsumer(st)
	pub.failures = 2
	if err := c.Handle(context.Background(), kmsg(t, fire(hazard.OpUpsert, 1))); err != nil {
		t.Fatal(err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("published = %d after retries", len(pub.published))
	}
}

type fakeReader struct {
	mu        sync.Mutex
	queue     []kafka.Message
	committed []int64
	cancel    context.CancelFunc
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queue) == 0 {
		r.cancel()
		return kafka.Message{}, ctx.Err()
	}
	m := r.queue[0]
	r.queue = r.queue[1:]
	return m, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Stats() kafka.ReaderStats { return kafka.ReaderStats{} }

func TestRunCommitsOnlyAfterHandling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m1, m2 := kmsg(t, fire(hazard.OpUpsert, 1)), kafka.Message{Value: []byte("garbage")}
	m1.Offset, m2.Offset = 10, 11
	reader := &fakeReader{queue: []kafka.Message{m1, m2}, cancel: cancel}
	c, pub, dlq := newConsumer(store.NewMemory())
	c.Reader = reader
	c.Run(ctx)
	if len(reader.committed) != 2 || reader.committed[0] != 10 || reader.committed[1] != 11 {
		t.Fatalf("committed = %v", reader.committed)
	}
	if len(pub.published) != 1 || len(dlq.reasons) != 1 {
		t.Fatalf("published=%d parked=%v", len(pub.published), dlq.reasons)
	}
}

func TestRunDoesNotCommitWhenStoppedMidRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := kmsg(t, fire(hazard.OpUpsert, 1))
	m.Offset = 5
	reader := &fakeReader{queue: []kafka.Message{m}, cancel: cancel}
	st := &flakyStore{Store: store.NewMemory(), failures: 1 << 30}
	c, _, _ := newConsumer(st)
	c.Reader = reader
	c.MinBackoff, c.MaxBackoff = 5*time.Millisecond, 5*time.Millisecond
	time.AfterFunc(50*time.Millisecond, cancel)
	c.Run(ctx)
	if len(reader.committed) != 0 {
		t.Fatalf("offset committed although the event was never stored: %v", reader.committed)
	}
}
