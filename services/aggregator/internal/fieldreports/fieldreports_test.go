package fieldreports

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/fieldreport"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
)

type memStore struct {
	docs       map[string]string
	deliveries map[string]int
	down       bool
}

func (s *memStore) Save(_ context.Context, r fieldreport.Report, hash string, _ time.Time) (bool, string, error) {
	if s.down {
		return false, "", errors.New("mongo down")
	}
	s.deliveries[r.ReportID]++
	if h, ok := s.docs[r.ReportID]; ok {
		return false, h, nil
	}
	s.docs[r.ReportID] = hash
	return true, hash, nil
}

func (s *memStore) Get(context.Context, string) (map[string]any, bool, error) { return nil, false, nil }

func message(t *testing.T, id, description string) kafka.Message {
	t.Helper()
	lat, lon := -7.5, 110.4
	b, _ := json.Marshal(fieldreport.Report{ReportID: id, Reporter: "tim-1", AcceptedAt: time.Now(), Input: fieldreport.Input{
		HazardType: hazard.TypeVolcanic, Latitude: &lat, Longitude: &lon, ObservedAt: time.Now(), Description: description,
	}})
	return kafka.Message{Key: []byte(id), Value: b}
}

func newConsumer() (*Consumer, *memStore, *dlq.MemoryStore) {
	st := &memStore{docs: map[string]string{}, deliveries: map[string]int{}}
	q := dlq.NewMemoryStore()
	return &Consumer{Store: st, DLQ: q, Now: time.Now}, st, q
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRedeliveryIsProcessedOnce(t *testing.T) {
	c, st, _ := newConsumer()
	m := message(t, "report-0001", "abu turun")
	for range 3 {
		if err := c.Handle(context.Background(), discard, m); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.docs) != 1 || st.deliveries["report-0001"] != 3 {
		t.Fatalf("docs %d, deliveries %d", len(st.docs), st.deliveries["report-0001"])
	}
	_ = c.Handle(context.Background(), discard, message(t, "report-0001", "laporan lain"))
	if len(st.docs) != 1 {
		t.Fatal("conflicting report stored twice")
	}
}

func TestStoreDownReturnsErrorSoMessageIsRetried(t *testing.T) {
	c, st, _ := newConsumer()
	st.down = true
	if err := c.Handle(context.Background(), discard, message(t, "report-0002", "x")); err == nil {
		t.Fatal("expected an error so RunNoDrop retries instead of committing")
	}
}

func TestBrokenMessageIsParked(t *testing.T) {
	c, st, q := newConsumer()
	bad := []kafka.Message{
		{Key: []byte("report-0003"), Value: []byte("{not json")},
		message(t, "report-0004", ""),
		{Key: []byte("other-key"), Value: message(t, "report-0005", "x").Value},
	}
	for _, m := range bad {
		if err := c.Handle(context.Background(), discard, m); err != nil {
			t.Fatalf("broken message should be parked, not retried: %v", err)
		}
	}
	if len(st.docs) != 0 || len(q.All()) != 3 || q.All()[0].Source != Source {
		t.Fatalf("stored %d, parked %+v", len(st.docs), q.All())
	}
}
