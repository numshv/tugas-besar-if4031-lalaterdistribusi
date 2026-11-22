package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestInboxCountsEachAlertOnceAndMeasuresLatency(t *testing.T) {
	box := &inbox{byID: map[string]Alert{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingested := time.Now().Add(-1500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	msg := kafka.Message{
		Value:   []byte(`{"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-11-12T00:00:00Z","verified":true}`),
		Headers: []kafka.Header{{Key: headerIngestedAt, Value: []byte(ingested)}},
	}

	for range 3 {
		if err := box.handle(context.Background(), log, msg); err != nil {
			t.Fatal(err)
		}
	}
	if len(box.alerts) != 1 {
		t.Fatalf("stored %d alerts, want 1 despite redelivery", len(box.alerts))
	}
	if ms := box.alerts[0].LatencyMS; ms < 1400 || ms > 5000 {
		t.Fatalf("latency_ms = %d, want about 1500", ms)
	}
}
