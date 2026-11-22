package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCountsAndEvents(t *testing.T) {
	s := NewMemoryStore()
	now := time.Now()
	for i, st := range []Status{StatusParked, StatusParked, StatusRetrying, StatusRecovered} {
		_, _ = s.Put(context.Background(), Message{ID: string(rune('a' + i)), Source: "PVMBG", Status: st, ReceivedAt: now})
	}
	got, err := s.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got[Key{"PVMBG", StatusParked}] != 2 || got[Key{"PVMBG", StatusRetrying}] != 1 || got[Key{"PVMBG", StatusRecovered}] != 1 {
		t.Fatalf("counts %v", got)
	}

	before := testutil.ToFloat64(events.WithLabelValues("BMKG", OutcomeReplayed))
	Observe("BMKG", OutcomeReplayed)
	if after := testutil.ToFloat64(events.WithLabelValues("BMKG", OutcomeReplayed)); after != before+1 {
		t.Fatalf("counter %v -> %v", before, after)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunGauge(ctx, s, time.Hour, nil)
	if v := testutil.ToFloat64(messages.WithLabelValues("PVMBG", string(StatusParked))); v != 2 {
		t.Fatalf("gauge parked = %v", v)
	}
}
