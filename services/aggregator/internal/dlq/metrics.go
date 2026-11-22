package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	events = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aggregator_dlq_events_total",
		Help: "Dead letter queue events on this replica, by source and outcome (parked, retrying, recovered, replayed).",
	}, []string{"source", "outcome"})
	messages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aggregator_dlq_messages",
		Help: "Messages in the unprocessed collection by source and status. Every replica reports the same collection: use max, not sum.",
	}, []string{"source", "status"})
)

const OutcomeReplayed = "replayed"

// Observe counts one dead letter queue event.
func Observe(source, outcome string) { events.WithLabelValues(source, outcome).Inc() }

type Key struct {
	Source string
	Status Status
}

type Counter interface {
	Counts(ctx context.Context) (map[Key]int, error)
}

// RunGauge refreshes aggregator_dlq_messages from the store until ctx ends.
func RunGauge(ctx context.Context, c Counter, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		counts, err := c.Counts(qctx)
		cancel()
		if err != nil {
			log.WarnContext(ctx, "dead letter queue count failed", "error", err)
		} else {
			messages.Reset()
			for k, n := range counts {
				messages.WithLabelValues(k.Source, string(k.Status)).Set(float64(n))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *MongoStore) Counts(ctx context.Context) (map[Key]int, error) {
	cur, err := s.col.Aggregate(ctx, bson.A{
		bson.M{"$group": bson.M{"_id": bson.M{"source": "$source", "status": "$status"}, "n": bson.M{"$sum": 1}}},
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := map[Key]int{}
	for cur.Next(ctx) {
		var row struct {
			ID struct {
				Source string `bson:"source"`
				Status Status `bson:"status"`
			} `bson:"_id"`
			N int `bson:"n"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, err
		}
		out[Key{row.ID.Source, row.ID.Status}] = row.N
	}
	return out, cur.Err()
}

func (s *MemoryStore) Counts(context.Context) (map[Key]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[Key]int{}
	for _, m := range s.msgs {
		out[Key{m.Source, m.Status}]++
	}
	return out, nil
}
