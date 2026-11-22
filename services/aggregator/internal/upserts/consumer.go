package upserts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/publisher"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

var (
	storeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hazard_store_latency_seconds",
		Help:    "Time from produced_at on a hazard-upserts message until the HazardEvent is stored.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
	}, []string{"source"})
	processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hazard_upserts_processed_total",
		Help: "hazard-upserts messages handled, by outcome (applied, stale, republished).",
	}, []string{"source", "op", "result"})
	deadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hazard_upserts_dead_lettered_total",
		Help: "hazard-upserts messages that can never succeed and were parked.",
	}, []string{"reason"})
	retries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hazard_upserts_retries_total",
		Help: "Retries of a hazard-upserts message, by the stage that failed (store, publish).",
	}, []string{"stage"})
	lag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hazard_upserts_lag_messages",
		Help: "Approximate consumer lag reported by the kafka-go reader.",
	})
)

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
}

type DeadLetter interface {
	Park(ctx context.Context, m kafka.Message, reason string, cause error) error
}

type LogDeadLetter struct{ Log *slog.Logger }

func (d LogDeadLetter) Park(ctx context.Context, m kafka.Message, reason string, cause error) error {
	d.Log.ErrorContext(ctx, "hazard_upsert_parked",
		"reason", reason, "error", cause, "topic", m.Topic, "partition", m.Partition, "offset", m.Offset,
		"key", string(m.Key), "payload_bytes", len(m.Value))
	return nil
}

type Consumer struct {
	Reader     Reader
	Store      store.Store
	Publisher  publisher.Publisher
	DeadLetter DeadLetter
	Log        *slog.Logger

	MinBackoff time.Duration
	MaxBackoff time.Duration
	Now        func() time.Time
}

func NewReader(brokers []string, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		GroupBalancers: []kafka.GroupBalancer{kafka.RangeGroupBalancer{}},
		MinBytes:       1,
		MaxBytes:       hazard.MaxUpsertBytes + 64<<10,
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
}

func (c *Consumer) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Consumer) Run(ctx context.Context) {
	c.Log.InfoContext(ctx, "hazard_upserts_consumer_started")
	for {
		m, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.Log.InfoContext(ctx, "hazard_upserts_consumer_stopped")
				return
			}
			c.Log.ErrorContext(ctx, "hazard_upserts_fetch_failed", "error", err)
			if !sleep(ctx, time.Second) {
				return
			}
			continue
		}
		lag.Set(float64(c.Reader.Stats().Lag))
		if err := c.Handle(ctx, m); err != nil {
			return
		}
		if err := c.Reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			c.Log.ErrorContext(ctx, "hazard_upserts_commit_failed", "error", err, "partition", m.Partition, "offset", m.Offset)
		}
	}
}

func (c *Consumer) Handle(ctx context.Context, m kafka.Message) error {
	cid := correlation.FromHeader(kafkax.Header(m, hazard.HeaderCorrelationID))
	if cid == "" {
		cid = correlation.New()
	}
	ctx = correlation.WithID(ctx, cid)
	log := c.Log.With("correlation_id", cid, "partition", m.Partition, "offset", m.Offset)

	msg, err := hazard.DecodeUpsert(m.Value)
	if err != nil {
		return c.park(ctx, m, reasonOf(err), err)
	}
	if msg.Op == hazard.OpPurge {
		return c.purge(ctx, log, m, msg)
	}

	var stored hazard.Event
	var applied bool
	err = c.retry(ctx, log, "store", func() error {
		var werr error
		if msg.Op == hazard.OpRetract {
			stored, applied, werr = c.Store.Retract(ctx, msg.Source, msg.SourceRefID, msg.Version)
		} else {
			stored, applied, werr = c.Store.Upsert(ctx, msg.Event())
		}
		return werr
	})
	if errors.Is(err, store.ErrInvalidEvent) {
		return c.park(ctx, m, "rejected_by_store", err)
	}
	if err != nil {
		return err
	}

	result := "applied"
	if applied {
		c.observeLatency(m, msg.Source)
	} else {
		var current hazard.Event
		err = c.retry(ctx, log, "store", func() error {
			var gerr error
			current, gerr = c.Store.Get(ctx, hazard.NewID(msg.Source, msg.SourceRefID))
			if errors.Is(gerr, store.ErrNotFound) {
				return nil
			}
			return gerr
		})
		if err != nil {
			return err
		}
		if current.HazardID == "" || current.Version != msg.Version {
			processed.WithLabelValues(string(msg.Source), msg.Op, "stale").Inc()
			log.InfoContext(ctx, "hazard_upsert_stale", "hazard_id", current.HazardID, "stored_version", current.Version, "version", msg.Version)
			return nil
		}
		stored, result = current, "republished"
	}

	if err := c.retry(ctx, log, "publish", func() error { return c.Publisher.Publish(ctx, stored) }); err != nil {
		return err
	}
	processed.WithLabelValues(string(msg.Source), msg.Op, result).Inc()
	log.InfoContext(ctx, "hazard_upsert_"+result, "hazard_id", stored.HazardID, "op", msg.Op, "version", stored.Version)
	return nil
}

func (c *Consumer) purge(ctx context.Context, log *slog.Logger, m kafka.Message, msg hazard.UpsertMessage) error {
	var deleted bool
	err := c.retry(ctx, log, "store", func() error {
		var perr error
		deleted, perr = c.Store.Purge(ctx, msg.Source, msg.SourceRefID)
		return perr
	})
	if errors.Is(err, store.ErrInvalidEvent) {
		return c.park(ctx, m, "purge_not_allowed", err)
	}
	if err != nil {
		return err
	}
	result := "purged"
	if !deleted {
		result = "purge_missing"
	}
	processed.WithLabelValues(string(msg.Source), msg.Op, result).Inc()
	log.InfoContext(ctx, "hazard_upsert_"+result, "source_ref_id", msg.SourceRefID)
	return nil
}

func (c *Consumer) observeLatency(m kafka.Message, source hazard.Source) {
	raw := kafkax.Header(m, hazard.HeaderProducedAt)
	if raw == "" {
		return
	}
	producedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return
	}
	storeLatency.WithLabelValues(string(source)).Observe(c.now().Sub(producedAt).Seconds())
}

func (c *Consumer) park(ctx context.Context, m kafka.Message, reason string, cause error) error {
	deadLettered.WithLabelValues(reason).Inc()
	return c.retry(ctx, c.Log, "dead_letter", func() error { return c.DeadLetter.Park(ctx, m, reason, cause) })
}

func reasonOf(err error) string {
	var invalid *hazard.InvalidUpsertError
	if errors.As(err, &invalid) {
		return invalid.Reason
	}
	return "invalid"
}

func (c *Consumer) retry(ctx context.Context, log *slog.Logger, stage string, fn func() error) error {
	backoff := c.MinBackoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	maxBackoff := c.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || errors.Is(err, store.ErrInvalidEvent) {
			return err
		}
		retries.WithLabelValues(stage).Inc()
		log.WarnContext(ctx, "hazard_upsert_retry", "stage", stage, "attempt", attempt, "error", err)
		if !sleep(ctx, backoff) {
			return fmt.Errorf("%s retry stopped: %w", stage, ctx.Err())
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
