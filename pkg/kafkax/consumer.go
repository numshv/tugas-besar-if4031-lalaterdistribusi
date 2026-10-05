package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, log *slog.Logger, msg kafka.Message) error

const maxAttempts = 5

func Run(ctx context.Context, log *slog.Logger, brokers []string, topic, groupID string, h Handler) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    1e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()
	log.Info("consumer_started", "topic", topic, "group", groupID, "brokers", brokers)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("consumer_stopped")
				return
			}
			log.Error("fetch_failed", "err", err)
			time.Sleep(time.Second)
			continue
		}

		mlog := log.With("correlation_id", Header(m, "X-Correlation-ID"),
			"partition", m.Partition, "offset", m.Offset)

		var herr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if herr = h(ctx, mlog, m); herr == nil {
				break
			}
			mlog.Warn("handler_failed", "attempt", attempt, "err", herr)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		if herr != nil {
			mlog.Error("message_dropped_after_retries", "err", herr)
		}
		if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			mlog.Error("commit_failed", "err", err)
		}
	}
}

func Header(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func Brokers() ([]string, error) {
	raw := os.Getenv("KAFKA_BROKERS")
	var out []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("KAFKA_BROKERS kosong: set mis. kafka:9092")
	}
	return out, nil
}

func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type Dedupe struct {
	mu    sync.Mutex
	cap   int
	order []string
	set   map[string]struct{}
}

func NewDedupe(capacity int) *Dedupe {
	return &Dedupe{cap: capacity, set: make(map[string]struct{}, capacity)}
}

func (d *Dedupe) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.set[id]; ok {
		return true
	}
	d.set[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.cap {
		delete(d.set, d.order[0])
		d.order = d.order[1:]
	}
	return false
}
