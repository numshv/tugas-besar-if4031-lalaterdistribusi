package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/audit"
)

const batchSize = 200

type Sender interface {
	Send(ctx context.Context, msgs []hazard.EncodedUpsert) error
}

type KafkaSender struct{ W *kafka.Writer }

func NewKafkaSender(brokers []string, topic string) *KafkaSender {
	return &KafkaSender{W: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    batchSize,
		BatchTimeout: 20 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}}
}

func (k *KafkaSender) Send(ctx context.Context, msgs []hazard.EncodedUpsert) error {
	out := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		km := kafka.Message{Key: m.Key, Value: m.Value}
		for hk, hv := range m.Headers {
			km.Headers = append(km.Headers, kafka.Header{Key: hk, Value: []byte(hv)})
		}
		out = append(out, km)
	}
	return k.W.WriteMessages(ctx, out...)
}

func (k *KafkaSender) Close() error { return k.W.Close() }

type InvalidRow struct {
	Row   int    `json:"row"`
	Error string `json:"error"`
}

type Summary struct {
	Mode         string       `json:"mode"`
	DryRun       bool         `json:"dry_run"`
	Read         int          `json:"read"`
	Valid        int          `json:"valid"`
	Invalid      int          `json:"invalid"`
	Duplicates   int          `json:"duplicate_ids"`
	Sent         int          `json:"sent"`
	Count        int          `json:"expected_count"`
	Checksum     string       `json:"expected_checksum,omitempty"`
	FirstInvalid []InvalidRow `json:"first_invalid,omitempty"`
	Elapsed      string       `json:"elapsed"`
}

type Options struct {
	DryRun        bool
	Strict        bool
	Rate          float64
	CorrelationID string
}

type batcher struct {
	ctx     context.Context
	sender  Sender
	limiter *rate.Limiter
	cid     string
	pending []hazard.EncodedUpsert
	sent    int
}

func newBatcher(ctx context.Context, s Sender, perSecond float64, cid string) *batcher {
	b := &batcher{ctx: ctx, sender: s, cid: cid}
	if perSecond > 0 {
		b.limiter = rate.NewLimiter(rate.Limit(perSecond), batchSize)
	}
	return b
}

func (b *batcher) add(m hazard.UpsertMessage) error {
	enc, err := hazard.EncodeUpsert(m, hazard.UpsertMeta{CorrelationID: b.cid})
	if err != nil {
		return err
	}
	b.pending = append(b.pending, enc)
	if len(b.pending) >= batchSize {
		return b.flush()
	}
	return nil
}

func (b *batcher) flush() error {
	if len(b.pending) == 0 || b.sender == nil {
		b.pending = b.pending[:0]
		return nil
	}
	if b.limiter != nil {
		if err := b.limiter.WaitN(b.ctx, len(b.pending)); err != nil {
			return err
		}
	}
	if err := b.sender.Send(b.ctx, b.pending); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	b.sent += len(b.pending)
	b.pending = b.pending[:0]
	return nil
}

func Load(ctx context.Context, src Source, conv *Converter, sender Sender, opt Options) (Summary, error) {
	start := time.Now()
	sum := Summary{Mode: "load", DryRun: opt.DryRun}
	if opt.DryRun {
		sender = nil
	}
	b := newBatcher(ctx, sender, opt.Rate, cidOr(opt.CorrelationID))
	seen := map[string]struct{}{}
	var ids []string
	for {
		rec, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		sum.Read++
		var msg hazard.UpsertMessage
		if err == nil {
			msg, err = conv.Message(rec)
		}
		if err != nil {
			sum.Invalid++
			if len(sum.FirstInvalid) < 20 {
				sum.FirstInvalid = append(sum.FirstInvalid, InvalidRow{Row: rec.Row, Error: err.Error()})
			}
			if opt.Strict {
				return finish(sum, b, ids, start), fmt.Errorf("row %d: %w (strict mode)", rec.Row, err)
			}
			continue
		}
		sum.Valid++
		if _, dup := seen[msg.SourceRefID]; dup {
			sum.Duplicates++
		} else {
			seen[msg.SourceRefID] = struct{}{}
			ids = append(ids, msg.SourceRefID)
		}
		if err := b.add(msg); err != nil {
			return finish(sum, b, ids, start), err
		}
	}
	if err := b.flush(); err != nil {
		return finish(sum, b, ids, start), err
	}
	return finish(sum, b, ids, start), nil
}

func SendAll(ctx context.Context, mode string, msgs []hazard.UpsertMessage, sender Sender, opt Options) (Summary, error) {
	start := time.Now()
	sum := Summary{Mode: mode, DryRun: opt.DryRun, Read: len(msgs), Valid: len(msgs)}
	if opt.DryRun {
		sender = nil
	}
	b := newBatcher(ctx, sender, opt.Rate, cidOr(opt.CorrelationID))
	for _, m := range msgs {
		if err := b.add(m); err != nil {
			return finish(sum, b, nil, start), err
		}
	}
	err := b.flush()
	return finish(sum, b, nil, start), err
}

func finish(sum Summary, b *batcher, ids []string, start time.Time) Summary {
	sum.Sent = b.sent
	if ids != nil {
		sum.Count = len(ids)
		sum.Checksum = audit.Checksum(ids)
	}
	sum.Elapsed = time.Since(start).Round(time.Millisecond).String()
	return sum
}

func cidOr(cid string) string {
	if cid != "" {
		return cid
	}
	return correlation.New()
}
