package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

type Publisher interface {
	Publish(ctx context.Context, e hazard.Event) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, e hazard.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal hazard event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(e.HazardID),
		Value: payload,
		Time:  time.Now(),
	}

	// correlation ID diteruskan sebagai Kafka header, dibaca balik oleh
	// kafkax.Run (consumer-dashboard, consumer-notifier) lewat
	// kafkax.Header(m, "X-Correlation-ID"), menyambungkan trace
	if id := correlation.FromContext(ctx); id != "" {
		msg.Headers = []kafka.Header{
			{Key: correlation.Header, Value: []byte(id)},
		}
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish hazard_id=%s: %w", e.HazardID, err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error { return p.writer.Close() }
