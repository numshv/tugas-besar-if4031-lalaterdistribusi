package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

// Interface supaya poller bisa diuji dengan fake publisher tanpa Kafka
// sungguhan menyala.
type Publisher interface {
	Publish(ctx context.Context, e hazard.Event) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		// Hash balancer: partisi ditentukan dari Key. Event dengan hazard_id
		// yang sama SELALU jatuh ke partisi yang sama, sehingga urutan versi
		// (mis. severity gempa naik dari SIAGA ke AWAS) terjamin di level
		// broker, tidak bergantung pengecekan manual di consumer saja.
		Balancer: &kafka.Hash{},
		// Sinkron (bukan Async) dan RequireOne: Publish() baru return sukses
		// setelah broker benar-benar menerima, ini yang membuat klaim
		// "at-least-once" di laporan valid, bukan cuma "fire and forget".
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
	msg := kafka.Message{Key: []byte(e.HazardID), Value: payload, Time: time.Now()}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish hazard_id=%s: %w", e.HazardID, err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error { return p.writer.Close() }
