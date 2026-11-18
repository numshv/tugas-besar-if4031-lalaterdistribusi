// Package snapshot keeps the latest engine snapshot in a compacted Kafka
// topic. Kafka already runs in the cluster and is not tied to the node the
// processor runs on, so a rescheduled pod finds the snapshot wherever it lands.
// The snapshot only saves time: without one the engine replays the feed.
package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
)

// MaxBytes bounds one snapshot message; the topic's max.message.bytes must be
// at least this.
const MaxBytes = 64 << 20

const key = "wildfire-processor"

func Encode(s *fire.Snapshot) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := gob.NewEncoder(zw).Encode(s); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	if buf.Len() > MaxBytes {
		return nil, fmt.Errorf("snapshot is %d bytes, more than %d", buf.Len(), MaxBytes)
	}
	return buf.Bytes(), nil
}

func Decode(b []byte) (*fire.Snapshot, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	var s fire.Snapshot
	if err := gob.NewDecoder(io.LimitReader(zr, 1<<30)).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

type Kafka struct {
	Brokers []string
	Topic   string
	w       *kafka.Writer
}

func NewKafka(brokers []string, topic string) *Kafka {
	return &Kafka{Brokers: brokers, Topic: topic, w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		BatchBytes:   MaxBytes + 1<<20,
		BatchTimeout: time.Millisecond,
		WriteTimeout: 30 * time.Second,
	}}
}

func (k *Kafka) Close() error { return k.w.Close() }

func (k *Kafka) Save(ctx context.Context, s *fire.Snapshot) (int, error) {
	b, err := Encode(s)
	if err != nil {
		return 0, err
	}
	return len(b), k.w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: b})
}

// Load returns the newest snapshot, or nil when the topic is empty.
func (k *Kafka) Load(ctx context.Context) (*fire.Snapshot, error) {
	var lastErr error
	for _, broker := range k.Brokers {
		s, err := k.loadFrom(ctx, broker)
		if err == nil {
			return s, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (k *Kafka) loadFrom(ctx context.Context, broker string) (*fire.Snapshot, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, k.Topic, 0)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	last, err := conn.ReadLastOffset()
	if err != nil {
		return nil, err
	}
	first, err := conn.ReadFirstOffset()
	if err != nil {
		return nil, err
	}
	if last <= first {
		return nil, nil
	}
	if _, err := conn.Seek(last-1, kafka.SeekAbsolute); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	batch := conn.ReadBatch(1, MaxBytes+1<<20)
	defer batch.Close()
	var newest []byte
	for {
		m, err := batch.ReadMessage()
		if err != nil {
			if errors.Is(err, io.EOF) || newest != nil {
				break
			}
			return nil, err
		}
		if string(m.Key) == key {
			newest = m.Value
		}
	}
	if newest == nil {
		return nil, nil
	}
	return Decode(newest)
}
