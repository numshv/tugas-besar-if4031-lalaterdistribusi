package snapshot

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
)

func sample(next int64) *fire.Snapshot {
	return &fire.Snapshot{
		Format: fire.SnapshotFormat, Config: fire.DefaultConfig(), Next: next, Profile: "practice",
		Watermark:  time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC),
		Detections: []fire.SnapshotDetection{{Key: "N|2026-11-14|6|-2.00000|113.00000", Lat: -2, Lon: 113.0000123, Conf: "n", FRP: 3.25, Offset: 1}},
		Emitted:    []fire.SnapshotEmitted{{ID: "fire-1", Hash: "h", Alive: true}},
	}
}

func TestEncodeDecode(t *testing.T) {
	in := sample(42)
	b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.Detections, out.Detections) || out.Next != 42 || !out.Watermark.Equal(in.Watermark) {
		t.Fatalf("round trip changed the snapshot: %+v", out)
	}
	if _, err := Decode([]byte("not gzip")); err == nil {
		t.Fatal("garbage must not decode")
	}
}

// Runs only against a real broker: KAFKA_TEST_BROKERS=localhost:9092 go test ./...
func TestKafkaKeepsTheNewest(t *testing.T) {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	list := strings.Split(brokers, ",")
	topic := fmt.Sprintf("wildfire-snapshot-test-%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", list[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{{ConfigName: "cleanup.policy", ConfigValue: "compact"}, {ConfigName: "max.message.bytes", ConfigValue: "68157440"}},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	k := NewKafka(list, topic)
	defer k.Close()
	if s, err := k.Load(ctx); err != nil || s != nil {
		t.Fatalf("empty topic: %v %v", s, err)
	}
	for _, next := range []int64{10, 20, 30} {
		if _, err := k.Save(ctx, sample(next)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := k.Load(ctx)
	if err != nil || s == nil || s.Next != 30 {
		t.Fatalf("want the newest snapshot (30), got %+v, %v", s, err)
	}
}
