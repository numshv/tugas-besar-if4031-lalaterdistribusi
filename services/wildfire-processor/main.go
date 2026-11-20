package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/envx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/feed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/processor"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/snapshot"
)

const service = "wildfire-processor"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	token := os.Getenv("SATELLITE_TOKEN")
	if token == "" {
		return errors.New("SATELLITE_TOKEN is required")
	}
	brokers, err := kafkax.Brokers()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := fire.DefaultConfig()
	cfg.AllowedLateness = time.Duration(envx.Int("ALLOWED_LATENESS_MINUTES", 360)) * time.Minute
	cfg.VersionEpoch = int64(envx.Int("VERSION_EPOCH", 0))

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        envx.String("KAFKA_UPSERTS_TOPIC", "bnpb.hazard-upserts.v1"),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	proc := &processor.Processor{
		Engine: fire.New(cfg),
		Feed: &feed.Client{
			BaseURL:     envx.String("SATELLITE_BASE_URL", "https://satellite.naufarrel.dev"),
			Token:       token,
			HTTP:        &http.Client{},
			IdleTimeout: time.Duration(envx.Int("FEED_IDLE_TIMEOUT_SECONDS", 45)) * time.Second,
		},
		Pub:           kafkaPublisher{w: writer, log: log},
		Log:           log,
		FlushInterval: time.Duration(envx.Int("FLUSH_INTERVAL_MS", 500)) * time.Millisecond,
		FlushEvents:   envx.Int("FLUSH_EVENTS", 500),
		SnapshotEvery: time.Duration(envx.Int("SNAPSHOT_EVERY_SECONDS", 60)) * time.Second,
	}
	if topic := envx.String("SNAPSHOT_TOPIC", "bnpb.wildfire-snapshots.v1"); topic != "none" {
		snaps := snapshot.NewKafka(brokers, topic)
		defer snaps.Close()
		proc.Snapshots = snaps
	}

	var verifier *jwtauth.Verifier
	if url := os.Getenv("AUTH_JWKS_URL"); url != "" {
		keys := jwtauth.NewBackgroundKeyProvider(ctx, url, log, time.Minute)
		verifier = &jwtauth.Verifier{Keys: keys, Issuer: os.Getenv("JWT_ISSUER"), Audience: os.Getenv("JWT_AUDIENCE")}
	}
	auth := jwtauth.RequireInternal(verifier, envx.List("AUDIT_TOKENS"), "hanya BNPB Internal Ops yang boleh melihat data kebakaran internal")

	root := http.NewServeMux()
	root.Handle("GET /metrics", promhttp.Handler())
	root.Handle("/", httpx.Middleware(log)(api.Routes(proc, auth)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := proc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("processor stopped", "error", err)
		}
	}()
	err = httpx.ListenAndServe(ctx, ":"+envx.String("WILDFIRE_PORT", "8086"), root, log)
	// Wait for the last flush and snapshot before the Kafka writers close.
	<-done
	return err
}

type kafkaPublisher struct {
	w   *kafka.Writer
	log *slog.Logger
}

// Publish sends one batch; produced_at is the moment the oldest feed event
// behind each message reached the processor, so the aggregator's store
// latency histogram measures feed arrival until the HazardEvent is stored.
func (p kafkaPublisher) Publish(ctx context.Context, emits []fire.Emit) error {
	msgs := make([]kafka.Message, 0, len(emits))
	cid := correlation.New()
	for _, em := range emits {
		enc, err := hazard.EncodeUpsert(em.Message, hazard.UpsertMeta{CorrelationID: cid, ProducedAt: em.ArrivedAt})
		if err != nil {
			// Retrying cannot fix an invalid message; skip it loudly instead of blocking the rest.
			p.log.Error("wildfire message rejected before sending", "fire_id", em.Message.SourceRefID, "error", err)
			continue
		}
		m := kafka.Message{Key: enc.Key, Value: enc.Value}
		for k, v := range enc.Headers {
			m.Headers = append(m.Headers, kafka.Header{Key: k, Value: []byte(v)})
		}
		msgs = append(msgs, m)
	}
	return p.w.WriteMessages(ctx, msgs...)
}
