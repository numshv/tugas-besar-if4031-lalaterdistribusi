package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/lease"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/coastal-ingester/internal/ingest"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/coastal-ingester/internal/sensor"
)

const service = "coastal-ingester"

var breakerState = map[string]float64{"closed": 0, "half-open": 1, "open": 2}

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	token := os.Getenv("PANTAI_AUTH_TOKEN")
	if token == "" {
		return errors.New("PANTAI_AUTH_TOKEN is required")
	}
	brokers, err := kafkax.Brokers()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := sensor.New(sensor.Config{
		BaseURL:         env("SENSOR_BASE_URL", "http://sensor-pantai:8080"),
		Token:           token,
		RequestTimeout:  millis("SENSOR_REQUEST_TIMEOUT_MS", 2000),
		MaxRetries:      envInt("SENSOR_MAX_RETRIES", 2),
		BreakerFailures: uint(envInt("SENSOR_BREAKER_FAILURES", 5)),
		BreakerDelay:    millis("SENSOR_BREAKER_OPEN_MS", 10000),
		MaxConcurrency:  uint(envInt("SENSOR_MAX_CONCURRENCY", 2)),
		StuckAfter:      millis("SENSOR_STUCK_AFTER_MS", 15000),
		StaleAfter:      time.Duration(envInt("SENSOR_STALE_AFTER_SECONDS", 900)) * time.Second,
		OnBreakerChanged: func(state string) {
			breakerGauge.Set(breakerState[state])
			log.Warn("sensor circuit breaker changed", "state", state)
		},
	}, &http.Client{})

	producer := newProducer(brokers)
	defer producer.Close()

	in := ingest.New(ingest.Config{
		CriticalTopic:   env("KAFKA_CRITICAL_TOPIC", "bnpb.critical-alerts.v1"),
		UpsertsTopic:    env("KAFKA_UPSERTS_TOPIC", "bnpb.hazard-upserts.v1"),
		RejectedTopic:   env("KAFKA_REJECTED_TOPIC", "bnpb.sensor-rejected.v1"),
		PollInterval:    millis("SENSOR_POLL_INTERVAL_MS", 1000),
		PageLimit:       envInt("SENSOR_PAGE_LIMIT", 200),
		MaxPagesPerPoll: envInt("SENSOR_MAX_PAGES_PER_POLL", 20),
	}, client, producer, log)

	go lead(ctx, log, in)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) { httpx.JSON(w, http.StatusOK, in.Status()) })
	return httpx.ListenAndServe(ctx, ":"+env("PORT", "8088"), httpx.Middleware(log)(mux), log)
}

// lead lets only one replica talk to the sensor, which sees a single client however
// many replicas run.
func lead(ctx context.Context, log *slog.Logger, in *ingest.Ingester) {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		in.Run(ctx)
		return
	}
	elector, err := lease.InCluster("ingest-sensor-pantai", lease.DefaultTiming, log)
	if err != nil {
		log.Error("leader election unavailable, not polling", "error", err)
		return
	}
	elector.Run(ctx, func(term context.Context, _ int64) { in.Run(term) })
}

type producer struct{ w *kafka.Writer }

func newProducer(brokers []string) *producer {
	return &producer{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}}
}

func (p *producer) Produce(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	msg := kafka.Message{Topic: topic, Key: key, Value: value, Time: time.Now()}
	for k, v := range headers {
		if v != "" {
			msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
		}
	}
	msg.Headers = kafkax.InjectTrace(ctx, msg.Headers)
	return p.w.WriteMessages(ctx, msg)
}

func (p *producer) Close() error { return p.w.Close() }

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return fallback
}

func millis(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Millisecond
}
