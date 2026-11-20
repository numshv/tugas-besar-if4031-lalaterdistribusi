// field-receiver stands in for the Tim Lapangan notification receiver: it records every
// critical coastal alert once and measures how long delivery took.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
)

const (
	service          = "field-receiver"
	headerIngestedAt = "X-Ingested-At"
)

var (
	received = promauto.NewCounter(prometheus.CounterOpts{
		Name: "critical_alert_received_total", Help: "Distinct critical alerts received.",
	})
	duplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "critical_alert_duplicates_total", Help: "Critical alerts received again and ignored.",
	})
	latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "critical_alert_latency_seconds",
		Help:    "From the ingester first reading a critical observation to this receiver getting it.",
		Buckets: []float64{.1, .25, .5, 1, 2, 3, 5, 10, 30, 60},
	})
)

type Alert struct {
	ObservationID string    `json:"observation_id"`
	StationID     string    `json:"station_id"`
	ObservedAt    time.Time `json:"observed_at"`
	Verified      bool      `json:"verified"`
	IngestedAt    time.Time `json:"ingested_at"`
	ReceivedAt    time.Time `json:"received_at"`
	LatencyMS     int64     `json:"latency_ms"`
}

type inbox struct {
	mu     sync.Mutex
	byID   map[string]Alert
	alerts []Alert
}

func (b *inbox) handle(_ context.Context, log *slog.Logger, m kafka.Message) error {
	var a Alert
	if err := json.Unmarshal(m.Value, &a); err != nil || a.ObservationID == "" {
		log.Error("unreadable critical alert, skipped", "error", err)
		return nil
	}
	a.ReceivedAt = time.Now().UTC()
	if t, err := time.Parse(time.RFC3339Nano, kafkax.Header(m, headerIngestedAt)); err == nil {
		a.IngestedAt = t
		a.LatencyMS = a.ReceivedAt.Sub(t).Milliseconds()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, seen := b.byID[a.ObservationID]; seen {
		duplicates.Inc()
		return nil
	}
	b.byID[a.ObservationID] = a
	b.alerts = append(b.alerts, a)
	received.Inc()
	if !a.IngestedAt.IsZero() {
		latency.Observe(a.ReceivedAt.Sub(a.IngestedAt).Seconds())
	}
	log.Warn("critical alert received", "observation_id", a.ObservationID, "station_id", a.StationID, "latency_ms", a.LatencyMS)
	return nil
}

func (b *inbox) list(w http.ResponseWriter, _ *http.Request) {
	b.mu.Lock()
	out := append([]Alert(nil), b.alerts...)
	b.mu.Unlock()
	httpx.JSON(w, http.StatusOK, map[string]any{"count": len(out), "items": out})
}

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	brokers, err := kafkax.Brokers()
	if err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	box := &inbox{byID: map[string]Alert{}}
	topic := os.Getenv("KAFKA_CRITICAL_TOPIC")
	if topic == "" {
		topic = "bnpb.critical-alerts.v1"
	}
	// Wait for the consumer on shutdown: closing it leaves the group at once, so a
	// replacement pod reads alerts right away instead of after the session timeout.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		kafkax.RunNoDrop(ctx, log, brokers, topic, "bnpb.field-receiver", box.handle)
	}()
	defer func() {
		select {
		case <-consumerDone:
		case <-time.After(10 * time.Second):
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	mux.HandleFunc("GET /alerts", box.list)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8089"
	}
	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(mux), log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
