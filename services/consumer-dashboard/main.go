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

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/segmentio/kafka-go"
)

const service = "consumer-dashboard"
const maxLatest = 100

type event struct {
	HazardID   string    `json:"hazard_id"`
	Source     string    `json:"source"`
	HazardType string    `json:"hazard_type"`
	Severity   string    `json:"severity"`
	AreaName   string    `json:"area_name"`
	OccurredAt time.Time `json:"occurred_at"`
	IngestedAt time.Time `json:"ingested_at"`
}

type state struct {
	mu         sync.RWMutex
	latest     []event
	bySeverity map[string]int
	byType     map[string]int
	total      int
	lastUpdate time.Time
}

func (s *state) apply(e event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = append([]event{e}, s.latest...)
	if len(s.latest) > maxLatest {
		s.latest = s.latest[:maxLatest]
	}
	s.bySeverity[e.Severity]++
	s.byType[e.HazardType]++
	s.total++
	s.lastUpdate = time.Now().UTC()
}

func (s *state) snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	latest := append([]event(nil), s.latest...)
	bySeverity := make(map[string]int, len(s.bySeverity))
	for severity, count := range s.bySeverity {
		bySeverity[severity] = count
	}
	byType := make(map[string]int, len(s.byType))
	for hazardType, count := range s.byType {
		byType[hazardType] = count
	}
	return map[string]any{
		"total":       s.total,
		"by_severity": bySeverity,
		"by_type":     byType,
		"last_update": s.lastUpdate,
		"latest":      latest,
	}
}

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	brokers, err := kafkax.Brokers()
	if err != nil {
		log.Error("invalid Kafka broker configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	st := &state{
		latest:     make([]event, 0, maxLatest),
		bySeverity: make(map[string]int),
		byType:     make(map[string]int),
	}
	dedupe := kafkax.NewDedupe(10000)
	handler := func(_ context.Context, mlog *slog.Logger, m kafka.Message) error {
		var item event
		if err := json.Unmarshal(m.Value, &item); err != nil {
			mlog.Error("poison_message_skipped", "err", err)
			return nil
		}
		if item.HazardID == "" {
			mlog.Error("poison_message_skipped", "err", "missing hazard_id")
			return nil
		}
		if dedupe.Seen(item.HazardID) {
			mlog.Info("duplicate_skipped", "hazard_id", item.HazardID)
			return nil
		}
		st.apply(item)
		mlog.Info("dashboard_updated", "hazard_id", item.HazardID, "severity", item.Severity, "type", item.HazardType)
		return nil
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		kafkax.Run(ctx, log, brokers,
			kafkax.Env("KAFKA_TOPIC", "bnpb.hazard-events.v1"),
			kafkax.Env("KAFKA_GROUP_ID", "bnpb.dashboard"), handler)
	}()

	port := os.Getenv("CONSUMER_DASHBOARD_PORT")
	if port == "" {
		port = "8085"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, st.snapshot())
	})

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(mux), log); err != nil {
		log.Error("server stopped", "error", err)
		cancel()
		<-consumerDone
		os.Exit(1)
	}
	cancel()
	<-consumerDone
}
