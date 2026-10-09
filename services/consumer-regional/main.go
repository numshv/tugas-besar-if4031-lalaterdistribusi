package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/segmentio/kafka-go"
)

const service = "consumer-regional"

type event struct {
	HazardID   string    `json:"hazard_id"`
	Source     string    `json:"source"`
	HazardType string    `json:"hazard_type"`
	Severity   string    `json:"severity"`
	AreaName   string    `json:"area_name"`
	OccurredAt time.Time `json:"occurred_at"`
	IngestedAt time.Time `json:"ingested_at"`
}

// feed menyimpan event terakhir per wilayah (area_name), simulating
// feed yang diteruskan ke portal pemda wilayah terkait.
type feed struct {
	mu           sync.RWMutex
	byArea       map[string]event
	total        int
	lastReceived time.Time
}

func (f *feed) apply(e event) string {
	area := strings.TrimSpace(e.AreaName)
	if area == "" {
		area = "UNKNOWN"
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byArea[area] = e
	f.total++
	f.lastReceived = time.Now().UTC()
	return area
}

func (f *feed) snapshot() map[string]any {
	f.mu.RLock()
	defer f.mu.RUnlock()
	areas := make(map[string]event, len(f.byArea))
	for area, e := range f.byArea {
		areas[area] = e
	}
	return map[string]any{
		"total_received": f.total,
		"last_received":  f.lastReceived,
		"area_count":     len(areas),
		"areas":          areas,
	}
}

func (f *feed) byAreaName(area string) (event, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.byArea[area]
	return e, ok
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

	fd := &feed{byArea: make(map[string]event)}
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
		area := fd.apply(item)
		mlog.Info("regional_feed_forwarded", "hazard_id", item.HazardID, "area_name", area,
			"severity", item.Severity, "type", item.HazardType)
		return nil
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		kafkax.Run(ctx, log, brokers,
			kafkax.Env("KAFKA_TOPIC", "bnpb.hazard-events.v1"),
			kafkax.Env("KAFKA_GROUP_ID", "regional"), handler)
	}()

	port := os.Getenv("CONSUMER_REGIONAL_PORT")
	if port == "" {
		port = "8087"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	mux.HandleFunc("GET /regional", func(w http.ResponseWriter, r *http.Request) {
		if area := r.URL.Query().Get("area"); area != "" {
			e, ok := fd.byAreaName(area)
			if !ok {
				httpx.Error(w, http.StatusNotFound, "area_not_found", "no hazard event received for this area")
				return
			}
			httpx.JSON(w, http.StatusOK, e)
			return
		}
		httpx.JSON(w, http.StatusOK, fd.snapshot())
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
