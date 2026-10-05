package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/segmentio/kafka-go"
)

const service = "consumer-notifier"

type event struct {
	HazardID   string    `json:"hazard_id"`
	Source     string    `json:"source"`
	HazardType string    `json:"hazard_type"`
	Severity   string    `json:"severity"`
	AreaName   string    `json:"area_name"`
	OccurredAt time.Time `json:"occurred_at"`
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

	delayMs := 0
	if rawDelay := os.Getenv("NOTIFIER_SEND_DELAY_MS"); rawDelay != "" {
		parsedDelay, err := strconv.Atoi(rawDelay)
		if err != nil || parsedDelay < 0 {
			log.Warn("invalid notifier delay; using zero", "value", rawDelay)
		} else {
			delayMs = parsedDelay
		}
	}
	dedupe := kafkax.NewDedupe(10000)
	handler := func(ctx context.Context, mlog *slog.Logger, m kafka.Message) error {
		var item event
		if err := json.Unmarshal(m.Value, &item); err != nil {
			mlog.Error("poison_message_skipped", "err", err)
			return nil
		}
		if item.HazardID == "" {
			mlog.Error("poison_message_skipped", "err", "missing hazard_id")
			return nil
		}
		if item.Severity != "SIAGA" && item.Severity != "AWAS" {
			mlog.Debug("below_threshold_skipped", "hazard_id", item.HazardID, "severity", item.Severity)
			return nil
		}

		start := time.Now()
		if delayMs > 0 {
			timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else if err := ctx.Err(); err != nil {
			return err
		}
		if dedupe.Seen(item.HazardID) {
			mlog.Info("duplicate_skipped", "hazard_id", item.HazardID)
			return nil
		}
		mlog.Info("notification_sent",
			"hazard_id", item.HazardID, "severity", item.Severity,
			"area", item.AreaName, "type", item.HazardType,
			"latency_ms", time.Since(start).Milliseconds())
		return nil
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		kafkax.Run(ctx, log, brokers,
			kafkax.Env("KAFKA_TOPIC", "bnpb.hazard-events.v1"),
			kafkax.Env("KAFKA_GROUP_ID", "bnpb.notifier"), handler)
	}()

	port := os.Getenv("CONSUMER_NOTIFIER_PORT")
	if port == "" {
		port = "8086"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(mux), log); err != nil {
		log.Error("server stopped", "error", err)
		cancel()
		<-consumerDone
		os.Exit(1)
	}
	cancel()
	<-consumerDone
}
