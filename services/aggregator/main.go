package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/poller"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/publisher"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

const service = "aggregator"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoClient, err := store.NewMongoClient(ctx, cfg.MongoURI)
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	mongoStore := store.NewMongoStore(mongoClient, cfg.MongoDB)
	volcanoResolver, err := store.NewMongoVolcanoResolver(ctx, mongoClient, cfg.MongoDB)
	if err != nil {
		return fmt.Errorf("init volcano resolver: %w", err)
	}

	kafkaPub := publisher.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer kafkaPub.Close()

	policy := mapper.ParsePolicy(cfg.UnknownFieldPolicy)
	httpTimeout := cfg.HTTPTimeout

	bmkgClient := poller.SourceClient{
		HTTP:    httpx.NewClient(cfg.BMKGBaseURL, httpTimeout, log),
		BaseURL: cfg.BMKGBaseURL,
		Header:  http.Header{"X-BMKG-Key": {cfg.BMKGAPIKey}},
	}
	pvmbgClient := poller.SourceClient{
		HTTP:    httpx.NewClient(cfg.PVMBGBaseURL, httpTimeout, log),
		BaseURL: cfg.PVMBGBaseURL,
		Header:  http.Header{"Authorization": {"Bearer " + cfg.PVMBGToken}},
	}

	retryQ := newPublishRetryQueue(500)

	p := poller.New(log, cfg.PollInterval, httpTimeout, bmkgClient, pvmbgClient,
		func(ctx context.Context, events, warnings []map[string]any) error {
			hazards, mapErrs := mapper.MapSeismic(events, warnings, policy)
			return persistAndPublish(ctx, mongoStore, kafkaPub, retryQ, hazards, mapErrs, log)
		},
		func(ctx context.Context, reports []map[string]any) error {
			hazards, mapErrs := mapper.MapVolcanic(reports, volcanoResolver, policy)
			return persistAndPublish(ctx, mongoStore, kafkaPub, retryQ, hazards, mapErrs, log)
		},
	)

	go p.Run(ctx)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				retryQ.flush(ctx, kafkaPub, log)
			}
		}
	}()

	handler := api.New(mongoStore, p, log, cfg.MaxConcurrency)
	return httpx.ListenAndServe(ctx, ":"+cfg.Port, httpx.Middleware(log)(handler), log)
}

func persistAndPublish(ctx context.Context, s store.Store, pub publisher.Publisher, retryQ *publishRetryQueue, events []hazard.Event, mapErrs []error, log *slog.Logger) error {
	for _, me := range mapErrs {
		log.WarnContext(ctx, "skipping malformed record", "error", me)
	}

	for _, e := range events {
		if err := s.Upsert(ctx, e); err != nil {
			return fmt.Errorf("upsert %s: %w", e.HazardID, err)
		}
		if err := pub.Publish(ctx, e); err != nil {
			log.ErrorContext(ctx, "publish failed, queued for retry", "hazard_id", e.HazardID, "error", err)
			retryQ.enqueue(e, log)
			continue
		}
	}
	return nil
}

type config struct {
	Port               string
	PollInterval       time.Duration
	HTTPTimeout        time.Duration
	MaxConcurrency     int
	UnknownFieldPolicy string

	BMKGBaseURL  string
	BMKGAPIKey   string
	PVMBGBaseURL string
	PVMBGToken   string

	MongoURI string
	MongoDB  string

	KafkaBrokers []string
	KafkaTopic   string
}

func loadConfig() (config, error) {
	cfg := config{
		Port:               envString("AGGREGATOR_PORT", "8083"),
		PollInterval:       envSeconds("POLL_INTERVAL_SECONDS", 3),
		HTTPTimeout:        envMillis("HTTP_CLIENT_TIMEOUT_MS", 5000),
		MaxConcurrency:     envInt("AGG_MAX_CONCURRENCY", 64),
		UnknownFieldPolicy: envString("UNKNOWN_FIELD_POLICY", "passthrough"),
		BMKGBaseURL:        os.Getenv("BMKG_BASE_URL"),
		BMKGAPIKey:         os.Getenv("BMKG_API_KEY"),
		PVMBGBaseURL:       os.Getenv("PVMBG_BASE_URL"),
		PVMBGToken:         os.Getenv("PVMBG_TOKEN"),
		MongoURI:           os.Getenv("MONGO_URI"),
		MongoDB:            os.Getenv("MONGO_DB"),
		KafkaBrokers:       strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		KafkaTopic:         os.Getenv("KAFKA_TOPIC"),
	}
	// fail fast: jangan sampai service nyala tapi diam-diam tidak bisa
	// otentikasi ke BMKG/PVMBG, ketahuan baru saat poll pertama gagal
	if cfg.BMKGAPIKey == "" {
		return cfg, errors.New("BMKG_API_KEY is required")
	}
	if cfg.PVMBGToken == "" {
		return cfg, errors.New("PVMBG_TOKEN is required")
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
func envSeconds(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Second
}
func envMillis(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Millisecond
}

type publishRetryQueue struct {
	mu    sync.Mutex
	items []hazard.Event
	max   int
}

func newPublishRetryQueue(max int) *publishRetryQueue {
	return &publishRetryQueue{max: max}
}

func (q *publishRetryQueue) enqueue(e hazard.Event, log *slog.Logger) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.max {
		log.Warn("publish retry queue full, dropping oldest pending event",
			"dropped_hazard_id", q.items[0].HazardID)
		q.items = q.items[1:]
	}
	q.items = append(q.items, e)
}

func (q *publishRetryQueue) flush(ctx context.Context, pub publisher.Publisher, log *slog.Logger) {
	q.mu.Lock()
	pending := q.items
	q.items = nil
	q.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	var stillFailing []hazard.Event
	for _, e := range pending {
		if err := pub.Publish(ctx, e); err != nil {
			log.WarnContext(ctx, "retry publish failed again", "hazard_id", e.HazardID, "error", err)
			stillFailing = append(stillFailing, e)
			continue
		}
		log.InfoContext(ctx, "retry publish succeeded", "hazard_id", e.HazardID)
	}

	if len(stillFailing) > 0 {
		q.mu.Lock()
		q.items = append(stillFailing, q.items...)
		q.mu.Unlock()
	}
}
