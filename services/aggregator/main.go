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

	mongoStore, err := store.NewMongoStore(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	volcanoResolver, err := store.NewMongoVolcanoResolver(ctx, cfg.MongoURI, cfg.MongoDB)
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

	p := poller.New(log, cfg.PollInterval, httpTimeout, bmkgClient, pvmbgClient,
		func(ctx context.Context, events, warnings []map[string]any) error {
			hazards, err := mapper.MapSeismic(events, warnings, policy)
			if err != nil {
				return err
			}
			return persistAndPublish(ctx, mongoStore, kafkaPub, hazards, log)
		},
		func(ctx context.Context, reports []map[string]any) error {
			hazards, err := mapper.MapVolcanic(reports, volcanoResolver, policy)
			if err != nil {
				return err
			}
			return persistAndPublish(ctx, mongoStore, kafkaPub, hazards, log)
		},
	)

	go p.Run(ctx)

	handler := api.New(mongoStore, p, log, cfg.MaxConcurrency)
	return httpx.ListenAndServe(ctx, ":"+cfg.Port, httpx.Middleware(log)(handler), log)
}

// persistAndPublish SENGAJA simpan dulu baru publish. Kalau publish gagal
// setelah simpan berhasil, data tetap aman di Canonical Store dan akan
// terpublish lagi di siklus poll berikutnya (aman karena upsert idempoten).
// Urutan sebaliknya (publish dulu) berisiko consumer melihat event yang
// ternyata gagal tersimpan.
func persistAndPublish(ctx context.Context, s store.Store, pub publisher.Publisher, events []hazard.Event, log *slog.Logger) error {
	for _, e := range events {
		if err := s.Upsert(ctx, e); err != nil {
			return fmt.Errorf("upsert %s: %w", e.HazardID, err)
		}
		if err := pub.Publish(ctx, e); err != nil {
			// satu event gagal publish tidak menghentikan batch; akan
			// tersusul di siklus poll berikutnya karena upsert idempoten
			log.ErrorContext(ctx, "publish failed, will retry next poll cycle",
				"hazard_id", e.HazardID, "error", err)
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
