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

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/fieldreports"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/poller"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/publisher"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/schema"
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
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			log.Warn("mongo disconnect failed", "error", err)
		}
	}()

	mongoStore := store.NewMongoStore(mongoClient, cfg.MongoDB)
	indexCtx, cancelIndexes := context.WithTimeout(ctx, 10*time.Second)
	if err := mongoStore.EnsureIndexes(indexCtx); err != nil {
		cancelIndexes()
		return fmt.Errorf("ensure mongo indexes: %w", err)
	}
	cancelIndexes()
	volcanoResolver, err := store.NewMongoVolcanoResolver(ctx, mongoClient, cfg.MongoDB)
	if err != nil {
		return fmt.Errorf("init volcano resolver: %w", err)
	}

	dlqStore := dlq.NewMongoStore(mongoClient, cfg.MongoDB)
	dlqIndexCtx, cancelDLQIndexes := context.WithTimeout(ctx, 10*time.Second)
	if err := dlqStore.EnsureIndexes(dlqIndexCtx); err != nil {
		cancelDLQIndexes()
		return fmt.Errorf("ensure dlq indexes: %w", err)
	}
	cancelDLQIndexes()

	rules, err := schema.Load(cfg.MappingRulesFile)
	if err != nil {
		return err
	}
	log.Info("mapping rules loaded", "file", cfg.MappingRulesFile, "sources", len(rules))

	kafkaPub := publisher.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer kafkaPub.Close()

	policy := mapper.ParsePolicy(cfg.UnknownFieldPolicy)
	httpTimeout := cfg.HTTPTimeout

	pipe := &pipeline.Pipeline{
		Store: mongoStore, Pub: kafkaPub, DLQ: dlqStore, Rules: rules,
		Resolver: volcanoResolver, Policy: policy, Log: log,
	}

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
		pipe.IngestBMKG, pipe.IngestPVMBG)

	go p.Run(ctx)
	go pipe.RunRetrier(ctx, 2*time.Second)

	fieldStore := fieldreports.NewMongoStore(mongoClient, cfg.MongoDB)
	if cfg.FieldReportsTopic != "" {
		consumer := &fieldreports.Consumer{Store: fieldStore, DLQ: dlqStore, Now: time.Now}
		go kafkax.RunNoDrop(ctx, log, cfg.KafkaBrokers, cfg.FieldReportsTopic, "aggregator.field-reports", consumer.Handle)
	}

	ops := &api.Ops{DLQ: dlqStore, Replayer: pipe, AuditToken: cfg.AuditToken, FieldReports: fieldStore}
	if cfg.JWKSURL != "" {
		keys := jwtauth.NewBackgroundKeyProvider(ctx, cfg.JWKSURL, log, time.Minute)
		ops.Verifier = &jwtauth.Verifier{Keys: keys, Issuer: cfg.JWTIssuer, Audience: cfg.JWTAudience}
	}
	if cfg.AuditToken == "" && ops.Verifier == nil {
		log.Warn("neither AUDIT_TOKEN nor AUTH_JWKS_URL is set; dead letter endpoints reject every request")
	}

	handler := api.New(mongoStore, p, log, cfg.MaxConcurrency, ops)
	return httpx.ListenAndServe(ctx, ":"+cfg.Port, httpx.Middleware(log)(handler), log)
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

	MappingRulesFile  string
	FieldReportsTopic string
	AuditToken        string
	JWKSURL           string
	JWTIssuer         string
	JWTAudience       string
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
		MappingRulesFile:   os.Getenv("MAPPING_RULES_FILE"),
		FieldReportsTopic:  os.Getenv("FIELD_REPORTS_TOPIC"),
		AuditToken:         os.Getenv("AUDIT_TOKEN"),
		JWKSURL:            os.Getenv("AUTH_JWKS_URL"),
		JWTIssuer:          os.Getenv("JWT_ISSUER"),
		JWTAudience:        os.Getenv("JWT_AUDIENCE"),
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
