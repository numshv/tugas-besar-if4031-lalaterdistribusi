package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/envx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/analytics"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/audit"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/fieldreports"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/poller"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/publisher"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/schema"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/storemon"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/upserts"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	if os.Getenv("STORE_EXPERIMENTS") == "on" {
		mongoStore.WithExperiments(true)
		log.Warn("STORE_EXPERIMENTS=on: every write is shadowed into exp_by_area and exp_by_ingested")
	}
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

	var members audit.Members
	if cfg.Topology != "" {
		units, monitor, err := startStoreMonitor(ctx, cfg, mongoStore, log)
		if err != nil {
			return fmt.Errorf("start store monitor: %w", err)
		}
		members = monitor
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			units.Close(closeCtx)
		}()
	} else {
		log.Info("TOPOLOGY_JSON not set, storage health monitor disabled")
	}

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

	go runPollers(ctx, log, p, mongoStore)
	go pipe.RunRetrier(ctx, 2*time.Second)

	fieldStore := fieldreports.NewMongoStore(mongoClient, cfg.MongoDB)
	if cfg.FieldReportsTopic != "" {
		consumer := &fieldreports.Consumer{Store: fieldStore, DLQ: dlqStore, Now: time.Now}
		go kafkax.RunNoDrop(ctx, log, cfg.KafkaBrokers, cfg.FieldReportsTopic, "aggregator.field-reports", consumer.Handle)
	}

	if cfg.UpsertsTopic != "" {
		reader := upserts.NewReader(cfg.KafkaBrokers, cfg.UpsertsTopic, cfg.UpsertsGroup)
		defer reader.Close()
		consumer := &upserts.Consumer{
			Reader:     reader,
			Store:      mongoStore,
			Publisher:  kafkaPub,
			DeadLetter: upserts.LogDeadLetter{Log: log},
			Log:        log.With("topic", cfg.UpsertsTopic, "group", cfg.UpsertsGroup),
		}
		go consumer.Run(ctx)
	} else {
		log.Info("KAFKA_UPSERTS_TOPIC not set, hazard-upserts consumer disabled")
	}

	ops := &api.Ops{DLQ: dlqStore, Replayer: pipe, Retractor: pipe, AuditTokens: cfg.AuditTokens, FieldReports: fieldStore}
	if cfg.JWKSURL != "" {
		keys := jwtauth.NewBackgroundKeyProvider(ctx, cfg.JWKSURL, log, time.Minute)
		ops.Verifier = &jwtauth.Verifier{Keys: keys, Issuer: cfg.JWTIssuer, Audience: cfg.JWTAudience}
	}

	auditAPI := audit.New(mongoStore, mongoStore, members, audit.Config{
		Tokens:           cfg.AuditTokens,
		IncludeRetracted: cfg.AuditIncludeRetracted,
		PartialMode:      cfg.PartialMode,
	}, log.With("component", "audit"))
	auditAPI.Register("GET /internal/audit/unprocessed", ops.ListUnprocessed)
	if len(cfg.AuditTokens) == 0 {
		log.Warn("AUDIT_TOKENS not set, audit API answers 503 until a token is configured")
	}

	handler := api.New(mongoStore, p, log, cfg.MaxConcurrency, ops)
	root := http.NewServeMux()
	root.Handle("GET /metrics", promhttp.Handler())
	root.Handle("/internal/audit/", httpx.Middleware(log)(auditAPI))
	dashboard := analytics.NewHandler(mongoStore, cfg.AuditIncludeRetracted, log.With("component", "analytics")).
		Routes(jwtauth.RequireScopes(ops.Verifier, []string{jwtauth.ScopeAnalytics, jwtauth.ScopeInternal},
			"dashboard analitik hanya untuk analytics-viewer atau BNPB Internal Ops"))
	root.Handle("/analytics", httpx.Middleware(log)(dashboard))
	root.Handle("/analytics/", httpx.Middleware(log)(dashboard))
	root.Handle("/", httpx.Middleware(log)(handler))
	return httpx.ListenAndServe(ctx, ":"+cfg.Port, root, log)
}

func startStoreMonitor(ctx context.Context, cfg config, s *store.MongoStore, log *slog.Logger) (*storemon.MongoUnits, *storemon.Monitor, error) {
	topology, err := storemon.ParseTopology(cfg.Topology)
	if err != nil {
		return nil, nil, err
	}
	units := storemon.NewMongoUnits(cfg.MonitorUser, cfg.MonitorPassword, cfg.MongoDB)
	nodes := storemon.NodesFromEnvironment(ctx, cfg.Namespace, log)
	monitor := storemon.NewMonitor(topology.Units(), units, nodes, 2*time.Second, time.Second)
	monitor.ProbeOnce(ctx)
	go monitor.Run(ctx)

	collector := storemon.NewCollector(monitor, units, 10*time.Second, 3*time.Second)
	prometheus.MustRegister(collector)
	go collector.Run(ctx)

	s.WithHealth(monitor, cfg.PartialMode)
	log.Info("storage health monitor started", "units", len(topology.Units()), "partial_mode", cfg.PartialMode)
	return units, monitor, nil
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

	Topology        string
	MonitorUser     string
	MonitorPassword string
	PartialMode     store.PartialMode
	Namespace       string

	AuditTokens           []string
	AuditIncludeRetracted bool

	KafkaBrokers []string
	KafkaTopic   string
	UpsertsTopic string
	UpsertsGroup string

	MappingRulesFile  string
	FieldReportsTopic string
	JWKSURL           string
	JWTIssuer         string
	JWTAudience       string
}

func loadConfig() (config, error) {
	cfg := config{
		Port:                  envx.String("AGGREGATOR_PORT", "8083"),
		PollInterval:          envx.Seconds("POLL_INTERVAL_SECONDS", 3),
		HTTPTimeout:           envx.Millis("HTTP_CLIENT_TIMEOUT_MS", 5000),
		MaxConcurrency:        envx.Int("AGG_MAX_CONCURRENCY", 64),
		UnknownFieldPolicy:    envx.String("UNKNOWN_FIELD_POLICY", "passthrough"),
		BMKGBaseURL:           os.Getenv("BMKG_BASE_URL"),
		BMKGAPIKey:            os.Getenv("BMKG_API_KEY"),
		PVMBGBaseURL:          os.Getenv("PVMBG_BASE_URL"),
		PVMBGToken:            os.Getenv("PVMBG_TOKEN"),
		MongoURI:              os.Getenv("MONGO_URI"),
		MongoDB:               os.Getenv("MONGO_DB"),
		KafkaBrokers:          envx.List("KAFKA_BROKERS"),
		KafkaTopic:            os.Getenv("KAFKA_TOPIC"),
		UpsertsTopic:          os.Getenv("KAFKA_UPSERTS_TOPIC"),
		UpsertsGroup:          envx.String("KAFKA_UPSERTS_GROUP", "bnpb.aggregator.hazard-upserts"),
		Topology:              os.Getenv("TOPOLOGY_JSON"),
		MonitorUser:           os.Getenv("MONGO_MONITOR_USERNAME"),
		MonitorPassword:       os.Getenv("MONGO_MONITOR_PASSWORD"),
		Namespace:             os.Getenv("POD_NAMESPACE"),
		AuditTokens:           envx.List("AUDIT_TOKENS"),
		AuditIncludeRetracted: os.Getenv("AUDIT_INCLUDE_RETRACTED") == "true",
		MappingRulesFile:      os.Getenv("MAPPING_RULES_FILE"),
		FieldReportsTopic:     os.Getenv("FIELD_REPORTS_TOPIC"),
		JWKSURL:               os.Getenv("AUTH_JWKS_URL"),
		JWTIssuer:             os.Getenv("JWT_ISSUER"),
		JWTAudience:           os.Getenv("JWT_AUDIENCE"),
	}
	mode, err := store.ParsePartialMode(os.Getenv("STORE_PARTIAL_MODE"))
	if err != nil {
		return cfg, err
	}
	cfg.PartialMode = mode
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
