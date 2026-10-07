package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/pvmbg-mock/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/pvmbg-mock/internal/volcanic"
)

const (
	service      = "pvmbg-mock"
	minSeedCount = 20
)

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	token := os.Getenv("PVMBG_TOKEN")
	if token == "" {
		return errors.New("PVMBG_TOKEN is required")
	}
	if !strings.HasPrefix(token, api.TokenPrefix) {
		return fmt.Errorf("PVMBG_TOKEN must start with %q", api.TokenPrefix)
	}

	adminToken := os.Getenv("PVMBG_ADMIN_TOKEN")
	if adminToken == "" {
		return errors.New("PVMBG_ADMIN_TOKEN is required")
	}
	if adminToken == token {
		return errors.New("PVMBG_ADMIN_TOKEN must differ from PVMBG_TOKEN")
	}

	port := envString("PVMBG_PORT", "8082")
	delayMin := time.Duration(envInt("PVMBG_DELAY_MIN_MS", 500)) * time.Millisecond
	delayMax := time.Duration(envInt("PVMBG_DELAY_MAX_MS", 3000)) * time.Millisecond
	if delayMax < delayMin {
		return fmt.Errorf("PVMBG_DELAY_MAX_MS (%s) must not be below PVMBG_DELAY_MIN_MS (%s)", delayMax, delayMin)
	}

	outageMode := envString("PVMBG_OUTAGE_MODE", api.OutageModeError)
	if outageMode != api.OutageModeError && outageMode != api.OutageModeTimeout {
		return fmt.Errorf("PVMBG_OUTAGE_MODE must be %q or %q", api.OutageModeError, api.OutageModeTimeout)
	}

	schemaVersion := envInt("PVMBG_SCHEMA_VERSION", volcanic.SchemaLegacy)
	if schemaVersion != volcanic.SchemaLegacy && schemaVersion != volcanic.SchemaConfidence {
		return fmt.Errorf("PVMBG_SCHEMA_VERSION must be %d or %d", volcanic.SchemaLegacy, volcanic.SchemaConfidence)
	}

	interval := time.Duration(envInt("PVMBG_EVENT_INTERVAL_SECONDS", 10)) * time.Second
	if interval <= 0 {
		return errors.New("PVMBG_EVENT_INTERVAL_SECONDS must be positive")
	}

	seedCount := envInt("PVMBG_SEED_COUNT", minSeedCount)
	if seedCount < minSeedCount {
		log.Warn("seed count raised to contract minimum", "requested", seedCount, "applied", minSeedCount)
		seedCount = minSeedCount
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := volcanic.NewStore(seedCount)
	go store.Run(ctx, interval, log)

	log.Info("mock ready",
		"run_id", store.RunID(),
		"port", port,
		"seed_count", seedCount,
		"report_interval", interval.String(),
		"delay_min", delayMin.String(),
		"delay_max", delayMax.String(),
		"outage_mode", outageMode,
		"schema_version", schemaVersion)

	handler := api.New(store, api.Config{
		Service:       service,
		Token:         token,
		AdminToken:    adminToken,
		DelayMin:      delayMin,
		DelayMax:      delayMax,
		OutageMode:    outageMode,
		SchemaVersion: schemaVersion,
	}, log)

	return httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(handler), log)
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
