package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/bmkg-mock/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/bmkg-mock/internal/seismic"
)

const (
	service      = "bmkg-mock"
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
	apiKey := os.Getenv("BMKG_API_KEY")
	if apiKey == "" {
		return errors.New("BMKG_API_KEY is required")
	}

	port := envString("BMKG_PORT", "8081")
	delayMin := time.Duration(envInt("BMKG_DELAY_MIN_MS", 50)) * time.Millisecond
	delayMax := time.Duration(envInt("BMKG_DELAY_MAX_MS", 150)) * time.Millisecond
	if delayMax < delayMin {
		return fmt.Errorf("BMKG_DELAY_MAX_MS (%s) must not be below BMKG_DELAY_MIN_MS (%s)", delayMax, delayMin)
	}

	interval := time.Duration(envInt("BMKG_EVENT_INTERVAL_SECONDS", 10)) * time.Second
	if interval <= 0 {
		return errors.New("BMKG_EVENT_INTERVAL_SECONDS must be positive")
	}

	seedCount := envInt("BMKG_SEED_COUNT", minSeedCount)
	if seedCount < minSeedCount {
		log.Warn("seed count raised to contract minimum", "requested", seedCount, "applied", minSeedCount)
		seedCount = minSeedCount
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := seismic.NewStore(seedCount)
	go store.Run(ctx, interval, log)

	log.Info("mock ready",
		"port", port,
		"seed_count", seedCount,
		"event_interval", interval.String(),
		"delay_min", delayMin.String(),
		"delay_max", delayMax.String())

	handler := api.New(store, api.Config{
		Service:  service,
		APIKey:   apiKey,
		DelayMin: delayMin,
		DelayMax: delayMax,
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
