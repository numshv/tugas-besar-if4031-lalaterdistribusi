package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/client-api/internal/api"
)

const service = "client-api"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))

	port := os.Getenv("CLIENT_API_PORT")
	if port == "" {
		port = "8080"
	}

	// SESUDAH
	issuer := os.Getenv("JWT_ISSUER")
	audience := os.Getenv("JWT_AUDIENCE")
	jwksURL := os.Getenv("AUTH_JWKS_URL")
	if issuer == "" || audience == "" || jwksURL == "" {
		log.Error("JWT_ISSUER, JWT_AUDIENCE, and AUTH_JWKS_URL must be set")
		os.Exit(1)
	}

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	keys, err := api.NewKeyProvider(startupCtx, jwksURL)
	cancelStartup()
	if err != nil {
		log.Error("failed to fetch initial JWKS from auth-service", "error", err)
		os.Exit(1)
	}

	handler := api.NewHandler(os.Getenv("AGGREGATOR_BASE_URL"), log)

	limitedMux := http.NewServeMux()
	limitedMux.Handle("GET /hazards", api.RequireAuth(keys, issuer, audience)(http.HandlerFunc(handler.ListHazardsHandler)))

	maxConcurrency := getEnvInt("CLIENT_API_MAX_CONCURRENCY", 150)

	root := http.NewServeMux()
	root.HandleFunc("GET /health", httpx.Health(service))
	root.Handle("/", api.LimitConcurrency(maxConcurrency)(limitedMux))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	keys.StartAutoRefresh(ctx, log, 10*time.Minute)

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(root), log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return val
}
