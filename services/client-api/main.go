package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

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

	publicKey, err := loadPublicKey(os.Getenv("AUTH_PUBLIC_KEY_PATH"))
	if err != nil {
		log.Error("failed to load auth public key", "error", err)
		os.Exit(1)
	}

	handler := api.NewHandler(os.Getenv("AGGREGATOR_BASE_URL"), log)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	mux.Handle("GET /hazards", api.RequireAuth(publicKey)(http.HandlerFunc(handler.ListHazardsHandler)))

	maxConcurrency := getEnvInt("CLIENT_API_MAX_CONCURRENCY", 150)
	limited := api.LimitConcurrency(maxConcurrency)(mux)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(limited), log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func loadPublicKey(path string) (*rsa.PublicKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("invalid PEM in public key file")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("key is not RSA")
	}
	return rsaPub, nil
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