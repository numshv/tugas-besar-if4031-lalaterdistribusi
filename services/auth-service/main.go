package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/auth-service/internal/api"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/auth-service/internal/auth"
)

const service = "auth-service"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := auth.Load(); err != nil {
		log.Error("failed to load auth credentials", "error", err)
		os.Exit(1)
	}

	privateKey, err := loadPrivateKey(os.Getenv("JWT_PRIVATE_KEY_PATH"))
	if err != nil {
		log.Error("failed to load JWT private key", "error", err)
		os.Exit(1)
	}
	accessTTL, err := tokenTTL("ACCESS_TOKEN_TTL_SECONDS", 60)
	if err != nil {
		log.Error("invalid access token TTL", "error", err)
		os.Exit(1)
	}
	refreshTTL, err := tokenTTL("REFRESH_TOKEN_TTL_SECONDS", 3600)
	if err != nil {
		log.Error("invalid refresh token TTL", "error", err)
		os.Exit(1)
	}
	statePath := os.Getenv("AUTH_STATE_PATH")
	if statePath == "" {
		statePath = "./auth-state/revoked-refresh-tokens.json"
	}
	issuer, err := auth.NewIssuer(privateKey, accessTTL, refreshTTL, os.Getenv("JWT_ISSUER"), envOrDefault("JWT_AUDIENCE", "client-api"), statePath)
	if err != nil {
		log.Error("failed to initialize token issuer", "error", err)
		os.Exit(1)
	}

	port := os.Getenv("AUTH_PORT")
	if port == "" {
		port = "8084"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))
	handler := api.NewHandler(issuer, log)
	mux.HandleFunc("POST /login", handler.Login)
	mux.HandleFunc("POST /refresh", handler.Refresh)
	mux.HandleFunc("GET /.well-known/jwks.json", handler.JWKS)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(mux), log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("invalid PEM in private key file")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		privateKey, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not RSA")
		}
		return privateKey, nil
	}
	privateKey, pkcs1Err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if pkcs1Err != nil {
		return nil, fmt.Errorf("invalid RSA private key: %w", pkcs1Err)
	}
	return privateKey, nil
}

func tokenTTL(env string, fallbackSeconds int64) (time.Duration, error) {
	raw := os.Getenv(env)
	if raw == "" {
		return time.Duration(fallbackSeconds) * time.Second, nil
	}
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds < 1 || seconds > 315360000 {
		return 0, fmt.Errorf("%s must be between 1 and 315360000 seconds", env)
	}
	return time.Duration(seconds) * time.Second, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
