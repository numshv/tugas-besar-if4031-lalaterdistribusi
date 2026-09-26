package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
)

const service = "client-api"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))

	port := os.Getenv("CLIENT_API_PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health(service))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(mux), log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
