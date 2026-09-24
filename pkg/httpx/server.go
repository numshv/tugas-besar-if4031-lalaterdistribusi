package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
)

func Middleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := correlation.FromHeader(r.Header.Get(correlation.Header))
			ctx := correlation.WithID(r.Context(), id)
			w.Header().Set(correlation.Header, id)

			rec := &recorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()

			defer func() {
				if p := recover(); p != nil {
					log.ErrorContext(ctx, "handler panicked",
						"panic", p, "stack", string(debug.Stack()))
					if !rec.wrote {
						Error(rec, http.StatusInternalServerError, "internal_error", "internal server error")
					}
				}
				log.InfoContext(ctx, "request handled",
					"method", r.Method,
					"path", r.URL.Path,
					"status", rec.status,
					"duration_ms", msSince(start))
			}()

			next.ServeHTTP(rec, r.WithContext(ctx))
		})
	}
}

func ListenAndServe(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("listen %s: %w", addr, err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	log.Info("shutting down http server")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown %s: %w", addr, err)
	}
	return nil
}

func Health(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": service})
	}
}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response body", "error", err)
	}
}

func Error(w http.ResponseWriter, status int, code, message string) {
	JSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

type recorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *recorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.status, r.wrote = status, true
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
