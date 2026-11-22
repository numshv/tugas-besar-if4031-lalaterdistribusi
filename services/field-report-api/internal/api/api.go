package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"log/slog"
	"net/http"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/fieldreport"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
)

var (
	received = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "field_reports_received_total",
		Help: "POST /field-reports answers by result (accepted, missing_key, invalid, too_large, unauthorized, forbidden, kafka_unavailable).",
	}, []string{"result"})
	kafkaAck = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "field_report_kafka_ack_seconds",
		Help:    "Time until Kafka acknowledged a field report with acks=all, which is what the 202 waits for.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3},
	})
)

const HeaderIdempotencyKey = "Idempotency-Key"

// Producer writes a report durably; it returns only after the broker acknowledged it.
type Producer interface {
	Produce(ctx context.Context, r fieldreport.Report) error
}

type StatusLookup interface {
	Lookup(ctx context.Context, reportID string) (map[string]any, bool, error)
}

type Handler struct {
	Producer       Producer
	Status         StatusLookup
	Verifier       *jwtauth.Verifier
	Log            *slog.Logger
	ProduceTimeout time.Duration
	Now            func() time.Time
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Health("field-report-api"))
	mux.Handle("POST /field-reports", h.auth(http.HandlerFunc(h.submit)))
	mux.Handle("GET /field-reports/{id}", h.auth(http.HandlerFunc(h.status)))
	return mux
}

type ctxKey string

const subjectKey ctxKey = "sub"

func (h *Handler) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := jwtauth.BearerToken(r.Header.Get("Authorization"))
		if !ok {
			countRejected(r, "unauthorized")
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization Bearer wajib")
			return
		}
		scope, sub, err := h.Verifier.Verify(token)
		if err != nil {
			countRejected(r, "unauthorized")
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		if scope != jwtauth.ScopeField && scope != jwtauth.ScopeInternal {
			countRejected(r, "forbidden")
			httpx.Error(w, http.StatusForbidden, "forbidden", "hanya Tim Lapangan atau Internal Ops")
			return
		}
		ctx := context.WithValue(r.Context(), subjectKey, [2]string{sub, scope})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// countRejected counts a refused submission; a refused status lookup is not one.
func countRejected(r *http.Request, result string) {
	if r.Method == http.MethodPost {
		received.WithLabelValues(result).Inc()
	}
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(HeaderIdempotencyKey)
	if !fieldreport.ValidKey(key) {
		received.WithLabelValues("missing_key").Inc()
		httpx.Error(w, http.StatusBadRequest, "missing_idempotency_key",
			"header Idempotency-Key wajib, 8-128 karakter [A-Za-z0-9._-]")
		return
	}

	var in fieldreport.Input
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, fieldreport.MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			received.WithLabelValues("too_large").Inc()
			httpx.Error(w, http.StatusRequestEntityTooLarge, "too_large", "body maksimal 16 KiB")
			return
		}
		received.WithLabelValues("invalid").Inc()
		httpx.Error(w, http.StatusBadRequest, "bad_request", "body bukan JSON laporan yang valid")
		return
	}
	if err := in.Validate(); err != nil {
		received.WithLabelValues("invalid").Inc()
		httpx.Error(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	who := r.Context().Value(subjectKey).([2]string)
	report := fieldreport.Report{
		ReportID: key, Reporter: who[0], Scope: who[1], AcceptedAt: h.Now().UTC(), Input: in,
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.ProduceTimeout)
	defer cancel()
	produceStart := time.Now()
	err := h.Producer.Produce(ctx, report)
	kafkaAck.Observe(time.Since(produceStart).Seconds())
	if err != nil {
		received.WithLabelValues("kafka_unavailable").Inc()
		h.Log.ErrorContext(r.Context(), "field report not accepted", "report_id", key, "error", err)
		w.Header().Set("Retry-After", "2")
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable",
			"laporan belum diterima, kirim ulang dengan Idempotency-Key yang sama")
		return
	}

	received.WithLabelValues("accepted").Inc()
	h.Log.InfoContext(r.Context(), "field report accepted", "report_id", key, "reporter", report.Reporter,
		"correlation_id", correlation.FromContext(r.Context()))
	w.Header().Set("Location", "/field-reports/"+key)
	httpx.JSON(w, http.StatusAccepted, map[string]any{
		"report_id": key, "status": "accepted", "accepted_at": report.AcceptedAt.Format(time.RFC3339),
	})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	doc, found, err := h.Status.Lookup(ctx, id)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "status laporan belum bisa dicek")
		return
	}
	if !found {
		httpx.JSON(w, http.StatusOK, map[string]any{"report_id": id, "status": "pending"})
		return
	}
	doc["status"] = "processed"
	httpx.JSON(w, http.StatusOK, doc)
}
