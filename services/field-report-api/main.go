package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/envx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/fieldreport"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/kafkax"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/logging"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/field-report-api/internal/api"
)

const service = "field-report-api"

func main() {
	log := logging.New(service, os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	brokers, err := kafkax.Brokers()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := &kafkaProducer{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        envx.String("FIELD_REPORTS_TOPIC", "bnpb.field-reports.v1"),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 2 * time.Millisecond,
		WriteTimeout: 3 * time.Second,
	}}
	defer producer.w.Close()

	keys := jwtauth.NewBackgroundKeyProvider(ctx, os.Getenv("AUTH_JWKS_URL"), log, time.Minute)
	h := &api.Handler{
		Producer:       producer,
		Status:         aggregatorStatus{base: envx.String("AGGREGATOR_BASE_URL", "http://aggregator:8083"), http: httpx.NewClient("aggregator", 3*time.Second, log)},
		Verifier:       &jwtauth.Verifier{Keys: keys, Issuer: os.Getenv("JWT_ISSUER"), Audience: os.Getenv("JWT_AUDIENCE")},
		Log:            log,
		ProduceTimeout: time.Duration(envx.Int("PRODUCE_TIMEOUT_MS", 3000)) * time.Millisecond,
		Now:            time.Now,
	}

	port := envx.String("FIELD_REPORT_API_PORT", "8085")
	return httpx.ListenAndServe(ctx, ":"+port, httpx.Middleware(log)(h.Routes()), log)
}

type kafkaProducer struct{ w *kafka.Writer }

func (p *kafkaProducer) Produce(ctx context.Context, r fieldreport.Report) error {
	value, err := json.Marshal(r)
	if err != nil {
		return err
	}
	msg := kafka.Message{Key: []byte(r.ReportID), Value: value, Time: r.AcceptedAt}
	if id := correlation.FromContext(ctx); id != "" {
		msg.Headers = []kafka.Header{{Key: correlation.Header, Value: []byte(id)}}
	}
	return p.w.WriteMessages(ctx, msg)
}

type aggregatorStatus struct {
	base string
	http *httpx.Client
}

func (a aggregatorStatus) Lookup(ctx context.Context, id string) (map[string]any, bool, error) {
	var doc map[string]any
	err := a.http.GetJSON(ctx, a.base+"/internal/field-reports/"+url.PathEscape(id), nil, &doc)
	if se, ok := err.(*httpx.StatusError); ok && se.Status == http.StatusNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("lookup field report: %w", err)
	}
	return doc, true, nil
}
