package httpx

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/telemetry"
)

var (
	latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

	serverRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_server_requests_total",
		Help: "HTTP requests handled, by route and status code.",
	}, []string{"method", "route", "code"})
	serverDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "HTTP request latency, by route.",
		Buckets: latencyBuckets,
	}, []string{"method", "route"})
	clientRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_client_requests_total",
		Help: "Outbound HTTP calls, by upstream and status code (0 = transport error).",
	}, []string{"upstream", "code"})
	clientDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_client_request_duration_seconds",
		Help:    "Outbound HTTP call latency, by upstream.",
		Buckets: latencyBuckets,
	}, []string{"upstream"})

	metricsHandler = promhttp.Handler()
)

// startServerSpan continues the caller's trace from the traceparent header.
func startServerSpan(ctx context.Context, r *http.Request) (context.Context, trace.Span) {
	telemetry.Init()
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
	return telemetry.Tracer().Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer))
}

// endServerSpan records RED metrics labelled by the route pattern the mux matched on req
// (not the raw path, which would explode label cardinality) and closes the span.
func endServerSpan(span trace.Span, req *http.Request, status int, start time.Time) {
	route := req.Pattern
	if route == "" {
		route = "unmatched"
	}
	code := strconv.Itoa(status)
	serverRequests.WithLabelValues(req.Method, route, code).Inc()
	serverDuration.WithLabelValues(req.Method, route).Observe(time.Since(start).Seconds())

	span.SetName(route)
	span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
	if status >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, code)
	}
	span.End()
}

func startClientSpan(ctx context.Context, upstream string, req *http.Request) (context.Context, trace.Span) {
	ctx, span := telemetry.Tracer().Start(ctx, req.Method+" "+upstream+req.URL.Path, trace.WithSpanKind(trace.SpanKindClient))
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return ctx, span
}

func endClientSpan(span trace.Span, upstream string, start time.Time, status int, err error) {
	clientRequests.WithLabelValues(upstream, strconv.Itoa(status)).Inc()
	clientDuration.WithLabelValues(upstream).Observe(time.Since(start).Seconds())
	span.SetAttributes(attribute.Int("http.response.status_code", status))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if status >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, strconv.Itoa(status))
	}
	span.End()
}
