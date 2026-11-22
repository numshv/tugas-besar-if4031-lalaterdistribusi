package kafkax

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/telemetry"
)

var (
	consumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_total",
		Help: "Kafka messages handled, by topic, consumer group and result (ok, error).",
	}, []string{"topic", "group", "result"})
	handleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_consumer_handle_duration_seconds",
		Help:    "Time spent in one handler call.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"topic", "group"})
	lag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag_seconds",
		Help: "Age of the last handled message when it was handled.",
	}, []string{"topic", "group"})
)

// InjectTrace appends the W3C traceparent of ctx to Kafka headers, so the consumer span
// continues the producer's trace.
func InjectTrace(ctx context.Context, headers []kafka.Header) []kafka.Header {
	carrier := headerCarrier{&headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return headers
}

// observed wraps a handler with a consumer span (child of the producer trace) and metrics.
func observed(topic, group string, h Handler) Handler {
	telemetry.Init()
	return func(ctx context.Context, log *slog.Logger, m kafka.Message) error {
		ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{&m.Headers})
		ctx, span := telemetry.Tracer().Start(ctx, "consume "+topic, trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String("messaging.destination.name", topic),
				attribute.String("messaging.consumer.group.name", group)))
		defer span.End()

		start := time.Now()
		err := h(ctx, log, m)
		handleDuration.WithLabelValues(topic, group).Observe(time.Since(start).Seconds())
		if !m.Time.IsZero() {
			lag.WithLabelValues(topic, group).Set(time.Since(m.Time).Seconds())
		}
		if err != nil {
			consumed.WithLabelValues(topic, group, "error").Inc()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		consumed.WithLabelValues(topic, group, "ok").Inc()
		return nil
	}
}

type headerCarrier struct{ headers *[]kafka.Header }

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
