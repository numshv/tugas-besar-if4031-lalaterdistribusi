// Package telemetry turns on OpenTelemetry tracing when OTEL_EXPORTER_OTLP_ENDPOINT is set.
// Without an endpoint every span is a no-op, so local runs and tests need nothing.
package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentation = "github.com/lalaterdistribusi/tugas-besar-if4031"

var once sync.Once

func Init() {
	once.Do(func() {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
			return
		}
		exporter, err := otlptracehttp.New(context.Background())
		if err != nil {
			slog.Warn("tracing disabled", "error", err)
			return
		}
		res, _ := resource.Merge(resource.Default(),
			resource.NewSchemaless(attribute.String("service.name", serviceName())))
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res)))
	})
}

func Tracer() trace.Tracer {
	return otel.Tracer(instrumentation)
}

// serviceName prefers OTEL_SERVICE_NAME, else strips the ReplicaSet and pod suffixes
// from a Deployment pod name ("aggregator-7dc6f586c5-6zfnc" becomes "aggregator").
func serviceName() string {
	if name := os.Getenv("OTEL_SERVICE_NAME"); name != "" {
		return name
	}
	host, _ := os.Hostname()
	return deploymentOf(host)
}

func deploymentOf(pod string) string {
	parts := strings.Split(pod, "-")
	if len(parts) > 2 {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return pod
}
