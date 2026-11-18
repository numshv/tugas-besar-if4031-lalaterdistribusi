package store

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	queryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "store_query_duration_seconds",
		Help:    "Duration of hazard store operations against MongoDB.",
		Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"op"})

	experimentWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "store_experiment_write_errors_total",
		Help: "Failed shadow writes into the shard-key experiment collections.",
	}, []string{"collection"})

	partialReads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "store_partial_reads_total",
		Help: "Reads answered with partial=true because some storage units were unreachable.",
	}, []string{"op"})
)

func observe(op string, start time.Time) {
	queryDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}
