package processor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	latestOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_feed_latest_offset", Help: "Newest offset the feed has, from /v1/session every 5 s.",
	})
	processedOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_processed_offset", Help: "Last feed offset applied to the engine.",
	})
	lagEvents = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_lag_events", Help: "How many feed events the processor is behind.",
	})
	watermarkLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_watermark_lag_seconds", Help: "Feed virtual_now minus the watermark, in virtual seconds.",
	})
	clockAhead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_clock_ahead_seconds",
		Help: "Newest detection time minus the feed's virtual_now. Positive means a satellite clock runs ahead.",
	})
	caughtUp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_caught_up", Help: "1 once the rebuild from offset 0 reached the startup offset.",
	})
	rebuildSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_rebuild_seconds", Help: "Time the last rebuild from offset 0 took.",
	})
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wildfire_feed_events_total", Help: "Feed events read, by type.",
	}, []string{"type"})
	reconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wildfire_feed_reconnects_total", Help: "Feed reconnects, by reason.",
	}, []string{"reason"})
	sessionResets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wildfire_feed_session_resets_total", Help: "Times the feed session went back to an older offset or changed profile.",
	})
	offsetGaps = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_offset_gaps", Help: "Feed offsets that were skipped. Must stay 0.",
	})
	malformed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_malformed_events", Help: "Feed events that could not be read.",
	})
	detections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wildfire_detections", Help: "Detections by state (unique, duplicate, low_confidence, retracted, late, single).",
	}, []string{"state"})
	fires = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wildfire_fires", Help: "Fires by state (open, final, retracted).",
	}, []string{"state"})
	published = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wildfire_published_total", Help: "Messages sent to hazard-upserts, by op.",
	}, []string{"op"})
	publishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wildfire_publish_errors_total", Help: "Failed attempts to send to hazard-upserts (retried).",
	})
	restoredOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_restored_offset", Help: "Feed offset of the snapshot the processor started from (0 = full replay).",
	})
	snapshotOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_snapshot_offset", Help: "Feed offset of the last saved snapshot.",
	})
	snapshotBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_snapshot_bytes", Help: "Size of the last saved snapshot.",
	})
	snapshotSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wildfire_snapshot_seconds", Help: "Time the last snapshot took to save.",
	})
	snapshotErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wildfire_snapshot_errors_total", Help: "Snapshots that could not be saved.",
	})
	emitDelay = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wildfire_emit_delay_seconds",
		Help:    "From a feed event reaching the processor until Kafka acknowledged the change it caused.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
	})
)
