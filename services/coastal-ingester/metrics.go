package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var breakerGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "sensor_breaker_state",
	Help: "Sensor Pantai circuit breaker: 0 closed, 1 half-open, 2 open.",
})
