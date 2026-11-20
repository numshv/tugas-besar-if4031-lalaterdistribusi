package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/coastal-ingester/internal/sensor"
)

type Observation struct {
	ID         string    `json:"observation_id"`
	StationID  string    `json:"station_id"`
	ObservedAt time.Time `json:"observed_at"`
	SeaLevel   float64   `json:"sea_level_anomaly_m"`
	Wave       float64   `json:"wave_height_m"`
	Quality    string    `json:"quality"`
	Critical   bool      `json:"critical"`
}

var knownQuality = map[string]bool{"ok": true, "suspect": true, "bad": true}

// parse reads one observation leniently: it returns whatever it could read plus the
// first contract violation found, so a critical flag still counts on a broken record.
func parse(raw json.RawMessage, stations map[string]sensor.Station) (Observation, string) {
	var o Observation
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return o, "malformed_json"
	}

	var reason string
	fail := func(format string, a ...any) {
		if reason == "" {
			reason = fmt.Sprintf(format, a...)
		}
	}
	str := func(field string) string {
		v, ok := m[field]
		if !ok {
			fail("missing_field: %s", field)
			return ""
		}
		s, ok := v.(string)
		if !ok {
			fail("wrong_type: %s must be a string", field)
		}
		return s
	}
	num := func(field string) float64 {
		v, ok := m[field]
		if !ok {
			fail("missing_field: %s", field)
			return 0
		}
		n, ok := v.(json.Number)
		if !ok {
			fail("wrong_type: %s must be a number", field)
			return 0
		}
		f, err := n.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			fail("wrong_type: %s is not a finite number", field)
		}
		return f
	}

	o.ID = str("observation_id")
	o.StationID = str("station_id")
	observedAt := str("observed_at")
	if t, err := time.Parse(time.RFC3339, observedAt); err == nil {
		o.ObservedAt = t.UTC()
	} else if observedAt != "" {
		fail("wrong_type: observed_at is not RFC3339")
	}
	critical, isBool := m["critical"].(bool)
	o.Critical = isBool && critical
	if _, ok := m["critical"]; !ok {
		fail("missing_field: critical")
	} else if !isBool {
		fail("wrong_type: critical must be a boolean")
	}
	o.SeaLevel = num("sea_level_anomaly_m")
	o.Wave = num("wave_height_m")
	o.Quality = str("quality")

	switch {
	case reason != "":
	case len(stations) > 0 && stations[o.StationID].ID == "":
		fail("unknown_station: %s", o.StationID)
	case math.Abs(o.SeaLevel) > 20:
		fail("out_of_range: sea_level_anomaly_m=%v", o.SeaLevel)
	case o.Wave < 0 || o.Wave > 30:
		fail("out_of_range: wave_height_m=%v", o.Wave)
	case !knownQuality[o.Quality]:
		fail("unknown_quality: %q", o.Quality)
	}
	return o, reason
}

// criticalReadable says whether a critical record carries enough to route an alert.
func (o Observation) criticalReadable() bool {
	return o.Critical && o.ID != "" && o.StationID != "" && !o.ObservedAt.IsZero()
}

// severity is our reading of the base contract (documented in the report): a critical
// flag or a large anomaly is AWAS, then SIAGA, WASPADA, NORMAL by thresholds.
func severity(o Observation) hazard.Severity {
	sla := math.Abs(o.SeaLevel)
	switch {
	case o.Critical || sla >= 1.0 || o.Wave >= 4.0:
		return hazard.SeverityAwas
	case sla >= 0.5 || o.Wave >= 2.5:
		return hazard.SeveritySiaga
	case sla >= 0.3 || o.Wave >= 1.5:
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}
