// seismic.go
package mapper

import (
	"fmt"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

// Hanya field yang benar-benar dikonsumsi ke slot kanonik yang dikecualikan
// dari attributes. magnitude, depth_km, potential_tsunami SENGAJA tidak
// masuk daftar ini karena spec eksplisit minta mereka tetap muncul di
// attributes juga.
var seismicCanonicalOnlyFields = map[string]struct{}{
	"event_id": {}, "region_name": {}, "epicenter_lat": {}, "epicenter_lon": {}, "occurred_at": {},
}

func MapSeismic(events, warnings []map[string]any, policy UnknownFieldPolicy) ([]hazard.Event, error) {
	warningByEvent := make(map[string]map[string]any, len(warnings))
	for _, w := range warnings {
		if refID, _ := w["related_event_id"].(string); refID != "" {
			warningByEvent[refID] = w
		}
	}

	out := make([]hazard.Event, 0, len(events))
	for _, raw := range events {
		e, err := mapOneSeismic(raw, warningByEvent, policy)
		if err != nil {
			return nil, fmt.Errorf("map seismic event: %w", err)
		}
		out = append(out, e)
	}
	return out, nil
}

func mapOneSeismic(raw map[string]any, warningByEvent map[string]map[string]any, policy UnknownFieldPolicy) (hazard.Event, error) {
	eventID, err := reqString(raw, "event_id")
	if err != nil {
		return hazard.Event{}, err
	}
	magnitude, err := reqFloat(raw, "magnitude")
	if err != nil {
		return hazard.Event{}, err
	}
	occurredAt, err := reqTime(raw, "occurred_at")
	if err != nil {
		return hazard.Event{}, err
	}
	regionName, _ := raw["region_name"].(string)
	lat, _ := raw["epicenter_lat"].(float64)
	lon, _ := raw["epicenter_lon"].(float64)

	attrs := attributesOf(raw, seismicCanonicalOnlyFields, policy)

	warning, hasWarning := warningByEvent[eventID]
	threatLevel := ""
	if hasWarning {
		threatLevel, _ = warning["threat_level"].(string)
		// TsunamiWarning bukan entitas kanonik sendiri; seluruh fieldnya
		// diteruskan ke attributes dengan prefiks supaya tidak bentrok
		// dengan nama field milik SeismicEvent.
		for k, v := range warning {
			attrs["warning_"+k] = v
		}
	}

	return hazard.Event{
		HazardID:    hazard.NewID(hazard.SourceBMKG, eventID),
		Source:      hazard.SourceBMKG,
		SourceRefID: eventID,
		HazardType:  hazard.TypeSeismic,
		Severity:    resolveSeismicSeverity(magnitude, threatLevel, hasWarning),
		AreaName:    regionName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  occurredAt,
		IngestedAt:  time.Now().UTC(),
		Attributes:  attrs,
	}, nil
}

// resolveSeismicSeverity mengikuti aturan pemetaan di spesifikasi persis:
// AWAS kalau ada warning dengan threat_level Awas; kalau ada warning lain,
// ikuti threat_level-nya; kalau tidak ada warning sama sekali, pakai
// threshold magnitude.
func resolveSeismicSeverity(magnitude float64, threatLevel string, hasWarning bool) hazard.Severity {
	if hasWarning {
		if threatLevel == "Awas" {
			return hazard.SeverityAwas
		}
		return severityFromThreat(threatLevel)
	}
	switch {
	case magnitude >= 6.5:
		return hazard.SeveritySiaga
	case magnitude >= 5.0:
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}

func severityFromThreat(threat string) hazard.Severity {
	switch threat {
	case "Siaga":
		return hazard.SeveritySiaga
	case "Waspada":
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}
