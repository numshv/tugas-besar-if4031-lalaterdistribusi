package mapper

import (
	"fmt"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

var seismicCanonicalOnlyFields = map[string]struct{}{
	"event_id": {}, "region_name": {}, "epicenter_lat": {}, "epicenter_lon": {}, "occurred_at": {},
}

func MapSeismic(events, warnings []map[string]any, policy UnknownFieldPolicy) ([]hazard.Event, []error) {
	warningByEvent := make(map[string]map[string]any, len(warnings))
	for _, w := range warnings {
		if refID, _ := w["related_event_id"].(string); refID != "" {
			warningByEvent[refID] = w
		}
	}

	out := make([]hazard.Event, 0, len(events))
	var errs []error
	for _, raw := range events {
		e, err := mapOneSeismic(raw, warningByEvent, policy)
		if err != nil {
			errs = append(errs, fmt.Errorf("map seismic event: %w", err))
			continue
		}
		out = append(out, e)
	}
	return out, errs
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
		// FIX #1: field TsunamiWarning sekarang lewat attributesOf juga,
		// supaya tunduk ke UnknownFieldPolicy yang sama persis dengan field
		// SeismicEvent. Sebelumnya field ini SELALU ditambahkan apa pun
		// policy-nya, bikin perilaku tidak konsisten dengan kebijakan yang
		// dideklarasikan (P1.4).
		for k, v := range attributesOf(warning, map[string]struct{}{}, policy) {
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
