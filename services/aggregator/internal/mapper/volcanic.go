package mapper

import (
	"fmt"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

type VolcanoResolver interface {
	Resolve(volcanoID string) (name string, lat, lon float64, found bool)
}

var volcanicCanonicalOnlyFields = map[string]struct{}{
	"report_id": {}, "volcano_id": {}, "alert_level": {}, "reported_at": {},
}

// MapVolcanic jadinya gk stop seluruh batch kalau satu record bermasalah,
func MapVolcanic(reports []map[string]any, resolver VolcanoResolver, policy UnknownFieldPolicy) ([]hazard.Event, []error) {
	out := make([]hazard.Event, 0, len(reports))
	var errs []error
	for _, raw := range reports {
		e, err := mapOneVolcanic(raw, resolver, policy)
		if err != nil {
			errs = append(errs, fmt.Errorf("map volcanic report: %w", err))
			continue
		}
		out = append(out, e)
	}
	return out, errs
}

func mapOneVolcanic(raw map[string]any, resolver VolcanoResolver, policy UnknownFieldPolicy) (hazard.Event, error) {
	reportID, err := reqString(raw, "report_id")
	if err != nil {
		return hazard.Event{}, err
	}
	volcanoID, err := reqString(raw, "volcano_id")
	if err != nil {
		return hazard.Event{}, err
	}
	alertLevel, err := reqString(raw, "alert_level")
	if err != nil {
		return hazard.Event{}, err
	}
	reportedAt, err := reqTime(raw, "reported_at")
	if err != nil {
		return hazard.Event{}, err
	}

	name, lat, lon, found := resolver.Resolve(volcanoID)
	if !found {
		name = volcanoID
	}

	return hazard.Event{
		HazardID:    hazard.NewID(hazard.SourcePVMBG, reportID),
		Source:      hazard.SourcePVMBG,
		SourceRefID: reportID,
		HazardType:  hazard.TypeVolcanic,
		Severity:    severityFromAlertLevel(alertLevel),
		AreaName:    name,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  reportedAt,
		IngestedAt:  time.Now().UTC(),
		Attributes:  attributesOf(raw, volcanicCanonicalOnlyFields, policy),
	}, nil
}

func severityFromAlertLevel(level string) hazard.Severity {
	switch level {
	case "Awas":
		return hazard.SeverityAwas
	case "Siaga":
		return hazard.SeveritySiaga
	case "Waspada":
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}
