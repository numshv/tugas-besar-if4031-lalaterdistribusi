package mapper_test

import (
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
)

type fakeResolver struct{}

func (fakeResolver) Resolve(id string) (string, float64, float64, bool) {
	if id == "VOL-MERAPI" {
		return "Gunung Merapi", -7.54, 110.44, true
	}
	return "", 0, 0, false
}

func TestMapVolcanic_SchemaV1_NoConfidenceLevel(t *testing.T) {
	raw := []map[string]any{{
		"report_id": "VR-1", "volcano_id": "VOL-MERAPI", "alert_level": "Siaga",
		"eruption_count_24h": float64(3), "ash_column_height_m": float64(1200),
		"reported_at": "2026-09-27T08:00:00Z",
		// tidak ada confidence_level sama sekali, simulasi skema lama
	}}

	events, err := mapper.MapVolcanic(raw, fakeResolver{}, mapper.PolicyPassthrough)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, exists := events[0].Attributes["confidence_level"]; exists {
		t.Fatal("confidence_level should not exist under schema v1")
	}
	if events[0].AreaName != "Gunung Merapi" {
		t.Fatalf("area_name = %q, want resolved volcano name", events[0].AreaName)
	}
}

func TestMapVolcanic_SchemaV2_UnknownFieldPassesThrough(t *testing.T) {
	raw := []map[string]any{{
		"report_id": "VR-2", "volcano_id": "VOL-MERAPI", "alert_level": "Awas",
		"eruption_count_24h": float64(9), "ash_column_height_m": float64(3000),
		"reported_at":      "2026-09-27T09:00:00Z",
		"confidence_level": 0.87, // field baru, tidak pernah di-hardcode di mapper
	}}

	events, err := mapper.MapVolcanic(raw, fakeResolver{}, mapper.PolicyPassthrough)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := events[0].Attributes["confidence_level"].(float64)
	if !ok || got != 0.87 {
		t.Fatalf("confidence_level = %v, want 0.87 passed through unchanged", events[0].Attributes["confidence_level"])
	}
}

func TestMapSeismic_SeverityEscalatesWithAwasWarning(t *testing.T) {
	events := []map[string]any{{
		"event_id": "EQ-1", "magnitude": float64(5.5), "depth_km": float64(10),
		"epicenter_lat": -6.1, "epicenter_lon": 105.4, "region_name": "Selat Sunda",
		"occurred_at": "2026-09-27T08:00:00Z", "potential_tsunami": true,
	}}
	warnings := []map[string]any{{
		"warning_id": "TW-1", "related_event_id": "EQ-1", "threat_level": "Awas",
		"affected_zones": []any{"Lampung Selatan"}, "estimated_arrival": "2026-09-27T08:20:00Z",
	}}

	out, err := mapper.MapSeismic(events, warnings, mapper.PolicyPassthrough)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// magnitude 5.5 sendirian cuma WASPADA, tapi warning Awas harus menang
	if out[0].Severity != "AWAS" {
		t.Fatalf("severity = %s, want AWAS (magnitude alone would give WASPADA)", out[0].Severity)
	}
}

func TestMapSeismic_MissingRequiredFieldFailsLoudly(t *testing.T) {
	events := []map[string]any{{
		"magnitude": float64(5.0), // event_id sengaja hilang
	}}
	if _, err := mapper.MapSeismic(events, nil, mapper.PolicyPassthrough); err == nil {
		t.Fatal("expected error when required field event_id is missing, got nil")
	}
}

var _ = time.Now // placeholder kalau butuh waktu tetap di test lain
