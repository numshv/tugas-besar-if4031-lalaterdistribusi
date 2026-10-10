package mapper_test

import (
	"testing"

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
	}}

	events, errs := mapper.MapVolcanic(raw, fakeResolver{}, mapper.PolicyPassthrough)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
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

	events, errs := mapper.MapVolcanic(raw, fakeResolver{}, mapper.PolicyPassthrough)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
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

	out, errs := mapper.MapSeismic(events, warnings, mapper.PolicyPassthrough)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if out[0].Severity != "AWAS" {
		t.Fatalf("severity = %s, want AWAS (magnitude alone would give WASPADA)", out[0].Severity)
	}
}

// pastikan field warning ikut dibuang saat policy=drop,
func TestMapSeismic_WarningFieldsRespectDropPolicy(t *testing.T) {
	events := []map[string]any{{
		"event_id": "EQ-2", "magnitude": float64(6.0), "depth_km": float64(10),
		"epicenter_lat": -6.1, "epicenter_lon": 105.4, "region_name": "Selat Sunda",
		"occurred_at": "2026-09-27T08:00:00Z", "potential_tsunami": true,
	}}
	warnings := []map[string]any{{
		"warning_id": "TW-2", "related_event_id": "EQ-2", "threat_level": "Siaga",
		"affected_zones": []any{"Lampung Selatan"}, "estimated_arrival": "2026-09-27T08:20:00Z",
	}}

	out, errs := mapper.MapSeismic(events, warnings, mapper.PolicyDrop)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	for k := range out[0].Attributes {
		if k == "warning_threat_level" || k == "warning_affected_zones" {
			t.Fatalf("attribute %q should not exist under PolicyDrop, consistency with declared policy violated", k)
		}
	}
}

// Test satu record rusak tidak boleh menggugurkan record
func TestMapSeismic_MissingRequiredFieldIsSkippedNotFatal(t *testing.T) {
	events := []map[string]any{
		{"magnitude": float64(5.0)}, // event_id sengaja hilang, record ini rusak
		{
			"event_id": "EQ-OK", "magnitude": float64(4.0), "depth_km": float64(5),
			"epicenter_lat": -6.0, "epicenter_lon": 105.0, "region_name": "Normal",
			"occurred_at": "2026-09-27T08:00:00Z", "potential_tsunami": false,
		},
	}

	out, errs := mapper.MapSeismic(events, nil, mapper.PolicyPassthrough)
	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 error for the malformed record, got %d", len(errs))
	}
	if len(out) != 1 {
		t.Fatalf("expected the valid record to still be mapped, got %d events", len(out))
	}
	if out[0].SourceRefID != "EQ-OK" {
		t.Fatalf("expected EQ-OK to be mapped, got %s", out[0].SourceRefID)
	}
}
