package schema

import (
	"testing"
	"time"
)

func TestApply(t *testing.T) {
	rules, err := Parse([]byte(`{"PVMBG": {
		"rename":   {"level": "alert_level"},
		"values":   {"alert_level": {"1": "Normal", "3": "Siaga"}},
		"defaults": {"reported_at": "$received_at", "note": "filled"}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)

	renamed := rules.Apply("PVMBG", map[string]any{"level": "Awas"}, at)
	if renamed["alert_level"] != "Awas" || renamed["level"] != nil {
		t.Errorf("rename: %v", renamed)
	}
	numeric := rules.Apply("PVMBG", map[string]any{"alert_level": float64(3), "reported_at": "x"}, at)
	if numeric["alert_level"] != "Siaga" || numeric["reported_at"] != "x" {
		t.Errorf("values: %v", numeric)
	}
	missing := rules.Apply("PVMBG", map[string]any{"alert_level": "Normal"}, at)
	if missing["reported_at"] != "2026-11-17T08:00:00Z" || missing["note"] != "filled" {
		t.Errorf("defaults: %v", missing)
	}

	original := map[string]any{"level": "Awas"}
	rules.Apply("PVMBG", original, at)
	if _, still := original["level"]; !still {
		t.Error("Apply modified its input")
	}
	if got := rules.Apply("BMKG", original, at); got["level"] != "Awas" {
		t.Error("rules for PVMBG applied to BMKG")
	}
}

func TestLoadMissingFile(t *testing.T) {
	r, err := Load("does-not-exist.json")
	if err != nil || len(r) != 0 {
		t.Fatalf("Load missing = %v, %v", r, err)
	}
}
