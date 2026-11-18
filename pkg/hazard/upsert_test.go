package hazard

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validFire() UpsertMessage {
	return UpsertMessage{
		Op: OpUpsert, Source: SourceSatelit, SourceRefID: "fire-1", Version: 3,
		HazardType: TypeWildfire, Severity: SeverityAwas, Latitude: -2.5, Longitude: 113.8,
		OccurredAt: time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC),
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	enc, err := EncodeUpsert(validFire(), UpsertMeta{CorrelationID: "cid", Traceparent: "00-abc-def-01"})
	if err != nil {
		t.Fatal(err)
	}
	if string(enc.Key) != "SATELIT|fire-1" {
		t.Fatalf("key = %s", enc.Key)
	}
	if enc.Headers[HeaderCorrelationID] != "cid" || enc.Headers[HeaderTraceparent] == "" || enc.Headers[HeaderProducedAt] == "" {
		t.Fatalf("headers = %v", enc.Headers)
	}
	got, err := DecodeUpsert(enc.Value)
	if err != nil {
		t.Fatal(err)
	}
	ev := got.Event()
	if ev.HazardID != NewID(SourceSatelit, "fire-1") || ev.Version != 3 || ev.Retracted {
		t.Fatalf("event = %+v", ev)
	}
}

func TestDecodeIgnoresProducerHazardID(t *testing.T) {
	m, err := DecodeUpsert([]byte(`{"op":"retract","source":"SATELIT","source_ref_id":"fire-1","version":4,"hazard_id":"spoofed"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev := m.Event(); ev.HazardID != NewID(SourceSatelit, "fire-1") || !ev.Retracted {
		t.Fatalf("event = %+v", ev)
	}
}

func TestDecodeRejects(t *testing.T) {
	deep := strings.Repeat(`{"a":`, 20000) + "1" + strings.Repeat("}", 20000)
	cases := map[string]string{
		`{"op":"upsert"`: "invalid_json",
		`{"op":"upsert","source":"NASA","source_ref_id":"x","version":1}`:                                                                                                  "unknown_source",
		`{"op":"upsert","source":"SATELIT","source_ref_id":"","version":1}`:                                                                                                "bad_source_ref_id",
		`{"op":"upsert","source":"SATELIT","source_ref_id":"x","version":1,"hazard_type":"FLOOD"}`:                                                                         "unknown_hazard_type",
		`{"op":"upsert","source":"SATELIT","source_ref_id":"x","version":1,"hazard_type":"WILDFIRE","severity":"AWAS"}`:                                                    "missing_occurred_at",
		`{"op":"upsert","source":"SATELIT","source_ref_id":"x","version":1,"hazard_type":"WILDFIRE","severity":"AWAS","occurred_at":"2026-11-12T00:00:00Z","latitude":95}`: "bad_coordinates",
		`{"op":"upsert","source":"SATELIT","source_ref_id":"x","version":"1"}`:                                                                                             "invalid_json",
		deep: "invalid_json",
	}
	for payload, reason := range cases {
		_, err := DecodeUpsert([]byte(payload))
		var invalid *InvalidUpsertError
		if !errors.As(err, &invalid) || invalid.Reason != reason {
			t.Fatalf("payload %.60q: err = %v, want reason %s", payload, err, reason)
		}
	}
	if _, err := DecodeUpsert(make([]byte, MaxUpsertBytes+1)); err == nil || !strings.Contains(err.Error(), "too_large") {
		t.Fatalf("oversized payload err = %v", err)
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	bad := validFire()
	bad.Version = 0
	if _, err := EncodeUpsert(bad, UpsertMeta{}); err == nil {
		t.Fatal("producer must not be able to emit an invalid message")
	}
}
