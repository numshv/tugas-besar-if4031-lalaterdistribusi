package satfeed

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestReadEvents(t *testing.T) {
	stream := ": comment\n" +
		"id: 0\nevent: detection\ndata: {\"offset\":0}\n\n" +
		"event: heartbeat\ndata: {}\n\n" +
		"id: 2\ndata: line1\ndata: line2\n\n"

	var got []Event
	err := ReadEvents(strings.NewReader(stream), func(e Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Event{
		{ID: "0", Type: "detection", Data: []byte(`{"offset":0}`)},
		{Type: "heartbeat", Data: []byte(`{}`)},
		{ID: "2", Type: "message", Data: []byte("line1\nline2")},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Type != want[i].Type || string(got[i].Data) != string(want[i].Data) {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestDetectionKeyKeepsFeedText(t *testing.T) {
	raw := `{"offset":108,"detection":{"acq_date":"2026-11-14","acq_time":6,"latitude":-7.56336,"longitude":140.99999,"satellite":"N","confidence":"h"}}`
	var d DetectionData
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	if got, want := d.Detection.Key(), "N|2026-11-14|6|-7.56336|140.99999"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
	ts, err := d.Detection.Time()
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 14, 0, 6, 0, 0, time.UTC); !ts.Equal(want) {
		t.Errorf("time = %v, want %v", ts, want)
	}
}

func TestDetectionTimeRejectsInvalid(t *testing.T) {
	for _, acq := range []string{"2460", "2400", "-1", "abc"} {
		if _, err := DetectionTime("2026-11-13", acq); err == nil {
			t.Errorf("acq_time %q accepted", acq)
		}
	}
	ts, err := KeyTime("N20|2026-11-13|2344|-4.25726|123.31875")
	if err != nil || !ts.Equal(time.Date(2026, 11, 13, 23, 44, 0, 0, time.UTC)) {
		t.Errorf("KeyTime = %v, %v", ts, err)
	}
}
