// Package satfeed reads the assistant's Feed Satelit (Server-Sent Events).
//
// Numbers in a detection are kept as json.Number so the detection key is built
// from the exact text the feed sent. update and retract events reference a
// detection by that text (for example acq_time "6", not "0006"), so formatting
// a parsed float would produce a key that no longer matches.
package satfeed

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	TypeDetection = "detection"
	TypeUpdate    = "update"
	TypeRetract   = "retract"
	TypeHeartbeat = "heartbeat"
)

// Event is one raw SSE event.
type Event struct {
	ID   string
	Type string
	Data []byte
}

// maxEventBytes bounds a single SSE line so a broken stream cannot exhaust memory.
const maxEventBytes = 1 << 20

// ReadEvents parses an SSE stream and calls fn for every complete event.
// It returns when the stream ends, fn returns an error, or a read fails.
func ReadEvents(r io.Reader, fn func(Event) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), maxEventBytes)

	var ev Event
	var data bytes.Buffer
	hasData := false

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if hasData {
				if ev.Type == "" {
					ev.Type = "message"
				}
				ev.Data = bytes.Clone(bytes.TrimSuffix(data.Bytes(), []byte("\n")))
				if err := fn(ev); err != nil {
					return err
				}
			}
			ev, hasData = Event{}, false
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			ev.ID = value
		case "event":
			ev.Type = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
			hasData = true
		}
	}
	return sc.Err()
}

// Detection is one VIIRS hotspot row.
type Detection struct {
	Satellite  string      `json:"satellite"`
	AcqDate    string      `json:"acq_date"`
	AcqTime    json.Number `json:"acq_time"`
	Latitude   json.Number `json:"latitude"`
	Longitude  json.Number `json:"longitude"`
	Confidence string      `json:"confidence"`
	FRP        json.Number `json:"frp"`
	BrightTI4  json.Number `json:"bright_ti4"`
	BrightTI5  json.Number `json:"bright_ti5"`
	Scan       json.Number `json:"scan"`
	Track      json.Number `json:"track"`
	DayNight   string      `json:"daynight"`
	Instrument string      `json:"instrument"`
	Version    string      `json:"version"`
}

// Key returns satellite|acq_date|acq_time|latitude|longitude using the feed's own text.
func (d Detection) Key() string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime.String(), d.Latitude.String(), d.Longitude.String()}, "|")
}

// Time returns the detection time: acq_date plus acq_time (HHMM, unpadded) in UTC.
func (d Detection) Time() (time.Time, error) {
	return DetectionTime(d.AcqDate, d.AcqTime.String())
}

// DetectionTime combines acq_date (YYYY-MM-DD) and acq_time (HHMM, possibly unpadded).
func DetectionTime(acqDate, acqTime string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", acqDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("acq_date %q: %w", acqDate, err)
	}
	var hhmm int
	if _, err := fmt.Sscanf(acqTime, "%d", &hhmm); err != nil || hhmm < 0 || hhmm%100 > 59 || hhmm/100 > 23 {
		return time.Time{}, fmt.Errorf("acq_time %q is not HHMM", acqTime)
	}
	return day.Add(time.Duration(hhmm/100)*time.Hour + time.Duration(hhmm%100)*time.Minute), nil
}

// KeyTime extracts the detection time from a detection key.
func KeyTime(key string) (time.Time, error) {
	parts := strings.Split(key, "|")
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("detection key %q does not have 5 parts", key)
	}
	return DetectionTime(parts[1], parts[2])
}

// DetectionData is the payload of a detection event.
type DetectionData struct {
	Offset    int64     `json:"offset"`
	Detection Detection `json:"detection"`
}

// UpdateData is the payload of an update event: changed fields of an existing detection.
type UpdateData struct {
	Offset  int64                      `json:"offset"`
	Key     string                     `json:"key"`
	Changes map[string]json.RawMessage `json:"changes"`
}

// RetractData is the payload of a retract event: the detection is withdrawn.
type RetractData struct {
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

// Session is the response of GET /v1/session.
type Session struct {
	Group        string    `json:"group"`
	Profile      string    `json:"profile"`
	Speed        float64   `json:"speed"`
	VirtualNow   time.Time `json:"virtual_now"`
	LatestOffset int64     `json:"latest_offset"`
}
