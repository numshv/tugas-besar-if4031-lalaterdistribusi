// Package satfeed reads the Feed Satelit SSE stream. Numbers stay json.Number
// because update and retract reference a detection by the feed's exact text.
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

type Event struct {
	ID   string
	Type string
	Data []byte
}

// maxEventBytes bounds a single SSE line so a broken stream cannot exhaust memory.
const maxEventBytes = 1 << 20

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

func (d Detection) Key() string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime.String(), d.Latitude.String(), d.Longitude.String()}, "|")
}

func (d Detection) Time() (time.Time, error) {
	return DetectionTime(d.AcqDate, d.AcqTime.String())
}

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

func KeyTime(key string) (time.Time, error) {
	parts := strings.Split(key, "|")
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("detection key %q does not have 5 parts", key)
	}
	return DetectionTime(parts[1], parts[2])
}

type DetectionData struct {
	Offset    int64     `json:"offset"`
	Detection Detection `json:"detection"`
}

type UpdateData struct {
	Offset  int64                      `json:"offset"`
	Key     string                     `json:"key"`
	Changes map[string]json.RawMessage `json:"changes"`
}

type RetractData struct {
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

type Session struct {
	Group        string    `json:"group"`
	Profile      string    `json:"profile"`
	Speed        float64   `json:"speed"`
	VirtualNow   time.Time `json:"virtual_now"`
	LatestOffset int64     `json:"latest_offset"`
}
