package loader

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/geo"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
)

var defaultLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006/01/02 15:04:05",
	"02/01/2006 15:04:05",
	"02-01-2006 15:04:05",
	"2006-01-02",
}

type Converter struct {
	Columns Columns
	Layouts []string
	Loc     *time.Location
}

func NewConverter(cols Columns, layout string, loc *time.Location) *Converter {
	layouts := defaultLayouts
	if layout != "" {
		layouts = []string{layout}
	}
	if loc == nil {
		loc = time.UTC
	}
	return &Converter{Columns: cols, Layouts: layouts, Loc: loc}
}

func (c *Converter) parseTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range c.Layouts {
		if t, err := time.ParseInLocation(layout, raw, c.Loc); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("time %q matches no layout (set --time-layout)", raw)
}

func parseNumber(raw, name string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, ",") && !strings.Contains(raw, ".") {
		raw = strings.ReplaceAll(raw, ",", ".")
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a number", name, raw)
	}
	return v, nil
}

func (c *Converter) Message(rec Record) (hazard.UpsertMessage, error) {
	cols := c.Columns
	id := rec.Fields[cols.ID]
	if strings.TrimSpace(id) == "" {
		return hazard.UpsertMessage{}, errors.New("empty id")
	}
	timeRaw := rec.Fields[cols.Time]
	if cols.Date != "" || cols.Clock != "" {
		timeRaw = rec.Fields[cols.Date] + " " + rec.Fields[cols.Clock]
	}
	at, err := c.parseTime(timeRaw)
	if err != nil {
		return hazard.UpsertMessage{}, err
	}
	mag, err := parseNumber(rec.Fields[cols.Mag], "magnitude")
	if err != nil {
		return hazard.UpsertMessage{}, err
	}
	lat, err := parseNumber(rec.Fields[cols.Lat], "latitude")
	if err != nil {
		return hazard.UpsertMessage{}, err
	}
	lon, err := parseNumber(rec.Fields[cols.Lon], "longitude")
	if err != nil {
		return hazard.UpsertMessage{}, err
	}

	used := map[string]bool{cols.ID: true, cols.Time: true, cols.Date: true, cols.Clock: true,
		cols.Lat: true, cols.Lon: true, cols.Mag: true, cols.Depth: true, cols.Region: true}
	raw := map[string]any{
		"event_id":      id,
		"magnitude":     mag,
		"occurred_at":   at.Format(time.RFC3339Nano),
		"region_name":   strings.TrimSpace(rec.Fields[cols.Region]),
		"epicenter_lat": lat,
		"epicenter_lon": lon,
	}
	if cols.Depth != "" && strings.TrimSpace(rec.Fields[cols.Depth]) != "" {
		depth, err := parseNumber(rec.Fields[cols.Depth], "depth")
		if err != nil {
			return hazard.UpsertMessage{}, err
		}
		raw["depth_km"] = depth
	}
	for k, v := range rec.Fields {
		if !used[k] && k != "" {
			raw[k] = v
		}
	}
	return ToMessage(raw)
}

func ToMessage(raw map[string]any) (hazard.UpsertMessage, error) {
	ev, err := mapper.MapSeismicOne(raw, nil, mapper.ParsePolicy("passthrough"))
	if err != nil {
		return hazard.UpsertMessage{}, err
	}
	if ev.AreaName == "" {
		ev.AreaName = geo.NearestProvince(ev.Latitude, ev.Longitude)
	}
	msg := hazard.UpsertMessage{
		Op: hazard.OpUpsert, Source: ev.Source, SourceRefID: ev.SourceRefID, Version: 1,
		HazardType: ev.HazardType, Severity: ev.Severity, AreaName: ev.AreaName,
		Latitude: ev.Latitude, Longitude: ev.Longitude, OccurredAt: ev.OccurredAt, Attributes: ev.Attributes,
	}
	if err := msg.Validate(); err != nil {
		return hazard.UpsertMessage{}, err
	}
	return msg, nil
}
