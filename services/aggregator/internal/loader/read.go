package loader

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Record struct {
	Row    int
	Fields map[string]string
	Order  []string
}

type Columns struct {
	ID, Time, Date, Clock, Lat, Lon, Mag, Depth, Region, Tsunami string
}

var candidates = map[string][]string{
	"id":      {"id", "event_id", "eventid", "event id", "kode", "kode_gempa"},
	"time":    {"time", "datetime", "origin_time", "origintime", "occurred_at", "waktu", "timestamp"},
	"lat":     {"latitude", "lat", "lintang", "epicenter_lat"},
	"lon":     {"longitude", "lon", "lng", "long", "bujur", "epicenter_lon"},
	"mag":     {"mag", "magnitude", "magnitudo", "m"},
	"depth":   {"depth", "depth_km", "kedalaman", "depth (km)"},
	"region":  {"place", "region", "region_name", "area", "area_name", "wilayah", "lokasi", "remark"},
	"tsunami": {"tsunami", "potential_tsunami", "potensi_tsunami"},
}

func pick(header []string, override string, want string) string {
	if override != "" {
		return override
	}
	for _, c := range candidates[want] {
		for _, h := range header {
			if strings.EqualFold(strings.TrimSpace(h), c) {
				return h
			}
		}
	}
	return ""
}

func ResolveColumns(header []string, override Columns) (Columns, error) {
	cols := Columns{
		ID:      pick(header, override.ID, "id"),
		Time:    pick(header, override.Time, "time"),
		Date:    override.Date,
		Clock:   override.Clock,
		Lat:     pick(header, override.Lat, "lat"),
		Lon:     pick(header, override.Lon, "lon"),
		Mag:     pick(header, override.Mag, "mag"),
		Depth:   pick(header, override.Depth, "depth"),
		Region:  pick(header, override.Region, "region"),
		Tsunami: pick(header, override.Tsunami, "tsunami"),
	}
	present := map[string]bool{}
	for _, h := range header {
		present[h] = true
	}
	var missing []string
	need := map[string]string{"id": cols.ID, "latitude": cols.Lat, "longitude": cols.Lon, "magnitude": cols.Mag}
	if cols.Date != "" || cols.Clock != "" {
		need["date"], need["clock"] = cols.Date, cols.Clock
	} else {
		need["time"] = cols.Time
	}
	for name, col := range need {
		if col == "" || !present[col] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return Columns{}, fmt.Errorf("columns not found: %s (header: %s); set them with --col-* flags", strings.Join(missing, ", "), strings.Join(header, ","))
	}
	return cols, nil
}

type Source interface {
	Header() []string
	Next() (Record, error)
}

func Open(r io.Reader, format string) (Source, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	if format == "" || format == "auto" {
		peek, _ := br.Peek(512)
		trimmed := bytes.TrimLeft(peek, " \t\r\n\xef\xbb\xbf")
		format = "csv"
		if len(trimmed) > 0 && (trimmed[0] == '[' || trimmed[0] == '{') {
			format = "json"
		}
	}
	switch format {
	case "csv":
		return newCSV(br)
	case "json", "jsonl":
		return newJSON(br)
	}
	return nil, fmt.Errorf("unknown format %q (csv, json, jsonl, auto)", format)
}

type csvSource struct {
	r      *csv.Reader
	header []string
	row    int
}

func newCSV(r io.Reader) (*csvSource, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	if len(header) > 0 {
		header[0] = strings.TrimPrefix(header[0], "\ufeff")
	}
	return &csvSource{r: cr, header: header, row: 1}, nil
}

func (c *csvSource) Header() []string { return c.header }

func (c *csvSource) Next() (Record, error) {
	values, err := c.r.Read()
	c.row++
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		return Record{Row: c.row}, fmt.Errorf("row %d: %w", c.row, err)
	}
	rec := Record{Row: c.row, Fields: make(map[string]string, len(c.header)), Order: c.header}
	for i, h := range c.header {
		if i < len(values) {
			rec.Fields[h] = values[i]
		}
	}
	return rec, nil
}

type jsonSource struct {
	dec    *json.Decoder
	array  bool
	first  *Record
	header []string
	row    int
}

func newJSON(r *bufio.Reader) (*jsonSource, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	s := &jsonSource{dec: dec}
	peek, _ := r.Peek(512)
	if t := bytes.TrimLeft(peek, " \t\r\n\xef\xbb\xbf"); len(t) > 0 && t[0] == '[' {
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("read json array: %w", err)
		}
		s.array = true
	}
	first, err := s.read()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if err == nil {
		s.first = &first
		s.header = first.Order
	}
	return s, nil
}

func (s *jsonSource) Header() []string { return s.header }

func (s *jsonSource) read() (Record, error) {
	if s.array && !s.dec.More() {
		return Record{}, io.EOF
	}
	var obj map[string]any
	if err := s.dec.Decode(&obj); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		s.row++
		return Record{Row: s.row}, fmt.Errorf("row %d: %w", s.row, err)
	}
	s.row++
	rec := Record{Row: s.row, Fields: make(map[string]string, len(obj))}
	for k, v := range obj {
		rec.Order = append(rec.Order, k)
		switch val := v.(type) {
		case nil:
			rec.Fields[k] = ""
		case string:
			rec.Fields[k] = val
		case json.Number:
			rec.Fields[k] = val.String()
		default:
			raw, _ := json.Marshal(val)
			rec.Fields[k] = string(raw)
		}
	}
	return rec, nil
}

func (s *jsonSource) Next() (Record, error) {
	if s.first != nil {
		r := *s.first
		s.first = nil
		return r, nil
	}
	return s.read()
}
