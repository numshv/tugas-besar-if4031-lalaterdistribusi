package hazard

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	OpUpsert  = "upsert"
	OpRetract = "retract"
	OpPurge   = "purge"

	SimPrefix = "sim-"

	HeaderCorrelationID = "X-Correlation-ID"
	HeaderTraceparent   = "traceparent"
	HeaderProducedAt    = "produced_at"
	HeaderVersion       = "X-Hazard-Version"
	HeaderOp            = "X-Hazard-Op"

	MaxUpsertBytes    = 1 << 20
	maxSourceRefIDLen = 512
)

type UpsertMessage struct {
	Op          string         `json:"op"`
	Source      Source         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	Version     int64          `json:"version"`
	HazardType  Type           `json:"hazard_type,omitempty"`
	Severity    Severity       `json:"severity,omitempty"`
	AreaName    string         `json:"area_name,omitempty"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

type UpsertMeta struct {
	CorrelationID string
	Traceparent   string
	ProducedAt    time.Time
}

type EncodedUpsert struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

type InvalidUpsertError struct {
	Reason string
	Detail string
}

func (e *InvalidUpsertError) Error() string {
	return fmt.Sprintf("invalid hazard upsert (%s): %s", e.Reason, e.Detail)
}

func invalid(reason, format string, args ...any) error {
	return &InvalidUpsertError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

func (m UpsertMessage) Key() string {
	return string(m.Source) + "|" + m.SourceRefID
}

func (m UpsertMessage) Validate() error {
	if m.Op != OpUpsert && m.Op != OpRetract && m.Op != OpPurge {
		return invalid("unknown_op", "op %q must be %q, %q or %q", m.Op, OpUpsert, OpRetract, OpPurge)
	}
	if m.Op == OpPurge && !strings.HasPrefix(m.SourceRefID, SimPrefix) {
		return invalid("purge_not_allowed", "purge only deletes synthetic events whose source_ref_id starts with %q", SimPrefix)
	}
	if !m.Source.Known() {
		return invalid("unknown_source", "source %q is not known", m.Source)
	}
	if m.SourceRefID == "" || len(m.SourceRefID) > maxSourceRefIDLen {
		return invalid("bad_source_ref_id", "source_ref_id must be 1..%d bytes", maxSourceRefIDLen)
	}
	if m.Version <= 0 {
		return invalid("bad_version", "version must be > 0, got %d", m.Version)
	}
	if m.Op == OpRetract || m.Op == OpPurge {
		return nil
	}
	if !m.HazardType.Known() {
		return invalid("unknown_hazard_type", "hazard_type %q is not known", m.HazardType)
	}
	if !m.Severity.Known() {
		return invalid("unknown_severity", "severity %q is not known", m.Severity)
	}
	if m.OccurredAt.IsZero() {
		return invalid("missing_occurred_at", "occurred_at is required for upsert")
	}
	if m.Latitude < -90 || m.Latitude > 90 || m.Longitude < -180 || m.Longitude > 180 {
		return invalid("bad_coordinates", "latitude/longitude out of range: %v,%v", m.Latitude, m.Longitude)
	}
	return nil
}

func (m UpsertMessage) Event() Event {
	return Event{
		HazardID:    NewID(m.Source, m.SourceRefID),
		Source:      m.Source,
		SourceRefID: m.SourceRefID,
		HazardType:  m.HazardType,
		Severity:    m.Severity,
		AreaName:    m.AreaName,
		Latitude:    m.Latitude,
		Longitude:   m.Longitude,
		OccurredAt:  m.OccurredAt.UTC(),
		Attributes:  m.Attributes,
		Version:     m.Version,
		Retracted:   m.Op == OpRetract,
	}
}

func EncodeUpsert(m UpsertMessage, meta UpsertMeta) (EncodedUpsert, error) {
	if err := m.Validate(); err != nil {
		return EncodedUpsert{}, err
	}
	value, err := json.Marshal(m)
	if err != nil {
		return EncodedUpsert{}, fmt.Errorf("marshal hazard upsert: %w", err)
	}
	producedAt := meta.ProducedAt
	if producedAt.IsZero() {
		producedAt = time.Now()
	}
	headers := map[string]string{HeaderProducedAt: producedAt.UTC().Format(time.RFC3339Nano)}
	if meta.CorrelationID != "" {
		headers[HeaderCorrelationID] = meta.CorrelationID
	}
	if meta.Traceparent != "" {
		headers[HeaderTraceparent] = meta.Traceparent
	}
	return EncodedUpsert{Key: []byte(m.Key()), Value: value, Headers: headers}, nil
}

func DecodeUpsert(value []byte) (UpsertMessage, error) {
	if len(value) > MaxUpsertBytes {
		return UpsertMessage{}, invalid("too_large", "%d bytes exceeds %d", len(value), MaxUpsertBytes)
	}
	if !utf8.Valid(value) {
		return UpsertMessage{}, invalid("invalid_utf8", "payload is not valid UTF-8")
	}
	var m UpsertMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return UpsertMessage{}, invalid("invalid_json", "%v", err)
	}
	if err := m.Validate(); err != nil {
		return UpsertMessage{}, err
	}
	return m, nil
}

func EventHeaders(e Event) map[string]string {
	op := OpUpsert
	if e.Retracted {
		op = OpRetract
	}
	return map[string]string{
		HeaderVersion: strconv.FormatInt(e.Version, 10),
		HeaderOp:      op,
	}
}
