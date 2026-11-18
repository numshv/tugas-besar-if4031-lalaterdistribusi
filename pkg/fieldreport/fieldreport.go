// Package fieldreport is the message Tim Lapangan sends through POST /field-reports,
// shared by field-report-api (producer) and the aggregator (consumer).
package fieldreport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

const (
	MaxDescription = 2000
	MaxBodyBytes   = 16 << 10
)

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{8,128}$`)

// ValidKey reports whether k can be used as an Idempotency-Key and report_id.
func ValidKey(k string) bool { return keyPattern.MatchString(k) }

// Input is the request body.
type Input struct {
	HazardType       hazard.Type `json:"hazard_type"`
	Latitude         *float64    `json:"latitude"`
	Longitude        *float64    `json:"longitude"`
	ObservedAt       time.Time   `json:"observed_at"`
	Description      string      `json:"description"`
	AreaName         string      `json:"area_name,omitempty"`
	SeverityEstimate string      `json:"severity_estimate,omitempty"`
}

// Report is what goes to Kafka and is stored by the aggregator.
type Report struct {
	ReportID   string    `json:"report_id"`
	Reporter   string    `json:"reporter"`
	Scope      string    `json:"scope"`
	AcceptedAt time.Time `json:"accepted_at"`
	Input
}

var hazardTypes = map[hazard.Type]bool{
	hazard.TypeSeismic: true, hazard.TypeVolcanic: true, hazard.TypeWildfire: true, hazard.TypeCoastal: true,
}

var severities = map[string]bool{"": true, "NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}

// Validate returns the first problem with the input, or nil.
func (in Input) Validate() error {
	switch {
	case !hazardTypes[in.HazardType]:
		return errors.New("hazard_type harus SEISMIC, VOLCANIC, WILDFIRE, atau COASTAL")
	case in.Latitude == nil || *in.Latitude < -90 || *in.Latitude > 90:
		return errors.New("latitude wajib, antara -90 dan 90")
	case in.Longitude == nil || *in.Longitude < -180 || *in.Longitude > 180:
		return errors.New("longitude wajib, antara -180 dan 180")
	case in.ObservedAt.IsZero():
		return errors.New("observed_at wajib, format RFC 3339")
	case strings.TrimSpace(in.Description) == "":
		return errors.New("description wajib diisi")
	case utf8.RuneCountInString(in.Description) > MaxDescription:
		return fmt.Errorf("description maksimal %d karakter", MaxDescription)
	case !severities[in.SeverityEstimate]:
		return errors.New("severity_estimate harus NORMAL, WASPADA, SIAGA, atau AWAS")
	}
	return nil
}

// Hash fingerprints the reporter's content, to detect one key reused for different reports.
func (r Report) Hash() string {
	b, _ := json.Marshal(r.Input)
	sum := sha256.Sum256(append([]byte(r.Reporter+"\x00"), b...))
	return hex.EncodeToString(sum[:])
}
