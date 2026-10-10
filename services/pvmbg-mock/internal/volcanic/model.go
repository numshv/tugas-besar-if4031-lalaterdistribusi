package volcanic

import "time"

const (
	AlertNormal  = "Normal"
	AlertWaspada = "Waspada"
	AlertSiaga   = "Siaga"
	AlertAwas    = "Awas"
)

const (
	SchemaLegacy     = 1
	SchemaConfidence = 2
)

type Report struct {
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM float64
	ReportedAt       time.Time
	Confidence       float64
}

type ReportResponse struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
}

func (r Report) Response(schemaVersion int) ReportResponse {
	out := ReportResponse{
		ReportID:         r.ReportID,
		VolcanoID:        r.VolcanoID,
		AlertLevel:       r.AlertLevel,
		EruptionCount24h: r.EruptionCount24h,
		AshColumnHeightM: r.AshColumnHeightM,
		ReportedAt:       r.ReportedAt,
	}
	if schemaVersion >= SchemaConfidence {
		confidence := r.Confidence
		out.ConfidenceLevel = &confidence
	}
	return out
}

var volcanoIDs = []string{
	"VOL-MERAPI",
	"VOL-SEMERU",
	"VOL-SINABUNG",
	"VOL-KELUD",
	"VOL-BROMO",
	"VOL-AGUNG",
	"VOL-RAUNG",
	"VOL-KRAKATAU",
	"VOL-MARAPI",
	"VOL-LEWOTOBI",
	"VOL-ILE-LEWOTOLOK",
	"VOL-DUKONO",
	"VOL-IBU",
	"VOL-GAMALAMA",
	"VOL-SLAMET",
}
