package volcanic

import "time"

const (
	AlertNormal  = "Normal"
	AlertWaspada = "Waspada"
	AlertSiaga   = "Siaga"
	AlertAwas    = "Awas"
)

// Schema versions served by GET /volcanic-reports. 1 and 2 are the M1 contract.
// 3 to 5 are incompatible changes (they touch a field the consumer requires).
// 6 is a compatible change (an optional field disappears). 3 to 6 build on 2.
const (
	SchemaLegacy         = 1 // no confidence_level
	SchemaConfidence     = 2 // adds confidence_level
	SchemaRenamedLevel   = 3 // alert_level renamed to level
	SchemaNumericLevel   = 4 // alert_level becomes a number 1..4
	SchemaNoReportedAt   = 5 // required reported_at removed
	SchemaNoAshColumn    = 6 // optional ash_column_height_m removed
	SchemaMin, SchemaMax = SchemaLegacy, SchemaNoAshColumn
)

// ValidSchema reports whether v is a schema version the mock can serve.
func ValidSchema(v int) bool { return v >= SchemaMin && v <= SchemaMax }

var numericAlertLevel = map[string]int{AlertNormal: 1, AlertWaspada: 2, AlertSiaga: 3, AlertAwas: 4}

type Report struct {
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM float64
	ReportedAt       time.Time
	Confidence       float64
}

// Payload renders the report in the given schema version.
func (r Report) Payload(schemaVersion int) map[string]any {
	out := map[string]any{
		"report_id":           r.ReportID,
		"volcano_id":          r.VolcanoID,
		"alert_level":         r.AlertLevel,
		"eruption_count_24h":  r.EruptionCount24h,
		"ash_column_height_m": r.AshColumnHeightM,
		"reported_at":         r.ReportedAt,
	}
	if schemaVersion >= SchemaConfidence {
		out["confidence_level"] = r.Confidence
	}
	switch schemaVersion {
	case SchemaRenamedLevel:
		out["level"] = out["alert_level"]
		delete(out, "alert_level")
	case SchemaNumericLevel:
		out["alert_level"] = numericAlertLevel[r.AlertLevel]
	case SchemaNoReportedAt:
		delete(out, "reported_at")
	case SchemaNoAshColumn:
		delete(out, "ash_column_height_m")
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
