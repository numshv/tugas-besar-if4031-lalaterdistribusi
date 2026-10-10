package hazard

import "time"

type Source string

const (
	SourceBMKG  Source = "BMKG"
	SourcePVMBG Source = "PVMBG"
)

type Type string

const (
	TypeSeismic  Type = "SEISMIC"
	TypeVolcanic Type = "VOLCANIC"
)

type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

type Event struct {
	HazardID    string    `json:"hazard_id"     bson:"_id"`
	Source      Source    `json:"source"        bson:"source"`
	SourceRefID string    `json:"source_ref_id" bson:"source_ref_id"`
	HazardType  Type      `json:"hazard_type"   bson:"hazard_type"`
	Severity    Severity  `json:"severity"      bson:"severity"`
	AreaName    string    `json:"area_name"     bson:"area_name"`
	Latitude    float64   `json:"latitude"      bson:"latitude"`
	Longitude   float64   `json:"longitude"     bson:"longitude"`
	OccurredAt  time.Time `json:"occurred_at"   bson:"occurred_at"`
	IngestedAt  time.Time `json:"ingested_at"   bson:"ingested_at"`

	Attributes map[string]any `json:"attributes" bson:"attributes"`
}

var SummaryFields = []string{
	"hazard_id",
	"source",
	"hazard_type",
	"severity",
	"area_name",
	"occurred_at",
	"ingested_at",
}
