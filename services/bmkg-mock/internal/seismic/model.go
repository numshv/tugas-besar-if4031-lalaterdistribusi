package seismic

import "time"

const (
	ThreatWaspada = "Waspada"
	ThreatSiaga   = "Siaga"
	ThreatAwas    = "Awas"
)

type Event struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKm          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type Warning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

type region struct {
	name  string
	lat   float64
	lon   float64
	zones []string
}

var regions = []region{
	{"Selatan Bali", -9.10, 114.90, []string{"Badung", "Gianyar", "Klungkung"}},
	{"Barat Daya Bengkulu", -4.35, 101.60, []string{"Bengkulu", "Seluma", "Kaur"}},
	{"Selat Sunda", -6.45, 105.20, []string{"Pandeglang", "Serang", "Lampung Selatan"}},
	{"Barat Daya Nias", 0.95, 97.10, []string{"Nias Selatan", "Nias Barat"}},
	{"Utara Halmahera", 2.30, 128.40, []string{"Halmahera Utara", "Pulau Morotai"}},
	{"Selatan Malang", -9.20, 112.60, []string{"Malang", "Lumajang", "Blitar"}},
	{"Barat Daya Mentawai", -2.60, 99.20, []string{"Kepulauan Mentawai", "Pesisir Selatan"}},
	{"Laut Banda", -6.30, 129.90, []string{"Maluku Tengah", "Maluku Barat Daya"}},
	{"Teluk Tomini", -0.20, 120.30, []string{"Donggala", "Palu", "Sigi"}},
	{"Selatan Sumbawa", -9.30, 117.40, []string{"Sumbawa", "Dompu", "Bima"}},
	{"Barat Aceh", 4.20, 95.10, []string{"Aceh Barat", "Aceh Jaya", "Simeulue"}},
	{"Selatan Jawa Barat", -8.10, 107.30, []string{"Garut", "Tasikmalaya", "Cianjur"}},
}
