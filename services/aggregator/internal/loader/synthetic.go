package loader

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

type zone struct {
	name     string
	lat, lon float64
	spreadKM float64
	weight   float64
}

var zones = []zone{
	{"Barat Aceh", 3.0, 95.8, 120, 0.10},
	{"Barat Daya Nias", 0.9, 97.2, 120, 0.09},
	{"Barat Daya Mentawai", -2.4, 99.6, 140, 0.11},
	{"Barat Daya Bengkulu", -4.6, 101.8, 120, 0.09},
	{"Selat Sunda", -6.6, 105.2, 90, 0.08},
	{"Selatan Jawa Barat", -8.1, 107.6, 120, 0.09},
	{"Selatan Malang", -9.0, 112.6, 120, 0.08},
	{"Selatan Bali", -9.1, 115.0, 90, 0.06},
	{"Teluk Tomini", -0.4, 120.4, 110, 0.05},
	{"Laut Maluku", 1.6, 126.4, 120, 0.06},
	{"Utara Halmahera", 2.2, 128.3, 100, 0.04},
	{"Laut Banda", -6.4, 129.6, 150, 0.05},
}

const scatterWeight = 0.10

var (
	catalogStart = time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	catalogSpan  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Sub(catalogStart)
)

func gutenbergRichter(r *rand.Rand, min, max float64) float64 {
	m := min - math.Log10(1-r.Float64())
	return math.Round(math.Min(m, max)*10) / 10
}

func around(r *rand.Rand, lat, lon, radiusKM float64) (float64, float64) {
	dist := radiusKM * math.Sqrt(r.Float64())
	theta := 2 * math.Pi * r.Float64()
	dLat := dist * math.Cos(theta) / 111.0
	dLon := dist * math.Sin(theta) / (111.0 * math.Cos(lat*math.Pi/180))
	return round5(lat + dLat), round5(lon + dLon)
}

func round5(v float64) float64 { return math.Round(v*1e5) / 1e5 }

func pickZone(r *rand.Rand) (zone, bool) {
	x := r.Float64()
	if x < scatterWeight {
		return zone{}, false
	}
	x = (x - scatterWeight) / (1 - scatterWeight)
	var total float64
	for _, z := range zones {
		total += z.weight
	}
	acc := 0.0
	for _, z := range zones {
		acc += z.weight / total
		if x < acc {
			return z, true
		}
	}
	return zones[len(zones)-1], true
}

func Generate(w io.Writer, count int, seed uint64) error {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"id", "time", "latitude", "longitude", "depth", "mag", "place", "tsunami"}); err != nil {
		return err
	}
	for i := range count {
		var lat, lon float64
		place := ""
		if z, ok := pickZone(r); ok {
			lat, lon = around(r, z.lat, z.lon, z.spreadKM)
			place = z.name
		} else {
			lat, lon = round5(-10+16*r.Float64()), round5(95+46*r.Float64())
		}
		at := catalogStart.Add(time.Duration(r.Int64N(int64(catalogSpan/time.Second))) * time.Second)
		depth := math.Round(math.Min(5+r.ExpFloat64()*35, 650)*10) / 10
		mag := gutenbergRichter(r, 3.0, 9.0)
		tsunami := "0"
		if mag >= 7.0 && depth < 70 {
			tsunami = "1"
		}
		row := []string{
			fmt.Sprintf("cat-%07d", i+1), at.Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(lat, 'f', -1, 64), strconv.FormatFloat(lon, 'f', -1, 64),
			strconv.FormatFloat(depth, 'f', -1, 64), strconv.FormatFloat(mag, 'f', -1, 64), place, tsunami,
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

type Spike struct {
	Run      string
	Count    int
	Region   string
	Lat, Lon float64
	RadiusKM float64
	Seed     uint64
	Now      time.Time
}

func SpikeID(run string, i int) string {
	return fmt.Sprintf("%sspike-%s-%06d", hazard.SimPrefix, run, i)
}

func (s Spike) Messages() ([]hazard.UpsertMessage, error) {
	r := rand.New(rand.NewPCG(s.Seed, s.Seed^0x51ed270b))
	out := make([]hazard.UpsertMessage, 0, s.Count)
	for i := range s.Count {
		lat, lon := around(r, s.Lat, s.Lon, s.RadiusKM)
		raw := map[string]any{
			"event_id":      SpikeID(s.Run, i),
			"magnitude":     gutenbergRichter(r, 3.0, 7.5),
			"occurred_at":   s.Now.Add(-time.Duration(r.IntN(3600)) * time.Second).UTC().Format(time.RFC3339),
			"region_name":   s.Region,
			"epicenter_lat": lat,
			"epicenter_lon": lon,
			"depth_km":      math.Round((5+r.ExpFloat64()*20)*10) / 10,
			"synthetic":     "spike",
		}
		msg, err := ToMessage(raw)
		if err != nil {
			return nil, fmt.Errorf("spike %d: %w", i, err)
		}
		out = append(out, msg)
	}
	return out, nil
}

func PurgeMessages(run string, count int) []hazard.UpsertMessage {
	out := make([]hazard.UpsertMessage, 0, count)
	for i := range count {
		out = append(out, hazard.UpsertMessage{Op: hazard.OpPurge, Source: hazard.SourceBMKG, SourceRefID: SpikeID(run, i), Version: 1})
	}
	return out
}
