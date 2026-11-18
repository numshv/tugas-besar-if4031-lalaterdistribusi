package fire

import "math"

// Province is one entry of the static reference table.
type Province struct {
	Name     string
	Lat, Lon float64
}

// Provinces holds the area-weighted centroid of each of the 38 provinces,
// computed from "Indonesia GeoJSON & TopoJSON Maps (38 Provinces)" by
// denyherianto (CC BY 4.0), github.com/denyherianto/indonesia-geojson-topojson-maps-with-38-provinces.
var Provinces = []Province{
	{"Aceh", 4.2297, 96.9057},
	{"Sumatera Utara", 2.2017, 99.0624},
	{"Sumatera Barat", -0.8341, 100.4605},
	{"Riau", 0.5057, 101.7972},
	{"Jambi", -1.6935, 102.7167},
	{"Sumatera Selatan", -3.2164, 104.1695},
	{"Bengkulu", -3.5675, 102.3565},
	{"Lampung", -4.9113, 105.0147},
	{"Kepulauan Bangka Belitung", -2.4408, 106.5418},
	{"Kepulauan Riau", 1.5962, 105.6348},
	{"DKI Jakarta", -6.1997, 106.8360},
	{"Jawa Barat", -6.9160, 107.6097},
	{"Jawa Tengah", -7.2553, 110.1990},
	{"Daerah Istimewa Yogyakarta", -7.8822, 110.4372},
	{"Jawa Timur", -7.7259, 112.7067},
	{"Banten", -6.4636, 106.0953},
	{"Bali", -8.3680, 115.1047},
	{"Nusa Tenggara Barat", -8.6168, 117.4960},
	{"Nusa Tenggara Timur", -9.2651, 122.1723},
	{"Kalimantan Barat", -0.0896, 111.1265},
	{"Kalimantan Tengah", -1.6061, 113.4228},
	{"Kalimantan Selatan", -3.0060, 115.4401},
	{"Kalimantan Timur", 0.4564, 116.4415},
	{"Kalimantan Utara", 2.8982, 116.2204},
	{"Sulawesi Utara", 1.2234, 124.5019},
	{"Sulawesi Tengah", -1.0105, 121.1620},
	{"Sulawesi Selatan", -3.6888, 120.1605},
	{"Sulawesi Tenggara", -4.1043, 122.0400},
	{"Gorontalo", 0.6954, 122.3600},
	{"Sulawesi Barat", -2.4546, 119.3504},
	{"Maluku", -4.5712, 129.6439},
	{"Maluku Utara", 0.2857, 127.5746},
	{"Papua", -2.6324, 138.5687},
	{"Papua Barat", -2.6540, 133.6419},
	{"Papua Barat Daya", -1.1303, 132.0111},
	{"Papua Selatan", -6.6757, 139.5554},
	{"Papua Tengah", -3.9098, 136.6159},
	{"Papua Pegunungan", -4.2296, 139.4512},
}

// NearestProvince returns the province whose centroid is closest. Ties keep
// the earlier table entry, so the answer never depends on iteration order.
func NearestProvince(lat, lon float64) string {
	best, bestKM := "", math.Inf(1)
	for _, p := range Provinces {
		if d := HaversineKM(lat, lon, p.Lat, p.Lon); d < bestKM {
			best, bestKM = p.Name, d
		}
	}
	return best
}

const earthRadiusKM = 6371.0

func HaversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Min(1, math.Sqrt(a)))
}
