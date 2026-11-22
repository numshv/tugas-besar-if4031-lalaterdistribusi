package fire

import "github.com/lalaterdistribusi/tugas-besar-if4031/pkg/geo"

type Province = geo.Province

var Provinces = geo.Provinces

func NearestProvince(lat, lon float64) string { return geo.NearestProvince(lat, lon) }

func HaversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	return geo.HaversineKM(lat1, lon1, lat2, lon2)
}
