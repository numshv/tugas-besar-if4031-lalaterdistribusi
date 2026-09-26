package httpx

import "time"

func msSince(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}
