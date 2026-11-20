package analytics

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

const (
	CategoryGempa    = "GEMPA"
	CategoryTsunami  = "TSUNAMI"
	CategoryVolcanic = "VOLCANIC"
	CategoryWildfire = "WILDFIRE"
	CategoryCoastal  = "COASTAL"
)

var Categories = []string{CategoryGempa, CategoryTsunami, CategoryVolcanic, CategoryWildfire, CategoryCoastal}

var Severities = []string{
	string(hazard.SeverityNormal), string(hazard.SeverityWaspada), string(hazard.SeveritySiaga), string(hazard.SeverityAwas),
}

func Category(hazardType hazard.Type, attributes map[string]any) string {
	switch hazardType {
	case hazard.TypeSeismic:
		if tsunami, _ := attributes["potential_tsunami"].(bool); tsunami {
			return CategoryTsunami
		}
		return CategoryGempa
	case hazard.TypeVolcanic:
		return CategoryVolcanic
	case hazard.TypeWildfire:
		return CategoryWildfire
	case hazard.TypeCoastal:
		return CategoryCoastal
	}
	return string(hazardType)
}

const (
	BucketHour  = "hour"
	BucketDay   = "day"
	BucketWeek  = "week"
	BucketMonth = "month"

	maxBuckets = 1000
)

func Truncate(t time.Time, bucket string) time.Time {
	t = t.UTC()
	switch bucket {
	case BucketHour:
		return t.Truncate(time.Hour)
	case BucketWeek:
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
	case BucketMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
}

func next(t time.Time, bucket string) time.Time {
	switch bucket {
	case BucketHour:
		return t.Add(time.Hour)
	case BucketWeek:
		return t.AddDate(0, 0, 7)
	case BucketMonth:
		return t.AddDate(0, 1, 0)
	default:
		return t.AddDate(0, 0, 1)
	}
}

func autoBucket(span time.Duration) string {
	switch {
	case span <= 3*24*time.Hour:
		return BucketHour
	case span <= 120*24*time.Hour:
		return BucketDay
	case span <= 3*365*24*time.Hour:
		return BucketWeek
	default:
		return BucketMonth
	}
}

var ranges = map[string]time.Duration{
	"24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour, "1y": 365 * 24 * time.Hour,
}

type Query struct {
	From     time.Time
	To       time.Time
	Bucket   string
	AllTime  bool
	CacheKey string
}

var ErrBadQuery = errors.New("invalid analytics query")

func ParseQuery(v url.Values, now time.Time) (Query, error) {
	now = now.UTC()
	q := Query{To: now}
	rng := v.Get("range")
	switch {
	case v.Get("from") != "" || v.Get("to") != "":
		from, err := time.Parse(time.RFC3339, v.Get("from"))
		if err != nil {
			return Query{}, fmt.Errorf("%w: from must be RFC3339", ErrBadQuery)
		}
		to, err := time.Parse(time.RFC3339, v.Get("to"))
		if err != nil {
			return Query{}, fmt.Errorf("%w: to must be RFC3339", ErrBadQuery)
		}
		if !from.Before(to) {
			return Query{}, fmt.Errorf("%w: from must be before to", ErrBadQuery)
		}
		q.From, q.To = from.UTC(), to.UTC()
		q.CacheKey = "abs|" + q.From.Format(time.RFC3339) + "|" + q.To.Format(time.RFC3339)
	case rng == "all":
		q.AllTime = true
		q.From = time.Unix(0, 0).UTC()
		q.CacheKey = "all"
	case rng == "":
		rng = "30d"
		fallthrough
	default:
		span, ok := ranges[rng]
		if !ok {
			return Query{}, fmt.Errorf("%w: range must be 24h, 7d, 30d, 1y or all", ErrBadQuery)
		}
		q.From = now.Add(-span)
		q.CacheKey = "rel|" + rng
	}
	switch b := v.Get("bucket"); b {
	case "":
	case BucketHour, BucketDay, BucketWeek, BucketMonth:
		q.Bucket = b
	default:
		return Query{}, fmt.Errorf("%w: bucket must be hour, day, week or month", ErrBadQuery)
	}
	q.CacheKey += "|" + q.Bucket
	return q, nil
}

func (q Query) ResolveBucket() string {
	if q.Bucket != "" {
		return q.Bucket
	}
	return autoBucket(q.To.Sub(q.From))
}

func (q Query) resolveBucket(first, last time.Time) string {
	if q.Bucket != "" {
		return q.Bucket
	}
	from := q.From
	if q.AllTime {
		from = first
	}
	to := q.To
	if q.AllTime {
		to = last
	}
	return autoBucket(to.Sub(from))
}

type AreaCount struct {
	AreaName string `json:"area_name"`
	Count    int64  `json:"count"`
}

type Result struct {
	AsOf             time.Time          `json:"as_of"`
	From             time.Time          `json:"from"`
	To               time.Time          `json:"to"`
	Bucket           string             `json:"bucket"`
	Buckets          []time.Time        `json:"buckets"`
	Series           map[string][]int64 `json:"series"`
	BySeverity       map[string]int64   `json:"by_severity"`
	ByArea           []AreaCount        `json:"by_area"`
	ByCategory       map[string]int64   `json:"by_category"`
	Total            int64              `json:"total"`
	Partial          bool               `json:"partial"`
	UnavailableUnits []string           `json:"unavailable_units"`
	Method           string             `json:"method"`
}

type Point struct {
	Category string
	Severity string
	AreaName string
	At       time.Time
}

type Accumulator struct {
	points []Point
}

func (a *Accumulator) Add(p Point) { a.points = append(a.points, p) }

func (a *Accumulator) Result(q Query) Result {
	series := map[string]map[time.Time]int64{}
	r := Result{BySeverity: map[string]int64{}, ByCategory: map[string]int64{}}
	area := map[string]int64{}
	var first, last time.Time
	for i, p := range a.points {
		if i == 0 || p.At.Before(first) {
			first = p.At
		}
		if i == 0 || p.At.After(last) {
			last = p.At
		}
	}
	bucket := q.resolveBucket(first, last)
	for _, p := range a.points {
		t := Truncate(p.At, bucket)
		if series[p.Category] == nil {
			series[p.Category] = map[time.Time]int64{}
		}
		series[p.Category][t]++
		r.BySeverity[p.Severity]++
		r.ByCategory[p.Category]++
		area[p.AreaName]++
		r.Total++
	}
	return Assemble(q, bucket, first, last, series, r.BySeverity, area, r.Total)
}

func Assemble(q Query, bucket string, first, last time.Time, series map[string]map[time.Time]int64, bySeverity, byArea map[string]int64, total int64) Result {
	r := Result{From: q.From, To: q.To, Bucket: bucket, Series: map[string][]int64{}, BySeverity: map[string]int64{},
		ByCategory: map[string]int64{}, Total: total, UnavailableUnits: []string{}}
	for _, s := range Severities {
		r.BySeverity[s] = 0
	}
	for k, v := range bySeverity {
		r.BySeverity[k] = v
	}
	start, end := q.From, q.To
	if q.AllTime {
		if total == 0 {
			start, end = q.To, q.To
		} else {
			start, end = first, last
		}
		r.From, r.To = start, end
	}
	for t := Truncate(start, bucket); !t.After(end) && len(r.Buckets) < maxBuckets; t = next(t, bucket) {
		r.Buckets = append(r.Buckets, t)
	}
	index := make(map[time.Time]int, len(r.Buckets))
	for i, t := range r.Buckets {
		index[t] = i
	}
	for _, c := range Categories {
		r.Series[c] = make([]int64, len(r.Buckets))
		r.ByCategory[c] = 0
	}
	for c, points := range series {
		if _, ok := r.Series[c]; !ok {
			r.Series[c] = make([]int64, len(r.Buckets))
		}
		for t, n := range points {
			if i, ok := index[t]; ok {
				r.Series[c][i] += n
			}
			r.ByCategory[c] += n
		}
	}
	for name, n := range byArea {
		r.ByArea = append(r.ByArea, AreaCount{AreaName: name, Count: n})
	}
	sort.Slice(r.ByArea, func(i, j int) bool {
		if r.ByArea[i].Count != r.ByArea[j].Count {
			return r.ByArea[i].Count > r.ByArea[j].Count
		}
		return r.ByArea[i].AreaName < r.ByArea[j].AreaName
	})
	if r.ByArea == nil {
		r.ByArea = []AreaCount{}
	}
	return r
}

type Source interface {
	Analytics(ctx context.Context, q Query, includeRetracted bool) (Result, error)
}
