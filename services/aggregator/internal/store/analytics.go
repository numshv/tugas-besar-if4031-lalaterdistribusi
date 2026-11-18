package store

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/analytics"
)

func inRange(q analytics.Query, at time.Time) bool {
	return q.AllTime || (!at.Before(q.From) && at.Before(q.To))
}

func (m *Memory) Analytics(_ context.Context, q analytics.Query, includeRetracted bool) (analytics.Result, error) {
	m.mu.Lock()
	var acc analytics.Accumulator
	for _, e := range m.events {
		if (e.Retracted && !includeRetracted) || !inRange(q, e.OccurredAt) {
			continue
		}
		acc.Add(analytics.Point{Category: analytics.Category(e.HazardType, e.Attributes), Severity: string(e.Severity), AreaName: e.AreaName, At: e.OccurredAt})
	}
	m.mu.Unlock()
	r := acc.Result(q)
	r.Method = "memory"
	return r, nil
}

const analyticsTimeout = 10 * time.Second

func analyticsMatch(q analytics.Query, includeRetracted bool) bson.M {
	match := bson.M{}
	if !q.AllTime {
		match["occurred_at"] = bson.M{"$gte": q.From, "$lt": q.To}
	}
	if !includeRetracted {
		match["retracted"] = bson.M{"$ne": true}
	}
	return match
}

var categoryExpr = bson.M{"$switch": bson.M{
	"branches": bson.A{
		bson.M{"case": bson.M{"$and": bson.A{
			bson.M{"$eq": bson.A{"$hazard_type", string(hazard.TypeSeismic)}},
			bson.M{"$eq": bson.A{"$attributes.potential_tsunami", true}},
		}}, "then": analytics.CategoryTsunami},
		bson.M{"case": bson.M{"$eq": bson.A{"$hazard_type", string(hazard.TypeSeismic)}}, "then": analytics.CategoryGempa},
	},
	"default": "$hazard_type",
}}

type facetRow struct {
	ID struct {
		C string    `bson:"c"`
		T time.Time `bson:"t"`
	} `bson:"_id"`
	N int64 `bson:"n"`
}

type keyRow struct {
	ID *string `bson:"_id"`
	N  int64   `bson:"n"`
}

type facetResult struct {
	Series   []facetRow `bson:"series"`
	Severity []keyRow   `bson:"severity"`
	Area     []keyRow   `bson:"area"`
	Stats    []struct {
		N     int64     `bson:"n"`
		First time.Time `bson:"first"`
		Last  time.Time `bson:"last"`
	} `bson:"stats"`
}

func (s *MongoStore) Analytics(ctx context.Context, q analytics.Query, includeRetracted bool) (analytics.Result, error) {
	defer observe("analytics", time.Now())
	ctx, cancel := context.WithTimeout(ctx, analyticsTimeout)
	defer cancel()

	r, err := s.analyticsAggregate(ctx, q, includeRetracted)
	if err != nil {
		r, err = s.analyticsScan(ctx, q, includeRetracted)
		if err != nil {
			return analytics.Result{}, fmt.Errorf("analytics: %w", err)
		}
		r.Partial = true
		r.Method = "partial_scan"
		partialReads.WithLabelValues("analytics").Inc()
	}
	if s.health != nil {
		units, shardsDown := s.health.Unavailable()
		if units != nil {
			r.UnavailableUnits = units
		}
		if len(shardsDown) > 0 && !r.Partial {
			r.Partial = true
			partialReads.WithLabelValues("analytics").Inc()
		}
	}
	return r, nil
}

func (s *MongoStore) analyticsAggregate(ctx context.Context, q analytics.Query, includeRetracted bool) (analytics.Result, error) {
	match := analyticsMatch(q, includeRetracted)
	bucket := q.Bucket
	if bucket == "" && q.AllTime {
		var bounds []struct {
			First time.Time `bson:"first"`
			Last  time.Time `bson:"last"`
		}
		cur, err := s.col.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: match}},
			{{Key: "$group", Value: bson.M{"_id": nil, "first": bson.M{"$min": "$occurred_at"}, "last": bson.M{"$max": "$occurred_at"}}}},
		}, options.Aggregate().SetMaxTime(analyticsTimeout))
		if err != nil {
			return analytics.Result{}, err
		}
		if err := cur.All(ctx, &bounds); err != nil {
			return analytics.Result{}, err
		}
		if len(bounds) > 0 {
			bucket = analytics.Query{From: bounds[0].First, To: bounds[0].Last}.ResolveBucket()
		} else {
			bucket = analytics.BucketDay
		}
	}
	if bucket == "" {
		bucket = q.ResolveBucket()
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$project", Value: bson.M{
			"cat": categoryExpr, "severity": 1, "area_name": 1, "occurred_at": 1,
			"t": bson.M{"$dateTrunc": bson.M{"date": "$occurred_at", "unit": bucket, "timezone": "UTC", "startOfWeek": "monday"}},
		}}},
		{{Key: "$facet", Value: bson.M{
			"series":   bson.A{bson.M{"$group": bson.M{"_id": bson.M{"c": "$cat", "t": "$t"}, "n": bson.M{"$sum": 1}}}},
			"severity": bson.A{bson.M{"$group": bson.M{"_id": "$severity", "n": bson.M{"$sum": 1}}}},
			"area":     bson.A{bson.M{"$group": bson.M{"_id": "$area_name", "n": bson.M{"$sum": 1}}}},
			"stats":    bson.A{bson.M{"$group": bson.M{"_id": nil, "n": bson.M{"$sum": 1}, "first": bson.M{"$min": "$occurred_at"}, "last": bson.M{"$max": "$occurred_at"}}}},
		}}},
	}
	cur, err := s.col.Aggregate(ctx, pipeline, options.Aggregate().SetMaxTime(analyticsTimeout).SetAllowDiskUse(true))
	if err != nil {
		return analytics.Result{}, err
	}
	var out []facetResult
	if err := cur.All(ctx, &out); err != nil {
		return analytics.Result{}, err
	}
	if len(out) == 0 {
		return analytics.Result{}, fmt.Errorf("empty $facet result")
	}
	f := out[0]
	series := map[string]map[time.Time]int64{}
	for _, row := range f.Series {
		if series[row.ID.C] == nil {
			series[row.ID.C] = map[time.Time]int64{}
		}
		series[row.ID.C][row.ID.T.UTC()] += row.N
	}
	sev, area := map[string]int64{}, map[string]int64{}
	for _, row := range f.Severity {
		sev[deref(row.ID)] += row.N
	}
	for _, row := range f.Area {
		area[deref(row.ID)] += row.N
	}
	var total int64
	var first, last time.Time
	if len(f.Stats) > 0 {
		total, first, last = f.Stats[0].N, f.Stats[0].First.UTC(), f.Stats[0].Last.UTC()
	}
	r := analytics.Assemble(q, bucket, first, last, series, sev, area, total)
	r.Method = "aggregate"
	return r, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (s *MongoStore) analyticsScan(ctx context.Context, q analytics.Query, includeRetracted bool) (analytics.Result, error) {
	cur, err := s.col.Find(ctx, analyticsMatch(q, includeRetracted), options.Find().
		SetProjection(bson.M{"_id": 0, "hazard_type": 1, "severity": 1, "area_name": 1, "occurred_at": 1, "attributes.potential_tsunami": 1}).
		SetAllowPartialResults(true).
		SetBatchSize(5000).
		SetMaxTime(analyticsTimeout))
	if err != nil {
		return analytics.Result{}, err
	}
	defer cur.Close(ctx)
	var acc analytics.Accumulator
	for cur.Next(ctx) {
		var d struct {
			HazardType string         `bson:"hazard_type"`
			Severity   string         `bson:"severity"`
			AreaName   string         `bson:"area_name"`
			OccurredAt time.Time      `bson:"occurred_at"`
			Attributes map[string]any `bson:"attributes"`
		}
		if err := cur.Decode(&d); err != nil {
			return analytics.Result{}, err
		}
		acc.Add(analytics.Point{Category: analytics.Category(hazard.Type(d.HazardType), d.Attributes), Severity: d.Severity, AreaName: d.AreaName, At: d.OccurredAt.UTC()})
	}
	if err := cur.Err(); err != nil {
		return analytics.Result{}, err
	}
	return acc.Result(q), nil
}
