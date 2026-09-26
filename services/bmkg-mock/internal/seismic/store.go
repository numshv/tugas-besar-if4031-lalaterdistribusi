package seismic

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

var seedSpecs = []struct {
	minutesAgo int
	magnitude  float64
	depthKm    float64
	region     int
	threat     string
}{
	{1440, 4.1, 32.0, 0, ""},
	{1320, 3.6, 12.5, 5, ""},
	{1250, 5.2, 48.0, 1, ""},
	{1180, 4.8, 25.0, 11, ""},
	{1090, 6.7, 18.0, 3, ""},
	{1010, 3.9, 60.0, 8, ""},
	{950, 5.8, 22.0, 2, ThreatWaspada},
	{880, 4.4, 88.0, 6, ""},
	{800, 6.2, 15.0, 7, ThreatSiaga},
	{742, 3.2, 41.0, 9, ""},
	{690, 5.4, 10.0, 2, ThreatAwas},
	{610, 4.9, 55.0, 10, ""},
	{540, 5.0, 30.0, 4, ""},
	{470, 4.2, 19.0, 5, ""},
	{400, 7.3, 24.0, 10, ThreatAwas},
	{330, 3.8, 72.0, 1, ""},
	{265, 6.5, 40.0, 6, ""},
	{200, 4.99, 27.0, 11, ""},
	{155, 5.6, 33.0, 8, ThreatWaspada},
	{110, 4.6, 64.0, 0, ""},
	{70, 6.9, 12.0, 3, ThreatSiaga},
	{25, 3.4, 45.0, 9, ""},
}

type warningRecord struct {
	Warning
	eventAt time.Time
}

type Store struct {
	mu         sync.RWMutex
	events     []Event
	warnings   []warningRecord
	eventSeq   int
	warningSeq int
}

func NewStore(seedCount int) *Store {
	s := &Store{}
	now := time.Now().UTC()

	for extra := seedCount - len(seedSpecs); extra > 0; extra-- {
		mag, depth, reg, threat := randomQuake()
		s.append(now.Add(-time.Duration(1440+extra*37)*time.Minute), mag, depth, reg, threat)
	}

	for _, spec := range seedSpecs {
		s.append(now.Add(-time.Duration(spec.minutesAgo)*time.Minute),
			spec.magnitude, spec.depthKm, regions[spec.region], spec.threat)
	}

	return s
}

func (s *Store) append(at time.Time, magnitude, depthKm float64, reg region, threat string) Event {
	at = at.Truncate(time.Second)
	s.eventSeq++
	e := Event{
		EventID:          fmt.Sprintf("EQ-%s-%04d", at.Format("2006-0102"), s.eventSeq),
		Magnitude:        magnitude,
		DepthKm:          depthKm,
		EpicenterLat:     reg.lat,
		EpicenterLon:     reg.lon,
		RegionName:       reg.name,
		OccurredAt:       at,
		PotentialTsunami: threat != "",
	}
	s.events = append(s.events, e)

	if threat != "" {
		s.warningSeq++
		s.warnings = append(s.warnings, warningRecord{
			Warning: Warning{
				WarningID:        fmt.Sprintf("TW-%s-%04d", at.Format("2006-0102"), s.warningSeq),
				RelatedEventID:   e.EventID,
				ThreatLevel:      threat,
				AffectedZones:    reg.zones,
				EstimatedArrival: at.Add(time.Duration(20+rand.IntN(26)) * time.Minute),
			},
			eventAt: at,
		})
	}
	return e
}

func randomQuake() (magnitude, depthKm float64, reg region, threat string) {
	magnitude = float64(30+rand.IntN(46)) / 10
	depthKm = float64(50+rand.IntN(1150)) / 10
	reg = regions[rand.IntN(len(regions))]

	if magnitude >= 6.0 && depthKm <= 70 {
		switch {
		case magnitude >= 7.0:
			threat = ThreatAwas
		case magnitude >= 6.5:
			threat = ThreatSiaga
		default:
			threat = ThreatWaspada
		}
	}
	return magnitude, depthKm, reg, threat
}

func (s *Store) EventsSince(since time.Time) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Event, 0, len(s.events))
	for _, e := range s.events {
		if !e.OccurredAt.Before(since) {
			out = append(out, e)
		}
	}
	return out
}

func (s *Store) WarningsSince(since time.Time) []Warning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Warning, 0, len(s.warnings))
	for _, w := range s.warnings {
		if !w.eventAt.Before(since) {
			out = append(out, w.Warning)
		}
	}
	return out
}

func (s *Store) Generate() Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	mag, depth, reg, threat := randomQuake()
	return s.append(time.Now().UTC(), mag, depth, reg, threat)
}

func (s *Store) Run(ctx context.Context, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e := s.Generate()
			log.InfoContext(ctx, "seismic event generated",
				"event_id", e.EventID,
				"magnitude", e.Magnitude,
				"region_name", e.RegionName,
				"potential_tsunami", e.PotentialTsunami)
		}
	}
}
