package volcanic

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
	volcano    int
	alertLevel string
	eruptions  int
	ashHeightM float64
	confidence float64
}{
	{1400, 0, AlertSiaga, 3, 1800, 0.88},
	{1330, 1, AlertAwas, 11, 4200, 0.91},
	{1270, 4, AlertNormal, 0, 0, 0.95},
	{1200, 11, AlertWaspada, 2, 900, 0.72},
	{1140, 8, AlertSiaga, 5, 2100, 0.83},
	{1060, 14, AlertNormal, 0, 0, 0.94},
	{990, 9, AlertAwas, 14, 3800, 0.79},
	{930, 2, AlertWaspada, 1, 650, 0.68},
	{860, 12, AlertSiaga, 6, 1500, 0.86},
	{790, 5, AlertNormal, 0, 0, 0.92},
	{720, 10, AlertWaspada, 3, 1100, 0.74},
	{650, 7, AlertSiaga, 8, 2600, 0.81},
	{580, 6, AlertNormal, 0, 0, 0.90},
	{510, 13, AlertWaspada, 2, 750, 0.70},
	{440, 3, AlertSiaga, 4, 1950, 0.85},
	{370, 1, AlertAwas, 16, 4500, 0.93},
	{300, 0, AlertSiaga, 2, 1400, 0.87},
	{230, 11, AlertWaspada, 1, 820, 0.71},
	{160, 8, AlertAwas, 9, 3100, 0.80},
	{90, 4, AlertNormal, 0, 0, 0.96},
}

type Store struct {
	run     string
	mu      sync.RWMutex
	reports []Report
	seq     int
}

func NewStore(seedCount int) *Store {
	s := &Store{run: fmt.Sprintf("%06x", rand.Uint32N(1<<24))}
	now := time.Now().UTC()

	for extra := seedCount - len(seedSpecs); extra > 0; extra-- {
		level, eruptions, ash := randomActivity()
		s.append(now.Add(-time.Duration(1400+extra*43)*time.Minute),
			volcanoIDs[rand.IntN(len(volcanoIDs))], level, eruptions, ash, randomConfidence())
	}

	for _, spec := range seedSpecs {
		s.append(now.Add(-time.Duration(spec.minutesAgo)*time.Minute),
			volcanoIDs[spec.volcano], spec.alertLevel, spec.eruptions, spec.ashHeightM, spec.confidence)
	}

	return s
}

func (s *Store) RunID() string { return s.run }

func (s *Store) append(at time.Time, volcanoID, alertLevel string, eruptions int, ashHeightM, confidence float64) Report {
	at = at.Truncate(time.Second)
	s.seq++
	r := Report{
		ReportID:         fmt.Sprintf("VR-%s-%s-%04d", at.Format("2006-0102"), s.run, s.seq),
		VolcanoID:        volcanoID,
		AlertLevel:       alertLevel,
		EruptionCount24h: eruptions,
		AshColumnHeightM: ashHeightM,
		ReportedAt:       at,
		Confidence:       confidence,
	}
	s.reports = append(s.reports, r)
	return r
}

func randomActivity() (alertLevel string, eruptions int, ashHeightM float64) {
	switch rand.IntN(10) {
	case 0:
		return AlertAwas, 8 + rand.IntN(12), float64(3000 + rand.IntN(1800))
	case 1, 2, 3:
		return AlertSiaga, 2 + rand.IntN(6), float64(1200 + rand.IntN(1600))
	case 4, 5, 6:
		return AlertWaspada, rand.IntN(4), float64(500 + rand.IntN(800))
	default:
		return AlertNormal, 0, 0
	}
}

func randomConfidence() float64 {
	return float64(60+rand.IntN(40)) / 100
}

func (s *Store) ReportsSince(since time.Time) []Report {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Report, 0, len(s.reports))
	for _, r := range s.reports {
		if !r.ReportedAt.Before(since) {
			out = append(out, r)
		}
	}
	return out
}

func (s *Store) Generate() Report {
	s.mu.Lock()
	defer s.mu.Unlock()

	level, eruptions, ash := randomActivity()
	return s.append(time.Now().UTC(), volcanoIDs[rand.IntN(len(volcanoIDs))], level, eruptions, ash, randomConfidence())
}

func (s *Store) Run(ctx context.Context, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r := s.Generate()
			log.InfoContext(ctx, "volcanic report generated",
				"report_id", r.ReportID,
				"volcano_id", r.VolcanoID,
				"alert_level", r.AlertLevel)
		}
	}
}
