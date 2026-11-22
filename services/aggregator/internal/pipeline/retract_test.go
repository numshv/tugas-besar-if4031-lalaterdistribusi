package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

func TestRetractSourceClearsOnlyThatSource(t *testing.T) {
	p, _, _, _ := newPipeline()
	mem := store.NewMemory()
	p.Store = mem
	ctx := context.Background()
	put := func(src hazard.Source, typ hazard.Type, ref string, version int64) {
		_, _, err := mem.Upsert(ctx, hazard.Event{
			HazardID: hazard.NewID(src, ref), Source: src, SourceRefID: ref, HazardType: typ,
			Severity: hazard.SeverityWaspada, OccurredAt: time.Now().UTC(), Version: version,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range 7 {
		put(hazard.SourceSatelit, hazard.TypeWildfire, fmt.Sprintf("fire-%d", i), 1_763_000_000_000_000_000+int64(i))
	}
	put(hazard.SourceBMKG, hazard.TypeSeismic, "EQ-1", 1)

	n, err := p.RetractSource(ctx, hazard.SourceSatelit)
	if err != nil || n != 7 {
		t.Fatalf("retracted %d, err %v", n, err)
	}
	page, _ := mem.ListLatest(ctx, store.LatestQuery{Source: hazard.SourceSatelit, Limit: 100})
	if len(page.Items) != 0 {
		t.Fatalf("%d SATELIT events still live", len(page.Items))
	}
	page, _ = mem.ListLatest(ctx, store.LatestQuery{Source: hazard.SourceBMKG, Limit: 100})
	if len(page.Items) != 1 {
		t.Fatal("BMKG event was touched")
	}
	if n, _ := p.RetractSource(ctx, hazard.SourceSatelit); n != 0 {
		t.Fatalf("second call retracted %d", n)
	}
	// The next feed session's versions are far above old+1, so it still wins.
	put(hazard.SourceSatelit, hazard.TypeWildfire, "fire-0", 1_764_000_000_000_000_001)
	page, _ = mem.ListLatest(ctx, store.LatestQuery{Source: hazard.SourceSatelit, Limit: 100})
	if len(page.Items) != 1 {
		t.Fatal("a newer session could not revive its fire")
	}
}
