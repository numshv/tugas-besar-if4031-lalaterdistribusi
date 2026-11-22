package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

var ErrPartialRead = errors.New("part of the storage is unavailable; retract again once it is back")

// RetractSource retracts every live HazardEvent of a source, each one version
// above what is stored. Internal Ops use it to clear a replaced feed session
// when the processor no longer knows the old fires; anything the source sends
// later carries a newer version and revives what it needs.
func (p *Pipeline) RetractSource(ctx context.Context, source hazard.Source) (int, error) {
	retracted, cursor := 0, ""
	for {
		page, err := p.Store.ListLatest(ctx, store.LatestQuery{Source: source, Ascending: true, Limit: 500, Cursor: cursor})
		if err != nil {
			return retracted, err
		}
		if page.Partial {
			return retracted, ErrPartialRead
		}
		for _, ev := range page.Items {
			stored, applied, err := p.Store.Retract(ctx, ev.Source, ev.SourceRefID, ev.Version+1)
			if err != nil {
				return retracted, fmt.Errorf("retract %s: %w", ev.HazardID, err)
			}
			if !applied {
				continue
			}
			retracted++
			if err := p.Pub.Publish(ctx, stored); err != nil {
				p.Log.WarnContext(ctx, "retract stored but not published", "hazard_id", stored.HazardID, "error", err)
			}
		}
		if page.NextCursor == "" {
			return retracted, nil
		}
		cursor = page.NextCursor
	}
}
