package pipeline

import (
	"context"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

// Fence ties a poll's writes to the leader term that started it. Writes under a fence
// go through Store.CommitPoll, which rejects them once a newer term has written.
type Fence struct {
	Source hazard.Source
	Epoch  int64
	Cursor string
}

type fenceKey struct{}

func WithFence(ctx context.Context, f Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}

func fenceFrom(ctx context.Context) (Fence, bool) {
	f, ok := ctx.Value(fenceKey{}).(Fence)
	return f, ok
}

// write stores ev, fenced when the context carries a Fence.
func (p *Pipeline) write(ctx context.Context, ev hazard.Event) (hazard.Event, bool, error) {
	f, ok := fenceFrom(ctx)
	if !ok {
		return p.Store.Upsert(ctx, ev)
	}
	applied, err := p.Store.CommitPoll(ctx, f.Source, f.Epoch, f.Cursor, []hazard.Event{ev})
	if err != nil || len(applied) == 0 {
		return hazard.Event{}, false, err
	}
	return applied[0], true, nil
}
