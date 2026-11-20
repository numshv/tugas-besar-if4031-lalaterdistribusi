package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

func TestFencedWriteFromOlderTermIsRejectedNotParked(t *testing.T) {
	p, _, q, c := newPipeline()
	mem := store.NewMemory()
	p.Store = mem
	ctx := context.Background()
	if _, err := mem.CommitPoll(ctx, hazard.SourcePVMBG, 5, "", nil); err != nil {
		t.Fatal(err)
	}

	stale := WithFence(ctx, Fence{Source: hazard.SourcePVMBG, Epoch: 4})
	err := p.IngestPVMBG(stale, pvmbg(`[`+v1+`]`, c.now()))
	if !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("err = %v, want ErrStaleEpoch", err)
	}
	if n := len(unprocessed(t, q)); n != 0 {
		t.Fatalf("a fenced write was set aside as bad data (%d messages)", n)
	}

	current := WithFence(ctx, Fence{Source: hazard.SourcePVMBG, Epoch: 5})
	if err := p.IngestPVMBG(current, pvmbg(`[`+v1+`]`, c.now())); err != nil {
		t.Fatalf("current leader rejected: %v", err)
	}
	id := hazard.NewID(hazard.SourcePVMBG, "r1")
	if _, err := mem.Get(ctx, id); err != nil {
		t.Fatalf("current leader's event not stored: %v", err)
	}
}
