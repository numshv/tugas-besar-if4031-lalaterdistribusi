package store_test

import (
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store/storetest"
)

func TestMemoryContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T, now func() time.Time) store.Store {
		return store.NewMemory().WithClock(now)
	})
}
