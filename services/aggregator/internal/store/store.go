package store

import (
	"context"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

// Interface, bukan tipe konkret, supaya poller/API internal bisa diuji
// dengan fake store tanpa Mongo sungguhan menyala.
type Store interface {
	Upsert(ctx context.Context, e hazard.Event) error
	ListByType(ctx context.Context, hazardType hazard.Type, since time.Time, cursor *hazard.Cursor, limit int) ([]hazard.Event, bool, error)
}
