package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

var (
	ErrStaleEpoch    = errors.New("stale epoch")
	ErrNotFound      = errors.New("hazard not found")
	ErrInvalidCursor = errors.New("invalid cursor")
	ErrInvalidQuery  = errors.New("invalid query")
	ErrInvalidEvent  = errors.New("invalid event")
)

const MaxLatestLimit = 1000

// Interface, bukan tipe konkret, supaya poller/API internal bisa diuji
// dengan fake store tanpa Mongo sungguhan menyala.
//
// ingested_at selalu diisi store saat event pertama kali disimpan (jam
// aggregator, dibulatkan ke milidetik) dan tidak pernah berubah sesudahnya;
// nilai IngestedAt dari caller diabaikan. Event hanya ditulis bila belum ada
// atau Version-nya lebih besar dari yang tersimpan.
type Store interface {
	Upsert(ctx context.Context, e hazard.Event) (stored hazard.Event, applied bool, err error)
	CommitPoll(ctx context.Context, source hazard.Source, epoch int64, cursor string, events []hazard.Event) (applied []hazard.Event, err error)
	LoadCheckpoint(ctx context.Context, source hazard.Source) (cursor string, epoch int64, err error)
	Get(ctx context.Context, hazardID string) (hazard.Event, error)
	ListLatest(ctx context.Context, q LatestQuery) (LatestPage, error)
	ListByType(ctx context.Context, hazardType hazard.Type, since time.Time, cursor *hazard.Cursor, limit int) ([]hazard.Event, bool, error)
}

type LatestQuery struct {
	Type      hazard.Type
	Source    hazard.Source
	Since     time.Time
	Ascending bool
	Limit     int
	Cursor    string
}

type LatestPage struct {
	Items            []hazard.Event
	NextCursor       string
	Partial          bool
	UnavailableUnits []string
}

type Health interface {
	Unavailable() (units []string, shardsDown []string)
}

type PartialMode string

const (
	PartialLiteral PartialMode = "literal"
	PartialData    PartialMode = "data"
)

func ParsePartialMode(raw string) (PartialMode, error) {
	switch PartialMode(raw) {
	case "", PartialLiteral:
		return PartialLiteral, nil
	case PartialData:
		return PartialData, nil
	}
	return "", fmt.Errorf("STORE_PARTIAL_MODE must be %q or %q", PartialLiteral, PartialData)
}

func partialStatus(h Health, mode PartialMode, mongosPartial bool) (bool, []string) {
	units := []string{}
	var shardsDown []string
	if h != nil {
		u, s := h.Unavailable()
		if u != nil {
			units = u
		}
		shardsDown = s
	}
	partial := mongosPartial
	switch mode {
	case PartialData:
		partial = partial || len(shardsDown) > 0
	default:
		partial = partial || len(units) > 0
	}
	return partial, units
}

type latestCursor struct {
	T  int64  `json:"t"`
	ID string `json:"id"`
	F  string `json:"f"`
}

type position struct {
	at time.Time
	id string
}

func filterKey(q LatestQuery) string {
	var since int64
	if !q.Since.IsZero() {
		since = q.Since.UnixMilli()
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%s|%d|%t", q.Type, q.Source, since, q.Ascending))
	return hex.EncodeToString(sum[:4])
}

func encodeLatestCursor(q LatestQuery, last hazard.Event) string {
	raw, _ := json.Marshal(latestCursor{T: last.IngestedAt.UnixMilli(), ID: last.HazardID, F: filterKey(q)})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func validateLatest(q LatestQuery) (*position, error) {
	if q.Limit < 1 || q.Limit > MaxLatestLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxLatestLimit)
	}
	if q.Cursor == "" {
		return nil, nil
	}
	if len(q.Cursor) > 1024 {
		return nil, ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var c latestCursor
	if err := json.Unmarshal(raw, &c); err != nil || c.ID == "" || c.F != filterKey(q) {
		return nil, ErrInvalidCursor
	}
	return &position{at: time.UnixMilli(c.T).UTC(), id: c.ID}, nil
}

func validateEvent(e hazard.Event) error {
	if e.HazardID == "" {
		return fmt.Errorf("%w: hazard_id is required", ErrInvalidEvent)
	}
	return nil
}

func ingestTime(now func() time.Time) time.Time {
	return now().UTC().Truncate(time.Millisecond)
}
