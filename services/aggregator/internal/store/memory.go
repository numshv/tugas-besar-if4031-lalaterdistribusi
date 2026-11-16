package store

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

type memoryFence struct {
	epoch  int64
	cursor string
}

type Memory struct {
	mu     sync.Mutex
	now    func() time.Time
	events map[string]hazard.Event
	fences map[hazard.Source]memoryFence
}

func NewMemory() *Memory {
	return &Memory{
		now:    time.Now,
		events: map[string]hazard.Event{},
		fences: map[hazard.Source]memoryFence{},
	}
}

func (m *Memory) WithClock(now func() time.Time) *Memory {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
	return m
}

func copyEvent(e hazard.Event) hazard.Event {
	if e.Attributes == nil {
		e.Attributes = map[string]any{}
	} else {
		e.Attributes = maps.Clone(e.Attributes)
	}
	return e
}

func (m *Memory) applyLocked(e hazard.Event) (hazard.Event, bool) {
	current, exists := m.events[e.HazardID]
	if exists && e.Version <= current.Version {
		return hazard.Event{}, false
	}
	stored := copyEvent(e)
	if exists {
		stored.IngestedAt = current.IngestedAt
	} else {
		stored.IngestedAt = ingestTime(m.now)
	}
	m.events[e.HazardID] = stored
	return copyEvent(stored), true
}

func (m *Memory) Upsert(_ context.Context, e hazard.Event) (hazard.Event, bool, error) {
	if err := validateEvent(e); err != nil {
		return hazard.Event{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, applied := m.applyLocked(e)
	return stored, applied, nil
}

func (m *Memory) CommitPoll(_ context.Context, source hazard.Source, epoch int64, cursor string, events []hazard.Event) ([]hazard.Event, error) {
	for _, e := range events {
		if err := validateEvent(e); err != nil {
			return nil, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.fences[source]; ok && f.epoch > epoch {
		return nil, fmt.Errorf("%w: source=%s stored=%d mine=%d", ErrStaleEpoch, source, f.epoch, epoch)
	}
	m.fences[source] = memoryFence{epoch: epoch, cursor: cursor}
	applied := make([]hazard.Event, 0, len(events))
	for _, e := range events {
		if stored, ok := m.applyLocked(e); ok {
			applied = append(applied, stored)
		}
	}
	return applied, nil
}

func (m *Memory) LoadCheckpoint(_ context.Context, source hazard.Source) (string, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.fences[source]
	return f.cursor, f.epoch, nil
}

func (m *Memory) Get(_ context.Context, hazardID string) (hazard.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[hazardID]
	if !ok {
		return hazard.Event{}, ErrNotFound
	}
	return copyEvent(e), nil
}

func before(a, b position) bool {
	if !a.at.Equal(b.at) {
		return a.at.Before(b.at)
	}
	return a.id < b.id
}

func (m *Memory) ListLatest(_ context.Context, q LatestQuery) (LatestPage, error) {
	after, err := validateLatest(q)
	if err != nil {
		return LatestPage{}, err
	}
	since := q.Since.UTC().Truncate(time.Millisecond)
	m.mu.Lock()
	matched := make([]hazard.Event, 0, len(m.events))
	for _, e := range m.events {
		if q.Type != "" && e.HazardType != q.Type {
			continue
		}
		if q.Source != "" && e.Source != q.Source {
			continue
		}
		if !q.Since.IsZero() && e.IngestedAt.Before(since) {
			continue
		}
		matched = append(matched, copyEvent(e))
	}
	m.mu.Unlock()

	pos := func(e hazard.Event) position { return position{at: e.IngestedAt, id: e.HazardID} }
	sort.Slice(matched, func(i, j int) bool {
		if q.Ascending {
			return before(pos(matched[i]), pos(matched[j]))
		}
		return before(pos(matched[j]), pos(matched[i]))
	})

	items := make([]hazard.Event, 0, q.Limit+1)
	for _, e := range matched {
		if after != nil {
			if q.Ascending && !before(*after, pos(e)) {
				continue
			}
			if !q.Ascending && !before(pos(e), *after) {
				continue
			}
		}
		items = append(items, e)
		if len(items) > q.Limit {
			break
		}
	}
	page := LatestPage{Items: items, UnavailableUnits: []string{}}
	if len(items) > q.Limit {
		page.Items = items[:q.Limit]
		page.NextCursor = encodeLatestCursor(q, page.Items[q.Limit-1])
	}
	return page, nil
}

func (m *Memory) ListByType(_ context.Context, hazardType hazard.Type, since time.Time, cursor *hazard.Cursor, limit int) ([]hazard.Event, bool, error) {
	if limit < 1 || limit > hazard.MaxPageLimit {
		return nil, false, fmt.Errorf("limit must be between 1 and %d", hazard.MaxPageLimit)
	}
	m.mu.Lock()
	matched := make([]hazard.Event, 0, len(m.events))
	for _, e := range m.events {
		if e.HazardType != hazardType {
			continue
		}
		if !since.IsZero() && e.OccurredAt.Before(since) {
			continue
		}
		if cursor != nil && !before(position{at: e.OccurredAt, id: e.HazardID}, position{at: cursor.OccurredAt, id: cursor.HazardID}) {
			continue
		}
		matched = append(matched, copyEvent(e))
	}
	m.mu.Unlock()

	sort.Slice(matched, func(i, j int) bool {
		return before(position{at: matched[j].OccurredAt, id: matched[j].HazardID}, position{at: matched[i].OccurredAt, id: matched[i].HazardID})
	})
	hasMore := len(matched) > limit
	if hasMore {
		matched = matched[:limit]
	}
	return matched, hasMore, nil
}
