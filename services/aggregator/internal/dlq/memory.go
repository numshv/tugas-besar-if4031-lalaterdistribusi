package dlq

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is an in-process Store for tests.
type MemoryStore struct {
	mu   sync.Mutex
	msgs map[string]Message
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{msgs: map[string]Message{}} }

func (s *MemoryStore) Put(_ context.Context, m Message) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.msgs[m.ID]; ok {
		return false, nil
	}
	s.msgs[m.ID] = m
	return true, nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.msgs[id]
	if !ok {
		return m, ErrNotFound
	}
	return m, nil
}

// All returns every message, ordered by (received_at, id).
func (s *MemoryStore) All() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Message, 0, len(s.msgs))
	for _, m := range s.msgs {
		out = append(out, m)
	}
	sortByReceived(out)
	return out
}

func sortByReceived(ms []Message) {
	sort.Slice(ms, func(i, j int) bool {
		if !ms[i].ReceivedAt.Equal(ms[j].ReceivedAt) {
			return ms[i].ReceivedAt.Before(ms[j].ReceivedAt)
		}
		return ms[i].ID < ms[j].ID
	})
}

func (s *MemoryStore) ClaimDue(_ context.Context, now time.Time) (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *Message
	for _, m := range s.msgs {
		if m.Status == StatusRetrying && !m.NextAttemptAt.After(now) && (best == nil || m.NextAttemptAt.Before(best.NextAttemptAt)) {
			m := m
			best = &m
		}
	}
	if best == nil {
		return nil, nil
	}
	best.NextAttemptAt, best.ClaimedAt = now.Add(ClaimLease), now
	s.msgs[best.ID] = *best
	return best, nil
}

func (s *MemoryStore) ClaimParked(_ context.Context, source string, passStart, now time.Time) (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var candidates []Message
	for _, m := range s.msgs {
		if m.Source != source {
			continue
		}
		if (m.Status == StatusParked && m.UpdatedAt.Before(passStart)) ||
			(m.Status == StatusReplaying && m.ClaimedAt.Before(now.Add(-ReplayStale))) {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sortByReceived(candidates)
	m := candidates[0]
	m.Status, m.ClaimedAt = StatusReplaying, now
	s.msgs[m.ID] = m
	return &m, nil
}

func (s *MemoryStore) MarkRecovered(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.msgs[id]
	m.Status, m.RecoveredAt, m.UpdatedAt = StatusRecovered, &at, at
	s.msgs[id] = m
	return nil
}

func (s *MemoryStore) MarkFailed(_ context.Context, id, reason string, retryable bool, status Status, attempts int, next, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.msgs[id]
	m.Reason, m.Retryable, m.Status, m.Attempts, m.NextAttemptAt, m.UpdatedAt = reason, retryable, status, attempts, next, now
	s.msgs[id] = m
	return nil
}

func (s *MemoryStore) List(_ context.Context, source string, after *Cursor, limit int) ([]Message, error) {
	var out []Message
	for _, m := range s.All() {
		if m.Status == StatusRecovered || (source != "" && m.Source != source) {
			continue
		}
		if after != nil && (m.ReceivedAt.Before(after.ReceivedAt) ||
			(m.ReceivedAt.Equal(after.ReceivedAt) && m.ID <= after.ID)) {
			continue
		}
		m.Payload = nil
		out = append(out, m)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
