package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock() *Clock {
	return &Clock{now: time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC)}
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now.Add(123 * time.Microsecond)
}

type Factory func(t *testing.T, now func() time.Time) store.Store

func event(ref string, typ hazard.Type, version int64) hazard.Event {
	src := hazard.SourceBMKG
	if typ == hazard.TypeVolcanic {
		src = hazard.SourcePVMBG
	}
	return hazard.Event{
		HazardID:    hazard.NewID(src, ref),
		Source:      src,
		SourceRefID: ref,
		HazardType:  typ,
		Severity:    hazard.SeverityWaspada,
		AreaName:    "Area " + ref,
		Latitude:    -6.2,
		Longitude:   106.8,
		OccurredAt:  time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		IngestedAt:  time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
		Attributes:  map[string]any{"ref": ref},
		Version:     version,
	}
}

func mustGet(t *testing.T, s store.Store, id string) hazard.Event {
	t.Helper()
	got, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return got
}

func mustUpsert(t *testing.T, s store.Store, e hazard.Event, want bool) {
	t.Helper()
	stored, applied, err := s.Upsert(context.Background(), e)
	if err != nil {
		t.Fatalf("upsert %s: %v", e.SourceRefID, err)
	}
	if applied != want {
		t.Fatalf("upsert %s v%d applied=%v, want %v", e.SourceRefID, e.Version, applied, want)
	}
	if !applied {
		return
	}
	got := mustGet(t, s, e.HazardID)
	if stored.HazardID != e.HazardID || stored.Version != e.Version || !stored.IngestedAt.Equal(got.IngestedAt) {
		t.Fatalf("upsert returned %+v, but store holds %+v", stored, got)
	}
}

func refs(items []hazard.Event) []string {
	out := make([]string, len(items))
	for i, e := range items {
		out[i] = e.SourceRefID
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func pageAll(t *testing.T, s store.Store, q store.LatestQuery, between func()) []string {
	t.Helper()
	var all []string
	for range 100 {
		page, err := s.ListLatest(context.Background(), q)
		if err != nil {
			t.Fatalf("list latest: %v", err)
		}
		all = append(all, refs(page.Items)...)
		if page.NextCursor == "" {
			return all
		}
		if len(page.Items) != q.Limit {
			t.Fatalf("non-final page has %d items, want %d", len(page.Items), q.Limit)
		}
		q.Cursor = page.NextCursor
		if between != nil {
			between()
			between = nil
		}
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func Run(t *testing.T, newStore Factory) {
	ctx := context.Background()

	t.Run("insert sets ingested_at from store clock in milliseconds", func(t *testing.T) {
		clock := NewClock()
		s := newStore(t, clock.Now)
		e := event("ins-1", hazard.TypeSeismic, 1)
		mustUpsert(t, s, e, true)
		got := mustGet(t, s, e.HazardID)
		if got.IngestedAt.Year() == 1999 {
			t.Fatal("caller ingested_at must be ignored")
		}
		if got.IngestedAt.Nanosecond()%int(time.Millisecond) != 0 {
			t.Fatalf("ingested_at %v is not truncated to milliseconds", got.IngestedAt)
		}
		if got.Version != 1 || got.Severity != hazard.SeverityWaspada || got.Attributes["ref"] != "ins-1" {
			t.Fatalf("stored event mismatch: %+v", got)
		}
	})

	t.Run("same or lower version is a no-op and keeps ingested_at", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		e := event("ver-1", hazard.TypeSeismic, 5)
		mustUpsert(t, s, e, true)
		first := mustGet(t, s, e.HazardID)

		same := e
		same.Severity = hazard.SeverityAwas
		mustUpsert(t, s, same, false)
		lower := e
		lower.Version = 4
		lower.Severity = hazard.SeverityAwas
		mustUpsert(t, s, lower, false)

		got := mustGet(t, s, e.HazardID)
		if got.Severity != hazard.SeverityWaspada || !got.IngestedAt.Equal(first.IngestedAt) {
			t.Fatalf("stale write changed the event: %+v", got)
		}
	})

	t.Run("higher version replaces content but keeps ingested_at", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		e := event("ver-2", hazard.TypeSeismic, 1)
		mustUpsert(t, s, e, true)
		first := mustGet(t, s, e.HazardID)

		newer := e
		newer.Version = 2
		newer.Severity = hazard.SeverityAwas
		newer.Attributes = nil
		mustUpsert(t, s, newer, true)

		got := mustGet(t, s, e.HazardID)
		if got.Severity != hazard.SeverityAwas || got.Version != 2 {
			t.Fatalf("newer version not applied: %+v", got)
		}
		if !got.IngestedAt.Equal(first.IngestedAt) {
			t.Fatalf("ingested_at changed from %v to %v", first.IngestedAt, got.IngestedAt)
		}
		if got.Attributes == nil || len(got.Attributes) != 0 {
			t.Fatalf("nil attributes must be stored as an empty object, got %#v", got.Attributes)
		}
	})

	t.Run("get unknown returns ErrNotFound", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		if _, err := s.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("empty hazard_id is rejected", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		bad := event("bad", hazard.TypeSeismic, 1)
		bad.HazardID = ""
		if _, _, err := s.Upsert(ctx, bad); !errors.Is(err, store.ErrInvalidEvent) {
			t.Fatalf("upsert err = %v, want ErrInvalidEvent", err)
		}
		if _, err := s.CommitPoll(ctx, hazard.SourceBMKG, 1, "c", []hazard.Event{bad}); !errors.Is(err, store.ErrInvalidEvent) {
			t.Fatalf("commit err = %v, want ErrInvalidEvent", err)
		}
	})

	t.Run("checkpoint is empty before the first commit", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		cursor, epoch, err := s.LoadCheckpoint(ctx, hazard.SourceBMKG)
		if err != nil || cursor != "" || epoch != 0 {
			t.Fatalf("checkpoint = (%q, %d, %v), want empty", cursor, epoch, err)
		}
	})

	t.Run("commit poll fences stale epochs", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		a, b, c := event("cp-a", hazard.TypeSeismic, 1), event("cp-b", hazard.TypeSeismic, 1), event("cp-c", hazard.TypeSeismic, 1)

		applied, err := s.CommitPoll(ctx, hazard.SourceBMKG, 1, "cursor-1", []hazard.Event{a, b, a})
		if err != nil {
			t.Fatalf("first commit: %v", err)
		}
		if got := refs(applied); !equal(got, []string{"cp-a", "cp-b"}) {
			t.Fatalf("applied = %v, want [cp-a cp-b]", got)
		}
		for _, e := range applied {
			if e.IngestedAt.Year() == 1999 {
				t.Fatal("applied events must carry the stored ingested_at")
			}
		}

		applied, err = s.CommitPoll(ctx, hazard.SourceBMKG, 1, "cursor-2", []hazard.Event{a, c})
		if err != nil {
			t.Fatalf("same epoch commit: %v", err)
		}
		if got := refs(applied); !equal(got, []string{"cp-c"}) {
			t.Fatalf("applied = %v, want [cp-c]", got)
		}

		if _, err := s.CommitPoll(ctx, hazard.SourceBMKG, 3, "cursor-3", nil); err != nil {
			t.Fatalf("takeover commit: %v", err)
		}

		late := event("cp-late", hazard.TypeSeismic, 1)
		_, err = s.CommitPoll(ctx, hazard.SourceBMKG, 2, "cursor-stale", []hazard.Event{late})
		if !errors.Is(err, store.ErrStaleEpoch) {
			t.Fatalf("stale commit err = %v, want ErrStaleEpoch", err)
		}
		if _, err := s.Get(ctx, late.HazardID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("event from a stale epoch was stored (err=%v)", err)
		}
		cursor, epoch, err := s.LoadCheckpoint(ctx, hazard.SourceBMKG)
		if err != nil || cursor != "cursor-3" || epoch != 3 {
			t.Fatalf("checkpoint = (%q, %d, %v), want (cursor-3, 3)", cursor, epoch, err)
		}
		other, otherEpoch, _ := s.LoadCheckpoint(ctx, hazard.SourcePVMBG)
		if other != "" || otherEpoch != 0 {
			t.Fatal("fences must be per source")
		}
	})

	t.Run("retract soft-deletes, keeps content, and a newer upsert revives", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		e := event("fire-1", hazard.TypeSeismic, 3)
		mustUpsert(t, s, e, true)
		first := mustGet(t, s, e.HazardID)

		if _, applied, err := s.Retract(ctx, e.Source, e.SourceRefID, 3); err != nil || applied {
			t.Fatalf("retract with same version: applied=%v err=%v, want no-op", applied, err)
		}
		stored, applied, err := s.Retract(ctx, e.Source, e.SourceRefID, 4)
		if err != nil || !applied || !stored.Retracted || stored.Version != 4 {
			t.Fatalf("retract v4: stored=%+v applied=%v err=%v", stored, applied, err)
		}
		got := mustGet(t, s, e.HazardID)
		if !got.Retracted || got.AreaName != e.AreaName || !got.IngestedAt.Equal(first.IngestedAt) {
			t.Fatalf("retract must keep content and ingested_at: %+v", got)
		}

		stale := e
		stale.Version = 4
		mustUpsert(t, s, stale, false)

		revived := e
		revived.Version = 5
		mustUpsert(t, s, revived, true)
		if got := mustGet(t, s, e.HazardID); got.Retracted {
			t.Fatal("a newer upsert must clear retracted")
		}
	})

	t.Run("nested attribute objects come back as objects", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		e := event("nested-1", hazard.TypeSeismic, 1)
		e.Attributes = map[string]any{
			"satellites": []any{"N20", "N21"},
			"warning":    map[string]any{"threat": "Siaga", "zones": []any{"Padang"}},
		}
		mustUpsert(t, s, e, true)
		got := mustGet(t, s, e.HazardID)
		raw, err := json.Marshal(got.Attributes)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"satellites":["N20","N21"],"warning":{"threat":"Siaga","zones":["Padang"]}}`
		if string(raw) != want {
			t.Fatalf("attributes JSON = %s, want %s", raw, want)
		}
	})

	t.Run("source_ref_ids and listing respect retracted visibility", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		for _, ref := range []string{"r1", "r2", "r3"} {
			mustUpsert(t, s, event(ref, hazard.TypeSeismic, 1), true)
		}
		mustUpsert(t, s, event("v1", hazard.TypeVolcanic, 1), true)
		if _, _, err := s.Retract(ctx, hazard.SourceBMKG, "r2", 2); err != nil {
			t.Fatal(err)
		}

		visible, err := s.SourceRefIDs(ctx, hazard.SourceBMKG, false)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(visible.IDs)
		if strings.Join(visible.IDs, ",") != "r1,r3" || visible.UnavailableUnits == nil {
			t.Fatalf("visible ids = %v units=%v", visible.IDs, visible.UnavailableUnits)
		}
		all, _ := s.SourceRefIDs(ctx, hazard.SourceBMKG, true)
		if len(all.IDs) != 3 {
			t.Fatalf("with retracted = %v", all.IDs)
		}
		every, _ := s.SourceRefIDs(ctx, "", false)
		if len(every.IDs) != 3 {
			t.Fatalf("all sources (r1, r3, v1) = %v", every.IDs)
		}

		hidden := pageAll(t, s, store.LatestQuery{Source: hazard.SourceBMKG, Limit: 10}, nil)
		shown := pageAll(t, s, store.LatestQuery{Source: hazard.SourceBMKG, Limit: 10, IncludeRetracted: true}, nil)
		if len(hidden) != 2 || len(shown) != 3 {
			t.Fatalf("list latest hidden=%v shown=%v", hidden, shown)
		}
	})

	t.Run("purge removes only synthetic events", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		sim := event("sim-spike-s1-000001", hazard.TypeSeismic, 1)
		mustUpsert(t, s, sim, true)
		deleted, err := s.Purge(ctx, sim.Source, sim.SourceRefID)
		if err != nil || !deleted {
			t.Fatalf("purge sim: deleted=%v err=%v", deleted, err)
		}
		if _, err := s.Get(ctx, sim.HazardID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("purged event still readable: %v", err)
		}
		if deleted, err := s.Purge(ctx, sim.Source, sim.SourceRefID); err != nil || deleted {
			t.Fatalf("second purge must be a no-op: deleted=%v err=%v", deleted, err)
		}
		real := event("EQ-real", hazard.TypeSeismic, 1)
		mustUpsert(t, s, real, true)
		if _, err := s.Purge(ctx, real.Source, real.SourceRefID); !errors.Is(err, store.ErrInvalidEvent) {
			t.Fatalf("purge of a real event err = %v, want ErrInvalidEvent", err)
		}
		mustGet(t, s, real.HazardID)
	})

	t.Run("retract before upsert leaves a tombstone that blocks older upserts", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		e := event("fire-2", hazard.TypeSeismic, 2)
		stored, applied, err := s.Retract(ctx, e.Source, e.SourceRefID, 7)
		if err != nil || !applied || stored.HazardID != e.HazardID || !stored.Retracted {
			t.Fatalf("tombstone: stored=%+v applied=%v err=%v", stored, applied, err)
		}
		mustUpsert(t, s, e, false)
		if got := mustGet(t, s, e.HazardID); !got.Retracted || got.Version != 7 {
			t.Fatalf("older upsert revived a tombstone: %+v", got)
		}
		if _, _, err := s.Retract(ctx, "", "x", 1); !errors.Is(err, store.ErrInvalidEvent) {
			t.Fatalf("retract without source err = %v, want ErrInvalidEvent", err)
		}
	})

	t.Run("concurrent leaders: the newer epoch wins and the older is fenced", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		var wg sync.WaitGroup
		errs := make(chan error, 30)
		for _, epoch := range []int64{5, 6} {
			wg.Add(1)
			go func(epoch int64) {
				defer wg.Done()
				for i := range 15 {
					e := event(fmt.Sprintf("race-%d-%d", epoch, i), hazard.TypeSeismic, 1)
					_, err := s.CommitPoll(ctx, hazard.SourcePVMBG, epoch, fmt.Sprintf("c-%d-%d", epoch, i), []hazard.Event{e})
					if err != nil && !errors.Is(err, store.ErrStaleEpoch) {
						errs <- err
					}
				}
			}(epoch)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("unexpected commit error: %v", err)
		}
		if _, epoch, _ := s.LoadCheckpoint(ctx, hazard.SourcePVMBG); epoch != 6 {
			t.Fatalf("final epoch = %d, want 6", epoch)
		}
		if _, err := s.CommitPoll(ctx, hazard.SourcePVMBG, 5, "late", nil); !errors.Is(err, store.ErrStaleEpoch) {
			t.Fatalf("epoch 5 after 6 err = %v, want ErrStaleEpoch", err)
		}
	})

	t.Run("list latest pages newest first without gaps or duplicates", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		var want []string
		for i := range 7 {
			e := event(fmt.Sprintf("lt-%d", i), hazard.TypeSeismic, 1)
			mustUpsert(t, s, e, true)
			want = append([]string{e.SourceRefID}, want...)
		}
		insertNewer := func() { mustUpsert(t, s, event("lt-new", hazard.TypeSeismic, 1), true) }
		got := pageAll(t, s, store.LatestQuery{Limit: 3}, insertNewer)
		if !equal(got, want) {
			t.Fatalf("pages = %v, want %v", got, want)
		}
	})

	t.Run("list latest ascending with since and filters", func(t *testing.T) {
		clock := NewClock()
		s := newStore(t, clock.Now)
		mustUpsert(t, s, event("old-1", hazard.TypeSeismic, 1), true)
		since := mustGet(t, s, event("old-1", hazard.TypeSeismic, 1).HazardID).IngestedAt.Add(time.Millisecond)
		var want []string
		for i := range 5 {
			e := event(fmt.Sprintf("asc-%d", i), hazard.TypeSeismic, 1)
			mustUpsert(t, s, e, true)
			want = append(want, e.SourceRefID)
			mustUpsert(t, s, event(fmt.Sprintf("vol-%d", i), hazard.TypeVolcanic, 1), true)
		}
		got := pageAll(t, s, store.LatestQuery{Ascending: true, Since: since, Source: hazard.SourceBMKG, Limit: 2}, nil)
		if !equal(got, want) {
			t.Fatalf("ascending = %v, want %v", got, want)
		}
		vol := pageAll(t, s, store.LatestQuery{Type: hazard.TypeVolcanic, Limit: 10}, nil)
		if len(vol) != 5 {
			t.Fatalf("volcanic count = %d, want 5", len(vol))
		}
		exact := pageAll(t, s, store.LatestQuery{Ascending: true, Since: since.Add(-time.Millisecond), Source: hazard.SourceBMKG, Limit: 50}, nil)
		if len(exact) != 6 || exact[0] != "old-1" {
			t.Fatalf("since must be inclusive, got %v", exact)
		}
	})

	t.Run("list latest validates limit and cursor", func(t *testing.T) {
		s := newStore(t, NewClock().Now)
		for i := range 3 {
			mustUpsert(t, s, event(fmt.Sprintf("val-%d", i), hazard.TypeSeismic, 1), true)
		}
		for _, limit := range []int{0, store.MaxLatestLimit + 1} {
			if _, err := s.ListLatest(ctx, store.LatestQuery{Limit: limit}); !errors.Is(err, store.ErrInvalidQuery) {
				t.Fatalf("limit %d err = %v, want ErrInvalidQuery", limit, err)
			}
		}
		page, err := s.ListLatest(ctx, store.LatestQuery{Limit: 1})
		if err != nil || page.NextCursor == "" {
			t.Fatalf("first page: %v cursor=%q", err, page.NextCursor)
		}
		if _, err := s.ListLatest(ctx, store.LatestQuery{Limit: 1, Ascending: true, Cursor: page.NextCursor}); !errors.Is(err, store.ErrInvalidCursor) {
			t.Fatalf("cursor reused with another filter: err = %v, want ErrInvalidCursor", err)
		}
		if _, err := s.ListLatest(ctx, store.LatestQuery{Limit: 1, Cursor: "not-a-cursor"}); !errors.Is(err, store.ErrInvalidCursor) {
			t.Fatalf("garbage cursor err = %v, want ErrInvalidCursor", err)
		}
		last, err := s.ListLatest(ctx, store.LatestQuery{Limit: 3})
		if err != nil || len(last.Items) != 3 || last.NextCursor != "" {
			t.Fatalf("exact-size page must end pagination: items=%d cursor=%q err=%v", len(last.Items), last.NextCursor, err)
		}
	})
}
