// Package pipeline turns raw upstream responses into HazardEvents and sets aside
// what cannot be processed. Replays are safe to repeat because hazard_id is
// derived from the source's id.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/guard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/mapper"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/publisher"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/schema"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

const (
	PathVolcanic = "/volcanic-reports"
	PathSeismic  = "/seismic-events"
	PathTsunami  = "/tsunami-warnings"
)

type Response struct {
	Path       string
	Body       []byte
	TooLarge   bool // Body holds only the first guard.MaxBodyBytes bytes
	ReceivedAt time.Time
}

type Failure struct {
	Reason    string
	Retryable bool
}

func (f *Failure) Error() string { return f.Reason }

func incompatible(format string, a ...any) *Failure {
	return &Failure{Reason: "schema_incompatible: " + fmt.Sprintf(format, a...)}
}

// ErrMalformedResponse means a whole response was set aside; the poll should not advance.
var ErrMalformedResponse = errors.New("malformed response set aside")

type Pipeline struct {
	Store    store.Store
	Pub      publisher.Publisher
	DLQ      dlq.Store
	Rules    schema.Rules
	Resolver mapper.VolcanoResolver
	Policy   mapper.UnknownFieldPolicy
	Log      *slog.Logger
	Now      func() time.Time

	replayMu sync.Mutex
}

func (p *Pipeline) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p *Pipeline) IngestPVMBG(ctx context.Context, r Response) error {
	items, err := p.split(ctx, hazard.SourcePVMBG, r)
	if err != nil {
		return err
	}
	for _, item := range items {
		f := p.safely(func() *Failure { return p.processPVMBG(ctx, item, r.ReceivedAt) })
		if err := p.setAsideRecord(ctx, hazard.SourcePVMBG, item, refHint(item, "report_id"), r.ReceivedAt, f); err != nil {
			return err
		}
	}
	return nil
}

// bmkgRecord keeps a seismic event together with its tsunami warning, because
// severity depends on both.
type bmkgRecord struct {
	Event   json.RawMessage `json:"event"`
	Warning json.RawMessage `json:"warning"`
}

func (p *Pipeline) IngestBMKG(ctx context.Context, events, warnings Response) error {
	eventItems, err := p.split(ctx, hazard.SourceBMKG, events)
	if err != nil {
		return err
	}
	warningItems, err := p.split(ctx, hazard.SourceBMKG, warnings)
	if err != nil {
		return err
	}

	rawByEvent := map[string]json.RawMessage{}
	for _, w := range warningItems {
		var m map[string]any
		if json.Unmarshal(w, &m) != nil {
			continue
		}
		if ref, _ := m["related_event_id"].(string); ref != "" {
			rawByEvent[ref] = w
		}
	}

	for _, item := range eventItems {
		eventID := refHint(item, "event_id")
		rec, _ := json.Marshal(bmkgRecord{Event: item, Warning: rawByEvent[eventID]})
		f := p.safely(func() *Failure { return p.processBMKG(ctx, rec, events.ReceivedAt) })
		if err := p.setAsideRecord(ctx, hazard.SourceBMKG, rec, eventID, events.ReceivedAt, f); err != nil {
			return err
		}
	}
	return nil
}

// split parks an unreadable response whole and fails, so the poller fetches the
// same window again.
func (p *Pipeline) split(ctx context.Context, source hazard.Source, r Response) ([]json.RawMessage, error) {
	var items []json.RawMessage
	var err error
	if r.TooLarge {
		err = guard.Oversize()
	} else {
		items, err = guard.SplitArray(r.Body)
	}
	if err == nil {
		return items, nil
	}

	m := p.newMessage(source, dlq.KindResponse, r.Body, r.ReceivedAt, &Failure{Reason: err.Error()})
	m.Path, m.PayloadTruncated = r.Path, r.TooLarge
	if perr := p.put(ctx, m); perr != nil {
		return nil, perr
	}
	return nil, fmt.Errorf("%w: %s %s", ErrMalformedResponse, r.Path, err)
}

func (p *Pipeline) processPVMBG(ctx context.Context, item json.RawMessage, receivedAt time.Time) *Failure {
	var raw map[string]any
	if err := json.Unmarshal(item, &raw); err != nil || raw == nil {
		return incompatible("record is not a JSON object")
	}
	raw = p.Rules.Apply(string(hazard.SourcePVMBG), raw, receivedAt)
	ev, err := mapper.MapVolcanicOne(raw, p.Resolver, p.Policy)
	if err != nil {
		return incompatible("%v", err)
	}
	return p.persist(ctx, ev)
}

func (p *Pipeline) processBMKG(ctx context.Context, rec []byte, receivedAt time.Time) *Failure {
	var r bmkgRecord
	if err := json.Unmarshal(rec, &r); err != nil {
		return incompatible("record envelope is not valid")
	}
	var event, warning map[string]any
	if err := json.Unmarshal(r.Event, &event); err != nil || event == nil {
		return incompatible("seismic event is not a JSON object")
	}
	if len(r.Warning) > 0 && string(r.Warning) != "null" {
		if err := json.Unmarshal(r.Warning, &warning); err != nil {
			return incompatible("tsunami warning is not a JSON object")
		}
		warning = p.Rules.Apply(string(hazard.SourceBMKG), warning, receivedAt)
	}
	event = p.Rules.Apply(string(hazard.SourceBMKG), event, receivedAt)
	ev, err := mapper.MapSeismicOne(event, warning, p.Policy)
	if err != nil {
		return incompatible("%v", err)
	}
	return p.persist(ctx, ev)
}

func (p *Pipeline) persist(ctx context.Context, ev hazard.Event) *Failure {
	stored, applied, err := p.Store.Upsert(ctx, ev)
	if err != nil {
		return &Failure{Reason: "store_unavailable: " + err.Error(), Retryable: true}
	}
	if !applied {
		return nil
	}
	if err := p.Pub.Publish(ctx, stored); err != nil {
		return &Failure{Reason: "publish_failed: " + err.Error(), Retryable: true}
	}
	return nil
}

// safely keeps one bad record from taking the aggregator down.
func (p *Pipeline) safely(fn func() *Failure) (f *Failure) {
	defer func() {
		if r := recover(); r != nil {
			f = &Failure{Reason: fmt.Sprintf("internal_error: panic: %v", r)}
		}
	}()
	return fn()
}

func (p *Pipeline) newMessage(source hazard.Source, kind dlq.Kind, payload []byte, receivedAt time.Time, f *Failure) dlq.Message {
	now := p.now()
	m := dlq.Message{
		ID: dlq.NewID(string(source), kind, payload), Source: string(source), Kind: kind,
		Payload: payload, ReceivedAt: receivedAt.UTC(), Reason: f.Reason, Retryable: f.Retryable,
		Attempts: 1, Status: dlq.StatusParked, UpdatedAt: now,
	}
	if f.Retryable {
		m.Status, m.NextAttemptAt = dlq.StatusRetrying, now.Add(dlq.Backoff(1))
	}
	return m
}

func (p *Pipeline) setAsideRecord(ctx context.Context, source hazard.Source, payload []byte, ref string, receivedAt time.Time, f *Failure) error {
	if f == nil {
		return nil
	}
	m := p.newMessage(source, dlq.KindRecord, payload, receivedAt, f)
	m.RefHint = ref
	return p.put(ctx, m)
}

func (p *Pipeline) put(ctx context.Context, m dlq.Message) error {
	m.CorrelationID = correlation.FromContext(ctx)
	inserted, err := p.DLQ.Put(ctx, m)
	if err != nil {
		return fmt.Errorf("dead letter queue unavailable: %w", err)
	}
	if inserted {
		p.Log.WarnContext(ctx, "message set aside", "message_id", m.ID, "source", m.Source,
			"kind", m.Kind, "status", m.Status, "reason", m.Reason)
	}
	return nil
}

func refHint(item json.RawMessage, field string) string {
	var m map[string]any
	if json.Unmarshal(item, &m) != nil {
		return ""
	}
	s, _ := m[field].(string)
	return s
}

func (p *Pipeline) Reprocess(ctx context.Context, m dlq.Message) *Failure {
	return p.safely(func() *Failure {
		switch m.Kind {
		case dlq.KindRecord:
			switch hazard.Source(m.Source) {
			case hazard.SourcePVMBG:
				return p.processPVMBG(ctx, m.Payload, m.ReceivedAt)
			case hazard.SourceBMKG:
				return p.processBMKG(ctx, m.Payload, m.ReceivedAt)
			}
			return &Failure{Reason: "unsupported source " + m.Source}
		case dlq.KindResponse:
			return p.reprocessResponse(ctx, m)
		}
		return &Failure{Reason: "unsupported message kind " + string(m.Kind)}
	})
}

func (p *Pipeline) reprocessResponse(ctx context.Context, m dlq.Message) *Failure {
	if m.PayloadTruncated {
		return &Failure{Reason: m.Reason + " (only a prefix was kept)"}
	}
	items, err := guard.SplitArray(m.Payload)
	if err != nil {
		return &Failure{Reason: err.Error()}
	}
	for _, item := range items {
		var f *Failure
		ref := ""
		payload := []byte(item)
		switch {
		case hazard.Source(m.Source) == hazard.SourcePVMBG:
			ref = refHint(item, "report_id")
			f = p.processPVMBG(ctx, item, m.ReceivedAt)
		case m.Path == PathSeismic:
			ref = refHint(item, "event_id")
			payload, _ = json.Marshal(bmkgRecord{Event: item})
			f = p.processBMKG(ctx, payload, m.ReceivedAt)
		default:
			// Warnings alone are meaningless; the poller refetches them with their events.
			continue
		}
		if err := p.setAsideRecord(ctx, hazard.Source(m.Source), payload, ref, m.ReceivedAt, f); err != nil {
			return &Failure{Reason: err.Error(), Retryable: true}
		}
	}
	return nil
}

func (p *Pipeline) RunRetrier(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.RetryDue(ctx, 50)
		}
	}
}

func (p *Pipeline) RetryDue(ctx context.Context, max int) int {
	n := 0
	for ; n < max; n++ {
		m, err := p.DLQ.ClaimDue(ctx, p.now())
		if err != nil {
			p.Log.WarnContext(ctx, "claim retrying message failed", "error", err)
			return n
		}
		if m == nil {
			return n
		}
		mctx := correlation.WithID(ctx, correlation.New())
		f := p.Reprocess(mctx, *m)
		p.settle(mctx, *m, f, false)
	}
	return n
}

func (p *Pipeline) settle(ctx context.Context, m dlq.Message, f *Failure, replay bool) {
	now := p.now()
	if f == nil {
		if err := p.DLQ.MarkRecovered(ctx, m.ID, now); err != nil {
			p.Log.ErrorContext(ctx, "mark recovered failed", "message_id", m.ID, "error", err)
			return
		}
		p.Log.InfoContext(ctx, "message recovered", "message_id", m.ID, "source", m.Source, "replay", replay)
		return
	}
	attempts := m.Attempts + 1
	status, next := dlq.StatusParked, time.Time{}
	if f.Retryable && !replay && attempts < dlq.MaxAttempts {
		status, next = dlq.StatusRetrying, now.Add(dlq.Backoff(attempts))
	}
	if err := p.DLQ.MarkFailed(ctx, m.ID, f.Reason, f.Retryable, status, attempts, next, now); err != nil {
		p.Log.ErrorContext(ctx, "mark failed failed", "message_id", m.ID, "error", err)
		return
	}
	p.Log.WarnContext(ctx, "message still failing", "message_id", m.ID, "source", m.Source,
		"status", status, "attempts", attempts, "reason", f.Reason)
}

type ReplayResult struct {
	Source    string `json:"source"`
	Replayed  int    `json:"replayed"`
	Recovered int    `json:"recovered"`
	Parked    int    `json:"still_parked"`
	Skipped   int    `json:"skipped_behind_failed"`
}

// Replay reprocesses parked messages oldest first. Once a message fails, later
// ones with the same source_ref_id stay parked so order per source_ref_id holds.
func (p *Pipeline) Replay(ctx context.Context, source string) (ReplayResult, error) {
	p.replayMu.Lock()
	defer p.replayMu.Unlock()

	res := ReplayResult{Source: source}
	passStart := p.now()
	failedRefs := map[string]bool{}
	for {
		m, err := p.DLQ.ClaimParked(ctx, source, passStart, p.now())
		if err != nil {
			return res, fmt.Errorf("claim parked message: %w", err)
		}
		if m == nil {
			return res, nil
		}
		res.Replayed++
		if m.RefHint != "" && failedRefs[m.RefHint] {
			res.Skipped++
			res.Parked++
			now := p.now()
			if err := p.DLQ.MarkFailed(ctx, m.ID, m.Reason, m.Retryable, dlq.StatusParked, m.Attempts, time.Time{}, now); err != nil {
				return res, err
			}
			continue
		}
		f := p.Reprocess(ctx, *m)
		p.settle(ctx, *m, f, true)
		if f == nil {
			res.Recovered++
			continue
		}
		res.Parked++
		if m.RefHint != "" {
			failedRefs[m.RefHint] = true
		}
	}
}
