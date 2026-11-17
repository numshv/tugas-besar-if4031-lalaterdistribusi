package api

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/jwtauth"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/pipeline"
)

// Replayer reprocesses parked messages of one source.
type Replayer interface {
	Replay(ctx context.Context, source string) (pipeline.ReplayResult, error)
}

// Ops wires the dead letter queue endpoints. They are reachable from outside the
// cluster through the ingress, so the aggregator checks the caller itself:
// only the assistants' audit token or a JWT with scope "internal" (BNPB
// Internal Ops) is accepted.
type Ops struct {
	DLQ        dlq.Store
	Replayer   Replayer
	AuditToken string
	Verifier   *jwtauth.Verifier
}

func (o *Ops) register(mux *http.ServeMux) {
	mux.Handle("GET /internal/audit/unprocessed", o.requireInternal(http.HandlerFunc(o.listUnprocessed)))
	mux.Handle("GET /internal/ops/unprocessed/{id}", o.requireInternal(http.HandlerFunc(o.getUnprocessed)))
	mux.Handle("POST /internal/ops/replay", o.requireInternal(http.HandlerFunc(o.replay)))
}

func (o *Ops) requireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization header is required")
			return
		}
		token, ok := jwtauth.BearerToken(header)
		if !ok {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "Authorization must use the Bearer scheme")
			return
		}
		if o.AuditToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(o.AuditToken)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		if o.Verifier == nil {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		scope, _, err := o.Verifier.Verify(token)
		if err != nil {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		if scope != jwtauth.ScopeInternal {
			httpx.Error(w, http.StatusForbidden, "forbidden", "hanya BNPB Internal Ops yang boleh memeriksa pesan gagal")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type unprocessedItem struct {
	MessageID  string     `json:"message_id"`
	Source     string     `json:"source"`
	ReceivedAt string     `json:"received_at"`
	Reason     string     `json:"reason"`
	Retryable  bool       `json:"retryable"`
	Attempts   int        `json:"attempts"`
	Status     dlq.Status `json:"status"`
}

func (o *Ops) listUnprocessed(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > dlq.ListMaxLimit {
			httpx.Error(w, http.StatusBadRequest, "bad_request", "limit harus antara 1 dan 1000")
			return
		}
		limit = n
	}
	var after *dlq.Cursor
	if raw := q.Get("cursor"); raw != "" {
		c, err := dlq.DecodeCursor(raw)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "bad_request", "cursor tidak valid")
			return
		}
		after = &c
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	msgs, err := o.DLQ.List(ctx, q.Get("source"), after, limit+1)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan pesan gagal tidak dapat dijangkau")
		return
	}

	var next *string
	if len(msgs) > limit {
		msgs = msgs[:limit]
		last := msgs[len(msgs)-1]
		s := dlq.EncodeCursor(dlq.Cursor{ReceivedAt: last.ReceivedAt, ID: last.ID})
		next = &s
	}
	items := make([]unprocessedItem, 0, len(msgs))
	for _, m := range msgs {
		items = append(items, unprocessedItem{
			MessageID: m.ID, Source: m.Source, ReceivedAt: m.ReceivedAt.UTC().Format(time.RFC3339),
			Reason: m.Reason, Retryable: m.Retryable, Attempts: m.Attempts, Status: m.ReportedStatus(),
		})
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

// getUnprocessed shows one message in full, including its payload, so Ops can
// see exactly what the upstream sent.
func (o *Ops) getUnprocessed(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	m, err := o.DLQ.Get(ctx, r.PathValue("id"))
	if errors.Is(err, dlq.ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "not_found", "pesan tidak ditemukan")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan pesan gagal tidak dapat dijangkau")
		return
	}
	out := map[string]any{
		"message_id": m.ID, "source": m.Source, "kind": m.Kind, "path": m.Path, "ref_hint": m.RefHint,
		"received_at": m.ReceivedAt.UTC().Format(time.RFC3339), "reason": m.Reason, "retryable": m.Retryable,
		"attempts": m.Attempts, "status": m.Status, "updated_at": m.UpdatedAt.UTC().Format(time.RFC3339),
		"correlation_id": m.CorrelationID, "payload_bytes": len(m.Payload), "payload_truncated": m.PayloadTruncated,
	}
	if m.RecoveredAt != nil {
		out["recovered_at"] = m.RecoveredAt.UTC().Format(time.RFC3339)
	}
	// Valid UTF-8 is shown as text; anything else (for example the invalid_utf8
	// case) as base64 so the exact bytes survive the JSON response.
	if utf8.Valid(m.Payload) {
		out["payload"] = string(m.Payload)
	} else {
		out["payload_base64"] = base64.StdEncoding.EncodeToString(m.Payload)
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (o *Ops) replay(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "parameter source wajib diisi, contoh source=PVMBG")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	res, err := o.Replayer.Replay(ctx, source)
	if err != nil {
		httpx.JSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":   map[string]string{"code": "unavailable", "message": err.Error()},
			"partial": res,
		})
		return
	}
	httpx.JSON(w, http.StatusOK, res)
}
