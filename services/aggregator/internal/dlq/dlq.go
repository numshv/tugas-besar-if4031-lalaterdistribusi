// Package dlq is the dead letter queue for upstream messages the aggregator
// could not process. Every message is kept byte for byte with its reason,
// source and receive time.
//
// A message is either retrying (a transient failure; the retrier tries again
// with exponential backoff) or parked (it will not succeed until the system
// changes; Ops adapt the system and replay it). Recovered messages are kept
// for the audit trail but no longer reported as unprocessed.
package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

type Status string

const (
	StatusRetrying  Status = "retrying"
	StatusParked    Status = "parked"
	StatusReplaying Status = "replaying" // claimed by a replay; reported as parked
	StatusRecovered Status = "recovered"
)

// Kind says what Payload holds.
type Kind string

const (
	// KindResponse is a whole upstream response that could not be read at all.
	KindResponse Kind = "response"
	// KindRecord is one record of a readable response.
	KindRecord Kind = "record"
)

// Retry policy for transient failures.
const (
	MaxAttempts  = 8
	BaseBackoff  = time.Second
	MaxBackoff   = 5 * time.Minute
	ClaimLease   = time.Minute     // how long a claimed message is hidden from other replicas
	ReplayStale  = 2 * time.Minute // a replay claim older than this is abandoned
	ListMaxLimit = 1000
)

var ErrNotFound = errors.New("message not found")

type Message struct {
	ID               string     `bson:"_id"`
	Source           string     `bson:"source"`
	Kind             Kind       `bson:"kind"`
	Path             string     `bson:"path,omitempty"`
	RefHint          string     `bson:"ref_hint,omitempty"`
	Payload          []byte     `bson:"payload"`
	PayloadTruncated bool       `bson:"payload_truncated,omitempty"`
	ReceivedAt       time.Time  `bson:"received_at"`
	Reason           string     `bson:"reason"`
	Retryable        bool       `bson:"retryable"`
	Attempts         int        `bson:"attempts"`
	Status           Status     `bson:"status"`
	NextAttemptAt    time.Time  `bson:"next_attempt_at"`
	ClaimedAt        time.Time  `bson:"claimed_at,omitempty"`
	UpdatedAt        time.Time  `bson:"updated_at"`
	RecoveredAt      *time.Time `bson:"recovered_at,omitempty"`
	CorrelationID    string     `bson:"correlation_id,omitempty"`
}

// NewID derives the message id from its content, so the same message received
// twice (polls overlap by a couple of seconds) is stored once.
func NewID(source string, kind Kind, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(source))
	h.Write([]byte{0})
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write(payload)
	return "msg-" + hex.EncodeToString(h.Sum(nil))[:24]
}

// Backoff returns the delay before attempt number attempts+1.
func Backoff(attempts int) time.Duration {
	d := BaseBackoff
	for i := 1; i < attempts && d < MaxBackoff; i++ {
		d *= 2
	}
	return min(d, MaxBackoff)
}

// ReportedStatus is the status shown by the audit API (retrying or parked).
func (m Message) ReportedStatus() Status {
	if m.Status == StatusReplaying {
		return StatusParked
	}
	return m.Status
}

// Cursor is a position in the (received_at, id) order.
type Cursor struct {
	ReceivedAt time.Time `json:"r"`
	ID         string    `json:"i"`
}

func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (Cursor, error) {
	var c Cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil || c.ID == "" {
		return c, errors.New("invalid cursor")
	}
	return c, nil
}

// Store persists messages. Claims are atomic so several aggregator replicas
// never process the same message at once.
type Store interface {
	// Put inserts m unless a message with the same id exists. It reports whether it inserted.
	Put(ctx context.Context, m Message) (bool, error)
	Get(ctx context.Context, id string) (Message, error)
	// ClaimDue claims the retrying message whose next attempt is earliest and due.
	ClaimDue(ctx context.Context, now time.Time) (*Message, error)
	// ClaimParked claims the oldest parked message of source not touched since passStart.
	ClaimParked(ctx context.Context, source string, passStart, now time.Time) (*Message, error)
	MarkRecovered(ctx context.Context, id string, at time.Time) error
	// MarkFailed records a failed attempt and the new status.
	MarkFailed(ctx context.Context, id string, reason string, retryable bool, status Status, attempts int, next, now time.Time) error
	// List returns retrying and parked messages in (received_at, id) order after cursor.
	List(ctx context.Context, source string, after *Cursor, limit int) ([]Message, error)
}
