// Package dlq is the dead letter queue: messages the aggregator could not process,
// kept byte for byte. Recovered messages stay for the audit trail.
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

type Kind string

const (
	KindResponse Kind = "response" // a whole response that could not be read
	KindRecord   Kind = "record"
)

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

// NewID is content based because polls overlap and the same message arrives twice.
func NewID(source string, kind Kind, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(source))
	h.Write([]byte{0})
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write(payload)
	return "msg-" + hex.EncodeToString(h.Sum(nil))[:24]
}

func Backoff(attempts int) time.Duration {
	d := BaseBackoff
	for i := 1; i < attempts && d < MaxBackoff; i++ {
		d *= 2
	}
	return min(d, MaxBackoff)
}

// ReportedStatus maps to the two statuses the audit contract knows.
func (m Message) ReportedStatus() Status {
	if m.Status == StatusReplaying {
		return StatusParked
	}
	return m.Status
}

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

// Store claims are atomic so several aggregator replicas never process the same message.
type Store interface {
	Put(ctx context.Context, m Message) (inserted bool, err error)
	Get(ctx context.Context, id string) (Message, error)
	ClaimDue(ctx context.Context, now time.Time) (*Message, error)
	// ClaimParked skips messages touched since passStart, so one replay pass sees each message once.
	ClaimParked(ctx context.Context, source string, passStart, now time.Time) (*Message, error)
	MarkRecovered(ctx context.Context, id string, at time.Time) error
	MarkFailed(ctx context.Context, id string, reason string, retryable bool, status Status, attempts int, next, now time.Time) error
	List(ctx context.Context, source string, after *Cursor, limit int) ([]Message, error)
}
