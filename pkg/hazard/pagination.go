package hazard

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	DefaultPageLimit = 100
	MaxPageLimit     = 500
	NextCursorHeader = "X-Next-Cursor"
)

type Cursor struct {
	OccurredAt time.Time `json:"occurred_at"`
	HazardID   string    `json:"hazard_id"`
}

func ParsePageLimit(raw string) (int, error) {
	if raw == "" {
		return DefaultPageLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > MaxPageLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", MaxPageLimit)
	}
	return limit, nil
}

func EncodeCursor(cursor Cursor) (string, error) {
	if cursor.OccurredAt.IsZero() || cursor.HazardID == "" {
		return "", errors.New("cursor requires occurred_at and hazard_id")
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeCursor(encoded string) (Cursor, error) {
	if encoded == "" || len(encoded) > 1024 {
		return Cursor{}, errors.New("invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return Cursor{}, errors.New("invalid cursor")
	}
	var cursor Cursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return Cursor{}, errors.New("invalid cursor")
	}
	if cursor.OccurredAt.IsZero() || cursor.HazardID == "" {
		return Cursor{}, errors.New("invalid cursor")
	}
	return cursor, nil
}
