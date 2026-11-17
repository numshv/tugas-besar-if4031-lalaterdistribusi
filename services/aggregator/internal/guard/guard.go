// Package guard rejects broken payloads before decoding. encoding/json silently
// replaces invalid UTF-8, allows 10,000 nesting levels and reads any size.
package guard

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	MaxBodyBytes = 4 << 20 // a full PVMBG history grows ~1.7 MB per day
	MaxDepth     = 32      // contracts nest at most 3 levels
)

type MalformedError struct{ Reason string }

func (e *MalformedError) Error() string { return "malformed: " + e.Reason }

func malformed(format string, a ...any) error {
	return &MalformedError{Reason: fmt.Sprintf(format, a...)}
}

func IsMalformed(err error) bool {
	var m *MalformedError
	return errors.As(err, &m)
}

func Oversize() error {
	return malformed("body exceeds %d bytes", MaxBodyBytes)
}

func Check(b []byte) error {
	if len(b) > MaxBodyBytes {
		return Oversize()
	}
	if !utf8.Valid(b) {
		return malformed("invalid utf-8")
	}
	if d := depth(b); d > MaxDepth {
		return malformed("nesting depth exceeds %d", MaxDepth)
	}
	if !json.Valid(b) {
		return malformed("invalid or truncated json")
	}
	return nil
}

// depth ignores brackets inside strings and stops once MaxDepth is exceeded.
func depth(b []byte) int {
	cur, max := 0, 0
	inString, escaped := false, false
	for _, c := range b {
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '[', '{':
			cur++
			if cur > max {
				max = cur
				if max > MaxDepth {
					return max
				}
			}
		case ']', '}':
			cur--
		}
	}
	return max
}

// SplitArray keeps each record's raw bytes so it can be set aside on its own.
func SplitArray(b []byte) ([]json.RawMessage, error) {
	if err := Check(b); err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(b, &items); err != nil {
		return nil, malformed("top-level value is not an array")
	}
	return items, nil
}
