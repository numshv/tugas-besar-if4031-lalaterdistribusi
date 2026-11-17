// Package guard rejects broken upstream payloads before they are decoded.
//
// encoding/json silently replaces invalid UTF-8 with U+FFFD, accepts nesting up
// to 10,000 levels and reads bodies of any size. Each of those is checked here
// first, so a broken message is set aside with a precise reason instead of
// being stored corrupted or exhausting memory.
package guard

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	// MaxBodyBytes bounds one upstream response. A full PVMBG history grows by
	// about 1.7 MB per day, so 4 MiB leaves room for legitimate full fetches.
	MaxBodyBytes = 4 << 20
	// MaxDepth bounds JSON nesting. Upstream contracts nest at most 3 levels.
	MaxDepth = 32
)

// MalformedError means the payload can never be read as it is.
type MalformedError struct{ Reason string }

func (e *MalformedError) Error() string { return "malformed: " + e.Reason }

func malformed(format string, a ...any) error {
	return &MalformedError{Reason: fmt.Sprintf(format, a...)}
}

// IsMalformed reports whether err is a MalformedError.
func IsMalformed(err error) bool {
	var m *MalformedError
	return errors.As(err, &m)
}

// Oversize builds the error for a body that exceeded MaxBodyBytes.
func Oversize() error {
	return malformed("body exceeds %d bytes", MaxBodyBytes)
}

// Check validates size, UTF-8, nesting depth and JSON syntax, in that order.
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

// depth returns the maximum nesting of arrays and objects, ignoring brackets
// inside strings. It stops counting once MaxDepth is exceeded.
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

// SplitArray checks b and splits a top-level JSON array into its raw elements,
// so each record can be processed, and set aside, on its own.
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
