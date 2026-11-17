// Package schema holds mapping rules from a ConfigMap, so Ops adapt to a simple
// upstream schema change without a rebuild. Anything more complex is a code change.
//
//	{
//	  "PVMBG": {
//	    "rename":   {"level": "alert_level"},
//	    "values":   {"alert_level": {"1": "Normal", "4": "Awas"}},
//	    "defaults": {"reported_at": "$received_at"}
//	  }
//	}
package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"strconv"
	"time"
)

const ReceivedAt = "$received_at"

type SourceRules struct {
	Rename   map[string]string         `json:"rename,omitempty"`
	Values   map[string]map[string]any `json:"values,omitempty"` // keyed by the value's text form
	Defaults map[string]string         `json:"defaults,omitempty"`
}

// Rules are keyed by source (BMKG, PVMBG, ...).
type Rules map[string]SourceRules

// Load treats an empty path or a missing file as no rules.
func Load(path string) (Rules, error) {
	if path == "" {
		return Rules{}, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Rules{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read mapping rules: %w", err)
	}
	return Parse(b)
}

func Parse(b []byte) (Rules, error) {
	var r Rules
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("parse mapping rules: %w", err)
	}
	if r == nil {
		r = Rules{}
	}
	return r, nil
}

// Apply returns an adapted copy; raw itself is kept intact for the DLQ.
func (r Rules) Apply(source string, raw map[string]any, receivedAt time.Time) map[string]any {
	sr, ok := r[source]
	if !ok {
		return raw
	}
	out := maps.Clone(raw)

	for from, to := range sr.Rename {
		v, has := out[from]
		if !has {
			continue
		}
		if _, taken := out[to]; !taken {
			out[to] = v
		}
		delete(out, from)
	}
	for field, table := range sr.Values {
		v, has := out[field]
		if !has {
			continue
		}
		if repl, ok := table[textOf(v)]; ok {
			out[field] = repl
		}
	}
	for field, def := range sr.Defaults {
		if _, has := out[field]; has {
			continue
		}
		if def == ReceivedAt {
			out[field] = receivedAt.UTC().Format(time.RFC3339)
		} else {
			out[field] = def
		}
	}
	return out
}

func textOf(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	}
	b, _ := json.Marshal(v)
	return string(b)
}
