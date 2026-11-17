// Package schema holds the mapping rules Ops use to adapt to an upstream schema
// change without rebuilding the aggregator.
//
// Rules live in a JSON file mounted from a ConfigMap. Changing the ConfigMap
// restarts the pods (Kustomize hashes its name), after which Ops replay the
// parked messages. Only three simple adaptations are supported on purpose:
// renaming a field, translating values and filling a missing field. Anything
// more complex is a code change.
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

// ReceivedAt is the default value that stands for the time BNPB received the message.
const ReceivedAt = "$received_at"

// SourceRules adapt one source's records.
type SourceRules struct {
	// Rename maps an upstream field name to the name the mapper expects.
	Rename map[string]string `json:"rename,omitempty"`
	// Values translates a field's value, keyed by the value's text form.
	Values map[string]map[string]any `json:"values,omitempty"`
	// Defaults fills a field that is missing. "$received_at" uses the receive time.
	Defaults map[string]string `json:"defaults,omitempty"`
}

// Rules are keyed by HazardEvent source (BMKG, PVMBG, ...).
type Rules map[string]SourceRules

// Load reads rules from path. An empty path or a missing file means no rules.
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

// Parse decodes rules from JSON.
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

// Apply returns a copy of raw adapted by the source's rules. raw is not modified.
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
