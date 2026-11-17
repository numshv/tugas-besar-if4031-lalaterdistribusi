package guard

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	cases := map[string]struct {
		body   string
		reason string
	}{
		"valid":          {`[{"a":"[[[[ inside a string ]]]]"}]`, ""},
		"truncated":      {`[{"report_id":"r1","alert_le`, "invalid or truncated json"},
		"invalid utf-8":  {"[{\"report_id\":\"\xff\xfe\"}]", "invalid utf-8"},
		"deep":           {strings.Repeat("[", 100) + strings.Repeat("]", 100), "nesting depth exceeds 32"},
		"depth at limit": {strings.Repeat("[", 32) + strings.Repeat("]", 32), ""},
		"oversize":       {`"` + strings.Repeat("x", MaxBodyBytes) + `"`, "body exceeds"},
	}
	for name, c := range cases {
		err := Check([]byte(c.body))
		if c.reason == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", name, err)
			}
			continue
		}
		if !IsMalformed(err) || !strings.Contains(err.Error(), c.reason) {
			t.Errorf("%s: error = %v, want malformed %q", name, err, c.reason)
		}
	}
}

func TestSplitArray(t *testing.T) {
	items, err := SplitArray([]byte(`[{"a":1}, {"b":2}]`))
	if err != nil || len(items) != 2 || string(items[1]) != `{"b":2}` {
		t.Fatalf("items = %q, err = %v", items, err)
	}
	if _, err := SplitArray([]byte(`{"a":1}`)); !IsMalformed(err) {
		t.Errorf("object accepted as array: %v", err)
	}
}
