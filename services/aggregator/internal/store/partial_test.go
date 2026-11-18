package store

import "testing"

type fixedHealth struct {
	units, shards []string
}

func (f fixedHealth) Unavailable() ([]string, []string) {
	return f.units, f.shards
}

func TestPartialStatus(t *testing.T) {
	oneNode := fixedHealth{units: []string{"shard-0-b", "shard-1-b"}}
	wholeShard := fixedHealth{units: []string{"shard-1-b"}, shards: []string{"shard-1"}}

	cases := []struct {
		name          string
		health        Health
		mode          PartialMode
		mongosPartial bool
		want          bool
		wantUnits     int
	}{
		{"healthy", fixedHealth{}, PartialLiteral, false, false, 0},
		{"no monitor", nil, PartialLiteral, false, false, 0},
		{"literal: dead replica marks partial", oneNode, PartialLiteral, false, true, 2},
		{"data: dead replica keeps data complete", oneNode, PartialData, false, false, 2},
		{"data: dead shard marks partial", wholeShard, PartialData, false, true, 1},
		{"mongos flag always wins", fixedHealth{}, PartialData, true, true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, units := partialStatus(c.health, c.mode, c.mongosPartial)
			if got != c.want || len(units) != c.wantUnits || units == nil {
				t.Fatalf("partial=%v units=%v, want partial=%v with %d units (never nil)", got, units, c.want, c.wantUnits)
			}
		})
	}
}

func TestParsePartialMode(t *testing.T) {
	for raw, want := range map[string]PartialMode{"": PartialLiteral, "literal": PartialLiteral, "data": PartialData} {
		got, err := ParsePartialMode(raw)
		if err != nil || got != want {
			t.Fatalf("ParsePartialMode(%q) = %q, %v", raw, got, err)
		}
	}
	if _, err := ParsePartialMode("strict"); err == nil {
		t.Fatal("unknown mode must be rejected")
	}
}
