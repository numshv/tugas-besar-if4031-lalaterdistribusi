package storemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type Member struct {
	Host     string `json:"host"`
	Slot     string `json:"slot"`
	Priority int    `json:"priority"`
}

type ReplicaSet struct {
	Name    string   `json:"name"`
	Members []Member `json:"members"`
}

type Topology struct {
	Config ReplicaSet   `json:"config"`
	Shards []ReplicaSet `json:"shards"`
}

type Unit struct {
	Name       string
	ReplicaSet string
	Slot       string
	Host       string
	Data       bool
}

func UnitName(replicaSet, slot string) string {
	return replicaSet + "-" + slot
}

func ParseTopology(raw string) (Topology, error) {
	var t Topology
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return Topology{}, fmt.Errorf("parse topology: %w", err)
	}
	if len(t.Shards) == 0 {
		return Topology{}, errors.New("parse topology: no shards")
	}
	for _, rs := range append([]ReplicaSet{t.Config}, t.Shards...) {
		if rs.Name == "" || len(rs.Members) == 0 {
			return Topology{}, fmt.Errorf("parse topology: replica set %q has no name or members", rs.Name)
		}
		for _, m := range rs.Members {
			if m.Host == "" || m.Slot == "" {
				return Topology{}, fmt.Errorf("parse topology: member of %s needs host and slot", rs.Name)
			}
		}
	}
	return t, nil
}

func (t Topology) Units() []Unit {
	var units []Unit
	add := func(rs ReplicaSet, data bool) {
		for _, m := range rs.Members {
			units = append(units, Unit{Name: UnitName(rs.Name, m.Slot), ReplicaSet: rs.Name, Slot: m.Slot, Host: m.Host, Data: data})
		}
	}
	add(t.Config, false)
	for _, rs := range t.Shards {
		add(rs, true)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].Name < units[j].Name })
	return units
}
