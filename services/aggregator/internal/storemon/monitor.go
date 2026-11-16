package storemon

import (
	"context"
	"sort"
	"sync"
	"time"
)

const failuresToDown = 2

type Prober interface {
	Hello(ctx context.Context, host string) (primary bool, err error)
}

type UnitStatus struct {
	Unit
	Node    string
	Up      bool
	Primary bool
}

type unitState struct {
	up      bool
	primary bool
	everUp  bool
	fails   int
}

type Monitor struct {
	units    []Unit
	prober   Prober
	nodes    NodeResolver
	interval time.Duration
	timeout  time.Duration

	mu    sync.RWMutex
	state map[string]*unitState
}

func NewMonitor(units []Unit, prober Prober, nodes NodeResolver, interval, timeout time.Duration) *Monitor {
	state := make(map[string]*unitState, len(units))
	for _, u := range units {
		state[u.Name] = &unitState{}
	}
	return &Monitor{units: units, prober: prober, nodes: nodes, interval: interval, timeout: timeout, state: state}
}

func (m *Monitor) Units() []Unit {
	return m.units
}

func (m *Monitor) ProbeOnce(ctx context.Context) {
	type result struct {
		name    string
		primary bool
		err     error
	}
	results := make(chan result, len(m.units))
	var wg sync.WaitGroup
	for _, u := range m.units {
		wg.Add(1)
		go func(u Unit) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			primary, err := m.prober.Hello(pctx, u.Host)
			results <- result{name: u.Name, primary: primary, err: err}
		}(u)
	}
	wg.Wait()
	close(results)

	m.mu.Lock()
	defer m.mu.Unlock()
	for r := range results {
		s := m.state[r.name]
		if r.err != nil {
			s.fails++
			s.up = s.everUp && s.fails < failuresToDown
			if !s.up {
				s.primary = false
			}
			continue
		}
		s.fails = 0
		s.up = true
		s.everUp = true
		s.primary = r.primary
	}
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.ProbeOnce(ctx)
		}
	}
}

func (m *Monitor) Snapshot() []UnitStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]UnitStatus, 0, len(m.units))
	for _, u := range m.units {
		s := m.state[u.Name]
		out = append(out, UnitStatus{Unit: u, Node: m.nodes.Node(u), Up: s.up, Primary: s.primary})
	}
	return out
}

func (m *Monitor) Unavailable() ([]string, []string) {
	units := []string{}
	alive := map[string]bool{}
	seen := map[string]bool{}
	for _, s := range m.Snapshot() {
		if !s.Data {
			continue
		}
		seen[s.ReplicaSet] = true
		if s.Up {
			alive[s.ReplicaSet] = true
			continue
		}
		units = append(units, s.Name)
	}
	var shardsDown []string
	for rs := range seen {
		if !alive[rs] {
			shardsDown = append(shardsDown, rs)
		}
	}
	sort.Strings(shardsDown)
	return units, shardsDown
}
