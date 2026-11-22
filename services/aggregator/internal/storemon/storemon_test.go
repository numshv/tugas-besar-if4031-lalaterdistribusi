package storemon

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const testTopology = `{"config":{"name":"cfg","members":[{"host":"cfg-a:27017","slot":"a","priority":2}]},
"shards":[
 {"name":"shard-0","members":[{"host":"s0a:27017","slot":"a","priority":2},{"host":"s0b:27017","slot":"b","priority":1}]},
 {"name":"shard-1","members":[{"host":"s1b:27017","slot":"b","priority":2}]}]}`

type fakeProber struct {
	mu      sync.Mutex
	down    map[string]bool
	primary map[string]bool
}

func (f *fakeProber) set(host string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down[host] = down
}

func (f *fakeProber) Hello(_ context.Context, host string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[host] {
		return false, errors.New("connection refused")
	}
	return f.primary[host], nil
}

func newTestMonitor(t *testing.T) (*Monitor, *fakeProber) {
	t.Helper()
	topo, err := ParseTopology(testTopology)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProber{down: map[string]bool{}, primary: map[string]bool{"s0a:27017": true, "s1b:27017": true, "cfg-a:27017": true}}
	return NewMonitor(topo.Units(), p, SlotNodes{}, time.Hour, time.Second), p
}

func TestTopologyUnits(t *testing.T) {
	topo, err := ParseTopology(testTopology)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, u := range topo.Units() {
		names = append(names, u.Name)
	}
	if got := strings.Join(names, ","); got != "cfg-a,shard-0-a,shard-0-b,shard-1-b" {
		t.Fatalf("units = %s", got)
	}
	if _, err := ParseTopology(`{"config":{"name":"cfg","members":[]},"shards":[]}`); err == nil {
		t.Fatal("empty topology must be rejected")
	}
}

func TestMonitorNeedsTwoFailuresBeforeDown(t *testing.T) {
	m, p := newTestMonitor(t)
	ctx := context.Background()
	m.ProbeOnce(ctx)
	if units, shards := m.Unavailable(); len(units) != 0 || len(shards) != 0 {
		t.Fatalf("all up: units=%v shards=%v", units, shards)
	}

	p.set("s0b:27017", true)
	m.ProbeOnce(ctx)
	if units, _ := m.Unavailable(); len(units) != 0 {
		t.Fatalf("one failed probe must not mark the unit down, got %v", units)
	}
	m.ProbeOnce(ctx)
	units, shards := m.Unavailable()
	if strings.Join(units, ",") != "shard-0-b" || len(shards) != 0 {
		t.Fatalf("after two failures: units=%v shards=%v", units, shards)
	}

	p.set("s0b:27017", false)
	m.ProbeOnce(ctx)
	if units, _ := m.Unavailable(); len(units) != 0 {
		t.Fatalf("one success must bring the unit back, got %v", units)
	}
}

func TestMonitorShardDownAndNeverSeen(t *testing.T) {
	m, p := newTestMonitor(t)
	p.set("s1b:27017", true)
	m.ProbeOnce(context.Background())
	units, shards := m.Unavailable()
	if strings.Join(units, ",") != "shard-1-b" || strings.Join(shards, ",") != "shard-1" {
		t.Fatalf("unit never seen up must be down at once: units=%v shards=%v", units, shards)
	}

	p.set("cfg-a:27017", true)
	m.ProbeOnce(context.Background())
	if units, _ := m.Unavailable(); strings.Contains(strings.Join(units, ","), "cfg") {
		t.Fatalf("config servers are not data units: %v", units)
	}
}

func TestKubeNodesResolvesFromPods(t *testing.T) {
	pod := func(name, replset, slot, node string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "bnpb", Labels: map[string]string{
				"bnpb/mongo-member": "true", "bnpb/replset": replset, "bnpb/slot": slot,
			}},
			Spec: corev1.PodSpec{NodeName: node},
		}
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, p := range []*corev1.Pod{
		pod("mongo-shard0-a-0", "shard-0", "a", "node-a"),
		pod("mongo-shard0-b-0", "shard-0", "b", "node-b"),
	} {
		if err := indexer.Add(p); err != nil {
			t.Fatal(err)
		}
	}
	nodes := &KubeNodes{pods: corelisters.NewPodLister(indexer)}
	if got := nodes.Node(Unit{ReplicaSet: "shard-0", Slot: "b"}); got != "node-b" {
		t.Fatalf("node = %s, want node-b", got)
	}
	if got := nodes.Node(Unit{ReplicaSet: "shard-1", Slot: "b"}); got != unknownNode {
		t.Fatalf("missing pod node = %s, want %s", got, unknownNode)
	}
}

type fakeStats struct{}

func (fakeStats) Stats(_ context.Context, u Unit) (UnitStats, error) {
	if u.Host == "s1b:27017" {
		return UnitStats{}, errors.New("unreachable")
	}
	stats := UnitStats{Ops: map[string]int64{"insert": 7}, ReplOps: map[string]int64{"insert": 3}}
	if u.Data {
		size := int64(1024)
		stats.Docs, stats.DataBytes = map[string]int64{"hazard_events": 42, "exp_by_area": 7}, &size
	}
	return stats, nil
}

func TestCollectorExportsMetrics(t *testing.T) {
	m, p := newTestMonitor(t)
	p.set("s1b:27017", true)
	m.ProbeOnce(context.Background())
	c := NewCollector(m, fakeStats{}, time.Hour, time.Second)
	c.Refresh(context.Background())

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	expected := `
# HELP store_docs Documents held by the storage unit, per collection.
# TYPE store_docs gauge
store_docs{collection="exp_by_area",node="slot-a",shard="shard-0",unit="shard-0-a"} 7
store_docs{collection="exp_by_area",node="slot-b",shard="shard-0",unit="shard-0-b"} 7
store_docs{collection="hazard_events",node="slot-a",shard="shard-0",unit="shard-0-a"} 42
store_docs{collection="hazard_events",node="slot-b",shard="shard-0",unit="shard-0-b"} 42
# HELP store_unit_up 1 if the storage unit answers hello, 0 otherwise.
# TYPE store_unit_up gauge
store_unit_up{node="slot-a",shard="cfg",unit="cfg-a"} 1
store_unit_up{node="slot-a",shard="shard-0",unit="shard-0-a"} 1
store_unit_up{node="slot-b",shard="shard-0",unit="shard-0-b"} 1
store_unit_up{node="slot-b",shard="shard-1",unit="shard-1-b"} 0
# HELP store_ops_total Operations served by the storage unit (serverStatus.opcounters).
# TYPE store_ops_total counter
store_ops_total{node="slot-a",op="insert",shard="cfg",unit="cfg-a"} 7
store_ops_total{node="slot-a",op="insert",shard="shard-0",unit="shard-0-a"} 7
store_ops_total{node="slot-b",op="insert",shard="shard-0",unit="shard-0-b"} 7
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), "store_docs", "store_unit_up", "store_ops_total"); err != nil {
		t.Fatal(err)
	}
}
