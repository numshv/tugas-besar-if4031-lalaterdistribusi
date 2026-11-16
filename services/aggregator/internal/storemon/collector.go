package storemon

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type UnitStats struct {
	Ops       map[string]int64
	ReplOps   map[string]int64
	Docs      *int64
	DataBytes *int64
}

type StatsSource interface {
	Stats(ctx context.Context, u Unit) (UnitStats, error)
}

var unitLabels = []string{"unit", "shard", "node"}

var (
	upDesc      = prometheus.NewDesc("store_unit_up", "1 if the storage unit answers hello, 0 otherwise.", unitLabels, nil)
	primaryDesc = prometheus.NewDesc("store_unit_primary", "1 if the storage unit is the primary of its replica set.", unitLabels, nil)
	docsDesc    = prometheus.NewDesc("store_docs", "hazard_events documents held by the storage unit.", unitLabels, nil)
	bytesDesc   = prometheus.NewDesc("store_data_bytes", "Uncompressed data size of the application database on the storage unit.", unitLabels, nil)
	opsDesc     = prometheus.NewDesc("store_ops_total", "Operations served by the storage unit (serverStatus.opcounters).", append(unitLabels, "op"), nil)
	replOpsDesc = prometheus.NewDesc("store_repl_ops_total", "Operations replicated onto the storage unit (serverStatus.opcountersRepl).", append(unitLabels, "op"), nil)
)

type Collector struct {
	monitor  *Monitor
	source   StatsSource
	interval time.Duration
	timeout  time.Duration

	mu    sync.RWMutex
	cache map[string]UnitStats
}

func NewCollector(monitor *Monitor, source StatsSource, interval, timeout time.Duration) *Collector {
	return &Collector{monitor: monitor, source: source, interval: interval, timeout: timeout, cache: map[string]UnitStats{}}
}

func (c *Collector) Refresh(ctx context.Context) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh := map[string]UnitStats{}
	for _, u := range c.monitor.Units() {
		wg.Add(1)
		go func(u Unit) {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			stats, err := c.source.Stats(sctx, u)
			if err != nil {
				return
			}
			mu.Lock()
			fresh[u.Name] = stats
			mu.Unlock()
		}(u)
	}
	wg.Wait()
	c.mu.Lock()
	c.cache = fresh
	c.mu.Unlock()
}

func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		c.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{upDesc, primaryDesc, docsDesc, bytesDesc, opsDesc, replOpsDesc} {
		ch <- d
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.monitor.Snapshot() {
		labels := []string{s.Name, s.ReplicaSet, s.Node}
		ch <- prometheus.MustNewConstMetric(upDesc, prometheus.GaugeValue, boolValue(s.Up), labels...)
		ch <- prometheus.MustNewConstMetric(primaryDesc, prometheus.GaugeValue, boolValue(s.Primary), labels...)
		stats, ok := c.cache[s.Name]
		if !ok {
			continue
		}
		if stats.Docs != nil {
			ch <- prometheus.MustNewConstMetric(docsDesc, prometheus.GaugeValue, float64(*stats.Docs), labels...)
		}
		if stats.DataBytes != nil {
			ch <- prometheus.MustNewConstMetric(bytesDesc, prometheus.GaugeValue, float64(*stats.DataBytes), labels...)
		}
		for op, v := range stats.Ops {
			ch <- prometheus.MustNewConstMetric(opsDesc, prometheus.CounterValue, float64(v), append(labels, op)...)
		}
		for op, v := range stats.ReplOps {
			ch <- prometheus.MustNewConstMetric(replOpsDesc, prometheus.CounterValue, float64(v), append(labels, op)...)
		}
	}
}
