package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/lease"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/poller"
)

// runPollers polls BMKG and PVMBG. Inside Kubernetes every source has its own Lease, so
// exactly one replica polls it at a time; elsewhere (docker compose) this replica polls both.
func runPollers(ctx context.Context, log *slog.Logger, p *poller.Poller, cps poller.Checkpoints) {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" || os.Getenv("LEADER_ELECTION") == "off" {
		log.Info("leader election off, polling every source from this replica")
		p.Run(ctx)
		return
	}
	timing := lease.Timing{
		LeaseDuration: envDuration("LEASE_DURATION_SECONDS", lease.DefaultTiming.LeaseDuration),
		RenewDeadline: envDuration("LEASE_RENEW_DEADLINE_SECONDS", lease.DefaultTiming.RenewDeadline),
		RetryPeriod:   envDuration("LEASE_RETRY_PERIOD_SECONDS", lease.DefaultTiming.RetryPeriod),
	}
	var wg sync.WaitGroup
	for _, source := range []hazard.Source{hazard.SourceBMKG, hazard.SourcePVMBG} {
		elector, err := lease.InCluster("poller-"+strings.ToLower(string(source)), timing, log)
		if err != nil {
			log.Error("leader election unavailable, this replica will not poll", "source", source, "error", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			elector.Run(ctx, func(term context.Context, epoch int64) {
				p.Lead(term, source, epoch, elector, cps)
			})
		}()
	}
	wg.Wait()
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return fallback
}
