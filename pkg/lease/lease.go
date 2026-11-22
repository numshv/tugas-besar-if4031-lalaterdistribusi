// Package lease elects one leader per job through a Kubernetes Lease.
//
// The API server is the single source of truth: expiry is judged by each candidate's own
// monotonic clock, never by comparing wall clocks between nodes, and every leadership
// change bumps spec.leaseTransitions, which callers use as a fencing epoch.
package lease

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	isLeader = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "leader_is_leader", Help: "1 while this pod holds the lease.",
	}, []string{"lock"})
	epochGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "leader_epoch", Help: "Fencing epoch (lease transitions) this pod leads with.",
	}, []string{"lock"})
	transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "leader_acquired_total", Help: "Times this pod became leader.",
	}, []string{"lock"})
)

type Timing struct {
	LeaseDuration, RenewDeadline, RetryPeriod time.Duration
}

var DefaultTiming = Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second}

type Elector struct {
	name, namespace, identity string
	client                    kubernetes.Interface
	timing                    Timing
	log                       *slog.Logger
	elector                   atomic.Pointer[leaderelection.LeaderElector]
	epoch                     atomic.Int64
}

// InCluster builds an elector from the pod's service account. Identity is POD_NAME
// (falling back to the hostname); namespace is POD_NAMESPACE.
func InCluster(name string, timing Timing, log *slog.Logger) (*Elector, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("lease %s: %w", name, err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("lease %s: %w", name, err)
	}
	identity := os.Getenv("POD_NAME")
	if identity == "" {
		identity, _ = os.Hostname()
	}
	return New(client, os.Getenv("POD_NAMESPACE"), name, identity, timing, log), nil
}

func New(client kubernetes.Interface, namespace, name, identity string, timing Timing, log *slog.Logger) *Elector {
	return &Elector{name: name, namespace: namespace, identity: identity, client: client, timing: timing,
		log: log.With("lock", name, "identity", identity)}
}

// Run campaigns until ctx ends. Each time this pod wins, lead runs with a context that
// is cancelled when leadership is lost, and with the epoch of that term. When lead
// returns on its own, the lease is released so another replica can take over. After each
// term the pod sits out one lease period, so a replica that keeps failing (for example,
// one that cannot reach storage) cannot win the lease it just released again and again.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, epoch int64)) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: e.name, Namespace: e.namespace},
		Client:     e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: e.identity},
	}
	for ctx.Err() == nil {
		term, stepDown := context.WithCancel(ctx)
		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   e.timing.LeaseDuration,
			RenewDeadline:   e.timing.RenewDeadline,
			RetryPeriod:     e.timing.RetryPeriod,
			ReleaseOnCancel: true,
			Name:            e.name,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(lctx context.Context) {
					epoch, err := e.readEpoch(lctx)
					if err != nil {
						e.log.Error("read lease epoch failed, stepping down", "error", err)
						return
					}
					e.epoch.Store(epoch)
					isLeader.WithLabelValues(e.name).Set(1)
					epochGauge.WithLabelValues(e.name).Set(float64(epoch))
					transitions.WithLabelValues(e.name).Inc()
					e.log.Info("leadership acquired", "epoch", epoch)
					lead(lctx, epoch)
					stepDown()
				},
				OnStoppedLeading: func() {
					isLeader.WithLabelValues(e.name).Set(0)
					e.log.Info("leadership lost")
				},
			},
		})
		if err != nil {
			stepDown()
			e.log.Error("leader election config invalid", "error", err)
			return
		}
		e.elector.Store(le)
		le.Run(term)
		stepDown()
		select {
		case <-ctx.Done():
		case <-time.After(e.timing.LeaseDuration):
		}
	}
}

// Confirm asks the API server, not the local clock, whether this pod still holds the
// lease in the same term. Callers check it right before touching an upstream, so a pod
// that wakes up from a freeze stops before sending anything.
func (e *Elector) Confirm(ctx context.Context) (int64, bool) {
	le := e.elector.Load()
	if le == nil || !le.IsLeader() {
		return 0, false
	}
	l, err := e.client.CoordinationV1().Leases(e.namespace).Get(ctx, e.name, metav1.GetOptions{})
	if err != nil || l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != e.identity {
		return 0, false
	}
	epoch := e.epoch.Load()
	if l.Spec.LeaseTransitions == nil || int64(*l.Spec.LeaseTransitions) != epoch {
		return 0, false
	}
	return epoch, true
}

func (e *Elector) readEpoch(ctx context.Context) (int64, error) {
	l, err := e.client.CoordinationV1().Leases(e.namespace).Get(ctx, e.name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != e.identity || l.Spec.LeaseTransitions == nil {
		return 0, fmt.Errorf("lease %s not held by %s", e.name, e.identity)
	}
	return int64(*l.Spec.LeaseTransitions), nil
}
