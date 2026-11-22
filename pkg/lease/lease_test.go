package lease

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

var fast = Timing{LeaseDuration: time.Second, RenewDeadline: 500 * time.Millisecond, RetryPeriod: 100 * time.Millisecond}

func campaign(t *testing.T, e *Elector) (epochs chan int64, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	epochs = make(chan int64, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Run(ctx, func(lctx context.Context, epoch int64) {
			epochs <- epoch
			<-lctx.Done()
		})
	}()
	return epochs, func() { cancel(); <-done }
}

func waitEpoch(t *testing.T, epochs chan int64) int64 {
	t.Helper()
	select {
	case e := <-epochs:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("never became leader")
		return 0
	}
}

func TestLeaderThatStopsWorkingStepsDown(t *testing.T) {
	client := fake.NewSimpleClientset()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := New(client, "ns", "poller-pvmbg", "pod-a", fast, log)
	b := New(client, "ns", "poller-pvmbg", "pod-b", fast, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	terms := make(chan int64, 8)
	var first atomic.Bool
	lead := func(term context.Context, epoch int64) {
		terms <- epoch
		if first.CompareAndSwap(false, true) {
			return // the first leader stops working while its context is still alive
		}
		<-term.Done()
	}
	go a.Run(ctx, lead)
	firstEpoch := waitEpoch(t, terms)
	go b.Run(ctx, lead)

	if next := waitEpoch(t, terms); next <= firstEpoch {
		t.Fatalf("lease was not released and re-acquired: first=%d next=%d", firstEpoch, next)
	}
}

func TestFailingLeaderLetsHealthyReplicaWin(t *testing.T) {
	client := fake.NewSimpleClientset()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sick := New(client, "ns", "poller-bmkg", "pod-sick", fast, log)
	healthy := New(client, "ns", "poller-bmkg", "pod-healthy", fast, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sickTerms atomic.Int32
	sickLed := make(chan int64, 1)
	go sick.Run(ctx, func(_ context.Context, epoch int64) {
		sickTerms.Add(1) // returns at once, like a leader that cannot reach storage
		select {
		case sickLed <- epoch:
		default:
		}
	})
	waitEpoch(t, sickLed)

	healthyEpochs, stop := campaign(t, healthy)
	defer stop()
	waitEpoch(t, healthyEpochs)
	if n := sickTerms.Load(); n != 1 {
		t.Fatalf("failing replica took the lease %d times before the healthy one got a turn", n)
	}
}

func TestHandoverBumpsEpochAndFencesOldLeader(t *testing.T) {
	client := fake.NewSimpleClientset()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := New(client, "ns", "poller-bmkg", "pod-a", fast, log)
	b := New(client, "ns", "poller-bmkg", "pod-b", fast, log)
	ctx := context.Background()

	aEpochs, stopA := campaign(t, a)
	epochA := waitEpoch(t, aEpochs)
	if got, ok := a.Confirm(ctx); !ok || got != epochA {
		t.Fatalf("leader A Confirm = (%d, %v), want (%d, true)", got, ok, epochA)
	}

	bEpochs, stopB := campaign(t, b)
	defer stopB()
	if _, ok := b.Confirm(ctx); ok {
		t.Fatal("follower B must not confirm while A leads")
	}

	stopA()
	epochB := waitEpoch(t, bEpochs)
	if epochB <= epochA {
		t.Fatalf("epoch must grow on handover: A=%d B=%d", epochA, epochB)
	}
	if _, ok := a.Confirm(ctx); ok {
		t.Fatal("old leader A must fail Confirm after handover")
	}
	if got, ok := b.Confirm(ctx); !ok || got != epochB {
		t.Fatalf("leader B Confirm = (%d, %v), want (%d, true)", got, ok, epochB)
	}
}
