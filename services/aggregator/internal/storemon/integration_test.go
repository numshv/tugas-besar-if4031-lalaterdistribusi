package storemon_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/storemon"
)

const hostTopology = `{"config":{"name":"cfg","members":[{"host":"127.0.0.1:27101","slot":"a","priority":2}]},
"shards":[{"name":"shard-0","members":[{"host":"127.0.0.1:27102","slot":"a","priority":2}]},
{"name":"shard-1","members":[{"host":"127.0.0.1:27103","slot":"b","priority":2}]}]}`

func docker(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestPartialReadWhenShardIsDown(t *testing.T) {
	uri := os.Getenv("STORE_TEST_MONGO_URI")
	if uri == "" || os.Getenv("STORE_TEST_DOCKER") != "1" {
		t.Skip("needs scripts/dev-mongo-sharded.sh up, STORE_TEST_MONGO_URI and STORE_TEST_DOCKER=1")
	}
	ctx := context.Background()
	client, err := store.NewMongoClient(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	dbName := fmt.Sprintf("partialtest_%d", time.Now().UnixNano())
	admin := client.Database("admin")
	if err := admin.RunCommand(ctx, bson.D{{Key: "enableSharding", Value: dbName}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "shardCollection", Value: dbName + ".hazard_events"}, {Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}}}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

	topo, err := storemon.ParseTopology(hostTopology)
	if err != nil {
		t.Fatal(err)
	}
	units := storemon.NewMongoUnits("bnpb_monitor", "devmonitor", dbName)
	t.Cleanup(func() { units.Close(context.Background()) })
	monitor := storemon.NewMonitor(topo.Units(), units, storemon.SlotNodes{}, time.Hour, time.Second)
	monitor.ProbeOnce(ctx)

	s := store.NewMongoStore(client, dbName).WithHealth(monitor, store.PartialLiteral)
	const total = 40
	for i := range total {
		ref := fmt.Sprintf("partial-%d", i)
		e := hazard.Event{HazardID: hazard.NewID(hazard.SourceBMKG, ref), Source: hazard.SourceBMKG, SourceRefID: ref, HazardType: hazard.TypeSeismic, Version: 1}
		if _, _, err := s.Upsert(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.ListLatest(ctx, store.LatestQuery{Limit: 100})
	if err != nil || len(page.Items) != total || page.Partial {
		t.Fatalf("healthy read: items=%d partial=%v err=%v", len(page.Items), page.Partial, err)
	}

	collector := storemon.NewCollector(monitor, units, time.Hour, 3*time.Second)
	collector.Refresh(ctx)

	docker(t, "stop", "-t", "30", "mongo-shard1-b")
	t.Cleanup(func() { _ = exec.Command("docker", "start", "mongo-shard1-b").Run() })
	waitFor(t, "shard-1-b marked down", 20*time.Second, func() bool {
		monitor.ProbeOnce(ctx)
		down, _ := monitor.Unavailable()
		return strings.Join(down, ",") == "shard-1-b"
	})

	start := time.Now()
	var degraded store.LatestPage
	waitFor(t, "mongos to answer with partial results", 30*time.Second, func() bool {
		degraded, err = s.ListLatest(ctx, store.LatestQuery{Limit: 100})
		if err != nil {
			t.Logf("read while shard down returned error (retrying): %v", err)
			return false
		}
		return true
	})
	t.Logf("partial read: %d of %d items, partial=%v, unavailable=%v, after %s", len(degraded.Items), total, degraded.Partial, degraded.UnavailableUnits, time.Since(start).Round(time.Millisecond))
	if !degraded.Partial || strings.Join(degraded.UnavailableUnits, ",") != "shard-1-b" {
		t.Fatalf("degraded read must be flagged: partial=%v units=%v", degraded.Partial, degraded.UnavailableUnits)
	}
	if len(degraded.Items) == 0 || len(degraded.Items) >= total {
		t.Fatalf("degraded read should return only shard-0 data, got %d of %d", len(degraded.Items), total)
	}

	docker(t, "start", "mongo-shard1-b")
	waitFor(t, "shard-1-b back up", 60*time.Second, func() bool {
		monitor.ProbeOnce(ctx)
		down, _ := monitor.Unavailable()
		return len(down) == 0
	})
	waitFor(t, "full read after recovery", 60*time.Second, func() bool {
		page, err = s.ListLatest(ctx, store.LatestQuery{Limit: 100})
		return err == nil && len(page.Items) == total && !page.Partial
	})
}
