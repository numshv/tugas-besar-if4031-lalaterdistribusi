package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store/storetest"
)

func TestMongoContract(t *testing.T) {
	uri := os.Getenv("STORE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("STORE_TEST_MONGO_URI not set; run scripts/dev-mongo-sharded.sh up")
	}
	ctx := context.Background()
	client, err := store.NewMongoClient(ctx, uri)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	dbName := fmt.Sprintf("storetest_%d", time.Now().UnixNano())
	admin := client.Database("admin")
	if err := admin.RunCommand(ctx, bson.D{{Key: "enableSharding", Value: dbName}}).Err(); err != nil {
		t.Fatalf("enableSharding: %v", err)
	}
	ns := dbName + ".hazard_events"
	if err := admin.RunCommand(ctx, bson.D{{Key: "shardCollection", Value: ns}, {Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}}}).Err(); err != nil {
		t.Fatalf("shardCollection: %v", err)
	}
	t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

	base := store.NewMongoStore(client, dbName)
	if err := base.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}

	storetest.Run(t, func(t *testing.T, now func() time.Time) store.Store {
		db := client.Database(dbName)
		for _, name := range []string{"hazard_events", "fences"} {
			if _, err := db.Collection(name).DeleteMany(ctx, bson.M{}); err != nil {
				t.Fatalf("reset %s: %v", name, err)
			}
		}
		return store.NewMongoStore(client, dbName).WithClock(now)
	})
}
