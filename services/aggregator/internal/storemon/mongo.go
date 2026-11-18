package storemon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoUnits struct {
	user     string
	password string
	dbName   string

	mu      sync.Mutex
	probes  map[string]*mongo.Client
	clients map[string]*mongo.Client
}

func NewMongoUnits(user, password, dbName string) *MongoUnits {
	return &MongoUnits{
		user: user, password: password, dbName: dbName,
		probes:  map[string]*mongo.Client{},
		clients: map[string]*mongo.Client{},
	}
}

func (m *MongoUnits) connect(host string, withAuth bool) (*mongo.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pool := m.probes
	if withAuth {
		pool = m.clients
	}
	if c, ok := pool[host]; ok {
		return c, nil
	}
	opts := options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s/?directConnection=true", host)).
		SetServerSelectionTimeout(time.Second).
		SetConnectTimeout(time.Second).
		SetMaxPoolSize(2)
	if withAuth {
		opts.SetAuth(options.Credential{Username: m.user, Password: m.password, AuthSource: "admin"})
	}
	c, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	pool[host] = c
	return c, nil
}

func (m *MongoUnits) Hello(ctx context.Context, host string) (bool, error) {
	c, err := m.connect(host, false)
	if err != nil {
		return false, err
	}
	var reply struct {
		IsWritablePrimary bool `bson:"isWritablePrimary"`
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&reply); err != nil {
		return false, err
	}
	return reply.IsWritablePrimary, nil
}

type serverStatusReply struct {
	Opcounters     map[string]int64 `bson:"opcounters"`
	OpcountersRepl map[string]int64 `bson:"opcountersRepl"`
}

func (m *MongoUnits) Stats(ctx context.Context, u Unit) (UnitStats, error) {
	c, err := m.connect(u.Host, true)
	if err != nil {
		return UnitStats{}, err
	}
	var status serverStatusReply
	cmd := bson.D{{Key: "serverStatus", Value: 1}, {Key: "repl", Value: 0}, {Key: "metrics", Value: 0}, {Key: "locks", Value: 0}}
	if err := c.Database("admin").RunCommand(ctx, cmd).Decode(&status); err != nil {
		return UnitStats{}, fmt.Errorf("serverStatus %s: %w", u.Name, err)
	}
	stats := UnitStats{Ops: status.Opcounters, ReplOps: status.OpcountersRepl}
	if !u.Data {
		return stats, nil
	}

	var db struct {
		DataSize float64 `bson:"dataSize"`
	}
	if err := c.Database(m.dbName).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&db); err != nil {
		return UnitStats{}, fmt.Errorf("dbStats %s: %w", u.Name, err)
	}
	dataBytes := int64(db.DataSize)
	stats.DataBytes = &dataBytes

	cur, err := c.Database(m.dbName).Collection("hazard_events").Aggregate(ctx,
		mongo.Pipeline{{{Key: "$collStats", Value: bson.D{{Key: "count", Value: bson.D{}}}}}})
	if err != nil {
		return UnitStats{}, fmt.Errorf("collStats %s: %w", u.Name, err)
	}
	var rows []struct {
		Count int64 `bson:"count"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return UnitStats{}, fmt.Errorf("collStats %s: %w", u.Name, err)
	}
	var docs int64
	for _, r := range rows {
		docs += r.Count
	}
	stats.Docs = &docs
	return stats, nil
}

func (m *MongoUnits) Close(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pool := range []map[string]*mongo.Client{m.probes, m.clients} {
		for host, c := range pool {
			_ = c.Disconnect(ctx)
			delete(pool, host)
		}
	}
}
