package store

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// volcano_reference jarang berubah, jadi di-cache in-memory setelah load
// pertama daripada query Mongo di setiap event vulkanik yang di-mapping.
type MongoVolcanoResolver struct {
	mu   sync.RWMutex
	data map[string]volcanoRef
	col  *mongo.Collection
}

type volcanoRef struct {
	Name string  `bson:"name"`
	Lat  float64 `bson:"latitude"`
	Lon  float64 `bson:"longitude"`
}

func NewMongoVolcanoResolver(ctx context.Context, uri, dbName string) (*MongoVolcanoResolver, error) {
	client, err := mongo.Connect(ctx, options().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	r := &MongoVolcanoResolver{col: client.Database(dbName).Collection("volcano_reference"), data: map[string]volcanoRef{}}
	if err := r.reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *MongoVolcanoResolver) reload(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cur, err := r.col.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	next := make(map[string]volcanoRef)
	for cur.Next(ctx) {
		var raw struct {
			ID   string  `bson:"_id"`
			Name string  `bson:"name"`
			Lat  float64 `bson:"latitude"`
			Lon  float64 `bson:"longitude"`
		}
		if err := cur.Decode(&raw); err != nil {
			return err
		}
		next[raw.ID] = volcanoRef{Name: raw.Name, Lat: raw.Lat, Lon: raw.Lon}
	}

	r.mu.Lock()
	r.data = next
	r.mu.Unlock()
	return cur.Err()
}

func (r *MongoVolcanoResolver) Resolve(volcanoID string) (string, float64, float64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.data[volcanoID]
	return v.Name, v.Lat, v.Lon, ok
}
