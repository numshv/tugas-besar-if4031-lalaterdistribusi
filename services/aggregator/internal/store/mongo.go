package store

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

type MongoStore struct {
	col *mongo.Collection
}

func NewMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOpts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		SetMaxPoolSize(50).
		SetRetryWrites(true)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	return client, nil
}

func NewMongoStore(client *mongo.Client, dbName string) *MongoStore {
	return &MongoStore{col: client.Database(dbName).Collection("hazard_events")}
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "hazard_type", Value: 1},
			{Key: "occurred_at", Value: -1},
			{Key: "_id", Value: -1},
		},
		Options: options.Index().SetName("hazard_type_occurred_at_id_desc"),
	})
	if err != nil {
		return fmt.Errorf("create hazard pagination index: %w", err)
	}
	return nil
}

// document adalah representasi BSON. hazard_id dipakai sebagai _id supaya
// upsert idempoten by design: dokumen yang sama selalu menimpa dirinya
// sendiri, tidak pernah membuat duplikat walau di-poll berkali-kali.
type document struct {
	ID          string         `bson:"_id"`
	Source      string         `bson:"source"`
	SourceRefID string         `bson:"source_ref_id"`
	HazardType  string         `bson:"hazard_type"`
	Severity    string         `bson:"severity"`
	AreaName    string         `bson:"area_name"`
	Latitude    float64        `bson:"latitude"`
	Longitude   float64        `bson:"longitude"`
	OccurredAt  time.Time      `bson:"occurred_at"`
	IngestedAt  time.Time      `bson:"ingested_at"`
	Attributes  map[string]any `bson:"attributes"`
}

func toDocument(e hazard.Event) document {
	attrs := e.Attributes
	if attrs == nil {
		attrs = map[string]any{} // jangan simpan null, konsisten selalu objek
	}
	return document{
		ID: e.HazardID, Source: string(e.Source), SourceRefID: e.SourceRefID,
		HazardType: string(e.HazardType), Severity: string(e.Severity),
		AreaName: e.AreaName, Latitude: e.Latitude, Longitude: e.Longitude,
		OccurredAt: e.OccurredAt, IngestedAt: e.IngestedAt, Attributes: attrs,
	}
}

func fromDocument(d document) hazard.Event {
	return hazard.Event{
		HazardID: d.ID, Source: hazard.Source(d.Source), SourceRefID: d.SourceRefID,
		HazardType: hazard.Type(d.HazardType), Severity: hazard.Severity(d.Severity),
		AreaName: d.AreaName, Latitude: d.Latitude, Longitude: d.Longitude,
		OccurredAt: d.OccurredAt, IngestedAt: d.IngestedAt, Attributes: d.Attributes,
	}
}

func (s *MongoStore) Upsert(ctx context.Context, e hazard.Event) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	doc := toDocument(e)
	_, err := s.col.ReplaceOne(ctx,
		bson.M{"_id": doc.ID},
		doc,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert hazard_id=%s: %w", doc.ID, err)
	}
	return nil
}

func (s *MongoStore) ListByType(ctx context.Context, hazardType hazard.Type, since time.Time, cursor *hazard.Cursor, limit int) ([]hazard.Event, bool, error) {
	if limit < 1 || limit > hazard.MaxPageLimit {
		return nil, false, fmt.Errorf("limit must be between 1 and %d", hazard.MaxPageLimit)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	filter := bson.M{"hazard_type": string(hazardType)}
	if !since.IsZero() {
		filter["occurred_at"] = bson.M{"$gte": since}
	}
	if cursor != nil {
		filter["$or"] = bson.A{
			bson.M{"occurred_at": bson.M{"$lt": cursor.OccurredAt}},
			bson.M{"occurred_at": cursor.OccurredAt, "_id": bson.M{"$lt": cursor.HazardID}},
		}
	}

	cur, err := s.col.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}).
		SetLimit(int64(limit+1)))
	if err != nil {
		return nil, false, fmt.Errorf("find by type=%s: %w", hazardType, err)
	}
	defer cur.Close(ctx)

	out := make([]hazard.Event, 0, limit+1)
	for cur.Next(ctx) {
		var d document
		if err := cur.Decode(&d); err != nil {
			return nil, false, fmt.Errorf("decode document: %w", err)
		}
		out = append(out, fromDocument(d))
	}
	if err := cur.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	return out, hasMore, nil
}
