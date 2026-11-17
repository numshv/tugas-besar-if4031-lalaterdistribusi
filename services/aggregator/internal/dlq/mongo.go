package dlq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	col *mongo.Collection
}

func NewMongoStore(client *mongo.Client, dbName string) *MongoStore {
	return &MongoStore{col: client.Database(dbName).Collection("unprocessed")}
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "next_attempt_at", Value: 1}}, Options: options.Index().SetName("status_next_attempt")},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "received_at", Value: 1}, {Key: "_id", Value: 1}}, Options: options.Index().SetName("status_received")},
		{Keys: bson.D{{Key: "source", Value: 1}, {Key: "status", Value: 1}, {Key: "received_at", Value: 1}, {Key: "_id", Value: 1}}, Options: options.Index().SetName("source_status_received")},
	})
	if err != nil {
		return fmt.Errorf("create unprocessed indexes: %w", err)
	}
	return nil
}

func (s *MongoStore) Put(ctx context.Context, m Message) (bool, error) {
	res, err := s.col.UpdateOne(ctx, bson.M{"_id": m.ID}, bson.M{"$setOnInsert": m}, options.Update().SetUpsert(true))
	if err != nil {
		return false, fmt.Errorf("put message %s: %w", m.ID, err)
	}
	return res.UpsertedCount == 1, nil
}

func (s *MongoStore) Get(ctx context.Context, id string) (Message, error) {
	var m Message
	err := s.col.FindOne(ctx, bson.M{"_id": id}).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return m, ErrNotFound
	}
	return m, err
}

func (s *MongoStore) claim(ctx context.Context, filter bson.M, set bson.M, sort bson.D) (*Message, error) {
	var m Message
	err := s.col.FindOneAndUpdate(ctx, filter, bson.M{"$set": set},
		options.FindOneAndUpdate().SetSort(sort).SetReturnDocument(options.After)).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *MongoStore) ClaimDue(ctx context.Context, now time.Time) (*Message, error) {
	return s.claim(ctx,
		bson.M{"status": StatusRetrying, "next_attempt_at": bson.M{"$lte": now}},
		bson.M{"next_attempt_at": now.Add(ClaimLease), "claimed_at": now},
		bson.D{{Key: "next_attempt_at", Value: 1}})
}

func (s *MongoStore) ClaimParked(ctx context.Context, source string, passStart, now time.Time) (*Message, error) {
	return s.claim(ctx,
		bson.M{"source": source, "$or": bson.A{
			bson.M{"status": StatusParked, "updated_at": bson.M{"$lt": passStart}},
			bson.M{"status": StatusReplaying, "claimed_at": bson.M{"$lt": now.Add(-ReplayStale)}},
		}},
		bson.M{"status": StatusReplaying, "claimed_at": now},
		bson.D{{Key: "received_at", Value: 1}, {Key: "_id", Value: 1}})
}

func (s *MongoStore) MarkRecovered(ctx context.Context, id string, at time.Time) error {
	_, err := s.col.UpdateByID(ctx, id, bson.M{"$set": bson.M{
		"status": StatusRecovered, "recovered_at": at, "updated_at": at,
	}})
	return err
}

func (s *MongoStore) MarkFailed(ctx context.Context, id, reason string, retryable bool, status Status, attempts int, next, now time.Time) error {
	_, err := s.col.UpdateByID(ctx, id, bson.M{"$set": bson.M{
		"reason": reason, "retryable": retryable, "status": status,
		"attempts": attempts, "next_attempt_at": next, "updated_at": now,
	}})
	return err
}

func (s *MongoStore) List(ctx context.Context, source string, after *Cursor, limit int) ([]Message, error) {
	filter := bson.M{"status": bson.M{"$in": bson.A{StatusRetrying, StatusParked, StatusReplaying}}}
	if source != "" {
		filter["source"] = source
	}
	if after != nil {
		filter["$or"] = bson.A{
			bson.M{"received_at": bson.M{"$gt": after.ReceivedAt}},
			bson.M{"received_at": after.ReceivedAt, "_id": bson.M{"$gt": after.ID}},
		}
	}
	cur, err := s.col.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "received_at", Value: 1}, {Key: "_id", Value: 1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"payload": 0}))
	if err != nil {
		return nil, fmt.Errorf("list unprocessed: %w", err)
	}
	defer cur.Close(ctx)
	var out []Message
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
