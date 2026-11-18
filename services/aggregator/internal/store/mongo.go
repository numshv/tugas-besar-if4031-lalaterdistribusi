package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

const (
	writeTimeout = 5 * time.Second
	readTimeout  = 3 * time.Second
)

type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database
	col    *mongo.Collection
	fences *mongo.Collection
	now    func() time.Time
	health Health
	mode   PartialMode

	experiments bool
	expByArea   *mongo.Collection
	expByIngest *mongo.Collection
}

func NewMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOpts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		SetMaxPoolSize(50).
		SetRetryWrites(true).
		SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true})

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
	collOpts := options.Collection().
		SetReadPreference(readpref.PrimaryPreferred()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority", WTimeout: writeTimeout})
	db := client.Database(dbName)
	return &MongoStore{
		client:      client,
		db:          db,
		col:         db.Collection("hazard_events", collOpts),
		fences:      db.Collection("fences", collOpts),
		expByArea:   db.Collection("exp_by_area", collOpts),
		expByIngest: db.Collection("exp_by_ingested", collOpts),
		now:         time.Now,
		mode:        PartialLiteral,
	}
}

func (s *MongoStore) WithExperiments(on bool) *MongoStore {
	s.experiments = on
	return s
}

func (s *MongoStore) shadow(ctx context.Context, events ...hazard.Event) {
	if !s.experiments {
		return
	}
	for _, e := range events {
		doc := contentFields(e)
		doc["_id"] = e.HazardID
		doc["ingested_at"] = e.IngestedAt
		targets := []struct {
			col    *mongo.Collection
			filter bson.M
		}{
			{s.expByArea, bson.M{"_id": e.HazardID, "area_name": e.AreaName}},
			{s.expByIngest, bson.M{"_id": e.HazardID, "ingested_at": e.IngestedAt}},
		}
		for _, t := range targets {
			if _, err := t.col.ReplaceOne(ctx, t.filter, doc, options.Replace().SetUpsert(true)); err != nil {
				experimentWriteErrors.WithLabelValues(t.col.Name()).Inc()
			}
		}
	}
}

func (s *MongoStore) Purge(ctx context.Context, source hazard.Source, sourceRefID string) (bool, error) {
	if err := validatePurge(source, sourceRefID); err != nil {
		return false, err
	}
	defer observe("purge", time.Now())
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	id := hazard.NewID(source, sourceRefID)
	res, err := s.col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return false, fmt.Errorf("purge hazard_id=%s: %w", id, err)
	}
	for _, c := range []*mongo.Collection{s.expByArea, s.expByIngest} {
		if _, err := c.DeleteMany(ctx, bson.M{"_id": id}); err != nil {
			experimentWriteErrors.WithLabelValues(c.Name()).Inc()
		}
	}
	return res.DeletedCount > 0, nil
}

func (s *MongoStore) WithClock(now func() time.Time) *MongoStore {
	s.now = now
	return s
}

func (s *MongoStore) WithHealth(h Health, mode PartialMode) *MongoStore {
	s.health = h
	s.mode = mode
	return s
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
	err = s.fences.Database().CreateCollection(ctx, s.fences.Name())
	var cmdErr mongo.CommandError
	if err != nil && !(errors.As(err, &cmdErr) && cmdErr.Name == "NamespaceExists") {
		return fmt.Errorf("create fences collection: %w", err)
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
	Version     int64          `bson:"version"`
	Retracted   bool           `bson:"retracted"`
}

func contentFields(e hazard.Event) bson.M {
	attrs := e.Attributes
	if attrs == nil {
		attrs = map[string]any{} // jangan simpan null, konsisten selalu objek
	}
	return bson.M{
		"source":        string(e.Source),
		"source_ref_id": e.SourceRefID,
		"hazard_type":   string(e.HazardType),
		"severity":      string(e.Severity),
		"area_name":     e.AreaName,
		"latitude":      e.Latitude,
		"longitude":     e.Longitude,
		"occurred_at":   e.OccurredAt,
		"attributes":    attrs,
		"version":       e.Version,
		"retracted":     e.Retracted,
	}
}

func fromDocument(d document) hazard.Event {
	attrs := d.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}
	return hazard.Event{
		HazardID: d.ID, Source: hazard.Source(d.Source), SourceRefID: d.SourceRefID,
		HazardType: hazard.Type(d.HazardType), Severity: hazard.Severity(d.Severity),
		AreaName: d.AreaName, Latitude: d.Latitude, Longitude: d.Longitude,
		OccurredAt: d.OccurredAt.UTC(), IngestedAt: d.IngestedAt.UTC(), Attributes: attrs,
		Version: d.Version, Retracted: d.Retracted,
	}
}

func (s *MongoStore) Retract(ctx context.Context, source hazard.Source, sourceRefID string, version int64) (hazard.Event, bool, error) {
	if err := validateRetract(source, sourceRefID); err != nil {
		return hazard.Event{}, false, err
	}
	defer observe("retract", time.Now())
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	id := hazard.NewID(source, sourceRefID)
	filter := bson.M{
		"_id": id,
		"$or": bson.A{
			bson.M{"version": bson.M{"$lt": version}},
			bson.M{"version": bson.M{"$exists": false}},
		},
	}
	update := bson.M{
		"$set": bson.M{"retracted": true, "version": version},
		"$setOnInsert": bson.M{
			"source":        string(source),
			"source_ref_id": sourceRefID,
			"attributes":    bson.M{},
			"ingested_at":   ingestTime(s.now),
		},
	}
	var d document
	err := s.col.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&d)
	if mongo.IsDuplicateKeyError(err) {
		return hazard.Event{}, false, nil
	}
	if err != nil {
		return hazard.Event{}, false, fmt.Errorf("retract hazard_id=%s: %w", id, err)
	}
	stored := fromDocument(d)
	s.shadow(ctx, stored)
	return stored, true, nil
}

func (s *MongoStore) Upsert(ctx context.Context, e hazard.Event) (hazard.Event, bool, error) {
	if err := validateEvent(e); err != nil {
		return hazard.Event{}, false, err
	}
	defer observe("upsert", time.Now())
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	filter := bson.M{
		"_id": e.HazardID,
		"$or": bson.A{
			bson.M{"version": bson.M{"$lt": e.Version}},
			bson.M{"version": bson.M{"$exists": false}},
		},
	}
	update := bson.M{
		"$set":         contentFields(e),
		"$setOnInsert": bson.M{"ingested_at": ingestTime(s.now)},
	}
	var d document
	err := s.col.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&d)
	if mongo.IsDuplicateKeyError(err) {
		return hazard.Event{}, false, nil
	}
	if err != nil {
		return hazard.Event{}, false, fmt.Errorf("upsert hazard_id=%s: %w", e.HazardID, err)
	}
	stored := fromDocument(d)
	s.shadow(ctx, stored)
	return stored, true, nil
}

type fenceDocument struct {
	Epoch  int64  `bson:"epoch"`
	Cursor string `bson:"cursor"`
}

type storedVersion struct {
	ID         string    `bson:"_id"`
	Version    int64     `bson:"version"`
	IngestedAt time.Time `bson:"ingested_at"`
}

func (s *MongoStore) CommitPoll(ctx context.Context, source hazard.Source, epoch int64, cursor string, events []hazard.Event) ([]hazard.Event, error) {
	for _, e := range events {
		if err := validateEvent(e); err != nil {
			return nil, err
		}
	}
	defer observe("commit_poll", time.Now())
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	sess, err := s.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	txnOpts := options.Transaction().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())

	result, err := sess.WithTransaction(ctx, func(sc mongo.SessionContext) (any, error) {
		return s.commitPollTxn(sc, source, epoch, cursor, events)
	}, txnOpts)
	if err != nil {
		if errors.Is(err, ErrStaleEpoch) {
			return nil, err
		}
		return nil, fmt.Errorf("commit poll source=%s epoch=%d: %w", source, epoch, err)
	}
	applied := result.([]hazard.Event)
	s.shadow(ctx, applied...)
	return applied, nil
}

func (s *MongoStore) commitPollTxn(sc mongo.SessionContext, source hazard.Source, epoch int64, cursor string, events []hazard.Event) ([]hazard.Event, error) {
	var fence fenceDocument
	err := s.fences.FindOne(sc, bson.M{"_id": string(source)}).Decode(&fence)
	switch {
	case err == nil && fence.Epoch > epoch:
		return nil, fmt.Errorf("%w: source=%s stored=%d mine=%d", ErrStaleEpoch, source, fence.Epoch, epoch)
	case err != nil && !errors.Is(err, mongo.ErrNoDocuments):
		return nil, fmt.Errorf("read fence: %w", err)
	}

	now := ingestTime(s.now)
	_, err = s.fences.UpdateOne(sc,
		bson.M{"_id": string(source)},
		bson.M{"$set": bson.M{"epoch": epoch, "cursor": cursor, "updated_at": now}},
		options.Update().SetUpsert(true))
	if err != nil {
		return nil, fmt.Errorf("write fence: %w", err)
	}

	applied := make([]hazard.Event, 0, len(events))
	if len(events) == 0 {
		return applied, nil
	}

	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.HazardID)
	}
	cur, err := s.col.Find(sc, bson.M{"_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"version": 1, "ingested_at": 1}))
	if err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	var existing []storedVersion
	if err := cur.All(sc, &existing); err != nil {
		return nil, fmt.Errorf("decode versions: %w", err)
	}
	known := make(map[string]storedVersion, len(existing))
	for _, v := range existing {
		known[v.ID] = v
	}

	for _, e := range events {
		prev, exists := known[e.HazardID]
		if exists && e.Version <= prev.Version {
			continue
		}
		ingestedAt := now
		if exists {
			ingestedAt = prev.IngestedAt.UTC()
		}
		_, err := s.col.UpdateOne(sc,
			bson.M{"_id": e.HazardID},
			bson.M{"$set": contentFields(e), "$setOnInsert": bson.M{"ingested_at": ingestedAt}},
			options.Update().SetUpsert(true))
		if err != nil {
			return nil, fmt.Errorf("write hazard_id=%s: %w", e.HazardID, err)
		}
		known[e.HazardID] = storedVersion{ID: e.HazardID, Version: e.Version, IngestedAt: ingestedAt}
		stored := copyEvent(e)
		stored.IngestedAt = ingestedAt
		applied = append(applied, stored)
	}
	return applied, nil
}

func (s *MongoStore) LoadCheckpoint(ctx context.Context, source hazard.Source) (string, int64, error) {
	defer observe("load_checkpoint", time.Now())
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	var fence fenceDocument
	err := s.fences.FindOne(ctx, bson.M{"_id": string(source)}).Decode(&fence)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("load checkpoint source=%s: %w", source, err)
	}
	return fence.Cursor, fence.Epoch, nil
}

func (s *MongoStore) Get(ctx context.Context, hazardID string) (hazard.Event, error) {
	defer observe("get", time.Now())
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	var d document
	err := s.col.FindOne(ctx, bson.M{"_id": hazardID}, options.FindOne().SetMaxTime(readTimeout)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return hazard.Event{}, ErrNotFound
	}
	if err != nil {
		return hazard.Event{}, fmt.Errorf("get hazard_id=%s: %w", hazardID, err)
	}
	return fromDocument(d), nil
}

func (s *MongoStore) ListLatest(ctx context.Context, q LatestQuery) (LatestPage, error) {
	after, err := validateLatest(q)
	if err != nil {
		return LatestPage{}, err
	}
	defer observe("list_latest", time.Now())
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	conds := bson.A{}
	if q.Type != "" {
		conds = append(conds, bson.M{"hazard_type": string(q.Type)})
	}
	if q.Source != "" {
		conds = append(conds, bson.M{"source": string(q.Source)})
	}
	if !q.Since.IsZero() {
		conds = append(conds, bson.M{"ingested_at": bson.M{"$gte": q.Since.UTC()}})
	}
	if !q.IncludeRetracted {
		conds = append(conds, bson.M{"retracted": bson.M{"$ne": true}})
	}
	op, dir := "$lt", -1
	if q.Ascending {
		op, dir = "$gt", 1
	}
	if after != nil {
		conds = append(conds, bson.M{"$or": bson.A{
			bson.M{"ingested_at": bson.M{op: after.at}},
			bson.M{"ingested_at": after.at, "_id": bson.M{op: after.id}},
		}})
	}
	filter := bson.M{}
	if len(conds) > 0 {
		filter["$and"] = conds
	}

	docs, mongosPartial, err := s.findPartial(ctx, filter, bson.D{{Key: "ingested_at", Value: dir}, {Key: "_id", Value: dir}}, q.Limit+1)
	if err != nil {
		return LatestPage{}, fmt.Errorf("list latest: %w", err)
	}
	items := make([]hazard.Event, 0, len(docs))
	for _, d := range docs {
		items = append(items, fromDocument(d))
	}
	page := LatestPage{Items: items}
	page.Partial, page.UnavailableUnits = partialStatus(s.health, s.mode, mongosPartial)
	if page.Partial {
		partialReads.WithLabelValues("list_latest").Inc()
	}
	if len(items) > q.Limit {
		page.Items = items[:q.Limit]
		page.NextCursor = encodeLatestCursor(q, page.Items[q.Limit-1])
	}
	return page, nil
}

const scanTimeout = 20 * time.Second

func (s *MongoStore) SourceRefIDs(ctx context.Context, source hazard.Source, includeRetracted bool) (RefIDs, error) {
	defer observe("source_ref_ids", time.Now())
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	filter := bson.M{}
	if source != "" {
		filter["source"] = string(source)
	}
	if !includeRetracted {
		filter["retracted"] = bson.M{"$ne": true}
	}
	cur, err := s.col.Find(ctx, filter, options.Find().
		SetProjection(bson.M{"_id": 0, "source_ref_id": 1}).
		SetAllowPartialResults(true).
		SetBatchSize(5000).
		SetMaxTime(scanTimeout))
	if err != nil {
		return RefIDs{}, fmt.Errorf("scan source_ref_id: %w", err)
	}
	defer cur.Close(ctx)

	ids := []string{}
	for cur.Next(ctx) {
		var row struct {
			SourceRefID string `bson:"source_ref_id"`
		}
		if err := cur.Decode(&row); err != nil {
			return RefIDs{}, fmt.Errorf("decode source_ref_id: %w", err)
		}
		ids = append(ids, row.SourceRefID)
	}
	if err := cur.Err(); err != nil {
		return RefIDs{}, fmt.Errorf("scan source_ref_id: %w", err)
	}
	out := RefIDs{IDs: ids}
	out.Partial, out.UnavailableUnits = partialStatus(s.health, s.mode, false)
	if out.Partial {
		partialReads.WithLabelValues("source_ref_ids").Inc()
	}
	return out, nil
}

var ErrNoShard = errors.New("no shard found in query plan")

func (s *MongoStore) ShardOf(ctx context.Context, hazardID string) (string, error) {
	defer observe("shard_of", time.Now())
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: s.col.Name()},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: hazardID}}},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}
	var plan bson.M
	if err := s.db.RunCommand(ctx, cmd).Decode(&plan); err != nil {
		return "", fmt.Errorf("explain hazard_id=%s: %w", hazardID, err)
	}
	if shard := findShardName(plan); shard != "" {
		return shard, nil
	}
	return "", ErrNoShard
}

func findShardName(v any) string {
	switch node := v.(type) {
	case bson.M:
		if name, ok := node["shardName"].(string); ok && name != "" {
			return name
		}
		for _, child := range node {
			if name := findShardName(child); name != "" {
				return name
			}
		}
	case bson.A:
		for _, child := range node {
			if name := findShardName(child); name != "" {
				return name
			}
		}
	}
	return ""
}

type findReply struct {
	Cursor struct {
		FirstBatch             []document `bson:"firstBatch"`
		PartialResultsReturned bool       `bson:"partialResultsReturned"`
	} `bson:"cursor"`
}

func (s *MongoStore) findPartial(ctx context.Context, filter bson.M, sort bson.D, limit int) ([]document, bool, error) {
	cmd := bson.D{
		{Key: "find", Value: s.col.Name()},
		{Key: "filter", Value: filter},
		{Key: "sort", Value: sort},
		{Key: "limit", Value: limit},
		{Key: "batchSize", Value: limit},
		{Key: "singleBatch", Value: true},
		{Key: "allowPartialResults", Value: true},
		{Key: "maxTimeMS", Value: readTimeout.Milliseconds()},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
	}
	var reply findReply
	err := s.db.RunCommand(ctx, cmd, options.RunCmd().SetReadPreference(readpref.PrimaryPreferred())).Decode(&reply)
	if err != nil {
		return nil, false, err
	}
	return reply.Cursor.FirstBatch, reply.Cursor.PartialResultsReturned, nil
}

func (s *MongoStore) ListByType(ctx context.Context, hazardType hazard.Type, since time.Time, cursor *hazard.Cursor, limit int) ([]hazard.Event, bool, error) {
	if limit < 1 || limit > hazard.MaxPageLimit {
		return nil, false, fmt.Errorf("limit must be between 1 and %d", hazard.MaxPageLimit)
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
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
