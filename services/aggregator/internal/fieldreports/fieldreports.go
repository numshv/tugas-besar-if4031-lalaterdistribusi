// Delivery is at-least-once (the offset is committed only after the report is
// stored) and storing is idempotent (report_id is the document id and is only
// written on insert), so every accepted report is processed exactly once in
// effect. Redeliveries only bump a counter.
package fieldreports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/fieldreport"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/dlq"
)

var storedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "field_reports_stored_total",
	Help: "Field report deliveries handled by the aggregator, by result (stored, redelivered, conflict, parked).",
}, []string{"result"})

const Source = "FIELD_REPORT"

type Store interface {
	// Save inserts r unless it exists, and counts the delivery either way.
	// When it already existed, it returns the stored content hash.
	Save(ctx context.Context, r fieldreport.Report, hash string, now time.Time) (inserted bool, storedHash string, err error)
	Get(ctx context.Context, id string) (map[string]any, bool, error)
}

type Consumer struct {
	Store Store
	DLQ   dlq.Store
	Now   func() time.Time
}

// Handle is a kafkax.Handler. Returning an error makes RunNoDrop retry the
// same message; it never skips one.
func (c *Consumer) Handle(ctx context.Context, log *slog.Logger, m kafka.Message) error {
	now := c.Now().UTC()
	var r fieldreport.Report
	err := json.Unmarshal(m.Value, &r)
	switch {
	case err != nil:
		err = errors.New("message is not a field report")
	case r.ReportID == "" || r.ReportID != string(m.Key):
		err = errors.New("report_id missing or different from the message key")
	default:
		err = r.Validate()
	}
	if err != nil {
		return c.park(ctx, log, m, now, err)
	}

	hash := r.Hash()
	inserted, storedHash, err := c.Store.Save(ctx, r, hash, now)
	if err != nil {
		return fmt.Errorf("store field report %s: %w", r.ReportID, err)
	}
	switch {
	case inserted:
		storedTotal.WithLabelValues("stored").Inc()
		log.Info("field_report_processed", "report_id", r.ReportID, "reporter", r.Reporter,
			"queue_ms", now.Sub(r.AcceptedAt).Milliseconds())
	case storedHash != hash:
		storedTotal.WithLabelValues("conflict").Inc()
		log.Warn("idempotency_conflict", "report_id", r.ReportID,
			"detail", "same key, different content; the first report is kept")
	default:
		storedTotal.WithLabelValues("redelivered").Inc()
		log.Info("field_report_redelivered", "report_id", r.ReportID)
	}
	return nil
}

func (c *Consumer) park(ctx context.Context, log *slog.Logger, m kafka.Message, now time.Time, cause error) error {
	msg := dlq.Message{
		ID: dlq.NewID(Source, dlq.KindRecord, m.Value), Source: Source, Kind: dlq.KindRecord,
		RefHint: string(m.Key), Payload: m.Value, ReceivedAt: now, Reason: "schema_incompatible: " + cause.Error(),
		Attempts: 1, Status: dlq.StatusParked, UpdatedAt: now, CorrelationID: correlation.FromContext(ctx),
	}
	inserted, err := c.DLQ.Put(ctx, msg)
	if err != nil {
		return fmt.Errorf("park field report: %w", err)
	}
	if inserted {
		dlq.Observe(Source, string(dlq.StatusParked))
	}
	storedTotal.WithLabelValues("parked").Inc()
	log.Warn("field_report_parked", "message_id", msg.ID, "reason", msg.Reason)
	return nil
}

type MongoStore struct{ col *mongo.Collection }

func NewMongoStore(client *mongo.Client, db string) *MongoStore {
	return &MongoStore{col: client.Database(db).Collection("field_reports")}
}

func (s *MongoStore) Save(ctx context.Context, r fieldreport.Report, hash string, now time.Time) (bool, string, error) {
	doc := bson.M{
		"reporter": r.Reporter, "scope": r.Scope, "hazard_type": r.HazardType,
		"latitude": r.Latitude, "longitude": r.Longitude, "observed_at": r.ObservedAt,
		"description": r.Description, "area_name": r.AreaName, "severity_estimate": r.SeverityEstimate,
		"accepted_at": r.AcceptedAt, "processed_at": now, "payload_hash": hash,
	}
	res, err := s.col.UpdateOne(ctx, bson.M{"_id": r.ReportID}, bson.M{
		"$setOnInsert": doc,
		"$inc":         bson.M{"deliveries": 1},
		"$set":         bson.M{"last_delivered_at": now},
	}, options.Update().SetUpsert(true))
	if err != nil {
		return false, "", err
	}
	if res.UpsertedCount == 1 {
		return true, hash, nil
	}
	var existing struct {
		Hash string `bson:"payload_hash"`
	}
	if err := s.col.FindOne(ctx, bson.M{"_id": r.ReportID}).Decode(&existing); err != nil {
		return false, "", err
	}
	return false, existing.Hash, nil
}

func (s *MongoStore) Get(ctx context.Context, id string) (map[string]any, bool, error) {
	var d struct {
		Reporter    string    `bson:"reporter"`
		HazardType  string    `bson:"hazard_type"`
		AcceptedAt  time.Time `bson:"accepted_at"`
		ProcessedAt time.Time `bson:"processed_at"`
		Deliveries  int       `bson:"deliveries"`
	}
	err := s.col.FindOne(ctx, bson.M{"_id": id}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return map[string]any{
		"report_id": id, "reporter": d.Reporter, "hazard_type": d.HazardType,
		"accepted_at": d.AcceptedAt.UTC().Format(time.RFC3339), "processed_at": d.ProcessedAt.UTC().Format(time.RFC3339),
		"deliveries": d.Deliveries,
	}, true, nil
}
