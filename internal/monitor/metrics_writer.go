package monitor

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	monitorDB      = "migration_monitor"
	metricsCol     = "verify_metrics"
	eventsCol      = "verify_events"
	ttlSeconds     = 60 * 60 * 24 * 7 // 7 days
)

type MetricsWriter struct {
	metrics *mongo.Collection
	events  *mongo.Collection
}

type Metric struct {
	Timestamp    time.Time   `bson:"timestamp"`
	Metadata     bson.M      `bson:"metadata"`
	NS           string      `bson:"ns"`
	Phase        string      `bson:"phase"`
	Processed    int64       `bson:"processed_docs"`
	Total        int64       `bson:"total_docs"`
	ProgressPct  float64     `bson:"progress_pct"`
	Missing      int         `bson:"missing_count"`
	Different    int         `bson:"different_count"`
	SrcCount     int64       `bson:"src_count"`
	TgtCount     int64       `bson:"tgt_count"`
	CountDiff    int64       `bson:"count_diff"`
	IsExact      bool        `bson:"is_exact"`
	DurationSecs float64     `bson:"duration_seconds"`
}

type Event struct {
	CreatedAt time.Time `bson:"created_at"`
	JobID     string    `bson:"job_id"`
	Level     string    `bson:"level"` // INFO / WARN / ERROR
	Message   string    `bson:"message"`
}

func NewMetricsWriter(client *mongo.Client) (*MetricsWriter, error) {
	db := client.Database(monitorDB)
	ctx := context.Background()

	// Create the Time Series collection if it doesn't already exist.
	colNames, _ := db.ListCollectionNames(ctx, bson.M{})
	hasMetrics := false
	hasEvents := false
	for _, n := range colNames {
		if n == metricsCol { hasMetrics = true }
		if n == eventsCol  { hasEvents = true }
	}

	if !hasMetrics {
		tsOpts := options.CreateCollection().
			SetTimeSeriesOptions(
				options.TimeSeries().
					SetTimeField("timestamp").
					SetMetaField("metadata").
					SetGranularity("seconds"),
			).
			SetExpireAfterSeconds(int64(ttlSeconds))
		if err := db.CreateCollection(ctx, metricsCol, tsOpts); err != nil {
			fmt.Printf("[monitor] failed to create time series collection (may already exist): %v\n", err)
		}
	}

	// Create the events collection + TTL index.
	if !hasEvents {
		_ = db.CreateCollection(ctx, eventsCol)
		evCol := db.Collection(eventsCol)
		_, _ = evCol.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "created_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(int32(ttlSeconds * 4)), // 30 days
		})
	}

	return &MetricsWriter{
		metrics: db.Collection(metricsCol),
		events:  db.Collection(eventsCol),
	}, nil
}

func (w *MetricsWriter) Write(m Metric) {
	if w == nil { return }
	m.Timestamp = time.Now().UTC()
	if m.Metadata == nil {
		m.Metadata = bson.M{"job": "migration_verify"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := w.metrics.InsertOne(ctx, m); err != nil {
		fmt.Printf("[monitor] failed to write metrics (verification unaffected): %v\n", err)
	}
}

func (w *MetricsWriter) LogEvent(jobID, level, message string) {
	if w == nil { return }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e := Event{
		CreatedAt: time.Now().UTC(),
		JobID:     jobID,
		Level:     level,
		Message:   message,
	}
	if _, err := w.events.InsertOne(ctx, e); err != nil {
		fmt.Printf("[monitor] failed to write event: %v\n", err)
	}
}
