// loadgen is a test-only helper for the mongogate E2E suite. It is not part
// of the mongogate product. It seeds baseline structures, generates
// continuous write load, mirrors changes from source to target with
// configurable lag/loss (to stand in for a real CDC pipeline), and injects
// specific, deterministic mismatches so each DeepCompare issue type can be
// exercised on demand without waiting on randomness.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: loadgen <seed-baseline|seed-users|diverge|write|mirror> [flags]")
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "seed-baseline":
		cmdSeedBaseline(args)
	case "seed-users":
		cmdSeedUsers(args)
	case "diverge":
		cmdDiverge(args)
	case "write":
		cmdWrite(args)
	case "mirror":
		cmdMirror(args)
	case "mock-webhook":
		cmdMockWebhook(args)
	default:
		log.Fatalf("unknown subcommand: %s", cmd)
	}
}

func connect(uri string) *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("connect failed [%s]: %v", uri, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("ping failed [%s]: %v", uri, err)
	}
	return client
}

// ── seed-baseline ──────────────────────────────────────────────────────────
// Creates an identical set of structural fixtures (collections, options,
// every index type, a view, GridFS) on both source and target so Phase 1
// starts from a clean, matching baseline.

func cmdSeedBaseline(args []string) {
	fs := flag.NewFlagSet("seed-baseline", flag.ExitOnError)
	src := fs.String("src", "", "source mongo URI")
	tgt := fs.String("tgt", "", "target mongo URI")
	db := fs.String("db", "migtest", "database name")
	_ = fs.Parse(args)
	if *src == "" || *tgt == "" {
		log.Fatal("--src and --tgt are required")
	}

	ctx := context.Background()
	srcCli := connect(*src)
	tgtCli := connect(*tgt)

	for _, cli := range []*mongo.Client{srcCli, tgtCli} {
		seedBaselineOn(ctx, cli, *db)
	}
	fmt.Println("seed-baseline: done")
}

func seedBaselineOn(ctx context.Context, cli *mongo.Client, dbName string) {
	database := cli.Database(dbName)

	// Plain collection with a handful of documents.
	plain := database.Collection("plain_docs")
	_, _ = plain.DeleteMany(ctx, bson.M{})
	var docs []interface{}
	for i := 0; i < 20; i++ {
		docs = append(docs, bson.M{
			"_id": fmt.Sprintf("plain-%03d", i),
			"name": fmt.Sprintf("item-%d", i),
			"count": int32(i),
			"price": float64(i) + 0.5,
		})
	}
	_, _ = plain.InsertMany(ctx, docs)

	// Capped collection.
	_ = database.CreateCollection(ctx, "capped_col", options.CreateCollection().
		SetCapped(true).SetSizeInBytes(1048576).SetMaxDocuments(1000))

	// Validator.
	_ = database.CreateCollection(ctx, "validated_col", options.CreateCollection().
		SetValidator(bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": bson.A{"status"},
				"properties": bson.M{
					"status": bson.M{"enum": bson.A{"active", "inactive"}},
				},
			},
		}))

	// TTL index.
	ttlCol := database.Collection("ttl_col")
	_, _ = ttlCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "createdAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(3600),
	})

	// Partial index.
	partialCol := database.Collection("partial_idx_col")
	_, _ = partialCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}},
		Options: options.Index().
			SetPartialFilterExpression(bson.M{"status": "active"}),
	})

	// Text index.
	textCol := database.Collection("text_col")
	_, _ = textCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "content", Value: "text"}},
	})

	// 2dsphere (geo) index.
	geoCol := database.Collection("geo_col")
	_, _ = geoCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "loc", Value: "2dsphere"}},
	})

	// A view on top of plain_docs.
	_ = database.CreateView(ctx, "plain_docs_view", "plain_docs", mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"count": bson.M{"$gte": 10}}}},
	})

	// GridFS: one small file. Uses a fixed _id so source and target end up
	// with the exact same file identity, not just the same content - an
	// independent upload on each side would mint two different ObjectIDs
	// and always mismatch.
	gridFSFileID, _ := primitive.ObjectIDFromHex("000000000000000000000001")
	bucket, err := gridfs.NewBucket(database)
	if err == nil {
		filesCol := database.Collection("fs.files")
		n, _ := filesCol.CountDocuments(ctx, bson.M{"_id": gridFSFileID})
		if n == 0 {
			stream, err := bucket.OpenUploadStreamWithID(gridFSFileID, "hello.txt")
			if err == nil {
				_, _ = stream.Write([]byte("hello from mongogate e2e tests"))
				_ = stream.Close()
			}
		}
	}
}

// ── seed-users ──────────────────────────────────────────────────────────────
// Creates a baseline user/role on both clusters, with an optional deliberate
// mismatch for Phase 1 auth-verification testing.

func cmdSeedUsers(args []string) {
	fs := flag.NewFlagSet("seed-users", flag.ExitOnError)
	src := fs.String("src", "", "source mongo URI (must already be authenticated as root)")
	tgt := fs.String("tgt", "", "target mongo URI (must already be authenticated as root)")
	mismatch := fs.Bool("mismatch", false, "give the target user a different role set, to test Phase 1 auth diffing")
	_ = fs.Parse(args)
	if *src == "" || *tgt == "" {
		log.Fatal("--src and --tgt are required")
	}

	ctx := context.Background()
	srcCli := connect(*src)
	tgtCli := connect(*tgt)

	createUserIfAbsent(ctx, srcCli, "appuser", "apppass123", bson.A{
		bson.M{"role": "readWrite", "db": "migtest"},
	})

	tgtRoles := bson.A{bson.M{"role": "readWrite", "db": "migtest"}}
	if *mismatch {
		tgtRoles = bson.A{bson.M{"role": "read", "db": "migtest"}}
	}
	createUserIfAbsent(ctx, tgtCli, "appuser", "apppass123", tgtRoles)

	fmt.Println("seed-users: done")
}

func createUserIfAbsent(ctx context.Context, cli *mongo.Client, user, pass string, roles bson.A) {
	admin := cli.Database("admin")
	var res bson.M
	err := admin.RunCommand(ctx, bson.D{{Key: "usersInfo", Value: user}}).Decode(&res)
	if err == nil {
		if arr, ok := res["users"].(bson.A); ok && len(arr) > 0 {
			_ = admin.RunCommand(ctx, bson.D{{Key: "dropUser", Value: user}})
		}
	}
	err = admin.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: user},
		{Key: "pwd", Value: pass},
		{Key: "roles", Value: roles},
	}).Err()
	if err != nil {
		log.Printf("createUser %s failed: %v", user, err)
	}
}

// ── diverge ─────────────────────────────────────────────────────────────────
// Injects one specific, deterministic mismatch scenario directly into source
// and/or target, so every DeepCompare IssueType and bidirectional case can be
// tested without waiting on a real CDC pipeline to reproduce it.

func cmdDiverge(args []string) {
	fs := flag.NewFlagSet("diverge", flag.ExitOnError)
	src := fs.String("src", "", "source mongo URI")
	tgt := fs.String("tgt", "", "target mongo URI")
	db := fs.String("db", "migtest", "database name")
	col := fs.String("col", "diverge_col", "collection name")
	scenario := fs.String("scenario", "", "scenario name (see README)")
	count := fs.Int("count", 1, "number of documents to generate")
	_ = fs.Parse(args)
	if *src == "" || *tgt == "" || *scenario == "" {
		log.Fatal("--src, --tgt and --scenario are required")
	}

	ctx := context.Background()
	srcCli := connect(*src)
	tgtCli := connect(*tgt)
	srcCol := srcCli.Database(*db).Collection(*col)
	tgtCol := tgtCli.Database(*db).Collection(*col)

	for i := 0; i < *count; i++ {
		id := fmt.Sprintf("%s-%04d", *scenario, i)
		runScenario(ctx, *scenario, id, srcCol, tgtCol)
	}
	fmt.Printf("diverge: scenario=%s count=%d done\n", *scenario, *count)
}

func runScenario(ctx context.Context, scenario, id string, srcCol, tgtCol *mongo.Collection) {
	switch scenario {
	case "missing_doc":
		// exists only in source -> Phase 3 forward scan reports MISSING_DOC
		mustInsert(ctx, srcCol, bson.M{"_id": id, "val": "only-in-source"})

	case "extra_in_target":
		// exists only in target -> bidirectional scan reports ExtraInTarget
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "val": "only-in-target"})

	case "type_mismatch":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "val": int32(42)})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "val": int64(42)})

	case "value_diff":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "val": "expected"})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "val": "wrong"})

	case "missing_field":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "val": "x", "extra_in_src": "present"})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "val": "x"})

	case "extra_field":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "val": "x"})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "val": "x", "extra_in_tgt": "surprise"})

	case "objectid_degraded":
		oid := primitive.NewObjectID()
		mustInsert(ctx, srcCol, bson.M{"_id": id, "ref": oid})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "ref": oid.Hex()})

	case "datetime_tz":
		// Same instant, represented through two different tz offsets - a
		// stand-in for a CDC hop that re-serializes datetimes through a
		// local timezone. With normalize_datetime=true this should NOT be
		// reported as a diff; with it off, it should.
		instant := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
		taipei := instant.In(time.FixedZone("Asia/Taipei", 8*3600))
		mustInsert(ctx, srcCol, bson.M{"_id": id, "ts": primitive.NewDateTimeFromTime(instant)})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "ts": primitive.NewDateTimeFromTime(taipei)})

	case "float_precision":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "amount": 0.1 + 0.2})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "amount": 0.3})

	case "decimal128_trailing_zero":
		d1, _ := primitive.ParseDecimal128("123.4500")
		d2, _ := primitive.ParseDecimal128("123.45")
		mustInsert(ctx, srcCol, bson.M{"_id": id, "amount": d1})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "amount": d2})

	case "binary_subtype":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "data": primitive.Binary{Subtype: 0x00, Data: []byte("payload")}})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "data": primitive.Binary{Subtype: 0x05, Data: []byte("payload")}})

	case "array_order":
		mustInsert(ctx, srcCol, bson.M{"_id": id, "tags": bson.A{"a", "b", "c"}})
		mustInsert(ctx, tgtCol, bson.M{"_id": id, "tags": bson.A{"c", "a", "b"}})

	default:
		log.Fatalf("unknown scenario: %s", scenario)
	}
}

func mustInsert(ctx context.Context, col *mongo.Collection, doc bson.M) {
	_, err := col.InsertOne(ctx, doc)
	if err != nil {
		log.Fatalf("insert into %s.%s failed: %v", col.Database().Name(), col.Name(), err)
	}
}

// ── write ───────────────────────────────────────────────────────────────────
// Generates continuous insert/update/delete load against a single cluster,
// simulating an application writing to the source during a live migration.

func cmdWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	uri := fs.String("uri", "", "mongo URI to write to")
	db := fs.String("db", "migtest", "database name")
	col := fs.String("col", "live_col", "collection name")
	rate := fs.Int("rate", 20, "operations per second")
	duration := fs.Duration("duration", 30*time.Second, "how long to run")
	statsEvery := fs.Duration("stats-every", 5*time.Second, "stats print interval")
	_ = fs.Parse(args)
	if *uri == "" {
		log.Fatal("--uri is required")
	}

	cli := connect(*uri)
	collection := cli.Database(*db).Collection(*col)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	statsTicker := time.NewTicker(*statsEvery)
	defer statsTicker.Stop()

	var inserted, updated, deleted int64
	var liveIDs []string
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("write: done inserted=%d updated=%d deleted=%d\n", inserted, updated, deleted)
			return
		case <-statsTicker.C:
			fmt.Printf("write: inserted=%d updated=%d deleted=%d live=%d\n", inserted, updated, deleted, len(liveIDs))
		case <-ticker.C:
			op := rng.Intn(10)
			switch {
			case op < 6 || len(liveIDs) == 0: // 60% insert
				id := primitive.NewObjectID()
				_, err := collection.InsertOne(ctx, bson.M{
					"_id": id, "seq": inserted, "payload": fmt.Sprintf("v%d", inserted),
					"updatedAt": time.Now().UTC(),
				})
				if err == nil {
					inserted++
					liveIDs = append(liveIDs, id.Hex())
				}
			case op < 9: // 30% update
				idx := rng.Intn(len(liveIDs))
				oid, _ := primitive.ObjectIDFromHex(liveIDs[idx])
				_, err := collection.UpdateOne(ctx, bson.M{"_id": oid},
					bson.M{"$set": bson.M{"payload": fmt.Sprintf("updated-%d", updated), "updatedAt": time.Now().UTC()}})
				if err == nil {
					updated++
				}
			default: // 10% delete
				idx := rng.Intn(len(liveIDs))
				oid, _ := primitive.ObjectIDFromHex(liveIDs[idx])
				_, err := collection.DeleteOne(ctx, bson.M{"_id": oid})
				if err == nil {
					deleted++
					liveIDs = append(liveIDs[:idx], liveIDs[idx+1:]...)
				}
			}
		}
	}
}

// ── mirror ──────────────────────────────────────────────────────────────────
// Tails a change stream on source and replays each change to target after a
// configurable delay, optionally dropping a fraction of events. This stands
// in for a real CDC pipeline (mongosync/Debezium/etc.) with realistic,
// controllable lag and loss.

func cmdMirror(args []string) {
	fs := flag.NewFlagSet("mirror", flag.ExitOnError)
	src := fs.String("src", "", "source mongo URI")
	tgt := fs.String("tgt", "", "target mongo URI")
	db := fs.String("db", "migtest", "database name")
	col := fs.String("col", "live_col", "collection name")
	lag := fs.Duration("lag", 500*time.Millisecond, "delay before replaying each change to target")
	dropRate := fs.Float64("drop-rate", 0, "fraction of changes (0..1) to silently drop, simulating lossy CDC")
	duration := fs.Duration("duration", 60*time.Second, "how long to run")
	_ = fs.Parse(args)
	if *src == "" || *tgt == "" {
		log.Fatal("--src and --tgt are required")
	}

	srcCli := connect(*src)
	tgtCli := connect(*tgt)
	srcCol := srcCli.Database(*db).Collection(*col)
	tgtCol := tgtCli.Database(*db).Collection(*col)

	// The watch context is cancelled once `duration` elapses, which stops us
	// picking up *new* changes. Each replay below uses its own short-lived
	// context instead of this one, so draining the backlog already pulled
	// off the change stream isn't cut off mid-flight by the same deadline -
	// otherwise every queued replay fails at once the moment the watch
	// deadline passes, which is a loadgen bug, not a mongogate one.
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	timer := time.AfterFunc(*duration, cancelWatch)
	defer timer.Stop()
	defer cancelWatch()

	stream, err := srcCol.Watch(watchCtx, mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		log.Fatalf("watch failed: %v", err)
	}
	defer stream.Close(context.Background())

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var replayed, dropped int64

	fmt.Println("mirror: watching change stream...")
	for stream.Next(watchCtx) {
		var event bson.M
		if err := stream.Decode(&event); err != nil {
			continue
		}
		if rng.Float64() < *dropRate {
			dropped++
			continue
		}
		time.Sleep(*lag)
		replayCtx, cancelReplay := context.WithTimeout(context.Background(), 10*time.Second)
		err := replayEvent(replayCtx, tgtCol, event)
		cancelReplay()
		if err != nil {
			log.Printf("mirror: replay failed: %v", err)
			continue
		}
		replayed++
		if (replayed+dropped)%50 == 0 {
			fmt.Printf("mirror: replayed=%d dropped=%d\n", replayed, dropped)
		}
	}
	if err := stream.Err(); err != nil && watchCtx.Err() == nil {
		log.Printf("mirror: change stream error: %v", err)
	}
	fmt.Printf("mirror: done replayed=%d dropped=%d\n", replayed, dropped)
}

func replayEvent(ctx context.Context, tgtCol *mongo.Collection, event bson.M) error {
	opType, _ := event["operationType"].(string)
	docKey, _ := event["documentKey"].(bson.M)

	switch opType {
	case "insert", "replace":
		fullDoc, _ := event["fullDocument"].(bson.M)
		if fullDoc == nil {
			return nil
		}
		_, err := tgtCol.ReplaceOne(ctx, bson.M{"_id": docKey["_id"]}, fullDoc, options.Replace().SetUpsert(true))
		return err
	case "update":
		fullDoc, _ := event["fullDocument"].(bson.M)
		if fullDoc == nil {
			return nil
		}
		_, err := tgtCol.ReplaceOne(ctx, bson.M{"_id": docKey["_id"]}, fullDoc, options.Replace().SetUpsert(true))
		return err
	case "delete":
		_, err := tgtCol.DeleteOne(ctx, bson.M{"_id": docKey["_id"]})
		return err
	default:
		return nil
	}
}

// ── mock-webhook ────────────────────────────────────────────────────────────
// A minimal stand-in for a Slack incoming webhook: logs every POST body it
// receives, so the E2E tests can inspect exactly what mongogate's alert
// manager actually sent (payload shape, de-dupe behavior) without needing a
// real Slack workspace.

func cmdMockWebhook(args []string) {
	fs := flag.NewFlagSet("mock-webhook", flag.ExitOnError)
	port := fs.Int("port", 8090, "port to listen on")
	_ = fs.Parse(args)

	var received int
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received++
		fmt.Printf("mock-webhook: received #%d: %s\n", received, string(body))
		w.WriteHeader(200)
	})
	fmt.Printf("mock-webhook: listening on :%d\n", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
