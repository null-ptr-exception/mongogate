package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

func VerifyGridFS(ctx context.Context, src, tgt *mongo.Client,
	dbName string, fullVerify bool, rpt *report.Report) {

	fmt.Printf("\n🗂️  [Phase] Verifying GridFS [%s]\n", dbName)
	var errors []string

	srcFiles := getGridFSFiles(ctx, src, dbName)
	tgtFiles := getGridFSFiles(ctx, tgt, dbName)

	for id, sf := range srcFiles {
		tf, ok := tgtFiles[id]
		if !ok {
			errors = append(errors,
				fmt.Sprintf("❌ Missing GridFS file: %s (%s)", sf["filename"], id))
			continue
		}
		// metadata comparison
		if fmt.Sprintf("%v", sf["length"]) != fmt.Sprintf("%v", tf["length"]) {
			errors = append(errors,
				fmt.Sprintf("❌ [%s] size differs: src=%v tgt=%v",
					sf["filename"], sf["length"], tf["length"]))
		}
		// MD5 (only checked in Phase 3 to avoid the extra cost)
		if fullVerify {
			if fmt.Sprintf("%v", sf["md5"]) != fmt.Sprintf("%v", tf["md5"]) {
				errors = append(errors,
					fmt.Sprintf("❌ [%s] MD5 differs", sf["filename"]))
			}
		}
		if fmt.Sprintf("%v", sf["filename"]) != fmt.Sprintf("%v", tf["filename"]) {
			errors = append(errors,
				fmt.Sprintf("⚠️  filename differs: src=%v tgt=%v",
					sf["filename"], tf["filename"]))
		}
	}
	for id, tf := range tgtFiles {
		if _, ok := srcFiles[id]; !ok {
			errors = append(errors,
				fmt.Sprintf("⚠️  Extra GridFS file in target: %s (%s)", tf["filename"], id))
		}
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetGridFS(dbName, res)
	printStatus(fmt.Sprintf("GridFS [%s]", dbName), res.Passed,
		fmt.Sprintf("src=%d tgt=%d files", len(srcFiles), len(tgtFiles)))
}

func getGridFSFiles(ctx context.Context, client *mongo.Client,
	dbName string) map[string]bson.M {

	cur, err := client.Database(dbName).Collection("fs.files").
		Find(ctx, bson.M{})
	if err != nil { return nil }
	defer cur.Close(ctx)

	files := make(map[string]bson.M)
	for cur.Next(ctx) {
		var doc bson.M
		_ = cur.Decode(&doc)
		id := fmt.Sprintf("%v", doc["_id"])
		files[id] = doc
	}
	return files
}
