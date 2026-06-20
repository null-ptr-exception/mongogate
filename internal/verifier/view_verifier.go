package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

// VerifyViews checks that view definitions match between source and target.
func VerifyViews(ctx context.Context, src, tgt *mongo.Client,
	dbName string, rpt *report.Report) {

	srcViews := getViews(ctx, src, dbName)
	tgtViews := getViews(ctx, tgt, dbName)

	if len(srcViews) == 0 && len(tgtViews) == 0 {
		return
	}

	fmt.Printf("\n👁️  [Phase1] Verifying views [%s]\n", dbName)
	var errors []string

	for name, srcDef := range srcViews {
		tgtDef, ok := tgtViews[name]
		if !ok {
			errors = append(errors, fmt.Sprintf("❌ Missing view: %s.%s", dbName, name))
			continue
		}
		// Compare viewOn (source collection)
		if fmt.Sprintf("%v", srcDef["viewOn"]) != fmt.Sprintf("%v", tgtDef["viewOn"]) {
			errors = append(errors,
				fmt.Sprintf("⚠️  View [%s.%s] viewOn differs: src=%v tgt=%v",
					dbName, name, srcDef["viewOn"], tgtDef["viewOn"]))
		}
		// Compare pipeline
		srcPipeline := fmt.Sprintf("%v", srcDef["pipeline"])
		tgtPipeline := fmt.Sprintf("%v", tgtDef["pipeline"])
		if srcPipeline != tgtPipeline {
			errors = append(errors,
				fmt.Sprintf("⚠️  View [%s.%s] pipeline differs", dbName, name))
		}
		// Compare collation
		if fmt.Sprintf("%v", srcDef["collation"]) != fmt.Sprintf("%v", tgtDef["collation"]) {
			errors = append(errors,
				fmt.Sprintf("⚠️  View [%s.%s] collation differs", dbName, name))
		}
	}
	for name := range tgtViews {
		if _, ok := srcViews[name]; !ok {
			errors = append(errors, fmt.Sprintf("⚠️  Extra view in target: %s.%s", dbName, name))
		}
	}

	ns := dbName + "._views"
	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetView(ns, res)
	printStatus(fmt.Sprintf("Views [%s]", dbName), res.Passed,
		fmt.Sprintf("%d views", len(srcViews)))
}

func getViews(ctx context.Context, client *mongo.Client, dbName string) map[string]bson.M {
	cur, err := client.Database(dbName).ListCollections(ctx,
		bson.M{"type": "view"})
	if err != nil {
		return nil
	}
	defer cur.Close(ctx)
	views := make(map[string]bson.M)
	for cur.Next(ctx) {
		var doc bson.M
		_ = cur.Decode(&doc)
		name, _ := doc["name"].(string)
		opts, _ := doc["options"].(bson.M)
		views[name] = opts
	}
	return views
}
