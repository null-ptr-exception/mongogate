package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

func VerifyIndexes(ctx context.Context, src, tgt *mongo.Client,
	dbName, colName string, rpt *report.Report) {

	ns := dbName + "." + colName
	srcIdxs := getIndexes(ctx, src, dbName, colName)
	tgtIdxs := getIndexes(ctx, tgt, dbName, colName)

	var errors []string

	// _id_ is no longer skipped: a clustered collection's defining
	// characteristic (unique/clustered flags) lives only on this index, so
	// skipping it by name silently passed plain-vs-clustered mismatches.
	for name, si := range srcIdxs {
		ti, ok := tgtIdxs[name]
		if !ok {
			errors = append(errors, fmt.Sprintf("❌ Missing index: %s", name))
			continue
		}
		errors = append(errors, compareIndex(name, si, ti)...)
	}
	for name := range tgtIdxs {
		if _, ok := srcIdxs[name]; !ok {
			errors = append(errors, fmt.Sprintf("⚠️  Extra index in target: %s", name))
		}
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetIndex(ns, res)
	if !res.Passed {
		fmt.Printf("  ❌ Index [%s]\n", ns)
		for _, e := range errors {
			fmt.Printf("     %s\n", e)
		}
	}
}

func compareIndex(name string, si, ti bson.M) []string {
	// Compare every attribute that matters
	fields := []string{
		"key",
		"unique",
		"clustered",
		"sparse",
		"hidden",
		"expireAfterSeconds",      // TTL
		"partialFilterExpression", // Partial
		"weights",                 // Text index
		"collation",
		"wildcardProjection",   // Wildcard
		"2dsphereIndexVersion", // Geo
		"default_language",     // Text
		"language_override",    // Text
	}
	return compareBSONFields(fmt.Sprintf("Index [%s]", name), fields, si, ti)
}

func getIndexes(ctx context.Context, client *mongo.Client,
	dbName, colName string) map[string]bson.M {

	cur, err := client.Database(dbName).Collection(colName).Indexes().List(ctx)
	if err != nil {
		return nil
	}
	defer cur.Close(ctx)
	result := make(map[string]bson.M)
	for cur.Next(ctx) {
		var idx bson.M
		_ = cur.Decode(&idx)
		if name, ok := idx["name"].(string); ok {
			result[name] = idx
		}
	}
	return result
}
