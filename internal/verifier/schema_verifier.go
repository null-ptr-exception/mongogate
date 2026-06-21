package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

// VerifyDatabases returns the list of databases present on both sides. dbInScope,
// when non-nil, narrows which databases are even considered (driven by
// --include-ns/--exclude-ns) so a scoped run doesn't report missing/extra
// databases outside the requested scope.
func VerifyDatabases(ctx context.Context, src, tgt *mongo.Client,
	skipDBs []string, dbInScope func(string) bool, rpt *report.Report) []string {

	fmt.Println("\n🗄️  [Phase1] Verifying database list")
	var errors []string

	srcDBs := listDBs(ctx, src, skipDBs, dbInScope)
	tgtDBs := listDBs(ctx, tgt, skipDBs, dbInScope)

	srcSet := toSet(srcDBs)
	tgtSet := toSet(tgtDBs)

	var common []string
	for db := range srcSet {
		if !tgtSet[db] {
			errors = append(errors, fmt.Sprintf("❌ Missing DB in target: %s", db))
		} else {
			common = append(common, db)
		}
	}
	for db := range tgtSet {
		if !srcSet[db] {
			errors = append(errors, fmt.Sprintf("⚠️  Extra DB in target: %s", db))
		}
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetDatabases(res)
	printStatus("Database list", res.Passed,
		fmt.Sprintf("src=%d tgt=%d common=%d", len(srcDBs), len(tgtDBs), len(common)))
	return common
}

// VerifyCollections returns the list of collections present on both sides.
// nsFilter, when non-nil, narrows which collections are considered (driven by
// --include-ns/--exclude-ns), so collections outside the requested scope are
// never compared and never reported as missing/extra.
func VerifyCollections(ctx context.Context, src, tgt *mongo.Client,
	dbName string, nsFilter func(string, string) bool, rpt *report.Report) []string {

	fmt.Printf("\n📁 [Phase1] Verifying [%s] collections\n", dbName)
	var errors []string

	inScope := func(col string) bool { return nsFilter == nil || nsFilter(dbName, col) }
	srcCols := filterStrings(listCollections(ctx, src, dbName), inScope)
	tgtCols := filterStrings(listCollections(ctx, tgt, dbName), inScope)
	srcSet := toSet(srcCols)
	tgtSet := toSet(tgtCols)

	var common []string
	for col := range srcSet {
		if !tgtSet[col] {
			errors = append(errors, fmt.Sprintf("❌ Missing collection: %s.%s", dbName, col))
		} else {
			common = append(common, col)
		}
	}
	for col := range tgtSet {
		if !srcSet[col] {
			errors = append(errors, fmt.Sprintf("⚠️  Extra collection: %s.%s", dbName, col))
		}
	}

	// Schema / options verification
	for _, col := range common {
		colErrors := verifyCollectionOptions(ctx, src, tgt, dbName, col)
		errors = append(errors, colErrors...)
	}

	ns := dbName + ".*"
	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetCollection(ns, res)
	printStatus(fmt.Sprintf("  %s collections", dbName), res.Passed,
		fmt.Sprintf("%d collections", len(common)))
	return common
}

func verifyCollectionOptions(ctx context.Context, src, tgt *mongo.Client,
	dbName, colName string) []string {

	ns := dbName + "." + colName
	srcOpts := getCollectionOptions(ctx, src, dbName, colName)
	tgtOpts := getCollectionOptions(ctx, tgt, dbName, colName)

	optKeys := []string{
		"capped", "size", "max",
		"validator", "validationLevel", "validationAction",
		"collation",
		"timeseries", "expireAfterSeconds",
		"changeStreamPreAndPostImages",
		"clusteredIndex",
	}
	return compareBSONFields(fmt.Sprintf("[%s]", ns), optKeys, srcOpts, tgtOpts)
}

func getCollectionOptions(ctx context.Context, client *mongo.Client,
	dbName, colName string) bson.M {

	cur, err := client.Database(dbName).ListCollections(ctx,
		bson.M{"name": colName})
	if err != nil {
		return bson.M{}
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		var doc bson.M
		_ = cur.Decode(&doc)
		if opts, ok := doc["options"].(bson.M); ok {
			return opts
		}
	}
	return bson.M{}
}

func listDBs(ctx context.Context, client *mongo.Client, skip []string, dbInScope func(string) bool) []string {
	names, _ := client.ListDatabaseNames(ctx, bson.M{})
	skipSet := toSet(skip)
	var result []string
	for _, n := range names {
		if skipSet[n] {
			continue
		}
		if dbInScope != nil && !dbInScope(n) {
			continue
		}
		result = append(result, n)
	}
	return result
}

func listCollections(ctx context.Context, client *mongo.Client, dbName string) []string {
	names, _ := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
	return names
}

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func filterStrings(ss []string, keep func(string) bool) []string {
	var out []string
	for _, s := range ss {
		if keep(s) {
			out = append(out, s)
		}
	}
	return out
}

func printStatus(label string, passed bool, detail string) {
	status := "✅ PASS"
	if !passed {
		status = "❌ FAIL"
	}
	if detail != "" {
		fmt.Printf("  %s %s (%s)\n", status, label, detail)
	} else {
		fmt.Printf("  %s %s\n", status, label)
	}
}
