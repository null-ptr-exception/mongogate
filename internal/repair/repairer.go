package repair

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

type Repairer struct {
	src *mongo.Client
	tgt *mongo.Client
}

type RepairResult struct {
	NS          string
	Fixed       int
	Failed      int
	Errors      []string
}

func New(src, tgt *mongo.Client) *Repairer {
	return &Repairer{src: src, tgt: tgt}
}

// RepairAll fixes every diff found: missing docs are copied over, different
// docs are overwritten from source.
func (r *Repairer) RepairAll(ctx context.Context, rpt *report.Report) []RepairResult {
	fmt.Println("\n🔧 Starting auto-repair...")
	var results []RepairResult

	for ns, data := range rpt.GetData() {
		if data.Passed {
			continue
		}
		result := r.repairCollection(ctx, ns, data)
		results = append(results, result)
		status := "✅"
		if result.Failed > 0 {
			status = "⚠️ "
		}
		fmt.Printf("  %s %-45s fixed=%d failed=%d\n",
			status, ns, result.Fixed, result.Failed)
	}
	return results
}

func (r *Repairer) repairCollection(ctx context.Context,
	ns string, data *report.DataResult) RepairResult {

	result := RepairResult{NS: ns}
	dbName, colName := splitNS(ns)
	srcCol := r.src.Database(dbName).Collection(colName)
	tgtCol := r.tgt.Database(dbName).Collection(colName)

	// Repair missing docs
	for _, idStr := range data.MissingSample {
		if err := r.repairDoc(ctx, srcCol, tgtCol, idStr); err != nil {
			result.Failed++
			result.Errors = append(result.Errors,
				fmt.Sprintf("MISSING repair [%s]: %v", idStr, err))
		} else {
			result.Fixed++
		}
	}

	// Repair different docs (overwrite target from source)
	for _, sample := range data.DiffSample {
		idStr := extractID(sample)
		if idStr == "" {
			continue
		}
		if err := r.repairDoc(ctx, srcCol, tgtCol, idStr); err != nil {
			result.Failed++
			result.Errors = append(result.Errors,
				fmt.Sprintf("DIFF repair [%s]: %v", idStr, err))
		} else {
			result.Fixed++
		}
	}
	return result
}

func (r *Repairer) repairDoc(ctx context.Context,
	srcCol, tgtCol *mongo.Collection, idStr string) error {

	// Pull the document from source
	var srcDoc bson.M
	err := srcCol.FindOne(ctx, bson.M{"_id": idStr}).Decode(&srcDoc)
	if err != nil {
		return fmt.Errorf("not found in source: %v", err)
	}

	// Upsert into target
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = tgtCol.ReplaceOne(
		ctx2,
		bson.M{"_id": srcDoc["_id"]},
		srcDoc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func splitNS(ns string) (string, string) {
	for i, c := range ns {
		if c == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return ns, ""
}

func extractID(sample string) string {
	// sample format: "id=xxxx | ..."
	if len(sample) > 3 && sample[:3] == "id=" {
		end := len(sample)
		for i, c := range sample[3:] {
			if c == ' ' || c == '|' {
				end = i + 3
				break
			}
		}
		return sample[3:end]
	}
	return ""
}
