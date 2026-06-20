package verifier

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// CollectionTask represents one collection to be verified.
type CollectionTask struct {
	DBName  string
	ColName string
}

// compareBSONFields compares a fixed list of fields between two bson.M maps
// (stringified via %v) and returns one warning per field that differs. A
// field the source side doesn't have at all ("<nil>") is skipped rather than
// reported, since that's a "doesn't apply" case, not a mismatch.
func compareBSONFields(label string, fields []string, src, tgt bson.M) []string {
	var errors []string
	for _, f := range fields {
		sv := fmt.Sprintf("%v", src[f])
		tv := fmt.Sprintf("%v", tgt[f])
		if sv != tv && sv != "<nil>" {
			errors = append(errors, fmt.Sprintf("⚠️  %s.%s: src=%s tgt=%s", label, f, sv, tv))
		}
	}
	return errors
}
