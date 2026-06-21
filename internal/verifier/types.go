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
// field neither side has ("<nil>" on both) is naturally not reported, since
// sv == tv already; a field only one side has IS reported, since that's
// exactly the signal that one side has a feature/option the other doesn't
// (e.g. a collection silently created without timeseries/clusteredIndex
// because the source server version didn't support it).
func compareBSONFields(label string, fields []string, src, tgt bson.M) []string {
	var errors []string
	for _, f := range fields {
		sv := fmt.Sprintf("%v", src[f])
		tv := fmt.Sprintf("%v", tgt[f])
		if sv != tv {
			errors = append(errors, fmt.Sprintf("⚠️  %s.%s: src=%s tgt=%s", label, f, sv, tv))
		}
	}
	return errors
}
