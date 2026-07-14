package verifier

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// CollectionTask represents one collection, or one _id sub-range of a large
// collection, to be verified. RangeTotal == 0 means "whole collection, not
// split" - the default zero value keeps every existing caller that only sets
// DBName/ColName behaving exactly as before. RangeMin/RangeMax are inclusive
// lower / exclusive upper _id bounds; nil means unbounded on that side.
type CollectionTask struct {
	DBName  string
	ColName string

	RangeIndex int // 0-based position among RangeTotal sibling ranges
	RangeTotal int // 0 or 1 = not split
	RangeMin   interface{}
	RangeMax   interface{}
}

// NS returns the task's dotted namespace ("db.col") - the same value used
// as the report/checkpoint key for a whole collection.
func (t CollectionTask) NS() string {
	return t.DBName + "." + t.ColName
}

// IsSplit reports whether this task is one of several sub-ranges of a
// larger collection rather than the whole thing.
func (t CollectionTask) IsSplit() bool {
	return t.RangeTotal > 1
}

// CheckpointKey returns the key used to persist/resume this task's scan
// position. A split task gets its own key per range so concurrent range
// workers don't clobber each other's checkpoint for the same namespace.
func (t CollectionTask) CheckpointKey() string {
	if !t.IsSplit() {
		return t.NS()
	}
	return fmt.Sprintf("%s#r%d/%d", t.NS(), t.RangeIndex, t.RangeTotal)
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
