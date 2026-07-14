package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IDRange is an _id bound: Min is inclusive, Max is exclusive. A nil Min
// means unbounded below (the first range); a nil Max means unbounded above
// (the last range) - this keeps the very first/last range robust to
// documents outside whatever min/max the boundary computation observed
// (e.g. concurrent inserts between the boundary snapshot and the scan).
type IDRange struct {
	Min interface{}
	Max interface{}
}

// computeRanges splits a collection into up to n _id ranges for concurrent
// scanning. It tries progressively-available strategies and falls back
// rather than failing - a collection this is called on is assumed to
// already be known "large enough to bother" (checked by the caller via
// EstimatedDocumentCount against range_split_threshold_docs); computeRanges
// itself does not re-check size.
//
//  1. splitVector: a storage-engine command that returns split points from
//     the index alone, no collection scan. Cheapest, but needs elevated
//     privileges and isn't available on every deployment (e.g. some managed
//     Atlas tiers reject it).
//  2. $bucketAuto over a {_id: 1}-projected aggregation: an index-covered
//     scan (reads only _id, not document bodies) - far cheaper than
//     reading every document, works anywhere aggregation does.
//  3. a single unbounded range - equivalent to "don't split". Always
//     succeeds, so computeRanges never returns an error to the caller.
//
// method is a short label ("splitVector", "bucketAuto", "none") for the
// caller to log, so a run that unexpectedly fell back (e.g. due to a
// permissions issue) is visible instead of silently slower.
func computeRanges(ctx context.Context, col *mongo.Collection, n int) (ranges []IDRange, method string) {
	if n <= 1 {
		return []IDRange{{}}, "none"
	}

	if r, err := splitVectorRanges(ctx, col, n); err == nil && len(r) > 1 {
		return r, "splitVector"
	}

	if r, err := bucketAutoRanges(ctx, col, n); err == nil && len(r) > 1 {
		return r, "bucketAuto"
	}

	return []IDRange{{}}, "none"
}

// splitVectorRanges asks the storage engine for split points sized so the
// result has roughly n chunks. maxChunkSize is derived from an estimated
// average document size (via collStats) rather than a fixed guess, since a
// fixed size would produce wildly wrong chunk counts across collections
// with very different document sizes.
func splitVectorRanges(ctx context.Context, col *mongo.Collection, n int) ([]IDRange, error) {
	count, err := col.EstimatedDocumentCount(ctx)
	if err != nil || count < 1 {
		return nil, fmt.Errorf("estimated count unavailable: %w", err)
	}

	var stats bson.M
	if err := col.Database().RunCommand(ctx,
		bson.D{{Key: "collStats", Value: col.Name()}}).Decode(&stats); err != nil {
		return nil, fmt.Errorf("collStats failed: %w", err)
	}
	avgObjSize := toInt64(stats["avgObjSize"])
	if avgObjSize < 1 {
		avgObjSize = 1024 // conservative fallback if collStats omits/zeroes it
	}

	maxChunkMB := int64(float64(count*avgObjSize) / (1024 * 1024) / float64(n))
	if maxChunkMB < 1 {
		maxChunkMB = 1
	}

	ns := col.Database().Name() + "." + col.Name()
	var out struct {
		SplitKeys []bson.M `bson:"splitKeys"`
	}
	if err := col.Database().RunCommand(ctx, bson.D{
		{Key: "splitVector", Value: ns},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "maxChunkSize", Value: maxChunkMB},
	}).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.SplitKeys) == 0 {
		return nil, fmt.Errorf("splitVector returned no split points")
	}

	ranges := make([]IDRange, 0, len(out.SplitKeys)+1)
	var prev interface{}
	for _, sk := range out.SplitKeys {
		ranges = append(ranges, IDRange{Min: prev, Max: sk["_id"]})
		prev = sk["_id"]
	}
	ranges = append(ranges, IDRange{Min: prev, Max: nil})
	return ranges, nil
}

// bucketAutoRanges computes roughly-equal _id buckets via an index-covered
// aggregation ($project to _id-only keeps the scan to the index, not the
// documents). Consecutive $bucketAuto buckets share a boundary (one
// bucket's max equals the next bucket's min), so using each bucket's own
// min/max directly produces a contiguous, gap-free set of ranges.
func bucketAutoRanges(ctx context.Context, col *mongo.Collection, n int) ([]IDRange, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$project", Value: bson.M{"_id": 1}}},
		{{Key: "$bucketAuto", Value: bson.M{"groupBy": "$_id", "buckets": n}}},
	}
	cur, err := col.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var buckets []struct {
		ID struct {
			Min interface{} `bson:"min"`
			Max interface{} `bson:"max"`
		} `bson:"_id"`
	}
	if err := cur.All(ctx, &buckets); err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("$bucketAuto returned no buckets")
	}

	ranges := make([]IDRange, 0, len(buckets))
	for i, b := range buckets {
		r := IDRange{Min: b.ID.Min, Max: b.ID.Max}
		if i == 0 {
			r.Min = nil // unbounded below - don't rely on the snapshot's observed min
		}
		if i == len(buckets)-1 {
			r.Max = nil // unbounded above, same reasoning
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		return 0
	}
}
