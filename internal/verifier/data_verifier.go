package verifier

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/null-ptr-exception/mongogate/internal/alert"
	"github.com/null-ptr-exception/mongogate/internal/monitor"
	"github.com/null-ptr-exception/mongogate/internal/progress"
	promMetrics "github.com/null-ptr-exception/mongogate/internal/prometheus"
	"github.com/null-ptr-exception/mongogate/internal/report"
	"github.com/null-ptr-exception/mongogate/internal/utils"
)

type DataVerifyOptions struct {
	BatchSize     int
	MaxWorkers    int
	RateLimitMS   int
	TimeoutSecs   int
	RetryCount    int
	RetryWaitMS   int
	SampleRate    float64
	Resume        bool
	Phase         string
	Bidirectional bool // also scan target → source
	DryRun        bool
	HashOpts      utils.HashOptions

	// RangeSplitThresholdDocs/RangeWorkersPerCollection control _id-range
	// splitting for collections too large for one cursor/goroutine to scan
	// in a reasonable time - see expandTasks and range_split.go.
	RangeSplitThresholdDocs   int64
	RangeWorkersPerCollection int
}

// sharedProgress is the per-namespace state every range sub-task of a
// split collection reports into: one progress bar and one set of running
// totals, instead of each range writing its own bar (which would garble
// stdout - see docs on progress.Bar) or its own disjoint counters (which
// would make the report/HTTP-API progress and Prometheus gauges flicker as
// different ranges overwrite each other's partial numbers). An unsplit
// collection also gets one of these, sized from its own count, so the rest
// of the pipeline doesn't need a split/unsplit branch.
type sharedProgress struct {
	bar       *progress.Bar
	total     int64 // denominator for aggregate percent - approximate for split collections, exact otherwise
	processed int64 // atomic
	missing   int64 // atomic
	different int64 // atomic

	// lastRenderNS throttles bar.Update calls to roughly barRenderInterval,
	// regardless of how many range workers share this bar or how fast they
	// process documents. Without this, every range worker calling
	// bar.Update on every single document serializes the whole scan behind
	// progress.Bar's mutex + a synchronous stdout write per document - with
	// N concurrent range workers that's N-way contention on one lock,
	// measured to make a higher range_workers_per_collection perform worse
	// than fewer, not better (the opposite of what range-split exists for).
	lastRenderNS int64 // atomic
}

const barRenderInterval = 100 * time.Millisecond

// VerifyAllData verifies every collection in parallel across MaxWorkers goroutines.
func VerifyAllData(
	ctx context.Context,
	src, tgt *mongo.Client,
	collections []CollectionTask,
	opts DataVerifyOptions,
	mw *monitor.MetricsWriter,
	am *alert.AlertManager,
	pm *promMetrics.MetricsServer,
	rpt *report.Report,
) {
	fmt.Printf("\n📊 [%s] Data verification (%d workers)", opts.Phase, opts.MaxWorkers)
	if opts.DryRun {
		fmt.Printf(" [DRY-RUN]")
	}
	if opts.Bidirectional {
		fmt.Printf(" [bidirectional]")
	}
	fmt.Println()

	tasks, bars := expandTasks(ctx, src, opts, collections, rpt)

	taskCh := make(chan CollectionTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < opts.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				result := verifyOneTask(ctx, src, tgt, task, opts, mw, am, pm, rpt, bars)
				combined, isLast := rpt.MergeData(result.NS, result)
				if !isLast {
					// Another range of this namespace is still outstanding -
					// counts are incomplete, nothing to print yet.
					continue
				}
				if sp, ok := bars[result.NS]; ok && sp.bar != nil {
					sp.bar.Done()
				}
				status := "✅"
				if !combined.Passed {
					status = "❌"
				}
				fmt.Printf("  %s %-45s src=%-8d missing=%-5d diff=%-5d extra_in_target=%-5d\n",
					status, combined.NS, combined.SrcCount,
					combined.MissingCount, combined.DifferentCount, combined.ExtraInTarget)
			}
		}()
	}
	wg.Wait()
}

// expandTasks looks at each collection's estimated size and, for
// collections above RangeSplitThresholdDocs, replaces the single
// whole-collection task with multiple _id-range sub-tasks that scan
// concurrently instead of being bound to one cursor/goroutine (see
// docs/TESTING.md "Parallelism is per-collection, not per-document" and
// range_split.go). This applies in both Phase 2 (sampled - gives stratified
// per-range sampling and cheap range-count reconciliation) and Phase 3
// (exact - gives real intra-collection parallelism), since a collection
// large enough to need one benefits from the other too.
//
// It also builds the sharedProgress every task (split or not) reports its
// progress into, sized from a single EstimatedDocumentCount call per
// collection - cheap even at 1TB, since it's a metadata read, not a scan.
// EstimatedDocumentCount calls run concurrently (bounded by MaxWorkers) so
// a run with many collections doesn't pay for them one at a time before
// any real work starts.
func expandTasks(
	ctx context.Context,
	src *mongo.Client,
	opts DataVerifyOptions,
	collections []CollectionTask,
	rpt *report.Report,
) ([]CollectionTask, map[string]*sharedProgress) {
	if opts.DryRun {
		// Dry-run only counts and exits - no bars, no splitting, matches
		// today's behavior exactly.
		return collections, map[string]*sharedProgress{}
	}

	canSplit := opts.RangeWorkersPerCollection > 1 && opts.RangeSplitThresholdDocs > 0

	type expanded struct {
		tasks []CollectionTask
		sp    *sharedProgress
	}
	results := make([]expanded, len(collections))

	sem := make(chan struct{}, max(opts.MaxWorkers, 1))
	var wg sync.WaitGroup
	for i, base := range collections {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, base CollectionTask) {
			defer wg.Done()
			defer func() { <-sem }()

			ns := base.NS()
			col := src.Database(base.DBName).Collection(base.ColName)
			estCount, _ := col.EstimatedDocumentCount(ctx)

			if canSplit && estCount > opts.RangeSplitThresholdDocs {
				if ranges, method := computeRanges(ctx, col, opts.RangeWorkersPerCollection); len(ranges) > 1 {
					fmt.Printf("  ✂️  [%s] ~%d docs > %d threshold, splitting into %d ranges (%s)\n",
						ns, estCount, opts.RangeSplitThresholdDocs, len(ranges), method)
					rpt.BeginRanges(ns, len(ranges))
					tasks := make([]CollectionTask, len(ranges))
					for j, rg := range ranges {
						t := base
						t.RangeIndex = j
						t.RangeTotal = len(ranges)
						t.RangeMin = rg.Min
						t.RangeMax = rg.Max
						tasks[j] = t
					}
					results[i] = expanded{tasks: tasks, sp: &sharedProgress{bar: progress.New(ns, estCount), total: estCount}}
					return
				}
			}
			results[i] = expanded{tasks: []CollectionTask{base}, sp: &sharedProgress{bar: progress.New(ns, estCount), total: estCount}}
		}(i, base)
	}
	wg.Wait()

	var tasks []CollectionTask
	bars := make(map[string]*sharedProgress, len(collections))
	for _, r := range results {
		tasks = append(tasks, r.tasks...)
		bars[r.tasks[0].NS()] = r.sp
	}
	return tasks, bars
}

func verifyOneTask(
	ctx context.Context,
	src, tgt *mongo.Client,
	task CollectionTask,
	opts DataVerifyOptions,
	mw *monitor.MetricsWriter,
	am *alert.AlertManager,
	pm *promMetrics.MetricsServer,
	rpt *report.Report,
	bars map[string]*sharedProgress,
) *report.DataResult {

	ns := task.NS()
	srcCol := src.Database(task.DBName).Collection(task.ColName)
	tgtCol := tgt.Database(task.DBName).Collection(task.ColName)
	startTime := time.Now()
	rangeFilter := idRangeFilter(task)

	var srcCount, tgtCount int64
	if task.IsSplit() {
		// A bounded range's count is an index-range scan, not a full
		// collection scan, so it stays cheap here even though a whole-
		// collection exact count would not be (see countWithRetry). This
		// doubles as Phase 2's range-count reconciliation signal below.
		srcCount, tgtCount = countRangeBothSides(ctx, srcCol, tgtCol, rangeFilter, opts)
	} else {
		fmt.Printf("  ⏳ [%s] counting documents (large collections can take a while)...\n", ns)
		srcCount, tgtCount = countBothSides(ctx, srcCol, tgtCol, opts)
	}

	result := &report.DataResult{
		NS:           ns,
		SrcCount:     srcCount,
		TgtCount:     tgtCount,
		CountMatch:   srcCount == tgtCount,
		CountIsExact: opts.SampleRate == 0,
		HashIsExact:  opts.SampleRate == 0,
	}

	if task.IsSplit() && srcCount != tgtCount {
		// Cheap, index-only signal that this specific sub-range has drifted -
		// surfaced as an error so it fails the namespace's Passed check even
		// if other ranges' drift happens to cancel out in the aggregate sum,
		// and so a human immediately knows which slice of the keyspace to
		// re-check with a full (unsampled) pass.
		result.Errors = append(result.Errors, fmt.Sprintf(
			"range %d/%d count mismatch: src=%d tgt=%d - re-check this range with phase 3",
			task.RangeIndex+1, task.RangeTotal, srcCount, tgtCount))
	}

	if opts.DryRun {
		result.Passed = true
		fmt.Printf("  [DRY-RUN] %s src=%d tgt=%d\n", ns, srcCount, tgtCount)
		return result
	}

	sp := bars[ns]

	// ── Forward: source → target ──
	processSrcToTgt(ctx, srcCol, tgtCol, task, rangeFilter, srcCount, startTime,
		opts, mw, am, pm, rpt, result, sp)

	// ── Reverse: target → source (find docs that only exist in target) ──
	if opts.Bidirectional {
		processTgtToSrc(ctx, srcCol, tgtCol, rangeFilter, opts, result)
		am.CheckExtraInTarget(ns, result.ExtraInTarget)
	}

	// len(Errors) == 0 is part of Passed: a collection whose scan errored
	// out (e.g. "find failed") was never actually verified, and previously
	// could slip through as passing when counts happened to match. For a
	// split task this is only this range's own partial Passed - the
	// authoritative combined Passed is computed once in
	// report.Report.MergeData after every range has reported in.
	result.Passed = result.CountMatch &&
		result.MissingCount == 0 &&
		result.DifferentCount == 0 &&
		result.ExtraInTarget == 0 &&
		len(result.Errors) == 0

	return result
}

// idRangeFilter returns the _id bound filter for a range sub-task, or an
// empty filter for a whole-collection task.
func idRangeFilter(task CollectionTask) bson.M {
	if !task.IsSplit() {
		return bson.M{}
	}
	cond := bson.M{}
	if task.RangeMin != nil {
		cond["$gte"] = task.RangeMin
	}
	if task.RangeMax != nil {
		cond["$lt"] = task.RangeMax
	}
	if len(cond) == 0 {
		return bson.M{}
	}
	return bson.M{"_id": cond}
}

// processSrcToTgt walks a cursor over source and looks each document up in target.
func processSrcToTgt(
	ctx context.Context,
	srcCol, tgtCol *mongo.Collection,
	task CollectionTask,
	rangeFilter bson.M,
	total int64, startTime time.Time,
	opts DataVerifyOptions,
	mw *monitor.MetricsWriter,
	am *alert.AlertManager,
	pm *promMetrics.MetricsServer,
	rpt *report.Report,
	result *report.DataResult,
	sp *sharedProgress,
) {
	ns := task.NS()
	cpKey := task.CheckpointKey()

	filter := bson.M{}
	for k, v := range rangeFilter {
		filter[k] = v
	}
	if opts.Resume {
		if lastID, ok := utils.LoadCheckpoint(cpKey); ok {
			idCond, _ := filter["_id"].(bson.M)
			if idCond == nil {
				idCond = bson.M{}
			}
			idCond["$gt"] = lastID
			filter["_id"] = idCond
			fmt.Printf("  🔄 [%s] resuming from checkpoint\n", cpKey)
		}
	}

	// allowDiskUse everywhere a server-side sort can happen: without it any
	// sort that exceeds MongoDB's 100MB in-memory limit fails with
	// QueryExceededMemoryLimitNoDiskUseAllowed. The $sample below is the
	// usual trigger - a sample size above 5% of the collection falls back to
	// a full scan + random blocking sort. (allowDiskUse on find requires
	// server >= 4.4; the supported matrix starts there.)
	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(int32(opts.BatchSize)).
		SetAllowDiskUse(true)

	var cur *mongo.Cursor
	var err error

	if opts.SampleRate > 0 && opts.SampleRate < 1 {
		sampleSize := int64(float64(total) * opts.SampleRate)
		if sampleSize < 1 {
			sampleSize = 1
		}
		// $sample only uses the storage engine's random cursor (cost
		// proportional to the sample size) when the sample is under 5% of
		// the collection; at 5% and above it degrades to a full scan plus a
		// blocking sort of every document by a random key - on a multi-GB
		// collection that's the difference between seconds and hours, all
		// spent before the cursor returns its first batch. Cap the sample
		// just below the threshold; the final phase does a full exact scan
		// anyway, so a bigger Phase 2 sample buys little.
		if maxFast := total/20 - 1; maxFast >= 1 && sampleSize > maxFast {
			fmt.Printf("  ℹ️  [%s] sample capped %d → %d docs to stay on $sample's random-cursor fast path (<5%% of collection)\n",
				ns, sampleSize, maxFast)
			sampleSize = maxFast
		}
		// A split task's `total` is this range's own count (see
		// countRangeBothSides), so sampleSize here is already this range's
		// proportional share of the namespace-wide sample - stratified
		// across the whole _id keyspace instead of trusting $sample's own
		// randomness to spread evenly over a huge collection. $match-ing to
		// the range first keeps the sample confined to it.
		pipeline := mongo.Pipeline{}
		if len(rangeFilter) > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$match", Value: rangeFilter}})
		}
		pipeline = append(pipeline, bson.D{{Key: "$sample", Value: bson.M{"size": sampleSize}}})
		cur, err = srcCol.Aggregate(ctx, pipeline,
			options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(opts.BatchSize)))
	} else {
		cur, err = srcCol.Find(ctx, filter, findOpts)
	}
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("find failed: %v", err))
		return
	}
	defer cur.Close(ctx)

	var processed int64
	batch := make([]bson.M, 0, opts.BatchSize)

	// flush compares one batch of source docs against target, fetched in a
	// single $in query instead of one FindOne round trip per document.
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ids := make([]interface{}, len(batch))
		for i, d := range batch {
			ids[i] = d["_id"]
		}
		// unverified tracks ids whose presence in target lookupBatchResilient
		// could not determine even after bisecting - those are skipped below
		// (not counted missing or different) instead of the old behavior of
		// abandoning the entire batch on one failed $in query.
		unverified := make(map[string]bool)
		recordLookupErr := func(prefix string) func([]interface{}, error) {
			return func(failedIDs []interface{}, err error) {
				for _, id := range failedIDs {
					unverified[idKey(id)] = true
				}
				if len(result.Errors) < 20 {
					result.Errors = append(result.Errors,
						fmt.Sprintf("%s (%d docs): %v", prefix, len(failedIDs), err))
				}
			}
		}
		tgtDocs := lookupBatchResilient(ctx, tgtCol, ids, false, opts, recordLookupErr("batch lookup failed"))

		// Re-check ids absent from the first read with the same retry budget
		// and waits the per-document path used to spend - replication lag can
		// make documents show up late - but as whole-batch $in queries. A
		// target that has fallen far behind costs RetryCount extra queries
		// per batch instead of ~1.5s per missing document; the per-document
		// version was a retry storm that made a badly lagging target look
		// like a hung verifier, slowest exactly when you most need the answer.
		missingIDs := make([]interface{}, 0)
		for _, d := range batch {
			key := idKey(d["_id"])
			if unverified[key] {
				continue
			}
			if _, ok := tgtDocs[key]; !ok {
				missingIDs = append(missingIDs, d["_id"])
			}
		}
		for attempt := 0; attempt < opts.RetryCount && len(missingIDs) > 0; attempt++ {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
			late := lookupBatchResilient(ctx, tgtCol, missingIDs, false, opts, recordLookupErr("recheck lookup failed"))
			still := missingIDs[:0]
			for _, id := range missingIDs {
				key := idKey(id)
				if unverified[key] {
					continue
				}
				if doc, ok := late[key]; ok {
					tgtDocs[key] = doc
				} else {
					still = append(still, id)
				}
			}
			missingIDs = still
		}

		for _, srcDoc := range batch {
			docID := srcDoc["_id"]
			key := idKey(docID)
			if unverified[key] {
				// Presence/content unknown even after bisected retries -
				// counting it either way would fabricate a result.
				processed++
				reportProgress(sp, atomic.AddInt64(&sp.processed, 1))
				continue
			}
			tgtDoc, found := tgtDocs[key]
			if !found {
				result.MissingCount++
				atomic.AddInt64(&sp.missing, 1)
				if len(result.MissingSample) < 50 {
					result.MissingSample = append(result.MissingSample, fmt.Sprintf("%v", docID))
				}
			}

			if found {
				passed, diffs := utils.DeepCompare(srcDoc, tgtDoc, opts.HashOpts)
				if !passed {
					result.DifferentCount++
					atomic.AddInt64(&sp.different, 1)
					if len(result.DiffSample) < 50 {
						result.DiffSample = append(result.DiffSample,
							formatDiffSample(docID, diffs))
					}
					for _, d := range diffs {
						if len(result.FieldErrors) < 200 {
							result.FieldErrors = append(result.FieldErrors, report.FieldError{
								DocID:     fmt.Sprintf("%v", docID),
								IssueType: d.IssueType,
								Path:      d.Path,
								SrcType:   d.SrcType,
								TgtType:   d.TgtType,
								SrcValue:  d.SrcValue,
								TgtValue:  d.TgtValue,
							})
						}
					}
				}
			}

			processed++
			newTotal := atomic.AddInt64(&sp.processed, 1)
			reportProgress(sp, newTotal)

			if processed%1000 == 0 {
				utils.SaveCheckpoint(cpKey, fmt.Sprintf("%v", docID))
				pct := aggregatePct(sp)
				rpt.UpdateProgress(ns, pct)

				mw.Write(monitor.Metric{
					NS: ns, Phase: opts.Phase,
					Processed: processed, Total: total, ProgressPct: pct,
					Missing: result.MissingCount, Different: result.DifferentCount,
					SrcCount: result.SrcCount, TgtCount: result.TgtCount,
					CountDiff:    result.SrcCount - result.TgtCount,
					IsExact:      opts.SampleRate == 0,
					DurationSecs: time.Since(startTime).Seconds(),
				})

				// Alert check
				am.Check(ns, result.MissingCount, result.DifferentCount, pct)

				// Prometheus - aggregate across ranges so a namespace split
				// into sub-ranges shows one coherent, monotonically
				// increasing set of gauges instead of whichever range's
				// update landed last overwriting the others'.
				label := strings.ReplaceAll(ns, ".", "_")
				pm.Set(fmt.Sprintf("ns_progress_%s", label), pct)
				pm.Set(fmt.Sprintf("ns_missing_%s", label), float64(atomic.LoadInt64(&sp.missing)))
				pm.Set(fmt.Sprintf("ns_different_%s", label), float64(atomic.LoadInt64(&sp.different)))

				if opts.RateLimitMS > 0 {
					time.Sleep(time.Duration(opts.RateLimitMS) * time.Millisecond)
				}
			}
		}
		batch = batch[:0]
	}

	for cur.Next(ctx) {
		var srcDoc bson.M
		if err := cur.Decode(&srcDoc); err != nil {
			continue
		}
		batch = append(batch, srcDoc)
		if len(batch) >= opts.BatchSize {
			flush()
		}
	}
	flush()
	if err := cur.Err(); err != nil && len(result.Errors) < 20 {
		// A cursor that dies mid-scan previously just ended the loop quietly.
		result.Errors = append(result.Errors, fmt.Sprintf("scan cursor error: %v", err))
	}
}

// reportProgress updates the namespace's shared bar with the aggregate
// processed count across every range (not just this task's own), so a
// split collection's bar reflects real combined progress instead of one
// range restarting the display from its own local count. Actual renders
// are throttled to barRenderInterval (see sharedProgress.lastRenderNS) -
// a skipped call costs one atomic load, no lock, no stdout write, so this
// stays cheap to call on every document even with many concurrent range
// workers. bar.Done() (called once, when the namespace's last range
// finishes) always renders a final 100% frame regardless of throttling.
func reportProgress(sp *sharedProgress, aggregateProcessed int64) {
	if sp == nil || sp.bar == nil {
		return
	}
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&sp.lastRenderNS)
	if now-last < int64(barRenderInterval) {
		return
	}
	if !atomic.CompareAndSwapInt64(&sp.lastRenderNS, last, now) {
		return // another goroutine just won the race to render
	}
	sp.bar.Update(aggregateProcessed)
}

func aggregatePct(sp *sharedProgress) float64 {
	if sp == nil || sp.total <= 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&sp.processed)) / float64(sp.total) * 100
}

// processTgtToSrc scans target for documents that don't exist in source.
func processTgtToSrc(
	ctx context.Context,
	srcCol, tgtCol *mongo.Collection,
	rangeFilter bson.M,
	opts DataVerifyOptions,
	result *report.DataResult,
) {
	cur, err := tgtCol.Find(ctx, rangeFilter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetBatchSize(int32(opts.BatchSize)).
			SetAllowDiskUse(true).
			SetProjection(bson.M{"_id": 1})) // only pull _id to save bandwidth
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("reverse find failed: %v", err))
		return
	}
	defer cur.Close(ctx)

	ids := make([]interface{}, 0, opts.BatchSize)

	// flush checks one batch of target _ids for existence in source with a
	// single projected $in query; only ids absent from the batch get an
	// individual retried lookup before being declared extra.
	flush := func() {
		if len(ids) == 0 {
			return
		}
		// unverified tracks ids bisection still couldn't resolve - excluded
		// below rather than either declared extra or dropping the whole
		// batch's worth of ids on one failed $in query.
		unverified := make(map[string]bool)
		recordLookupErr := func(prefix string) func([]interface{}, error) {
			return func(failedIDs []interface{}, err error) {
				for _, id := range failedIDs {
					unverified[idKey(id)] = true
				}
				if len(result.Errors) < 20 {
					result.Errors = append(result.Errors,
						fmt.Sprintf("%s (%d docs): %v", prefix, len(failedIDs), err))
				}
			}
		}
		srcDocs := lookupBatchResilient(ctx, srcCol, ids, true, opts, recordLookupErr("reverse batch lookup failed"))
		// Same batched re-check as the forward path: absent ids get the full
		// retry budget as whole-batch $in queries before being declared extra.
		missingIDs := make([]interface{}, 0)
		for _, docID := range ids {
			key := idKey(docID)
			if unverified[key] {
				continue
			}
			if _, ok := srcDocs[key]; !ok {
				missingIDs = append(missingIDs, docID)
			}
		}
		for attempt := 0; attempt < opts.RetryCount && len(missingIDs) > 0; attempt++ {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
			late := lookupBatchResilient(ctx, srcCol, missingIDs, true, opts, recordLookupErr("reverse recheck lookup failed"))
			still := missingIDs[:0]
			for _, id := range missingIDs {
				key := idKey(id)
				if unverified[key] {
					continue
				}
				if _, ok := late[key]; !ok {
					still = append(still, id)
				}
			}
			missingIDs = still
		}
		for _, docID := range missingIDs {
			// exists in target but not in source
			result.ExtraInTarget++
			if len(result.ExtraInTargetSample) < 20 {
				result.ExtraInTargetSample = append(result.ExtraInTargetSample,
					fmt.Sprintf("%v", docID))
			}
		}
		ids = ids[:0]
	}

	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		ids = append(ids, doc["_id"])
		if len(ids) >= opts.BatchSize {
			flush()
		}
	}
	flush()
	if err := cur.Err(); err != nil && len(result.Errors) < 20 {
		result.Errors = append(result.Errors, fmt.Sprintf("reverse scan cursor error: %v", err))
	}
}

// ── Retry helpers ──

// countBothSides runs the source and target counts concurrently rather
// than back to back - each one can itself take minutes on a huge
// collection (see countWithRetry), so this halves the wait instead of
// paying for both in sequence before the first byte of real scanning work
// happens.
func countBothSides(ctx context.Context, srcCol, tgtCol *mongo.Collection, opts DataVerifyOptions) (int64, int64) {
	var srcCount, tgtCount int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srcCount = countWithRetry(ctx, srcCol, opts) }()
	go func() { defer wg.Done(); tgtCount = countWithRetry(ctx, tgtCol, opts) }()
	wg.Wait()
	return srcCount, tgtCount
}

// countRangeBothSides counts a bounded _id range on both sides
// concurrently. Unlike countWithRetry's whole-collection CountDocuments,
// this is always an index-range scan bounded by the range filter, so it
// stays cheap even on a collection too large to count exactly up front -
// this is exactly why range-split collections skip the whole-collection
// precount entirely and rely on this instead (see verifyOneTask).
func countRangeBothSides(ctx context.Context, srcCol, tgtCol *mongo.Collection, filter bson.M, opts DataVerifyOptions) (int64, int64) {
	var srcCount, tgtCount int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srcCount = countRangeWithRetry(ctx, srcCol, filter, opts) }()
	go func() { defer wg.Done(); tgtCount = countRangeWithRetry(ctx, tgtCol, filter, opts) }()
	wg.Wait()
	return srcCount, tgtCount
}

func countRangeWithRetry(ctx context.Context, col *mongo.Collection, filter bson.M, opts DataVerifyOptions) int64 {
	for i := 0; i <= opts.RetryCount; i++ {
		n, err := col.CountDocuments(ctx, filter)
		if err == nil {
			return n
		}
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
	}
	return 0
}

// countWithRetry returns a whole collection's document count. Sampled
// phases use the O(1) metadata-based estimate: the exact CountDocuments
// scans the whole _id index, which on a multi-GB collection means minutes
// of silence before the progress bar even appears - for a phase whose
// verdict is approximate by design. The unsampled (final) phase keeps the
// exact scan, but only reaches this function at all for collections that
// stayed below RangeSplitThresholdDocs - collections large enough to be
// range-split skip this exact precount entirely and get their authoritative
// count from summing what the scan actually processed per range instead
// (see countRangeBothSides and expandTasks).
func countWithRetry(ctx context.Context, col *mongo.Collection, opts DataVerifyOptions) int64 {
	exact := opts.SampleRate == 0
	for i := 0; i <= opts.RetryCount; i++ {
		var n int64
		var err error
		if exact {
			n, err = col.CountDocuments(ctx, bson.M{})
		} else {
			n, err = col.EstimatedDocumentCount(ctx)
		}
		if err == nil {
			return n
		}
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
	}
	return 0
}

// lookupBatch fetches the documents for a batch of _ids in one $in query and
// returns them keyed by idKey. idOnly projects everything but _id away, for
// existence checks. Absence from the returned map is not authoritative -
// callers must re-check individually before treating a document as missing.
func lookupBatch(ctx context.Context, col *mongo.Collection,
	ids []interface{}, idOnly bool, opts DataVerifyOptions) (map[string]bson.M, error) {

	filter := bson.M{"_id": bson.M{"$in": ids}}
	findOpts := options.Find().SetBatchSize(int32(opts.BatchSize))
	if idOnly {
		findOpts.SetProjection(bson.M{"_id": 1})
	}

	var lastErr error
	for i := 0; i <= opts.RetryCount; i++ {
		tCtx, cancel := context.WithTimeout(ctx,
			time.Duration(opts.TimeoutSecs)*time.Second)
		docs, err := func() (map[string]bson.M, error) {
			cur, err := col.Find(tCtx, filter, findOpts)
			if err != nil {
				return nil, err
			}
			defer cur.Close(tCtx)
			out := make(map[string]bson.M, len(ids))
			for cur.Next(tCtx) {
				var d bson.M
				if err := cur.Decode(&d); err != nil {
					continue
				}
				out[idKey(d["_id"])] = d
			}
			return out, cur.Err()
		}()
		cancel()
		if err == nil {
			return docs, nil
		}
		lastErr = err
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
	}
	return nil, lastErr
}

// minLookupSubBatch bounds how far lookupBatchResilient bisects a failing
// batch: below this many ids, a sub-batch that still fails is reported to
// onError as unverifiable rather than split further. Unbounded bisection
// would turn one failing query into batch_size individually-retried
// queries under a total outage - the same retry storm the batched design
// replaced; capping the floor keeps worst-case amplification to
// O(log(batch_size/minLookupSubBatch)) instead of O(batch_size).
const minLookupSubBatch = 20

// lookupBatchResilient looks up ids in one $in query and, if that query
// still fails after lookupBatch's own retry budget, bisects into two
// sub-batches and retries those independently instead of abandoning the
// whole batch. A batch that times out under load often succeeds at half the
// size (response time scales with result size) or isolates the handful of
// ids actually causing trouble; onError is called only with the ids that
// remain unverifiable once bisection bottoms out, so a systemic hiccup
// costs at most a small sub-batch of comparisons instead of the whole batch.
func lookupBatchResilient(ctx context.Context, col *mongo.Collection,
	ids []interface{}, idOnly bool, opts DataVerifyOptions,
	onError func(failedIDs []interface{}, err error)) map[string]bson.M {

	docs, err := lookupBatch(ctx, col, ids, idOnly, opts)
	if err == nil {
		return docs
	}
	if len(ids) <= minLookupSubBatch {
		onError(ids, err)
		return map[string]bson.M{}
	}
	mid := len(ids) / 2
	out := lookupBatchResilient(ctx, col, ids[:mid], idOnly, opts, onError)
	for k, v := range lookupBatchResilient(ctx, col, ids[mid:], idOnly, opts, onError) {
		out[k] = v
	}
	return out
}

// idKey builds a map key for an _id that is faithful to BSON typing: the
// canonical marshaled bytes. fmt.Sprintf would collide e.g. int32(1),
// int64(1) and "1", silently matching documents whose _ids differ in type.
func idKey(id interface{}) string {
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return fmt.Sprintf("%v", id)
	}
	return string(b)
}

func formatDiffSample(docID interface{}, diffs []utils.DiffDetail) string {
	if len(diffs) == 0 {
		return fmt.Sprintf("id=%v | (hash mismatch)", docID)
	}
	parts := []string{fmt.Sprintf("id=%v", docID)}
	for i, d := range diffs {
		if i >= 3 {
			parts = append(parts, fmt.Sprintf("...(+%d more)", len(diffs)-3))
			break
		}
		parts = append(parts, fmt.Sprintf("%s:%s[%s→%s]",
			d.IssueType, d.Path, d.SrcValue, d.TgtValue))
	}
	return strings.Join(parts, " | ")
}

