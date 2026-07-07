package verifier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
}

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

	taskCh := make(chan CollectionTask, len(collections))
	for _, t := range collections {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < opts.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				result := verifyOneCollection(ctx, src, tgt, task, opts, mw, am, pm, rpt)
				rpt.SetData(result.NS, result)
				status := "✅"
				if !result.Passed {
					status = "❌"
				}
				fmt.Printf("  %s %-45s src=%-8d missing=%-5d diff=%-5d extra_in_target=%-5d\n",
					status, result.NS, result.SrcCount,
					result.MissingCount, result.DifferentCount, result.ExtraInTarget)
			}
		}()
	}
	wg.Wait()
}

func verifyOneCollection(
	ctx context.Context,
	src, tgt *mongo.Client,
	task CollectionTask,
	opts DataVerifyOptions,
	mw *monitor.MetricsWriter,
	am *alert.AlertManager,
	pm *promMetrics.MetricsServer,
	rpt *report.Report,
) *report.DataResult {

	ns := task.DBName + "." + task.ColName
	srcCol := src.Database(task.DBName).Collection(task.ColName)
	tgtCol := tgt.Database(task.DBName).Collection(task.ColName)
	startTime := time.Now()

	srcCount := countWithRetry(ctx, srcCol, opts)
	tgtCount := countWithRetry(ctx, tgtCol, opts)

	result := &report.DataResult{
		NS:           ns,
		SrcCount:     srcCount,
		TgtCount:     tgtCount,
		CountMatch:   srcCount == tgtCount,
		CountIsExact: opts.SampleRate == 0, // sampled phases use the metadata estimate
		HashIsExact:  opts.SampleRate == 0,
	}

	if opts.DryRun {
		result.Passed = true
		fmt.Printf("  [DRY-RUN] %s src=%d tgt=%d\n", ns, srcCount, tgtCount)
		return result
	}

	bar := progress.New(ns, srcCount)

	// ── Forward: source → target ──
	processSrcToTgt(ctx, srcCol, tgtCol, ns, srcCount, startTime,
		opts, mw, am, pm, rpt, result, bar)

	// ── Reverse: target → source (find docs that only exist in target) ──
	if opts.Bidirectional {
		processTgtToSrc(ctx, srcCol, tgtCol, ns, opts, result)
		am.CheckExtraInTarget(ns, result.ExtraInTarget)
	}

	bar.Done()
	// len(Errors) == 0 is part of Passed: a collection whose scan errored out
	// (e.g. "find failed") was never actually verified, and previously could
	// slip through as passing when counts happened to match.
	result.Passed = result.CountMatch &&
		result.MissingCount == 0 &&
		result.DifferentCount == 0 &&
		result.ExtraInTarget == 0 &&
		len(result.Errors) == 0

	return result
}

// processSrcToTgt walks a cursor over source and looks each document up in target.
func processSrcToTgt(
	ctx context.Context,
	srcCol, tgtCol *mongo.Collection,
	ns string, total int64, startTime time.Time,
	opts DataVerifyOptions,
	mw *monitor.MetricsWriter,
	am *alert.AlertManager,
	pm *promMetrics.MetricsServer,
	rpt *report.Report,
	result *report.DataResult,
	bar *progress.Bar,
) {
	filter := bson.M{}
	if opts.Resume {
		if lastID, ok := utils.LoadCheckpoint(ns); ok {
			filter = bson.M{"_id": bson.M{"$gt": lastID}}
			fmt.Printf("  🔄 [%s] resuming from checkpoint\n", ns)
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
		cur, err = srcCol.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$sample", Value: bson.M{"size": sampleSize}}},
		}, options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(opts.BatchSize)))
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
		tgtDocs, lookupErr := lookupBatch(ctx, tgtCol, ids, false, opts)
		if lookupErr != nil {
			// The whole batch is unknowable - record the failure instead of
			// fabricating len(batch) missing docs.
			if len(result.Errors) < 20 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("batch lookup failed (%d docs): %v", len(batch), lookupErr))
			}
			processed += int64(len(batch))
			bar.Update(processed)
			batch = batch[:0]
			return
		}

		for _, srcDoc := range batch {
			docID := srcDoc["_id"]
			tgtDoc, found := tgtDocs[idKey(docID)]
			if !found {
				// Absent from the batched read: re-check individually with
				// the retry budget before declaring it missing - replication
				// lag can make a document show up late.
				var late bson.M
				findErr := findWithRetry(ctx, tgtCol, docID, &late, opts)
				switch {
				case findErr == nil:
					tgtDoc, found = late, true
				case errors.Is(findErr, mongo.ErrNoDocuments):
					result.MissingCount++
					if len(result.MissingSample) < 50 {
						result.MissingSample = append(result.MissingSample, fmt.Sprintf("%v", docID))
					}
				default:
					// Transient failure (timeout, network): presence in
					// target is unknown - counting it as missing would
					// fabricate diffs whenever the server is merely slow.
					if len(result.Errors) < 20 {
						result.Errors = append(result.Errors,
							fmt.Sprintf("lookup failed for _id=%v: %v", docID, findErr))
					}
				}
			}

			if found {
				passed, diffs := utils.DeepCompare(srcDoc, tgtDoc, opts.HashOpts)
				if !passed {
					result.DifferentCount++
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
			bar.Update(processed)

			if processed%1000 == 0 {
				utils.SaveCheckpoint(ns, fmt.Sprintf("%v", docID))
				pct := float64(processed) / float64(total) * 100
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

				// Prometheus
				label := strings.ReplaceAll(ns, ".", "_")
				pm.Set(fmt.Sprintf("ns_progress_%s", label), pct)
				pm.Set(fmt.Sprintf("ns_missing_%s", label), float64(result.MissingCount))
				pm.Set(fmt.Sprintf("ns_different_%s", label), float64(result.DifferentCount))

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

// processTgtToSrc scans target for documents that don't exist in source.
func processTgtToSrc(
	ctx context.Context,
	srcCol, tgtCol *mongo.Collection,
	ns string,
	opts DataVerifyOptions,
	result *report.DataResult,
) {
	cur, err := tgtCol.Find(ctx, bson.M{},
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
		srcDocs, lookupErr := lookupBatch(ctx, srcCol, ids, true, opts)
		if lookupErr != nil {
			if len(result.Errors) < 20 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("reverse batch lookup failed (%d docs): %v", len(ids), lookupErr))
			}
			ids = ids[:0]
			return
		}
		for _, docID := range ids {
			if _, ok := srcDocs[idKey(docID)]; ok {
				continue
			}
			var srcDoc bson.M
			findErr := findWithRetry(ctx, srcCol, docID, &srcDoc, opts)
			switch {
			case findErr == nil:
			case errors.Is(findErr, mongo.ErrNoDocuments):
				// exists in target but not in source
				result.ExtraInTarget++
				if len(result.ExtraInTargetSample) < 20 {
					result.ExtraInTargetSample = append(result.ExtraInTargetSample,
						fmt.Sprintf("%v", docID))
				}
			default:
				if len(result.Errors) < 20 {
					result.Errors = append(result.Errors,
						fmt.Sprintf("reverse lookup failed for _id=%v: %v", docID, findErr))
				}
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

// countWithRetry returns the collection's document count. Sampled phases use
// the O(1) metadata-based estimate: the exact CountDocuments scans the whole
// _id index, which on a multi-GB collection means minutes of silence - twice
// per collection, before the progress bar even appears - for a phase whose
// verdict is approximate by design. The unsampled (final) phase keeps the
// exact scan.
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

// findWithRetry looks the document up with a per-attempt timeout. The
// returned error keeps its identity so callers can tell "genuinely absent"
// (mongo.ErrNoDocuments, retried anyway because replication lag can make a
// doc appear late) apart from transient failures like context deadlines -
// the two must be reported differently.
func findWithRetry(ctx context.Context, col *mongo.Collection,
	docID interface{}, result *bson.M, opts DataVerifyOptions) error {

	var lastErr error
	for i := 0; i <= opts.RetryCount; i++ {
		tCtx, cancel := context.WithTimeout(ctx,
			time.Duration(opts.TimeoutSecs)*time.Second)
		err := col.FindOne(tCtx, bson.M{"_id": docID}).Decode(result)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
	}
	return lastErr
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
