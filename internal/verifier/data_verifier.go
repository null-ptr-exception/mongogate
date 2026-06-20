package verifier

import (
	"context"
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
	if opts.DryRun        { fmt.Printf(" [DRY-RUN]") }
	if opts.Bidirectional { fmt.Printf(" [bidirectional]") }
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
				if !result.Passed { status = "❌" }
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
		CountIsExact: false,
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
	result.Passed = result.CountMatch &&
		result.MissingCount == 0 &&
		result.DifferentCount == 0 &&
		result.ExtraInTarget == 0

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

	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(int32(opts.BatchSize))

	var cur *mongo.Cursor
	var err error

	if opts.SampleRate > 0 && opts.SampleRate < 1 {
		sampleSize := int64(float64(total) * opts.SampleRate)
		if sampleSize < 1 { sampleSize = 1 }
		cur, err = srcCol.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$sample", Value: bson.M{"size": sampleSize}}},
		})
	} else {
		cur, err = srcCol.Find(ctx, filter, findOpts)
	}
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("find failed: %v", err))
		return
	}
	defer cur.Close(ctx)

	var processed int64
	for cur.Next(ctx) {
		var srcDoc bson.M
		if err := cur.Decode(&srcDoc); err != nil { continue }
		docID := srcDoc["_id"]

		var tgtDoc bson.M
		findErr := findWithRetry(ctx, tgtCol, docID, &tgtDoc, opts)

		if findErr != nil {
			result.MissingCount++
			if len(result.MissingSample) < 50 {
				result.MissingSample = append(result.MissingSample, fmt.Sprintf("%v", docID))
			}
		} else {
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
				CountDiff: result.SrcCount - result.TgtCount,
				IsExact: opts.SampleRate == 0,
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
			SetProjection(bson.M{"_id": 1})) // only pull _id to save bandwidth
	if err != nil { return }
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil { continue }
		docID := doc["_id"]

		var srcDoc bson.M
		if err := srcCol.FindOne(ctx, bson.M{"_id": docID}).Decode(&srcDoc); err != nil {
			// exists in target but not in source
			result.ExtraInTarget++
			if len(result.ExtraInTargetSample) < 20 {
				result.ExtraInTargetSample = append(result.ExtraInTargetSample,
					fmt.Sprintf("%v", docID))
			}
		}
	}
}

// ── Retry helpers ──

func countWithRetry(ctx context.Context, col *mongo.Collection, opts DataVerifyOptions) int64 {
	for i := 0; i <= opts.RetryCount; i++ {
		n, err := col.CountDocuments(ctx, bson.M{})
		if err == nil { return n }
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
	}
	return 0
}

func findWithRetry(ctx context.Context, col *mongo.Collection,
	docID interface{}, result *bson.M, opts DataVerifyOptions) error {

	for i := 0; i <= opts.RetryCount; i++ {
		tCtx, cancel := context.WithTimeout(ctx,
			time.Duration(opts.TimeoutSecs)*time.Second)
		err := col.FindOne(tCtx, bson.M{"_id": docID}).Decode(result)
		cancel()
		if err == nil { return nil }
		if i < opts.RetryCount {
			time.Sleep(time.Duration(opts.RetryWaitMS) * time.Millisecond)
		}
		_ = err
	}
	return fmt.Errorf("not found after %d retries", opts.RetryCount)
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
