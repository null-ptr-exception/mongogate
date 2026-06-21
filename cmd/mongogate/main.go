package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/null-ptr-exception/mongogate/internal/alert"
	"github.com/null-ptr-exception/mongogate/internal/config"
	"github.com/null-ptr-exception/mongogate/internal/export"
	"github.com/null-ptr-exception/mongogate/internal/httpapi"
	"github.com/null-ptr-exception/mongogate/internal/monitor"
	promMetrics "github.com/null-ptr-exception/mongogate/internal/prometheus"
	"github.com/null-ptr-exception/mongogate/internal/repair"
	"github.com/null-ptr-exception/mongogate/internal/report"
	"github.com/null-ptr-exception/mongogate/internal/utils"
	"github.com/null-ptr-exception/mongogate/internal/verifier"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	phase := flag.String("phase", "all", "1, 2, 3, or all")
	resume := flag.Bool("resume", false, "resume a Phase 3 run from checkpoint")
	dryRun := flag.Bool("dry-run", false, "scan without comparing (connectivity/counts only)")
	sample := flag.Float64("sample", -1, "override sample_rate (0-1)")
	includeNS := flag.String("include-ns", "", "single namespace to include (db.col or db.*)")
	excludeNS := flag.String("exclude-ns", "", "single namespace to exclude")
	autoRepair := flag.Bool("auto-repair", false, "automatically copy diffs from source to target")
	exportCSV := flag.String("export-csv", "", "path to export diff CSV (or 'auto')")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	if *dryRun {
		cfg.DryRun = true
	}
	if *sample >= 0 {
		cfg.SampleRate = *sample
	}
	if *includeNS != "" {
		cfg.IncludeNS = append(cfg.IncludeNS, *includeNS)
	}
	if *excludeNS != "" {
		cfg.ExcludeNS = append(cfg.ExcludeNS, *excludeNS)
	}
	if *autoRepair {
		cfg.AutoRepair = true
	}
	if *exportCSV != "" {
		cfg.ExportCSV = *exportCSV
	}

	// Job-scope the checkpoint file to this source/target pair: a single
	// hardcoded checkpoints.json would let two different migration jobs run
	// from the same working directory silently clobber each other's --resume
	// state.
	jobKey := sha256.Sum256([]byte(cfg.SourceURI + "|" + cfg.TargetURI))
	utils.SetCheckpointFile(fmt.Sprintf("checkpoints_%s.json", hex.EncodeToString(jobKey[:6])))

	runPhase1 := *phase == "1" || *phase == "all"
	runPhase2 := *phase == "2" || *phase == "all"
	runPhase3 := *phase == "3" || *phase == "all"
	if !runPhase1 && !runPhase2 && !runPhase3 {
		fmt.Fprintf(os.Stderr, "unknown --phase %q (want 1, 2, 3, or all)\n", *phase)
		os.Exit(1)
	}

	ctx := context.Background()

	src := mustConnect(ctx, "source", cfg.SourceURI)
	defer func() { _ = src.Disconnect(ctx) }()
	tgt := mustConnect(ctx, "target", cfg.TargetURI)
	defer func() { _ = tgt.Disconnect(ctx) }()

	var mw *monitor.MetricsWriter
	if monClient := softConnect(ctx, "monitor", cfg.MonitorURI); monClient != nil {
		defer func() { _ = monClient.Disconnect(ctx) }()
		mw, err = monitor.NewMetricsWriter(monClient)
		if err != nil {
			fmt.Printf("[monitor] unavailable, continuing without it: %v\n", err)
		}
	}

	rpt := report.New()
	verifier.FetchVersionInfo(ctx, src, tgt, rpt)
	am := alert.New(cfg.Alert)
	pm := promMetrics.New(cfg.PrometheusPort, rpt)
	pm.Start()
	httpSrv := httpapi.New(cfg.HTTPPort, rpt)
	httpSrv.Start()
	defer httpSrv.Stop()

	// Collection discovery is a hard prerequisite for Phase 2/3 (you can't
	// data-verify a collection you haven't enumerated), so it always runs
	// regardless of which --phase was requested; only the independent
	// structural checks (auth, cluster, indexes, views, GridFS metadata) are
	// gated behind runPhase1.
	dbs := verifier.VerifyDatabases(ctx, src, tgt, cfg.SkipDBs, cfg.DBInScope, rpt)

	if runPhase1 {
		if cfg.Verify.Auth {
			verifier.VerifyAuth(ctx, src, tgt, rpt)
		}
		if cfg.Verify.Cluster {
			verifier.VerifyCluster(ctx, src, tgt, cfg.Verify.Sharding, rpt)
		}
	}

	var collections []verifier.CollectionTask
	for _, db := range dbs {
		cols := verifier.VerifyCollections(ctx, src, tgt, db, cfg.NSFilter, rpt)

		if runPhase1 {
			if cfg.Verify.Index {
				for _, col := range cols {
					verifier.VerifyIndexes(ctx, src, tgt, db, col, rpt)
				}
			}
			if cfg.Verify.Views {
				verifier.VerifyViews(ctx, src, tgt, db, rpt)
			}
			if cfg.Verify.GridFS {
				// Metadata only here; content MD5 is deferred to Phase 3 (see
				// README's three-phase strategy) since hashing every file is
				// exact-comparison work, not structural.
				verifier.VerifyGridFS(ctx, src, tgt, db, false, rpt)
			}
		}

		for _, col := range cols {
			if isGridFSInternal(col) {
				// fs.files/fs.chunks are already verified properly by
				// VerifyGridFS (content hash + metadata, both sides read
				// through the GridFS API). Generic per-document comparison
				// here would always false-positive: fs.chunks' per-chunk
				// _id is a fresh ObjectID minted independently on each
				// upload, and fs.files' uploadDate is wall-clock time at
				// upload - neither matches across independently-seeded
				// sides even when the actual file content is identical.
				continue
			}
			collections = append(collections, verifier.CollectionTask{DBName: db, ColName: col})
		}
	}

	if *phase == "all" && (runPhase2 || runPhase3) {
		promptContinue("Phase 1 done. Press Enter to continue to Phase 2 (sampled)...")
	}

	if runPhase2 {
		opts := verifier.DataVerifyOptions{
			BatchSize: cfg.BatchSize, MaxWorkers: cfg.MaxWorkers,
			RateLimitMS: cfg.RateLimitMS, TimeoutSecs: cfg.TimeoutSecs,
			RetryCount: cfg.RetryCount, RetryWaitMS: cfg.RetryWaitMS,
			SampleRate: cfg.SampleRate, Phase: "Phase2",
			Bidirectional: cfg.Verify.Bidirectional, DryRun: cfg.DryRun,
			HashOpts: toHashOptions(cfg.HashOptions),
		}
		verifier.VerifyAllData(ctx, src, tgt, collections, opts, mw, am, pm, rpt)
	}

	if *phase == "all" && runPhase3 {
		promptContinue("Phase 2 done. Press Enter to continue to Phase 3 (full & exact)...")
	}

	if runPhase3 {
		opts := verifier.DataVerifyOptions{
			BatchSize: cfg.BatchSize, MaxWorkers: cfg.MaxWorkers,
			RateLimitMS: cfg.RateLimitMS, TimeoutSecs: cfg.TimeoutSecs,
			RetryCount: cfg.RetryCount, RetryWaitMS: cfg.RetryWaitMS,
			SampleRate: 0, Resume: *resume, Phase: "Phase3",
			Bidirectional: cfg.Verify.Bidirectional, DryRun: cfg.DryRun,
			HashOpts: toHashOptions(cfg.HashOptions),
		}
		verifier.VerifyAllData(ctx, src, tgt, collections, opts, mw, am, pm, rpt)

		if cfg.Verify.GridFS {
			for _, db := range dbs {
				verifier.VerifyGridFS(ctx, src, tgt, db, true, rpt)
			}
		}
	}

	rpt.Print()
	if err := rpt.Save(); err != nil {
		fmt.Printf("failed to save JSON report: %v\n", err)
	}
	if err := export.ExportDiffCSV(rpt, cfg.ExportCSV); err != nil {
		fmt.Printf("failed to export CSV: %v\n", err)
	}
	if cfg.AutoRepair {
		repair.New(src, tgt).RepairAll(ctx, rpt)
	}

	passed := rpt.AllPassed()
	level, title := "INFO", "✅ Verification passed"
	if !passed {
		level, title = "CRITICAL", "🚨 Verification failed"
	}
	am.Fire(alert.AlertEvent{Level: level, Title: title, Message: fmt.Sprintf("phase=%s", *phase)})
	am.Wait() // block for in-flight Slack/Email sends before os.Exit - see internal/alert/alert.go

	if passed {
		os.Exit(0)
	}
	os.Exit(1)
}

func mustConnect(ctx context.Context, label, uri string) *mongo.Client {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(cctx, options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s connect failed [%s]: %v\n", label, uri, err)
		os.Exit(1)
	}
	if err := client.Ping(cctx, nil); err != nil {
		fmt.Fprintf(os.Stderr, "%s ping failed [%s]: %v\n", label, uri, err)
		os.Exit(1)
	}
	return client
}

// softConnect is for the monitor connection only: monitoring is a nice-to-have
// (internal/monitor.MetricsWriter is nil-safe), so a failure here must not
// abort verification itself.
func softConnect(ctx context.Context, label, uri string) *mongo.Client {
	if uri == "" {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(cctx, options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Printf("[%s] connect failed, continuing without it: %v\n", label, err)
		return nil
	}
	if err := client.Ping(cctx, nil); err != nil {
		fmt.Printf("[%s] ping failed, continuing without it: %v\n", label, err)
		return nil
	}
	return client
}

func promptContinue(msg string) {
	fmt.Printf("\n%s ", msg)
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
}

// isGridFSInternal matches the default GridFS bucket prefix ("fs") used by
// both loadgen and this codebase's own VerifyGridFS. A custom bucket prefix
// (gridfs.NewBucket with BucketOptions.SetName) would not be caught here -
// out of scope for this check, since nothing in this codebase uses one.
func isGridFSInternal(colName string) bool {
	return colName == "fs.files" || colName == "fs.chunks"
}

func toHashOptions(h config.HashOptions) utils.HashOptions {
	return utils.HashOptions{
		NormalizeDatetime:       h.NormalizeDatetime,
		NormalizeFloatPrecision: h.NormalizeFloatPrecision,
		NormalizeDecimal128:     h.NormalizeDecimal128,
		SortArrays:              h.SortArrays,
		CheckBinarySubtype:      h.CheckBinarySubtype,
		StrictObjectID:          h.StrictObjectID,
	}
}
