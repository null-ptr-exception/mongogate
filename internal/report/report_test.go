package report

import (
	"sync"
	"testing"
)

func TestMergeData_UnsplitCompletesImmediately(t *testing.T) {
	r := New()
	// No BeginRanges call - mirrors every namespace that was never split,
	// which must behave exactly like today's single SetData call: complete
	// on the first (and only) MergeData.
	combined, isLast := r.MergeData("db.col", &DataResult{
		NS: "db.col", SrcCount: 100, TgtCount: 100,
	})
	if !isLast {
		t.Fatal("an unsplit namespace (never registered via BeginRanges) must complete on its first MergeData call")
	}
	if combined.SrcCount != 100 || combined.TgtCount != 100 {
		t.Errorf("combined counts = src=%d tgt=%d, want 100/100", combined.SrcCount, combined.TgtCount)
	}
	if !combined.Passed {
		t.Error("matching counts with no errors/missing/different should pass")
	}
}

func TestMergeData_AccumulatesAcrossRanges(t *testing.T) {
	r := New()
	r.BeginRanges("db.col", 3)

	var last bool
	var combined *DataResult
	deltas := []*DataResult{
		{NS: "db.col", SrcCount: 10, TgtCount: 10, MissingCount: 1},
		{NS: "db.col", SrcCount: 20, TgtCount: 20, DifferentCount: 2},
		{NS: "db.col", SrcCount: 30, TgtCount: 29, ExtraInTarget: 1},
	}
	for i, d := range deltas {
		combined, last = r.MergeData("db.col", d)
		wantLast := i == len(deltas)-1
		if last != wantLast {
			t.Fatalf("call %d: isLast = %v, want %v", i, last, wantLast)
		}
	}

	if combined.SrcCount != 60 {
		t.Errorf("SrcCount = %d, want 60 (10+20+30)", combined.SrcCount)
	}
	if combined.TgtCount != 59 {
		t.Errorf("TgtCount = %d, want 59 (10+20+29)", combined.TgtCount)
	}
	if combined.MissingCount != 1 || combined.DifferentCount != 2 || combined.ExtraInTarget != 1 {
		t.Errorf("combined = %+v, want missing=1 different=2 extra=1", combined)
	}
	if combined.CountMatch {
		t.Error("CountMatch should be false: 60 src vs 59 tgt")
	}
	if combined.Passed {
		t.Error("a namespace with missing/different/extra/count-mismatch must not pass")
	}
}

func TestMergeData_PassedNotSetBeforeLastRange(t *testing.T) {
	r := New()
	r.BeginRanges("db.col", 2)

	combined, isLast := r.MergeData("db.col", &DataResult{NS: "db.col", SrcCount: 5, TgtCount: 5})
	if isLast {
		t.Fatal("only 1 of 2 ranges reported in - isLast must be false")
	}
	// Passed/CountMatch must stay at their zero value until every range has
	// reported - a caller peeking at the accumulator mid-flight (e.g. the
	// HTTP API) must not see a premature "passed".
	if combined.Passed {
		t.Error("Passed must not be computed before the last range reports in")
	}
}

func TestMergeData_ConcurrentRangesAggregateExactly(t *testing.T) {
	r := New()
	const n = 50
	r.BeginRanges("db.col", n)

	var wg sync.WaitGroup
	var lastCount int32
	var mu sync.Mutex
	var finalCombined *DataResult

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			combined, isLast := r.MergeData("db.col", &DataResult{
				NS: "db.col", SrcCount: 1, TgtCount: 1, MissingCount: 1,
			})
			if isLast {
				mu.Lock()
				lastCount++
				finalCombined = combined
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if lastCount != 1 {
		t.Fatalf("exactly one call should observe isLast=true, got %d", lastCount)
	}
	if finalCombined.SrcCount != n || finalCombined.TgtCount != n {
		t.Errorf("combined counts = src=%d tgt=%d, want %d/%d", finalCombined.SrcCount, finalCombined.TgtCount, n, n)
	}
	if finalCombined.MissingCount != n {
		t.Errorf("MissingCount = %d, want %d", finalCombined.MissingCount, n)
	}
}

func TestMergeData_ExactFlagsSurviveAPriorUpdateProgressCall(t *testing.T) {
	// UpdateProgress creates a bare placeholder DataResult{NS, ProgressPct}
	// the moment the first progress tick arrives, mid-scan - well before
	// MergeData's own call for that task completes. That placeholder's
	// CountIsExact/HashIsExact are Go zero-values (false). A regression
	// here would make every Phase 3 (exact) run past the first progress
	// tick (1000 docs) permanently report exact=false.
	r := New()
	r.UpdateProgress("db.col", 42.0)

	combined, isLast := r.MergeData("db.col", &DataResult{
		NS: "db.col", SrcCount: 5000, TgtCount: 5000,
		CountIsExact: true, HashIsExact: true,
	})
	if !isLast {
		t.Fatal("an unsplit namespace must complete on its first MergeData call")
	}
	if !combined.CountIsExact {
		t.Error("CountIsExact = false, want true - a prior UpdateProgress call must not poison it")
	}
	if !combined.HashIsExact {
		t.Error("HashIsExact = false, want true - a prior UpdateProgress call must not poison it")
	}
}

func TestMergeData_SamplesCappedAcrossRanges(t *testing.T) {
	r := New()
	const ranges = 5
	r.BeginRanges("db.col", ranges)

	var combined *DataResult
	for i := 0; i < ranges; i++ {
		// 20 missing-sample ids per range * 5 ranges = 100 raw ids, but the
		// cap (50, matching the per-task cap in data_verifier.go) must hold
		// across the merge, not just within one range's own contribution.
		samples := make([]string, 20)
		for j := range samples {
			samples[j] = "id"
		}
		combined, _ = r.MergeData("db.col", &DataResult{
			NS: "db.col", MissingSample: samples,
		})
	}
	if len(combined.MissingSample) != 50 {
		t.Errorf("MissingSample length = %d, want capped at 50", len(combined.MissingSample))
	}
}
