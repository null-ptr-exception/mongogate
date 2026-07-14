package verifier

import (
	"context"
	"testing"
)

// computeRanges' n<=1 short-circuit is pure logic - it returns before
// touching the collection at all, so it's safe to exercise without a live
// MongoDB connection (unlike the splitVector/$bucketAuto paths, which need
// a real server and are covered by test/e2e instead).
func TestCapRanges_AlreadyWithinLimit(t *testing.T) {
	in := []IDRange{{Min: nil, Max: 1}, {Min: 1, Max: nil}}
	out := capRanges(in, 4)
	if len(out) != 2 {
		t.Errorf("capRanges should not change a list already at/under n, got %d ranges", len(out))
	}
}

func TestCapRanges_MergesDownToN(t *testing.T) {
	// 9 ranges (as splitVector's byte-size-based splitting can produce for
	// a requested n=4, if avgObjSize is even slightly off) must merge down
	// to exactly 4 - range_workers_per_collection is validated against
	// max_workers on the assumption it's a hard cap on concurrency.
	in := make([]IDRange, 9)
	for i := range in {
		var min, max interface{}
		if i > 0 {
			min = i
		}
		if i < 8 {
			max = i + 1
		}
		in[i] = IDRange{Min: min, Max: max}
	}
	out := capRanges(in, 4)
	if len(out) > 4 {
		t.Fatalf("capRanges(9 ranges, n=4) returned %d ranges, want <= 4", len(out))
	}
	// Merged ranges must stay contiguous and gap-free: first starts
	// unbounded, last ends unbounded, and each boundary in between must
	// chain to the next range's Min.
	if out[0].Min != nil {
		t.Errorf("first merged range should start unbounded, got Min=%v", out[0].Min)
	}
	if out[len(out)-1].Max != nil {
		t.Errorf("last merged range should end unbounded, got Max=%v", out[len(out)-1].Max)
	}
	for i := 0; i < len(out)-1; i++ {
		if out[i].Max != out[i+1].Min {
			t.Errorf("gap between merged range %d (Max=%v) and %d (Min=%v)", i, out[i].Max, i+1, out[i+1].Min)
		}
	}
}

func TestComputeRanges_NoSplitRequested(t *testing.T) {
	for _, n := range []int{0, 1} {
		ranges, method := computeRanges(context.Background(), nil, n)
		if len(ranges) != 1 {
			t.Fatalf("computeRanges(n=%d) returned %d ranges, want 1", n, len(ranges))
		}
		if ranges[0].Min != nil || ranges[0].Max != nil {
			t.Errorf("computeRanges(n=%d) range = %+v, want fully unbounded", n, ranges[0])
		}
		if method != "none" {
			t.Errorf("computeRanges(n=%d) method = %q, want %q", n, method, "none")
		}
	}
}
