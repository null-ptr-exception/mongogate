package verifier

import (
	"context"
	"testing"
)

// computeRanges' n<=1 short-circuit is pure logic - it returns before
// touching the collection at all, so it's safe to exercise without a live
// MongoDB connection (unlike the splitVector/$bucketAuto paths, which need
// a real server and are covered by test/e2e instead).
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
