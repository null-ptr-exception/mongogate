# Worker-count throughput benchmark

Interactive version (charts, hover tooltips): https://claude.ai/code/artifact/d961ccee-18c4-4da2-a75a-18f97a771b2b
(private link, scoped to the session that generated it - this file is the durable, in-repo record).

## Methodology - read this before the numbers

This sandbox has 154GB of free disk. A real 500GB/1TB source+target pair doesn't fit,
so this is **small-scale real measurement + linear extrapolation**, not a real 1TB run:

- **Measured**: ~1GB / 2,000,000 documents, two standalone `mongod` containers on one
  8-core / 62GB host, connected over the docker bridge network (loopback-class latency).
- **Extrapolated**: 10G/100G/500G/1T times, computed from the measured MB/s at each
  worker count, assuming throughput stays constant as data volume grows.

That assumption holds reasonably well locally, but ignores everything that changes at
real production scale: cross-host network latency (this test's RTT is sub-millisecond;
a real cross-region migration can be 20-100ms+ per round trip), the actual source/target
cluster's own CPU/IO headroom, and data compressibility (the test payload is a repeated
padding string - highly compressible, unlike most real production documents, which
understates real I/O cost). **Treat the 500G/1T numbers as an optimistic lower bound,
not a promise.**

Both source and target were seeded identically (0 diffs) to isolate scan throughput from
diff-handling overhead. Every run is Phase 3 (full, exact, bidirectional).

## Results

Two scenarios, since `mongogate` scales two different ways:

- **Cross-collection**: 8 collections x 125,000 docs, parallelism from `max_workers`.
- **Range-split**: 1 collection x 1,000,000 docs, forced above `range_split_threshold_docs`,
  parallelism from `range_workers_per_collection` - this is the scenario relevant to a
  single huge (1TB-class) collection, the case this feature was built for.

| Scenario | Workers | Elapsed | docs/sec | MB/sec | Speedup | Peak RSS | CPU cores used |
|---|---|---|---|---|---|---|---|
| Cross-collection | 1 | 40.5s | 24,689 | 22.3 | 1.00x | 36 MB | 1.08 |
| Cross-collection | 2 | 23.2s | 43,040 | 39.0 | 1.74x | 54 MB | 1.99 |
| Cross-collection | 4 | 14.9s | 66,984 | 60.6 | 2.71x | 97 MB | 3.50 |
| Cross-collection | 8 | 11.8s | 84,969 | 76.9 | 3.44x | 217 MB | 4.75 |
| Range-split | 1 | 38.0s | 26,297 | 23.8 | 1.00x | 34 MB | 1.10 |
| Range-split | 2 | 23.1s | 43,369 | 39.3 | 1.65x | 54 MB | 2.01 |
| Range-split | 4 | 14.4s | 69,286 | 62.7 | 2.63x | 92 MB | 3.57 |
| Range-split | 8 | 11.0s | 90,670 | 82.1 | 3.45x | 207 MB | 5.06 |

A single huge collection now scales almost identically to spreading the same volume
across many medium collections (3.45x vs 3.44x at 8 workers) - before range-split, a
single collection was capped at 1 worker regardless of `max_workers` (see
[DESIGN.md](DESIGN.md#9-performance--memory-design)).

## Extrapolated time by data volume (range-split throughput)

| Data volume | 1 worker | 2 workers | 4 workers | 8 workers (measured basis) |
|---|---|---|---|---|
| 10 GB | 7.2 min | 4.3 min | 2.7 min | 2.1 min |
| 100 GB | 1.2 hr | 43.5 min | 27.2 min | 20.8 min |
| 500 GB | 6.0 hr | 3.6 hr | 2.3 hr | 1.7 hr |
| 1 TB | 12.2 hr | 7.4 hr | 4.7 hr | 3.6 hr |

## Recommendations

1. **Diminishing returns show up already at this scale.** 1->2 workers is nearly linear
   (1.65-1.74x), but 4->8 only adds ~0.8x while CPU jumps from ~3.5 to ~5 cores and RSS
   roughly doubles. This host has 8 cores total, shared between both `mongod` containers
   and the tool itself - contention, not a `mongogate` ceiling. The real optimum for a
   production run depends on how much headroom the actual source/target clusters have,
   not this number. Start from `max_workers: 8` (current `config.yaml` default), watch
   source/target CPU/IO, and tune from there.
2. **Range-split delivers real parallelism for a single huge collection now** - matching
   cross-collection scaling instead of being stuck at one worker.
3. **Don't take the 500G/1T extrapolation at face value.** Re-run Phase 2 (sampled)
   against the real source/target cluster first, and recalibrate this table from its
   actual measured throughput - real network latency will very likely push the real
   numbers well past what's shown here.
4. **Memory scales with worker count** (~36MB -> ~217MB, 1->8 workers here). Before
   raising `max_workers` or `range_workers_per_collection` beyond what's shown here,
   verify headroom on a small run first.

## Bugs found while producing these numbers

Running a real ~1GB/2M-doc benchmark (rather than the few-thousand-doc smoke test used
during development) surfaced two real defects in the range-split implementation, both
fixed and covered by regression tests before this data was collected:

- `CountIsExact`/`HashIsExact` stayed permanently `false` past a namespace's first
  progress tick, because `UpdateProgress` pre-creates a bare placeholder `DataResult`
  before `MergeData`'s own call runs, and `MergeData` mistook the placeholder's
  existence for "already initialized."
- A collection split into 4+ concurrent ranges got *slower*, not faster - all range
  workers rendering the shared progress bar on every single document serialized the
  scan behind the bar's mutex and a synchronous stdout write per document. Fixed by
  throttling the bar render to ~10Hz regardless of range-worker count.
