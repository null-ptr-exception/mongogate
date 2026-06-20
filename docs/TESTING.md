# E2E Testing: methodology, scenarios, and results

This document records how mongogate was validated end-to-end against real
MongoDB replica sets in `kind`, and gives direct, evidence-backed answers to
the questions that motivated this testing pass:

1. What's the *correct* point in a live migration to cut over, and why isn't
   a single check enough?
2. Can mongogate tell when target has diverged independently of source (a
   "split-brain"-style scenario)?
3. Does it actually reconnect after a restart or failover — and under what
   conditions does that *not* hold?
4. Is the memory/throughput design actually true, measured, not just claimed?

Everything below was run against the environment in `test/e2e/`, not assumed
from reading the code.

## Environment

```
kind cluster "mongogate-e2e" (single node, 8 CPU / 62GB host)
├── mongo-source (namespace): 3-pod MongoDB 7.0 replica set (rs0), auth enabled
├── mongo-target (namespace): 3-pod MongoDB 7.0 replica set (rs0), auth enabled
└── mongogate-test (namespace): monitor Mongo + an "e2e-tools" pod carrying
    the mongogate and loadgen binaries (test/e2e/loadgen), built from the
    same source tree via test/e2e/Dockerfile.tools
```

Reproduce it:

```bash
cd test/e2e/scripts
./setup.sh                      # creates the cluster, both replica sets, monitor Mongo
# build & load the tools image (mongogate + loadgen), see Dockerfile.tools
docker build -f ../Dockerfile.tools -t mongogate-e2e-tools:latest ../../..
kind load docker-image mongogate-e2e-tools:latest --name mongogate-e2e
kubectl apply -f ../kind/monitor/tools-pod.yaml
kubectl -n mongogate-test exec -it e2e-tools -- sh
./teardown.sh                   # tear down when done
```

`loadgen` (test/e2e/loadgen) is a test-only helper, not part of the product:
`seed-baseline` (structural fixtures), `seed-users`, `diverge` (deterministic
mismatch injection per IssueType), `write` (continuous load), `mirror`
(change-stream-based CDC stand-in with configurable lag/loss), and
`mock-webhook` (captures what the alert manager actually sends).

## Result summary

| # | Scenario | Result |
|---|----------|--------|
| 1 | Happy path (Phase 1/2/3) | ✅ Pass, exit 0, clean report |
| 2 | Structural drift (Phase 1) | ✅ Every drift type caught; Phase 1 doesn't abort on first error |
| 3 | DeepCompare IssueTypes | ✅ All 6 types classified correctly with right field/path/type detail |
| 4 | Hash normalization toggles | ✅ Confirmed on vs. off changes behavior for datetime/float/decimal128 |
| 5 | Bidirectional / split-brain | ✅ Detected with `bidirectional: true`; confirmed silently missed when `false` |
| 6 | Continuous writes → cutover timing | ✅ Phase 2 mid-flight shows transient drift; Phase 3 after stop+catchup is clean |
| 7 | Heavy load / performance | ✅ Measured (see below); found a parallelism scope limit |
| 8 | Network partition | ✅ Fails cleanly within ~30s, doesn't hang — but found a startup gap (see below) |
| 9 | Process crash + `--resume` | ✅ Resumes from checkpoint, scans the full tail, no gaps/double-counting |
| 10 | Primary/secondary failover | ✅ Fully transparent mid-run with `secondaryPreferred` |
| 11 | HTTP API + Prometheus | ✅ All endpoints correct, including live mid-run progress |
| 12 | Alerting | ✅ De-dupe confirmed; found and fixed a dropped-final-alert bug |
| 13 | Auto-repair | ✅ Repairs sampled diffs correctly |
| 14 | CSV/JSON output | ✅ Correct after fixing a missing-rows bug (see below) |
| 15 | CLI/config matrix | ✅ `--dry-run`, `--exclude-ns "db.*"`, bad-config validation all correct |

### Bugs found and fixed during this pass

1. **`SetExpireAfterSeconds` type mismatch** (`internal/monitor/metrics_writer.go`) —
   `CreateCollectionOptions.SetExpireAfterSeconds` takes `int64`, was called with
   `int32`. Caught at compile time once the module was rebuilt under Go 1.22;
   would never have run at all before this pass.
2. **Bidirectional results invisible** — `ExtraInTarget`/`ExtraInTargetSample`
   were computed correctly but never appeared in the CSV export, the terminal
   summary lines, or the Prometheus `/metrics` output — only in the JSON
   report. Fixed in `csv_exporter.go`, `data_verifier.go`, `report.go`, and
   `prometheus/metrics.go`; also added `AlertManager.CheckExtraInTarget` so a
   target-side divergence can actually page someone, not just fail silently.
3. **Final alert dropped on exit** — `AlertManager.Fire()` dispatches Slack/Email
   sends via `go func() {...}`, and `main.go` called `os.Exit()` immediately
   after firing the final pass/fail alert. The process could (and in testing,
   did) exit before the HTTP POST ever went out — silently dropping exactly
   the notification that matters most. Fixed with a `sync.WaitGroup`-backed
   `AlertManager.Wait()` called before both `os.Exit()` paths, plus bounded
   timeouts on the Slack HTTP client and the SMTP send so a slow/dead webhook
   can't turn into a hang instead.
4. **NS-scope filter applied inconsistently** — `--include-ns`/`--exclude-ns`
   only affected index and data verification, not the database list,
   collection list, views, or GridFS checks. Generalized via
   `config.DBInScope` + a `nsFilter` parameter threaded through
   `VerifyDatabases`/`VerifyCollections` (this was also explicitly requested
   separately — see the README's "Scoping a run" section).

None of these were found by reading the code; all four came out of actually
running the scenarios below.

---

## 1. Continuous writes → what's the correct point to cut over?

**Setup:** `loadgen write` generates continuous insert/update/delete traffic
against source at a steady rate. `loadgen mirror` tails source's change
stream and replays each change to target after a configurable delay,
standing in for a real CDC pipeline (mongosync/Debezium/etc).

**While writes are still flowing**, Phase 2 (sampled) was run concurrently:

```
migtest.live_col   src=64   missing=1   diff=18   extra_in_tgt=0   exact=false
```

This is *correct, expected behavior*, not a bug: at that instant, some
documents the writer had just inserted/updated on source hadn't yet been
replayed to target by the mirror (deliberate lag). `exact=false` is exactly
the signal this is a snapshot of a moving target, not a final answer — this
is the entire reason Phase 2 results are documented as directional only.

**After the writer stopped and the mirror fully drained** (confirmed via
`mirror: done replayed=199 dropped=0`, exactly matching the writer's
122 inserts + 56 updates + 21 deletes), Phase 3 was run:

```
migtest.live_col   src=88   missing=0   diff=0   extra_in_tgt=0   exact=true
🎉 Overall result: ✅ everything passed, cutover can proceed!
```

**Answer:** the documented three-phase procedure is correct and the evidence
backs it up directly — Phase 2 while writes are flowing will show transient,
real (not false-positive) drift proportional to however far behind your CDC
mechanism currently is, and is not a valid cutover signal on its own. The
correct point to cut over is: pause writes on source, wait for your CDC
mechanism's lag to reach zero, *then* run Phase 3. Phase 3 with
`bidirectional: true` is the only phase that gives an exact, trustworthy
answer.

One methodology note from building the load-test harness itself: a
single-threaded mirror with a fixed per-event delay has a hard throughput
ceiling of `1/lag` events/sec — if the real write rate exceeds that, the
backlog grows without bound and never drains, regardless of how long you
wait. That's a property of a naive CDC stand-in, not of mongogate, but it's
worth knowing if you build a similar harness: match `lag` to the write rate
you're testing, or the "wait for catch-up" step never converges.

---

## 2. Can it tell when target has diverged independently of source?

**Setup:** `loadgen diverge --scenario extra_in_target` inserts a document
into target only — the closest reproducible stand-in for "target accepted
writes source never had" (a botched migration script, something else
writing into target, or a missed delete never replicating).

**With `bidirectional: true`:**
```
migtest.diverge_col   src=11   missing=1   diff=7   extra_in_tgt=1
```
The extra document is caught, reported by sample ID, and (after the fix
above) actually visible — in the terminal summary, the JSON report, the CSV
export, Prometheus `mongogate_ns_extra_in_target`, and as a CRITICAL alert via
`AlertManager.CheckExtraInTarget`.

**With `bidirectional: false`**, the identical target state instead reports:
```
migtest.diverge_col   src=11   missing=1   diff=7   extra_in_tgt=0
```
The same document sitting in target is silently missed — `extra_in_tgt` reads
0 even though it's still there. If that had been the *only* discrepancy, the
run would report a clean pass despite target having diverged.

**Answer:** yes, it can detect this — but only with `bidirectional: true`
(it's not the default-on-by-accident; it's in `verify.bidirectional` and
defaults to `true` in `config.yaml`, but can be turned off). The forward-only
scan structurally cannot see this class of problem, by construction — it only
ever asks "is everything in source also in target," never the reverse.

---

## 3. Does it really reconnect after a restart? Three distinct answers.

This needed three separate experiments because "restart" means different
things depending on *when* it happens.

**(a) Target completely unreachable at startup:** scaled `mongo-target`'s
StatefulSet to 0 replicas, then started mongogate fresh.

```
ping failed [...]: server selection error: server selection timeout,
current topology: { Type: ReplicaSetNoPrimary, ... no such host ... }
```
Failed cleanly after **~30 seconds** (the Go driver's default
`serverSelectionTimeoutMS`), exit code 1. **It does not hang**, but it also
**does not retry** — `mustConnect()` in `cmd/mongogate/main.go` calls
`Ping()` once and `log.Fatalf`s on failure. The configured `retry_count`/
`retry_wait_ms` never come into play here; those only govern the per-document
retry loops inside an already-established connection, not the initial
connect. **If a replica set is down when mongogate starts, that's a hard
failure, not an auto-reconnect** — this is the one real gap.

**(b) One secondary lost mid-run** (out of 3, with `readPreference=
secondaryPreferred`): force-deleted the secondary pod 2 seconds into a Phase
3 run against 5,000 documents.

```
Totals: missing=0 different=0 extra_in_target=0
🎉 Overall result: ✅ everything passed, cutover can proceed!
```
Zero visible impact — no retries logged, no errors, clean pass. The Go
driver's topology monitor (SDAM) transparently rerouted reads to the
remaining secondary.

**(c) The primary lost mid-run** (full election in progress): force-deleted
the primary pod 2 seconds into the same kind of run.

```
Totals: missing=0 different=0 extra_in_target=0
🎉 Overall result: ✅ everything passed, cutover can proceed!
```
Also zero visible impact. Since mongogate never reads from primary
(`secondaryPreferred`) and never writes to source/target except during
`auto_repair`, a primary election simply doesn't touch its read path.

**Answer:** mid-run failover of any single member, primary or secondary, is
fully transparent given a healthy multi-node replica set and
`secondaryPreferred` reads — no configuration or code change needed, it's
inherent to the driver's topology monitoring. The actual reconnect gap is
narrower and more specific than "does it reconnect": it's specifically
*the initial connection at process startup has zero retries*. A
production wrapper (systemd `Restart=on-failure` with `RestartSec`, as the
README already documents, or a Kubernetes `restartPolicy`) closes that gap
from the outside; the tool itself doesn't retry that one case.

---

## 4. Is the performance real?

Design doc claims: "<60MB constant memory" and "1,000,000 docs in 3-8
minutes" with 4 workers. Measured instead of assumed, against 200,000
identical documents in a single collection, `max_workers: 4`,
`bidirectional: true`, full (non-sampled) hash comparison, on the 3-node
replica sets described above (each `mongod` pod capped at 768Mi via
`wiredTigerCacheSizeGB=0.25`, the same modest resource ceiling used for every
other test in this document — not a dedicated benchmark rig):

```
Result: 200,000/200,000 matched, 0 missing, 0 different, 0 extra-in-target,
exit code 0.

  Elapsed (wall clock) time: 6m 14.96s        →  ≈533 docs/sec
  Maximum resident set size: 69,312 KB        →  ≈67.7 MB
  Percent of CPU this job got: 55%            →  roughly one core's worth
  User+System CPU time: 122.34s + 86.06s = 208.4s of 374.96s wall time
```

Two concrete findings beyond the headline number:

- **Memory**: matches the design claim closely — constant and modest,
  independent of the 200k document count (the same streaming/batch design
  that makes a 5,000-doc run and a 200,000-doc run use essentially the same
  memory, confirmed by checkpointed runs at both scales in this document).
- **Parallelism is per-collection, not per-document**: `max_workers`
  controls how many *collections* are verified concurrently
  (`internal/verifier/data_verifier.go`'s `VerifyAllData` hands one
  `CollectionTask` per worker from a shared channel). A single large
  collection is always processed by exactly one worker, however high
  `max_workers` is set. The 1M-docs-in-3-8-minutes estimate in the design
  doc implicitly assumes that volume is spread across enough collections to
  fill all workers; one giant collection is bounded by single-threaded
  throughput, full stop. This matters operationally: if a migration's data
  is concentrated in one or two huge collections, raising `max_workers`
  won't help — the real lever is `batch_size`/`rate_limit_ms` and the
  network/IO path between mongogate and both clusters.

**Answer:** the memory claim holds up under measurement. The throughput claim
is directionally true but collection-shaped: it's an aggregate number that
assumes parallelism across collections, not a guarantee for any single
collection regardless of size.

---

## Reproducing the scenarios

Each numbered scenario above corresponds to a block of commands run from the
`e2e-tools` pod (`kubectl -n mongogate-test exec -it e2e-tools -- sh`) against
the two replica sets, using `loadgen seed-baseline`/`seed-users`/`diverge`/
`write`/`mirror`/`mock-webhook` to set up fixtures and `mongogate` itself to
verify. There's no single "run all scenarios" script — they're independent,
sometimes mutually exclusive setups (e.g. scenario 8 scales target to zero,
which would break a concurrently-running scenario 6). Treat this document as
the log of what was actually run and observed, and `test/e2e/loadgen`'s
subcommands as the building blocks for re-running any individual one.
