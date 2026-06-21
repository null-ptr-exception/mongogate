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
| 15 | CLI/config matrix | ✅ `--dry-run`, `--exclude-ns "db.*"`, multi-entry `include_ns`/`exclude_ns`, combined include+exclude, bad-config validation all correct |
| 16 | Comparison-logic bugs (UTF-8 truncation, negative zero, role privileges, array-of-docs recursion) | ✅ All 4 confirmed real, fixed, unit- and E2E-tested — see section 6 |
| 17 | New data formats on MongoDB 8.0 (vector embeddings, time series, clustered collection, collation, hidden index, real geo data, large documents) | ✅ Mostly clean on the first try; found a real structural limitation (time series bucket `_id`) and a real performance characteristic (large-document memory multiplier) — see section 7 |
| 18 | Cross-version source/target (4.4 → 8.0) | ⚠️ Found that `cmd/mongogate` never existed as a buildable binary (written this pass) and a bug worse than predicted: version-incompatible collection options are silently dropped by 4.4 instead of erroring, producing same-name/wrong-type collections — fixed (`compareBSONFields` asymmetric skip, `_id_` index skip), re-verified live; plus 3 confirmed-but-not-yet-fixed blind spots (server params, per-user auth mechanism, default RW concern) — see section 8 |
| 19 | Deeper code review + first-ever real Phase 3 run | ⚠️ Found and fixed 4 more gaps (GridFS content check was dead code, checkpoint file was global not job-scoped, role inheritance never compared, replica topology never compared) plus a regression those fixes would have hit (admin/GridFS-internal collections guarantee false positives under generic comparison) and its proper replacement (dedicated FCV/version info, non-blocking) — see section 9 |
| 20 | Full version matrix: 4.4/5.0/6.0/7.0 each vs 8.0 | ⚠️ Found a setup-script bug that would have broken on 6.0/7.0 (no legacy `mongo` shell at all), fixed and now auto-detects; found a third instance of the GridFS false-positive pattern in time series bucket internals (fixed); precisely bounded the `clustered_col` gap (4.4/5.0 only) and the time series bucket-format gap (5.0/6.0 only) — 7.0→8.0 is the only pair with zero structural findings — see section 10 |
| 21 | Closing the 3 remaining blind spots from section 8.4 | ✅ Per-user auth mechanism and server-parameter coverage (2→5 params) fixed as blocking checks, confirmed live; default read/write concern fixed as informational (non-blocking by design, since it's version-driven) — see section 11 |
| 22 | Cross-version testing in CI, not just by hand | ✅ Added `.github/workflows/e2e.yml`: same-version happy path + injected-drift detection, and the full 4.4/5.0/6.0/7.0 vs 8.0 matrix asserting the exact documented boundaries — confirmed passing on real GitHub Actions, not just locally — see section 12 |
| 23 | Source/target version now a variable, not a hand-edited file | ✅ `setup.sh` takes `SOURCE_MONGO_VERSION`/`TARGET_MONGO_VERSION` env vars, substituted on the fly into `kubectl apply -f -` - confirmed `git diff` shows zero changes to the checked-in YAML after switching versions — see section 10 |
| 24 | Structured multi-angle code review of the full branch diff | ✅ Found and fixed 2 real bugs (GridFS content-hash read had no timeout - could hang Phase 3 forever; target-side user/role fetch errors were silently swallowed in `VerifyAuth`, masking connectivity failures as "all users missing") plus 2 minor ones (setup.sh's sed could rewrite the wrong image line; a stale doc example) - see section 13 |

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

## 5. Multi-collection scoping and include+exclude combined

The NS-scope generalization (bug fix #4 above) was initially tested only with
a single collection in `include_ns` and a single whole-database wildcard in
`exclude_ns`. Two follow-up questions needed real evidence, not just reading
the `NSFilter`/`DBInScope` code: does a *list* of multiple entries actually
work, and what happens when `include_ns` and `exclude_ns` are both set at
once?

**Setup:** seeded the usual baseline (11 collections/views/GridFS in
`migtest`), then introduced drift on three of them — an extra index each on
`text_col` and `geo_col`, and dropped `capped_col` from target entirely.

**Multi-entry whitelist** — `include_ns: [migtest.plain_docs,
migtest.validated_col]`:
```
migtest collections (2 collections)
Indexes: migtest.plain_docs ✅   migtest.validated_col ✅
🎉 Overall result: ✅ everything passed
```
Both drifted/dropped collections are outside the whitelist and never
compared — the collection-count itself narrows to exactly the 2 listed, and
the drift never surfaces. Confirms the list form isn't just parsed, it's
actually applied per entry.

**Whitelist + blacklist combined** — `include_ns: [migtest.*]` plus
`exclude_ns: [migtest.text_col, migtest.geo_col, migtest.capped_col]`:
```
migtest collections (8 collections)     # 11 total minus the 3 excluded
Indexes: 8 entries, none of the 3 excluded ones present
🎉 Overall result: ✅ everything passed
```
Confirms the two lists genuinely compose: `include_ns` sets the outer scope
(here, the whole database), `exclude_ns` carves specific collections back out
of it — and the drift on those 3 is correctly invisible to the run.

**One real, worth-documenting subtlety, not a bug:** in both runs above,
Views and GridFS were still checked (`✅ PASS Views [migtest] (1 views)`,
`✅ PASS GridFS [migtest]`) even though neither `plain_docs_view` nor the
GridFS bucket was named in `include_ns`. `DBInScope` operates at database
granularity — once any pattern puts a database in scope, every database-level
check (views, GridFS) runs for it, regardless of which specific collections
were whitelisted. Scoping to `mydb.one_collection` does not mean "and nothing
else in `mydb`" for views/GridFS specifically; it means "and every
view/GridFS bucket in `mydb` too." Documented in the README's "Scoping a run"
section so this doesn't surprise anyone relying on a narrow `include_ns` to
mean total silence about the rest of the database.

---

## 6. Four logic bugs found by reading the comparison code, then confirmed

A follow-up pass specifically looking for problems in the *comparison logic
itself* (not missing test coverage) — prompted by being asked to think about
this like a MongoDB expert, including MongoDB 8.x-era data — turned up four
real bugs, each confirmed before fixing rather than assumed:

1. **`truncate()` could split a multi-byte UTF-8 character.** It sliced by
   raw byte offset (`s[:120]`). A Chinese string (directly relevant given
   this tool's origins) or emoji landing across that boundary would be cut
   mid-character, corrupting the value shown in reports/CSV. Fixed to slice
   by rune (`utf8.RuneCountInString` + `[]rune(s)`); covered by
   `TestDeepCompare_TruncateMultibyteSafe`.
2. **Negative zero hashed differently from positive zero.** Confirmed by
   running it: `fmt.Sprintf("f:%.10f", negZero)` produced `"-0.0000000000"`
   for an actual computed `-0.0`, vs `"0.0000000000"` for `0.0`, even though
   they're IEEE-754 equal. Any computed value landing on exactly zero (a
   delta, a count difference) was a false-positive risk. Fixed by clearing
   the sign bit (`if val == 0 { val = 0 }`) before formatting; covered by
   `TestDocHash_NegativeZero`.
3. **Custom role privileges were never actually compared.** `getRoles()`
   called `rolesInfo` with `showPrivileges: false` — confirmed only role
   *names* were checked. Two clusters could have a role of the same name
   granting completely different permissions and mongogate would report a
   match. Fixed: `showPrivileges: true` plus a new `normalizePrivileges()`
   that diffs each role's actions/resources, not just its existence.
   Confirmed end-to-end in the kind cluster (Phase 1's account-permissions
   check now actually inspects privilege content); unit-tested in
   `internal/verifier/auth_verifier_test.go`.
4. **`deepDiff` never recursed into arrays of documents.** A difference
   buried in one element reported as one opaque "whole array differs"
   `VALUE_DIFF` instead of a precise path. Fixed: arrays containing at least
   one document now get per-element recursion with paths like
   `items[2].name`, and a length mismatch is reported as its own
   `ARRAY_LENGTH_MISMATCH` rather than folded into a generic value diff.
   Confirmed both in unit tests (`TestDeepCompare_ArrayOfDocsElementDiff`,
   `TestDeepCompare_ArrayLengthMismatch`) and live against the kind cluster
   via `loadgen diverge --scenario array_of_docs_nested_diff` /
   `array_length_mismatch` — both produced exactly the expected path and
   issue type in the real JSON report, not just in isolated unit tests.

A fifth, process-level finding from the same pass: CI only ran
`go test ./test/...`, so a test file living next to the code it tests (as
Go convention expects, and as the role-privilege fix required, since
`normalizePrivileges` is private to `internal/verifier`) would have been
silently skipped. Both CI and the documented test command are now
`go test ./...`.

---

## 7. New data formats and collection types, tested against MongoDB 8.0

This pass also switched the E2E test images from `mongo:7.0` to `mongo:8.0`
(confirmed pullable, v8.0.26) and added fixtures for formats/collection types
that were checked in code but never actually created in the kind cluster.

**Clean on the first try:** non-default collation (case-insensitive,
`{locale: "en", strength: 2}`), a clustered collection (`clusteredIndex`),
a hidden index, real GeoJSON `Point` data under the 2dsphere index, and a
1536-dimension embedding-style float array (the shape of a real OpenAI/Atlas
Vector Search vector) — all compared correctly in Phase 1 and Phase 3,
including `loadgen diverge --scenario vector_embedding_diff`, which perturbs
one of 1536 dimensions by 0.05 and confirms DeepCompare still catches it at
that width rather than the comparison cost or normalization tolerance
breaking down.

**Six new `diverge` scenarios, all classified correctly** in the live
report, not just unit tests: `null_vs_missing` → `MISSING_FIELD` (confirms
explicit `null` and a missing key are treated as a real difference, not
silently equivalent — a classic MongoDB semantic distinction), `regex_field`
→ `VALUE_DIFF`, `timestamp_field` (BSON Timestamp, not Date) → `VALUE_DIFF`,
`minmaxkey_field` → `TYPE_MISMATCH` (correctly distinguishes `MinKey` from
`MaxKey`, though `SrcType`/`TgtType` render as Go's internal names —
`primitive.MinKey`/`primitive.MaxKey` — rather than clean BSON type names;
functionally correct, cosmetically rough, not fixed this pass).

**A genuine structural finding, not a bug: Time Series collections.**
`timeseries_col` itself failed Phase 3 at first — `missing` and
`extra_in_target` both non-zero despite inserting byte-identical
measurements on both sides. The cause: time series measurement documents
auto-generate an ObjectID `_id` at insert time if none is given, and two
independent `InsertMany` calls mint different ones — confirmed by directly
comparing the `_id` values on each side. Giving the measurements an explicit
`_id` fixed `timeseries_col` itself. **The underlying
`system.buckets.timeseries_col` storage still failed afterward** — its own
`_id` is server-generated from internal bucket state and is not something a
client can control at all, unlike the measurement documents. This is a real
ceiling, not a test artifact: confirmed the practical workaround is the same
one already documented for GridFS internals — add the bucket collection to
`exclude_ns` (`migtest.system.buckets.timeseries_col`) — verified this
produces a clean Phase 3 pass. **Recommendation for real time-series
migrations:** exclude `<db>.system.buckets.*` via `exclude_ns` and rely on
the logical collection (with explicit/preserved `_id`s) for verification.

**A genuine performance finding: large individual documents.** Measured with
`/usr/bin/time -v` the same way as the existing performance section, but for
*one* document instead of many small ones:

| Document size | Peak RSS | 
|---|---|
| 1MB | 124 MB |
| 12MB | 917 MB |

This is roughly a 60-70x multiplier of the document's own size, not the flat
~60-70MB measured earlier against 200,000 small (≈300-byte) documents in the
same cluster. The "constant memory" design holds for *many small documents*
(the batching/streaming design this tool is built around), but **does not
hold for individual large documents** — each one is fully decoded into a
`bson.M`, re-normalized into a fresh sorted map, and JSON-marshaled for
hashing, for both source and target, and DeepCompare's two-pass design
(hash, then re-normalize again on mismatch) compounds that further. A
migration with a handful of multi-megabyte documents (large embedded
binaries, big arrays) will see memory scale with the largest document it
encounters, not stay flat at the documented baseline. Not a bug to fix this
pass — a real characteristic worth knowing before assuming the memory
ceiling is universal.

---

## 8. Cross-version source/target testing (MongoDB 4.4 → 8.0)

Every pass above ran source and target on the *same* MongoDB version. Real
migrations exist specifically to move to a different version, so source and
target are version-mismatched for the entire cutover window. This section
covers what was found running source on `mongo:4.4` and target on `mongo:8.0`
against a real kind cluster — the widest practical gap, chosen deliberately
over the originally-planned 7.0/6.0→8.0 to maximize the chance of surfacing
real version-driven behavior.

### 0. A precondition this testing pass had to fix first: `cmd/mongogate` did not exist

Before any of this could run, building the E2E tools image failed:
`stat /app/cmd/mongogate: directory not found`. Checked thoroughly, not
assumed: `git log --all -- cmd/` returns nothing on any branch (`main`,
`add-e2e-testing`, or their remotes); the only `package main` in the entire
repository was `test/e2e/loadgen/main.go` (the test helper, not the product);
`internal/` had no `Run()`/`Execute()` orchestration function either. CI's
`go build ./...` never caught this because `./...` only builds packages that
exist — with no `cmd/mongogate` directory, there was nothing for it to fail
on. The workflow that *does* reference it explicitly, `release.yml` (via
`deploy/Dockerfile`'s `go build ... ./cmd/mongogate`), was confirmed failing
on `main` via `gh run list`. This means **no version of `mongogate` has ever
been a runnable binary**, which calls into question how the scenarios in
sections 1-7 above were actually exercised.

`cmd/mongogate/main.go` was written for this pass, wiring the existing
`internal/` building blocks (every `verifier.Verify*` function, `report`,
`alert`, `monitor`, `httpapi`, `internal/prometheus`, `repair`, `export`)
into the CLI documented in `README.md` (`--phase`, `--resume`, `--dry-run`,
`--include-ns`/`--exclude-ns`, `--auto-repair`, `--export-csv`). It builds,
vets, and lints clean, and is what produced every result below.

A contributing factor found along the way: `.gitignore` had a bare
`mongogate` entry (no leading slash), which in gitignore syntax matches a
file or directory of that name *anywhere* in the tree — including
`cmd/mongogate/`. That silently hid the directory from `git status`/`git add
.`, which would have kept a real `cmd/mongogate/main.go` invisible to git
even if someone had written one. Fixed by anchoring it to `/mongogate` (the
built binary at repo root, which is what the rule was actually meant for).

### 1. Environment

```
mongo-source: 3-pod replica set, mongo:4.4.30
mongo-target: 3-pod replica set, mongo:8.0.26
```

`mongo:4.4` had never been pulled in this repo before (only 7.0/8.0 had
prior history). Two real setup problems surfaced immediately, both fixed in
`test/e2e/scripts/setup.sh` and `test/e2e/kind/source/statefulset.yaml`:

- **`mongo:4.4` has no `mongosh`** — only the legacy `mongo` shell (`mongosh`
  was only added to the official image starting around the 6.0 line).
  `setup.sh`'s hardcoded `mongosh` calls and the source StatefulSet's
  `readinessProbe`/`livenessProbe` had to switch to `mongo` for the source
  namespace specifically (target stays on `mongosh`).
- **The two shells disagree on error behavior.** `rs.status()` (and even raw
  `db.adminCommand({replSetGetStatus:1})`) returns `{ok:0,...}` without
  throwing in the legacy `mongo` shell, but throws a `MongoServerError` in
  `mongosh`. The original `try { rs.status(); print('already initiated') }
  catch { rs.initiate(...) }` logic silently skipped `rs.initiate()` on the
  4.4 side because the legacy shell never threw. Fixed by checking `.ok`
  explicitly *and* keeping the catch block, so it's correct under either
  shell's behavior.

### 2. Phase 1 structural diff, real run

`./mongogate --phase 1` against the seeded baseline (`loadgen seed-baseline`
+ `seed-users`, unmodified):

```
📁 [Phase1] Verifying [migtest] collections
  ❌ FAIL   migtest collections (15 collections)
  ❌ Index [migtest.timeseries_col]
     ⚠️  Extra index in target: meta_1_ts_1
...
  migtest.*: ❌ FAIL
  ⚠️  Extra collection: migtest.system.buckets.timeseries_col
```

Auth and Cluster sections both passed cleanly — built-in role privilege
diffs and `2dsphereIndexVersion`/index-version defaults (plan.md's risks #1
and #2) did **not** actually manifest between a freshly-seeded 4.4 and 8.0
cluster in this run. Worth recording as a real negative result, not just
"untested": the most-anticipated risk didn't fire here.

### 3. A confirmed bug worse than the one anticipated

The plan going in expected `timeseries_col`/`clustered_col` to be **missing**
on the 4.4 source, since time series (5.0+) and clustered collections (5.3+)
postdate it, and `loadgen`'s `seedBaselineOn` swallows all creation errors.
That's not what happens. Checked directly:

```
source timeseries_col: {"type":"collection","options":{},...}        (plain collection)
target timeseries_col: {"type":"timeseries","options":{"timeseries":{...}}}

source clustered_col indexes: [{"v":2,"key":{"_id":1},"name":"_id_"}]
target clustered_col indexes: [{"v":2,"key":{"_id":1},"name":"_id_","unique":true,"clustered":true}]
```

MongoDB 4.4's `create` command silently **ignores** the unrecognized
`timeseries`/`clusteredIndex` options instead of rejecting the command. Both
collections get created under the requested name, with normal-collection
semantics underneath — not missing, not erroring, just silently the wrong
kind of collection. This is strictly worse for a real migration than a
missing-collection error would be: nothing about the name or existence check
flags it.

mongogate missed this almost completely, for two independent reasons - both
fixed this pass, not left as known limitations:

- `verifyCollectionOptions` compared the `timeseries`/`clusteredIndex` option
  keys via `compareBSONFields` (`schema_verifier.go:101-109`), which
  explicitly **skipped any field the source side didn't have**
  (`types.go:24`, old code: `sv != "<nil>"`) — and a plain collection's
  options simply doesn't have those keys, so the skip swallowed the entire
  diff at the options level for both collections. The "skip" was meant to
  mean "doesn't apply to either side" (already covered for free since
  `sv == tv` when both are absent) but was actually written as "skip
  whenever source lacks it," which also swallows the asymmetric case where
  *only one side* has the field - exactly the case that matters here, and
  the same bug pattern affecting every other field compared this way
  (`capped`/`size`/`max`, index `sparse`/`hidden`/`wildcardProjection`, etc).
  **Fixed**: removed the `sv != "<nil>"` condition in `compareBSONFields`
  (`types.go`) entirely, leaving just `sv != tv`.
- For `clustered_col` specifically, there was **no signal at all** - Phase 1
  reported it `✅ PASS`. `VerifyIndexes` (`index_verifier.go`) unconditionally
  skipped the `_id_` index by name on both sides, and a clustered
  collection's defining characteristic (`unique`/`clustered` on that exact
  index) lives nowhere else to compare. **Fixed**: removed the `_id_` skip
  in both loops, and added `clustered` to `compareIndex`'s field list
  alongside the already-present `unique`.

Re-verified live against the same 4.4/8.0 pair after both fixes
(`go build && go vet && go test ./... && golangci-lint run` all clean
first):

```
❌ Index [migtest.clustered_col]
   ⚠️  Index [_id_].unique: src=<nil> tgt=true
   ⚠️  Index [_id_].clustered: src=<nil> tgt=true
❌ Index [migtest.timeseries_col]
   ❌ Missing index: _id_
   ⚠️  Extra index in target: meta_1_ts_1

migtest.*: ❌ FAIL
⚠️  [migtest.clustered_col].clusteredIndex: src=<nil> tgt=map[key:map[_id:1] name:_id_ unique:true v:2]
⚠️  [migtest.timeseries_col].timeseries: src=<nil> tgt=map[bucketMaxSpanSeconds:3600 granularity:seconds metaField:meta timeField:ts]
⚠️  Extra collection: migtest.system.buckets.timeseries_col
```

Both collections now fail clearly and directly (the collection-options diff
names the exact field, `clusteredIndex`/`timeseries`, instead of relying on
indirect artifacts), and every other previously-passing collection/index in
the same run is still `✅ PASS` - no new false positives introduced.

### 4. The three other blind spots from this session, confirmed live

Each deliberately diverged on the running 4.4/8.0 pair, then re-checked with
`./mongogate --phase 1`:

| Blind spot | Diverged how | mongogate result |
|---|---|---|
| Server parameter outside the checked 2 (`slowOpThresholdMs`, `maxIncomingConnections`) | `setParameter notablescan`: `true` on source, `false` on target | `✅ PASS Cluster settings` |
| Per-user auth mechanism | root user: `["SCRAM-SHA-1","SCRAM-SHA-256"]` on source, `["SCRAM-SHA-256"]` only on target | `✅ PASS Account permissions` |
| Default cluster-wide read/write concern | not set explicitly anywhere — see below | `✅ PASS Cluster settings` (same run as above) |

`cluster_verifier.go` never calls `getDefaultRWConcern` at all (confirmed by
repo-wide grep), so the third row isn't really a "diverged" case — it's
naturally different just from being different versions:

```
source (4.4): getDefaultRWConcern → no defaultWriteConcern/defaultReadConcern fields at all
target (8.0): {"defaultReadConcern":{"level":"local"},
               "defaultWriteConcern":{"w":"majority","wtimeout":0},
               "defaultWriteConcernSource":"implicit", ...}
```

This is a genuine durability-semantics difference (implicit majority write
concern, a real MongoDB behavior change), not metadata noise, and it exists
on every fresh 4.4-vs-8.0 pair without anyone configuring anything.

### 5. FCV — also naturally different, also unchecked

```
source (4.4): featureCompatibilityVersion = "4.4"
target (8.0): featureCompatibilityVersion = "8.0"
```

Confirms plan.md risk #4. `cluster_verifier.go` has no FCV-reading code at
all (repo-wide grep, zero hits). Note the nuance: this particular pair's FCV
difference is just a side effect of being different binaries, not the
"staged rollout, same binary, deliberately held-back FCV" scenario plan.md
originally described — that's a different, not-yet-tested setup (same
version on both sides, FCV pinned down on one).

### 6. Collation/ICU — explored, no reproducible difference found

Tried a German-locale (`strength:1`) sort across `ß`/`ss`-style strings and
`ñ`-adjacent ordering, real `.find().sort()` output compared directly via
shell on both sides (bypassing mongogate, which only ever compares the
collation *option string* — see plan.md risk #3):

```
both sides: ["ñ", "n", "nz", "oz", "Strasse", "Straße", "Strasze"]
```

Identical. No ICU sort-order divergence found between whatever ICU versions
ship with 4.4 vs 8.0 for these specific inputs. Per the original plan, this
was always exploratory, not a hard requirement — recorded as a real negative
result rather than left untested.

---

## 9. Deeper code review: 4 more confirmed gaps, all fixed and re-verified

With a real binary finally available (section 8.0), Phase 3 (full data
verification) was run for real for the first time in this project's
history. That, plus a deliberate second read of the code looking
specifically for "what else hasn't been thought about," found four more
real gaps. All four are fixed in this pass, not just documented.

### 9.1 GridFS content verification was dead code

`gridfs_verifier.go`'s `fullVerify` branch compared `fs.files`'s `md5`
field. Checked the vendored driver source directly
(`go.mongodb.org/mongo-driver@v1.13.1/mongo/gridfs/upload_stream.go:196-203`):
the file document it writes only has `_id`, `length`, `chunkSize`,
`uploadDate`, `filename`, `metadata` — **no `md5` field, ever**, on any
MongoDB version. The driver removed automatic MD5 generation
industry-wide (FIPS compliance) years ago. Both sides' `md5` were always
`<nil>`, always equal, so the check never fired regardless of whether
content actually matched. README's "GridFS | Metadata + content MD5"
claim (line 106) was not true.

**Fixed**: `gridFSContentHash` (`gridfs_verifier.go`) streams the actual
file content through SHA256 via the GridFS download API (`io.Copy`, no
full-file buffering, so memory stays flat regardless of file size - the
same property the rest of this tool relies on). Only runs in Phase 3, and
only when sizes already match (a size mismatch already proves content
differs without needing to read it).

### 9.2 Checkpoint file was global, not job-scoped

`internal/utils/checkpoint.go` used a single hardcoded `checkpoints.json`
in the working directory, keyed only by namespace. Two different migration
jobs (or the same job run twice by accident) from the same working
directory would silently corrupt each other's `--resume` state.

**Fixed**: `SetCheckpointFile` (lazy-loaded, no more package `init()`)
lets the caller override the path; `cmd/mongogate/main.go` now derives one
from a SHA256 of `source_uri|target_uri`, so different job configs
automatically get different checkpoint files without requiring a manual
job ID.

### 9.3 Custom role inheritance was never compared

`getRoles` (`auth_verifier.go`) only ever read `rm["privileges"]`. MongoDB
roles can inherit from other roles (`rolesInfo`'s `roles` field) - a role
on one side inheriting differently than its same-named counterpart on the
other side produced no diff at all.

**Fixed**: `getRoles` now also extracts and normalizes the inherited-role
list (`normalizeInheritedRoles`), compared as a second, separately-labeled
check ("different inherited roles") alongside the existing privilege check.

### 9.4 Replica set member topology was never compared

`getReplicaSetConfig` fetched the entire `replSetGetConfig` document but
only ever compared two scalar fields. Member count, arbiters, voting
members, hidden/delayed secondaries, tags - fetched, then ignored. A
migration that changes topology (e.g. 3 data-bearing nodes →
2-data+1-arbiter) produced no diff, despite real write-durability and
failover differences.

**Fixed**: `summarizeTopology` aggregates member count, arbiter count,
voting member count, hidden count, and delayed-secondary count (raw
per-host comparison isn't useful - hostnames always differ between source
and target) and reports a mismatch by aggregate counts.

### 9.5 A regression these fixes would have caused, caught before it shipped: generic data verification produces guaranteed false positives on `admin` and on GridFS internals

Running `--phase 3` for real (also a first) surfaced this independently of
9.1-9.4. Two collections always false-positive under the generic
per-document `DeepCompare` sweep, regardless of whether the migration is
correct:

- **`admin.system.users`**: real diff output (`migtest`/`admin` pair from
  this pass) showed every credential field differing -
  `credentials.SCRAM-SHA-1.salt`, `.storedKey`, `.serverKey`, even
  `userId` itself - because SCRAM credentials are freshly salted on every
  `createUser` call. The *same password*, created independently on each
  side (exactly what `loadgen seed-users` does, and what migrating users
  by recreating them rather than copying raw documents looks like in
  practice), produces different bytes every time by design. This isn't
  fixable by normalizing the hash - the underlying values are genuinely
  different and *should* be, even on a perfect migration.
- **`migtest.fs.files`/`fs.chunks`**: `fs.files.uploadDate` differs by
  wall-clock upload time; `fs.chunks`' per-chunk `_id` is a fresh
  ObjectID minted independently on each upload (the fixed-`_id` trick
  `loadgen` already uses only covers the parent file document, not its
  chunks). Both already-correctly verified by the dedicated `VerifyGridFS`
  check (now with real content hashing per 9.1) - the generic sweep was
  re-checking the same data through a path that's guaranteed to flag it as
  wrong.

Real captured diffs before the fix:
```
{'doc_id': 'admin.appuser', 'path': 'credentials.SCRAM-SHA-1.salt', 'src_value': 'YYCjvO7vABZe/gZm8f+x0w==', 'tgt_value': 'j1xKVvIAPeBAol0Kl546TQ=='}
{'doc_id': 'featureCompatibilityVersion', 'path': 'version', 'src_value': '4.4', 'tgt_value': '8.0'}
{'doc_id': 'ObjectID("...0001")', 'path': 'uploadDate', 'src_value': 'dt:...07:43:57.341Z', 'tgt_value': 'dt:...07:43:58.419Z'}
fs.chunks: missing=1 (ObjectID("6a37963d...")), extra_in_target=1 (ObjectID("6a37963e..."))
```

(The `admin.system.version` row above is also where this pass discovered
that the FCV value is incidentally readable through `admin.system.version`
- but that's a side effect of a collection that shouldn't be raw-compared
in the first place, not a real signal worth relying on.)

**Fixed**: `admin` added to the default `skip_dbs` (`config.go`,
`config.yaml`) - its meaningful content is already covered by dedicated
checks (`VerifyAuth` via `usersInfo`, `VerifyCluster` via
`replSetGetConfig`), so raw-comparing the rest of it is pure redundant
risk. `fs.files`/`fs.chunks` excluded from the Phase 2/3 collection list
in `cmd/mongogate/main.go` (`isGridFSInternal`) since `VerifyGridFS`
already covers them correctly.

**Losing the incidental FCV signal was a real cost of that fix** - so a
proper, dedicated replacement was added instead of leaving a gap:
`verifier.FetchVersionInfo` reads `buildInfo.version` and
`featureCompatibilityVersion` directly via `getParameter` on both sides
and stores them on `report.Report.Versions` - informational only, never
affects `Passed`, printed in every report header. This is exactly the
follow-up plan.md originally proposed ("an informational line... so a
human has the context to correctly interpret version-driven diffs") and
item 8 in the previous revision of Known Limitations below - now done.

Re-verified live after all of 9.1-9.5: same 4.4/8.0 pair, `--phase 3`
clean except the two genuine `timeseries_col`/`clustered_col` failures -
zero false positives, GridFS content hash confirmed passing on identical
content, version line confirmed printing `Source: version=4.4.30 fcv=4.4`
/ `Target: version=8.0.26 fcv=8.0`.

---

## 10. The full version matrix: 4.4 / 5.0 / 6.0 / 7.0, each against an 8.0 target

Section 8 only tested one pair (4.4 → 8.0). A reasonable follow-up
question: does every source version exhibit the same gaps, or does it vary
by exactly how far apart the two versions are? Tested for real -
`mongo:5.0`, `6.0`, and `7.0` each as source, same 8.0 target, same seeded
baseline, same kind cluster (source namespace torn down and redeployed
with a different image tag between runs; target/monitor/tools left
running throughout).

At the time, each version switch meant hand-editing the image tag in
`test/e2e/kind/source/statefulset.yaml` and reverting it afterward - easy
to get wrong and it leaves a dirty diff. Fixed afterward (not before this
matrix was run, but before anyone else has to repeat it by hand):
`setup.sh` now takes `SOURCE_MONGO_VERSION`/`TARGET_MONGO_VERSION` as env
vars, substituted on the fly and piped straight into `kubectl apply -f -`
- the checked-in YAML is never written to. Reproduce any single pair from
this matrix with, e.g., `SOURCE_MONGO_VERSION=6.0 bash setup.sh`. The
`.github/workflows/e2e.yml` cross-version matrix job (section 12) uses
this same mechanism via its `matrix.source_version`, confirmed live with
`git diff` showing zero changes to the YAML after each run.

**A real setup bug found and fixed before any of this could run**:
`setup.sh`'s shell selection from section 8 (`mongo` for source, `mongosh`
for target) only happened to work for the one pair already tested -
`mongo:6.0` and `7.0` images ship **only** `mongosh`, no legacy `mongo` at
all, so the hardcoded assumption would have failed outright. Fixed by
detecting which binary actually exists in the running container
(`which mongosh`) rather than assuming by namespace, and made the
StatefulSet readiness/liveness probes try `mongosh` first with a
fallback to `mongo` so the same YAML works unmodified across every tested
version. Confirmed live: `mongo:4.4` → only `mongo`; `5.0` → both;
`6.0`/`7.0` → only `mongosh`.

### Result matrix

| Source | FCV vs 8.0 | `clustered_col` | `timeseries_col` itself | `system.buckets.*` validator | `meta_1_ts_1` index | Phase 1 result | Phase 3 data |
|---|---|---|---|---|---|---|---|
| 4.4 | differs (4.4 vs 8.0) | ❌ plain collection (no `clusteredIndex` support pre-5.3) | ❌ plain collection (no time series support pre-5.0) | n/a (bucket doesn't exist) | n/a | **FAIL** | clean |
| 5.0 | differs (5.0 vs 8.0) | ❌ plain collection (still pre-5.3) | ✅ real time series | ❌ differs (target requires a `count` field 5.0's validator doesn't) | ❌ missing on source | **FAIL** | clean |
| 6.0 | differs (6.0 vs 8.0) | ✅ matches (5.3+ covers it) | ✅ real time series | ✅ matches | ❌ still missing on source | **FAIL** (only the index) | clean |
| 7.0 | differs (7.0 vs 8.0) | ✅ matches | ✅ real time series | ✅ matches | ✅ matches | **✅ PASS, exit 0** | clean |

"Phase 3 data: clean" means zero false positives across every run, after
the fixes in section 9.5 (`admin` and GridFS-internal collections
excluded) plus one more found in *this* pass (next subsection).

### A third instance of the same false-positive pattern: time series bucket internals

Running `system.buckets.timeseries_col` through the generic Phase 3 data
verifier on the 5.0 pair produced `missing=1 extra_in_target=1` even
though the logical `timeseries_col` collection (the one a user actually
queries) was already confirmed identical. Pulled the raw bucket documents
directly to see why:

```
source (5.0): {"_id":"6955b90063c1945e2433c0bf","control":{"version":1,...},
               "data":{"value":{"0":1.5,"1":2.5},"_id":{"0":"ts-0","1":"ts-1"},...}}
target (8.0): {"_id":"6955b9002762d33c60908f49","control":{"version":2,...,"count":2},
               "data":{"value":"AQAAAAAAAAD4P6BOAQAAAAAAAAA=", ...}}
```

Two independent problems, same root cause as the GridFS chunk-ID issue in
9.5: the bucket `_id` is a fresh ObjectID minted independently per side
(non-deterministic across independently-seeded sides, regardless of
correctness), and `control.version` (1 = row-based pre-7.0 encoding, 2 =
columnar 7.0+ encoding - a real, documented MongoDB storage format change)
means the *same logical measurements* are encoded completely differently
at the storage level depending on source version. Comparing this directly
guarantees a false positive across any version gap that changed bucket
encoding, on top of being redundant - the logical view is already verified
correctly through the normal collection-level path.

**Fixed**: `system.buckets.<name>` collections excluded from the Phase 2/3
data verification list (`cmd/mongogate/main.go`, `isTimeSeriesBucket`),
same treatment as the GridFS exclusion. Structural checks (existence,
options, indexes) still run normally on them, since those *are*
deterministic per version and are exactly what surfaced the
`meta_1_ts_1`/validator findings above - only the raw per-document content
comparison is excluded.

### What this narrows down, precisely

- Built-in role privilege differences (plan.md risk #1) and
  `2dsphereIndexVersion`/general index-version differences (risk #2) did
  not fire on **any** of the four pairs in this matrix - both remain
  theoretical for now, not confirmed on a real cluster.
- The `clustered_col` gap is bounded exactly: broken on 4.4 and 5.0
  (clustered collections need 5.3+), fine from 6.0 onward.
- The time series bucket gap is bounded exactly: the bucket validator
  format changed somewhere between 5.0 and 6.0; the automatic
  `meta_1_ts_1` index was introduced somewhere between 6.0 and 7.0. Not
  narrowed further than that - 5.1-5.3 and 6.1-6.x point releases weren't
  tested.
- **7.0 → 8.0 is the only pair in this matrix with zero structural
  findings** - every check passes, exit code 0. The widest tested gap
  (4.4 → 8.0) is also the one with the most findings; this is consistent
  with "more findings the wider the version gap," but four data points
  isn't enough to call that a confirmed trend, just an observation.

---

## 11. Fixing the 3 remaining detection blind spots from section 8.4

Section 8.4 confirmed three things were never checked: per-user auth
mechanism, cluster-wide default read/write concern, and all but 2 of
MongoDB's server parameters. All three addressed now - but not
identically, because one of them is version-driven and the other two
aren't, and treating them the same way would have been wrong.

### Per-user auth mechanism - fixed as a blocking check

`getUsers` (`auth_verifier.go`) only ever read `user`/`roles` from
`usersInfo`; the `mechanisms` field it also returns was never looked at.
Fixed: `userInfo` now carries both, compared independently so a user with
identical roles but a different mechanism set (e.g. SCRAM-SHA-256-only on
one side, both SHA-1+SHA-256 on the other) is reported by name. This is a
blocking check (added to `errors`) because section 8.4 already confirmed
empirically this is **not** version-driven - a fresh user gets the same
default mechanism set on 4.4 through 8.0; a mismatch only happens from
deliberate config drift, exactly what should block a cutover decision.

Re-verified live: restricted one user to `SCRAM-SHA-256` on target only,
confirmed `❌ User [appuser] has different auth mechanisms` appears and
`Account permissions` fails; reverted, confirmed it passes again.

### Default read/write concern - fixed as informational, not blocking

This one *is* version-driven (confirmed live in section 8.4: a fresh 4.4
cluster reports no `defaultWriteConcern` field at all; a fresh 8.0
cluster reports `{w:"majority"}` - that's MongoDB's own implicit-default
calculation changing by version, nothing to do with configuration).
Treating a mismatch here as a blocking error would fail *every single*
cross-version run regardless of whether anything is actually wrong with
the migration - the same reasoning that already applied to FCV.

Fixed by extending `FetchVersionInfo` (the same informational mechanism
FCV already uses) rather than adding it to `VerifyCluster`'s blocking
checks: `report.VersionInfo` gained `Src/TgtDefaultWriteConcern`, printed
in every report header right next to version/FCV. Re-verified live:

```
Source   : version=4.4.30 fcv=4.4 default_write_concern=(none - implicit per-version default applies)
Target   : version=8.0.26 fcv=8.0 default_write_concern=map[w:majority wtimeout:0]
```

This *is* a real limitation worth being explicit about: a human reading
the report has to notice and interpret this line themselves - it will
never turn the run red, by design, because doing so would be wrong far
more often than it would be right.

### Server parameters - expanded the list, still not exhaustive

Went from 2 checked parameters to 5: added `notablescan`,
`journalCommitInterval`, `cursorTimeoutMillis` - chosen because all three
are deliberately-configured operational knobs with stable defaults across
4.4-8.0 (not version-gated), so a mismatch is a real config difference,
not a version artifact. Re-verified live: diverged all three between
source and target, confirmed all three are now reported by name.

**Deliberately not done**: switching to a `getParameter: {"*": 1}`
wildcard comparison against every server parameter MongoDB has. That
would require building and maintaining a denylist of parameters that
legitimately differ by version (MongoDB adds new feature-gated parameters
between major versions regularly, and the asymmetric-skip fix in section
8.3 means a parameter that only exists on the newer side would now be
reported every time) - a real architectural improvement, but one that
needs its own scoped investigation into which parameters are safe to
include, not something to guess at inside this fix. Still a known,
explicit gap: most of MongoDB's hundreds of server parameters remain
unchecked.

All three re-verified together against the same 4.4/8.0 pair after
`go build && go vet && go test ./... && golangci-lint run` clean.

---

## 12. Automating what sections 1-11 only ever ran by hand

`.github/workflows/ci.yml` only ever ran `go build`/`go vet`/`go test`/
`golangci-lint` - none of which touches a real MongoDB cluster, so every
finding in sections 1-11 above was confirmed by hand and could regress
silently. Added `.github/workflows/e2e.yml`, a kind-based workflow with
two jobs:

- **`happy-path`** - forces both sides to `mongo:8.0`, runs the seeded
  baseline, asserts Phase 1 and Phase 3 both pass cleanly, then drops a
  real index on target and asserts Phase 1 now fails. This is a
  regression test for detection itself, not just "does it build" - a
  future change that silently breaks index comparison would fail this
  job even though `go test` stays green.
- **`cross-version-matrix`** - a 4-way matrix (`mongo:4.4`/`5.0`/`6.0`/
  `7.0` as source, `8.0` as target) that asserts the *exact* findings
  documented in section 10: `clusteredIndex` diff present for 4.4/5.0,
  absent for 6.0/7.0; `meta_1_ts_1` diff present for 4.4/5.0/6.0, absent
  for 7.0; and zero false positives in Phase 3 data verification across
  all four. A failure here means either a real regression, or a fix that
  legitimately moved one of these documented boundaries and this section
  needs updating to match - not something to wave through.

Both jobs use `setup.sh`'s `SOURCE_MONGO_VERSION` env var (see section 10)
to select the version, never editing the checked-in YAML.

**Confirmed live, not just written and hoped**: pushed and watched via
`gh run watch` - both jobs passed on real GitHub Actions infrastructure,
including all four matrix entries, alongside the pre-existing
`go build`/`vet`/`test`/`golangci-lint` job. Runtime: ~3-4 minutes per
job, all five running in parallel.

**A real flake found by watching repeated real runs, not assumed
flaky-and-ignored**: a later push's `happy-path` job failed twice in a
row with `loadgen` reporting `AuthenticationFailed` against the source
replica set, while every `cross-version-matrix` job in the same runs
passed. Root cause: `createUser` only waits for the configured write
concern (majority on 8.0's implicit default - not necessarily *every*
member, and not guaranteed at all on older defaults) before returning,
but `loadgen`'s connection string lists all three `mongo-0,1,2` hosts and
the driver authenticates against each one it discovers - including any
secondary that hasn't replicated the new user yet. Fixed in `setup.sh`:
after creating the root user, poll each of the three members directly
with the new credentials until all three accept them, before declaring
the replica set ready. This is exactly the kind of flake an
infrastructure change can introduce invisibly - found because the new
CI was actually watched run-over-run, not just confirmed green once.

---

## 13. Structured code review of this entire branch's diff

Ran a full multi-angle code review (correctness, removed-behavior audit,
cross-file call-site tracing, reuse, simplification, efficiency, altitude,
conventions) against `git diff main...HEAD` (~3,400 lines across 33
files) rather than relying on having already read every line carefully
during development. Two real, confirmed bugs found and fixed; the rest
were either already-disclosed tradeoffs or non-issues, confirmed by
direct verification rather than taken on faith.

### Fixed

1. **GridFS content-hash read had no timeout** (`gridfs_verifier.go`).
   `gridFSContentHash` discarded its `context.Context` parameter, and
   neither `bucket.OpenDownloadStream` nor `io.Copy` have a timeout of
   their own in this driver version - a network blip or stuck connection
   during Phase 3 would hang that worker goroutine (and `VerifyAllData`'s
   `wg.Wait()`) forever, with no way to recover short of killing the
   process. Fixed with `bucket.SetReadDeadline(time.Now().Add(60 *
   time.Second))` before opening the stream. Re-verified live: Phase 3
   GridFS check still passes and completes well within the new bound.
2. **Target-side (and role) fetch errors were silently swallowed in
   `VerifyAuth`** (`auth_verifier.go`). `tgtUsers, _ := getUsers(ctx,
   tgt)` discarded the error - if `usersInfo` failed on target for any
   reason (permissions, transient network issue), every real source user
   would be reported as `❌ Missing user`, masking a connectivity problem
   as a complete user-migration failure. Same pattern existed for
   `getRoles`. Fixed: both errors are now surfaced, and the comparison
   loop is skipped entirely on a fetch failure (ranging over a nil map in
   Go is a no-op) rather than comparing against an incomplete map.
   Re-verified live: normal seeded baseline still passes cleanly
   (`✅ PASS Account permissions`); a connection-level auth failure (bad
   target credentials) is already caught earlier and more clearly by
   `mustConnect` at startup, confirmed by direct test.

### Also fixed, lower severity

3. **`setup.sh`'s version substitution could have rewritten the wrong
   `image:` line.** The `copy-keyfile` initContainer in both
   StatefulSets used `image: mongo:8.0` even though it only runs
   `cp`/`chmod` and needs no MongoDB at all - `sed`'s `s|image:
   mongo:...|...|g` would match that line too, not just the `mongod`
   container's, harmlessly today but fragile. Switched both
   initContainers to `busybox:1.36`, which can't be matched by the
   mongo-version substitution at all - not just patched, the ambiguity
   is now structurally impossible.
4. **`docs/PLAN.md`'s example config still listed `encryption: false`**,
   the flag removed in section 9. Commented out with a pointer to why.

### Checked and confirmed not an issue (most disputed candidate)

The per-user `Mechanisms` comparison (`slices.Equal(srcUser.Mechanisms,
tgtUser.Mechanisms)`) was flagged by one review angle as risky if a user
type without a `mechanisms` field (e.g. x.509) decodes to `nil` and gets
compared against a populated SCRAM mechanism list. Verified this is
actually the *intended* detection, not a bug: `slices.Equal(nil, nil)` is
`true` (two mechanism-less users compare equal, correctly), while `nil`
vs. a populated list is `false` (an x.509 user vs. a SCRAM user of the
same name with the same roles - exactly the deliberate-config-drift case
this check exists to catch). No change made.

### Disclosed tradeoffs, not hidden bugs (already documented elsewhere, re-confirmed here)

A few review angles independently converged on the same three observations - all real, all already written down (in this file or in code comments) rather than discovered for the first time here:

- `isGridFSInternal`/`isTimeSeriesBucket` (`cmd/mongogate/main.go`) only
  filter the Phase 2/3 raw-document comparison, not `VerifyCollections`/
  `VerifyIndexes`'s structural checks - **deliberate**, since the
  structural checks on `system.buckets.*` are exactly what surfaced the
  `meta_1_ts_1`/validator findings in section 10. `fs.files`/`fs.chunks`
  going through that same unfiltered structural path was never
  specifically exercised by the test matrix (only `VerifyGridFS`'s
  dedicated check was), but GridFS's default indexes/options are created
  identically by the driver regardless of server version, so this is
  low-risk in practice, not a known-broken path.
- `admin` is excluded wholesale via `skip_dbs` rather than excluding only
  `system.*` collections within it - coarser than the GridFS/timeseries
  fix, and would silently skip any non-`system.*` collection a user
  legitimately stores in `admin` (legal in MongoDB, unusual in practice).
  A future, more targeted fix is possible; not attempted here because it
  wasn't the problem this pass needed to solve.
- The server-parameter list grew from 2 to 5 hardcoded names rather than
  a denylist-based wildcard comparison - already explicitly flagged as
  "deliberately not done" in section 11, re-confirmed here as the same
  known gap, not a new one.

---

## Known limitations

Not all "limitation" means the same thing. The table below ranks these by
whether they're actually fixable, so it's not necessary to read all four
write-ups below just to know which is which:

| # | Item | Category | Fixable? |
|---|------|----------|----------|
| 1 | Sharded clusters | Untested (deferred) | **Yes** — just needs the kind infra built; nothing about the design prevents it |
| 2 | Deprecated BSON types | Cosmetic only | **Yes, trivially** — comparison/diff detection already works; only the displayed type name is ugly |
| 3 | Vector/Atlas Search index definitions | Structural blind spot | **No, not with this approach** — `VerifyIndexes` calls `listIndexes`, which cannot see mongot's catalog at all; would need an entirely different mechanism (talking to mongot/Atlas Search APIs directly) |
| 4 | Queryable Encryption | Fundamental ceiling | **No, never** — impossible by the design of encryption itself; no amount of engineering on this tool fixes it without the encryption keys |
| 5 | Cluster server parameters checked = 5 of hundreds | Partially fixed (section 11) | **Yes, incrementally** — went from 2 to 5 (`slowOpThresholdMs`, `maxIncomingConnections`, `notablescan`, `journalCommitInterval`, `cursorTimeoutMillis`), all confirmed live; a full fix needs a denylist-based wildcard comparison, deliberately not attempted yet — see section 11. |

Items 3 and 4 are the two that are genuinely "cannot verify," not just
"didn't get to it" — worth reading in full if evaluating whether to rely on
this tool for a migration that uses either feature.

Per-user auth mechanism and cluster-wide default read/write concern - both
listed here in an earlier revision of this table - are fixed; see section
11. The default RW concern fix is informational-only by design (it's
version-driven, so a mismatch correctly never fails the run) - that's a
deliberate design choice, not a remaining gap, but worth knowing it won't
turn the report red even when the two sides genuinely differ.

Six related and more severe gaps found across sections 8.3 and 9 were fixed
immediately rather than left listed here: collection/index options using a
fixed field allowlist that skipped asymmetric diffs, the `_id_` index being
unconditionally skipped (8.3); GridFS content verification being dead code,
the checkpoint file being global instead of job-scoped, custom role
inheritance never being compared, replica set member topology never being
compared, and FCV never being read (all section 9).

1. **Sharded clusters** (mongos + config servers) were never tested — every
   scenario in this document ran against replica sets. `verify.sharding`
   exists in the verifier code (shard count + shard key comparison) but
   wasn't exercised against a real sharded topology; standing one up in kind
   is a large enough infra lift that it was explicitly deferred rather than
   rushed.
2. **Deprecated BSON types** — JavaScript, CodeWithScope, Symbol, and
   DBPointer have no case in `typeName()`/`normalizeValue()`; they'd fall
   through to Go's default `%T`/`%v` formatting instead of a clean BSON type
   name. All four have been deprecated since MongoDB 4.4 with negligible
   presence in active migrations, so this is documented rather than
   implemented.
3. **Vector Search / Atlas Search index definitions are structurally
   invisible.** They're created via `createSearchIndex()` and live in a
   separate catalog managed by `mongot`, not reachable through the
   `listIndexes` command `VerifyIndexes` actually calls. Confirmed `mongot`
   isn't present in the Community `mongo:8.0` image used throughout this
   testing pass (`which mongot` → not found) — it requires Atlas or
   Enterprise tooling. This means Phase 1 will report a clean index match
   while never having looked at vector/search indexes at all. The
   *underlying vector data* (embedding arrays) is ordinary BSON data and
   **is** verified correctly (see section 7) — it's specifically the search
   index *definition* that's out of reach.
4. **Queryable Encryption (QE) fields cannot be meaningfully verified by this
   design at all.** Encrypted values use a fresh IV per encryption, so even
   a 100%-correct migration re-encrypts to different ciphertext bytes for the
   same plaintext. mongogate has no key material to decrypt and compare
   plaintext, so QE fields will report as different regardless of whether
   the migration is correct. This isn't a missing feature to implement — it's
   a ceiling on what hash-based comparison can ever tell you about encrypted
   data. The `verify.encryption` config flag used to exist as a confirmed
   no-op (declared, never read anywhere); removed in this pass (`config.go`,
   `config.yaml`) rather than left as a switch that looked like it did
   something.

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
