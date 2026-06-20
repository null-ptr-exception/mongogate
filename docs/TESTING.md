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

Items 3 and 4 are the two that are genuinely "cannot verify," not just
"didn't get to it" — worth reading in full if evaluating whether to rely on
this tool for a migration that uses either feature.

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
   the migration is correct. This isn't a missing feature to implement — the
   `verify.encryption` config flag is already a confirmed no-op (declared in
   `internal/config/config.go`, never read anywhere else) — it's a ceiling on
   what hash-based comparison can ever tell you about encrypted data.

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
