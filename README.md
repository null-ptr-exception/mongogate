# mongogate

A MongoDB migration verification tool. It runs alongside mongomirror,
mongosync, Debezium, or a homegrown CDC pipeline and answers the question
those tools don't: **did the data actually land correctly on the other
side, and where exactly does it differ if it didn't?**

## Why this exists

Migrating MongoDB data — whatever the mechanism — runs into the same
recurring problems:

- You don't actually know whether data finished moving, only that the tool said it did.
- Verifying against production can't be allowed to slow production down.
- Data keeps changing while the migration is in flight, so naive diffing produces noise.
- Generic tools (e.g. mongosync's built-in checks) use gigabytes of memory and only check counts.
- CDC pipelines distort data in transit — timezones shift, float precision drifts, `ObjectId`s degrade to strings — so a raw byte comparison reports thousands of false positives.
- When something *is* different, "counts don't match" isn't enough; you need to know which field, on which document, and why.

mongogate addresses each of these directly: streaming comparison with constant
memory (~50-60MB regardless of collection size), secondary-preferred reads with
built-in rate limiting, a three-phase strategy that defers exact comparison
until data has stopped moving, six independent hash-normalization rules tuned
to common CDC distortions, and a field-level deep-compare that classifies
every mismatch by type (`MISSING_FIELD`, `TYPE_MISMATCH`, `OBJECTID_DEGRADED`,
etc.) instead of just saying "not equal."

See [`docs/DESIGN.md`](docs/DESIGN.md) for the full design rationale and
[`docs/TESTING.md`](docs/TESTING.md) for how this was validated end-to-end
against real MongoDB replica sets in `kind`, including answers to the harder
operational questions: what's the *correct* point in a live migration to cut
over, can it tell when target has diverged independently of source, does it
actually reconnect after a node restart or failover, and what does it
actually cost in memory/throughput under load.

## Validated against a real cluster — what's proven, what isn't

Every claim below is backed by command output captured against a real
3-node MongoDB replica set in `kind`, not assumed from reading the code —
see [`docs/TESTING.md`](docs/TESTING.md) for the full evidence.

**Confirmed working**, same-version and across the full 4.4 / 5.0 / 6.0 /
7.0 → 8.0 cross-version matrix: every structural check (auth, cluster,
schema, indexes, views), GridFS content verification (a real streamed
SHA256 — not the `fs.files.md5` field, which the driver stopped writing
years ago), per-user auth mechanism, replica set topology, version/FCV
info, bidirectional split-brain detection, checkpoint/resume, primary
failover transparency, alert de-dupe, auto-repair.

The cross-version matrix itself found real, version-specific gaps: a
collection silently created as the wrong type when the source version
doesn't support `timeseries`/`clusteredIndex` (MongoDB drops the option
instead of erroring — now caught directly), and time series' internal
bucket format/auto-index changing across versions even when the logical
data is identical (now correctly excluded from raw comparison, same as
GridFS internals). `7.0 → 8.0` is the only pair with zero structural
findings; the gaps above are each bounded to a specific version range, not
"somewhere in 4.4-8.0" — see `docs/TESTING.md` sections 8 and 10 for the
exact boundaries.

**Permanent limitations, not bugs** (see [Known
limitations](docs/TESTING.md#known-limitations) for why):
- Vector/Atlas Search index *definitions* — the underlying vector data is
  verified fine, just not the search index itself, which lives in a
  separate catalog (`mongot`) this tool has no way to reach.
- Queryable Encryption fields — ciphertext differs by design on every
  encryption, even on a 100%-correct migration. No comparison logic fixes
  this without the encryption keys.

**One deliberate non-blocking design choice**: default read/write concern
is surfaced informationally (printed in every report header), never as a
pass/fail check — it's version-driven (MongoDB's own implicit-default
calculation changed across versions), so treating a mismatch as an error
would fail every cross-version run regardless of whether the migration is
actually fine. Same reasoning already applied to FCV.

**A known gap, not yet fixed**: only 5 of MongoDB's hundreds of server
parameters are checked. A full fix needs a denylist-based comparison
(check everything, explicitly exclude what's known to legitimately differ
by version) rather than guessing at which of the hundreds to add next —
deliberately not attempted without that groundwork. See docs/TESTING.md.

**Worth knowing about this project's own history**: `cmd/mongogate` — the
binary this README describes — did not exist as buildable code in any
branch of this repository until this validation pass wrote it
(`docs/TESTING.md` section 8.0). Treat any claim dated before that as
design intent, not a confirmed result, unless `docs/TESTING.md` says
otherwise.

## How it works

### Three-phase strategy

Data is still changing throughout a migration, so verification is staged
from "can't possibly be distorted" to "fully exact":

```
T0          T1              T2              T3          T4
│           │               │               │           │
Migration   Initial         Oplog lag       Source      ✅ Cutover
starts      sync done       < 10s           write-stop
            │               │               │
            ▼               ▼               ▼
        Phase 1         Phase 2         Phase 3
        Structural      Sampled         Full exact
        (no distortion) (slight         (no distortion)
                          distortion)
```

- **Phase 1 — structural.** Users/roles/auth, replica set & sharding config,
  every collection's options/validator/collation, every index type, views,
  GridFS metadata. None of this changes while data is being written, so it's
  checked first and is never distorted.
- **Phase 2 — sampled.** A configurable percentage of documents are hash-compared
  while writes are still flowing. Results are directional only (tagged
  `is_exact: false`) — use this to catch large structural problems early,
  not as a cutover gate.
- **Phase 3 — full & exact.** Run only after the source stops accepting writes
  and the target has caught up. Every document is compared (both directions),
  every GridFS file's content is hash-verified. This is the actual cutover gate.

### Hash normalization

Hashing raw BSON bytes produces constant false positives after a CDC hop —
key order isn't stable, `int32` vs `int64` look identical once stringified
carelessly, timezones shift, floats round differently. Each document is
normalized field-by-field before hashing:

| Distortion | Cause | Fix |
|---|---|---|
| datetime timezone differs | CDC through Kafka shifts tz | normalize to UTC |
| `0.1 + 0.2 ≠ 0.3` | float precision | fixed decimal precision |
| `"123.45" ≠ "123.4500"` | Decimal128 trailing zeros | stripped before hashing |
| BinData subtype differs | CDC drops the subtype | subtype tagged into the hash |
| `ObjectId` → string | CDC serializes through JSON | flagged as `OBJECTID_DEGRADED` |
| array order differs | replay order isn't guaranteed | optional sort |

When a hash mismatches, `DeepCompare` walks both documents field-by-field and
reports exactly what's wrong — `MISSING_FIELD`, `EXTRA_FIELD`,
`TYPE_MISMATCH`, `VALUE_DIFF`, `OBJECTID_DEGRADED`, or (at the document level)
`MISSING_DOC` — with the field path, both values, and both BSON types.

### Bidirectional comparison

A one-directional scan (source → target) only finds documents missing from
target. It can't tell you if target has documents that don't exist in
source — which happens when a migration script has a bug, something else is
writing into target, or a delete never replicated. `bidirectional: true` adds
a reverse `_id`-only scan of target against source to catch exactly that.

### Large collections (100GB+ / 1TB+)

Parallelism (`max_workers`) is across *collections*, not within one — a
single huge collection was always bound by one cursor/goroutine's
throughput, no matter how high `max_workers` was set (measured at ~530
docs/sec for one collection in `docs/TESTING.md`; at that rate a
billion-document collection takes weeks, not hours).

Collections estimated above `range_split_threshold_docs` (default 2,000,000)
are now split into `range_workers_per_collection` concurrent `_id`-range
sub-tasks instead — see `internal/verifier/range_split.go` for how the
boundaries are computed (a `splitVector` admin command when available,
falling back to an index-covered `$bucketAuto` aggregation otherwise) and
`docs/DESIGN.md` for the parallelism model this replaces. Range workers
share the same `max_workers` pool rather than adding on top of it — one huge
collection splitting into 8 ranges can temporarily occupy most of the pool,
so smaller collections in the same run may queue behind it. Splitting also
applies to Phase 2, where it turns the sampled pass into a stratified sample
(spread evenly across the whole `_id` keyspace instead of trusting a single
`$sample` call's randomness on a huge collection) plus a cheap per-range
count reconciliation that flags which specific range drifted, instead of
only knowing the collection-wide count is off.

Recommended cadence once a collection is this large:

- **Routine monitoring: Phase 2 only.** It's sampled and cheap; don't run
  Phase 3 on every check.
- **Phase 3 (full, exact): once, near cutover.** Run it off-peak, with
  `--resume` available if it's interrupted — each range checkpoints
  independently (`checkpoints_*.json` keys look like `db.col#r3/8`), so a
  resumed run only re-scans the ranges that hadn't finished, not the whole
  collection.
- If Phase 2 flags a specific range's count as mismatched, you don't have to
  wait for a full Phase 3 pass to investigate it — `--include-ns` still
  scopes to the whole collection (range-splitting happens automatically
  underneath it, not per-range from the CLI).

## Architecture

```
cmd/mongogate/main.go    the only thing that wires everything below
                         together — parses flags, loads config, connects,
                         runs the requested phases, prints/saves the
                         report, fires the final alert, sets the exit code

internal/
  config/      config loading, validation, --include-ns/--exclude-ns scoping
  verifier/    the actual comparison logic, one file per concern:
               auth · cluster (+ topology, FCV/version) · schema · index ·
               view · gridfs (metadata + content hash) · data (the
               document-level Phase 2/3 comparison, parallel workers)
  utils/       SHA256 hashing + 6 normalization rules, DeepCompare,
               job-scoped checkpoint persistence (--resume)
  report/      Result/DataResult/Report structs; JSON + terminal output;
               AllPassed() is what decides the process's exit code
  alert/       Slack/Email, de-duplicated, with a final-alert-before-exit guarantee
  monitor/     writes progress to a separate "monitor" MongoDB for Grafana
  httpapi/     /health /status /progress /report /passed, for CI polling
  prometheus/  /metrics in Prometheus text format
  repair/      --auto-repair
  export/      --export-csv

test/e2e/      not part of the product — a kind-based E2E harness
               (loadgen + manifests) used to validate everything above
               against real MongoDB replica sets; see docs/TESTING.md
```

One sentence on data flow: `VerifyDatabases`/`VerifyCollections` enumerate
what's in scope (also running the structural checks along the way) → the
resulting collection list feeds `VerifyAllData`, which splits any collection
above `range_split_threshold_docs` into `_id`-range sub-tasks and fans
everything out across `max_workers` goroutines, each streaming one
collection (or one range of a large one) through `DeepCompare` → every
check writes into the same `*report.Report` (ranges of one collection merge
into a single combined result), which decides what gets printed, saved, and
the exit code.

Full breakdown — every file, the complete data-flow diagram, and the
rationale behind each design decision (why SHA256 not MD5, why
`is_exact: false` during Phase 2, how much bidirectional comparison costs,
etc.): [`docs/DESIGN.md`](docs/DESIGN.md#3-architecture-overview).

## Feature list

| Area | Features |
|---|---|
| Security | Users (+ per-user auth mechanism), roles (+ inherited sub-roles), custom roles, server-wide auth mechanism, LDAP |
| Cluster | Replica set config & topology, sharding, shard keys, server parameters, version/FCV/default-write-concern info |
| Schema | DB/collection lists, capped/validator/collation/TTL/time-series/change-stream/clustered-index options |
| Indexes | Every index attribute: unique, sparse, hidden, TTL, partial filter, text weights, 2dsphere version, wildcard projection, collation |
| Views | `viewOn`, pipeline, collation |
| GridFS | Metadata + content hash (SHA256, streamed) |
| Data | Count, SHA256 hash, 6 normalization rules, field-level DeepCompare, bidirectional scan |
| Execution | 3-phase / 1 / 2 / 3 individually, multi-worker, `--resume`, `--sample`, `--dry-run`, `--include-ns` / `--exclude-ns` with `db.*` wildcards, automatic retries, rate limiting |
| Output | Terminal progress bar with ETA, JSON report, CSV diff export, Slack/Email alerts (de-duplicated), HTTP API, Prometheus `/metrics`, Grafana dashboard |
| Safety | Auto-repair (capped at 50 sampled docs per type), config validation |

Full design detail and the complete numbered feature table:
[`docs/DESIGN.md`](docs/DESIGN.md#19-full-feature-list).

## Install

```bash
git clone https://github.com/null-ptr-exception/mongogate.git
cd mongogate
go mod tidy
go build -o mongogate ./cmd/mongogate
```

Or with Docker:

```bash
cd deploy
docker-compose up
# Grafana:    http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
# HTTP API:   http://localhost:8080
```

## Configure

Minimum required (`config.yaml`):

```yaml
source_uri: "mongodb://source:27017/?readPreference=secondaryPreferred"
target_uri: "mongodb://target:27017/?readPreference=secondaryPreferred"
monitor_uri: "mongodb://monitor:27017/"
```

Everything else has a safe default — see [`config.yaml`](config.yaml) for the
full annotated option list (batching, rate limiting, retries, hash
normalization, alerting, HTTP/Prometheus ports).

## Enabling and disabling features

Every check has an on/off switch under `verify:`, except Queryable
Encryption, which deliberately has none — see [Validated against a real
cluster](#validated-against-a-real-cluster--whats-proven-what-isnt)
above. Turning a switch off skips that check entirely; it won't appear in
the report at all, not even as "skipped."

| Switch | What it checks | Relative cost | Turn off when |
|---|---|---|---|
| `auth` | users (+ per-user auth mechanism), roles (+ inherited sub-roles), server-wide auth mechanism, LDAP | cheap | almost never |
| `cluster` | replica set config/topology, version/FCV/default-write-concern info, 5 server params, sharding (if `sharding: true`) | cheap | almost never |
| `schema` | DB/collection lists, collection options (validator, collation, TTL, time series, clustered index, ...) | cheap | almost never |
| `index` | every index attribute | cheap | almost never |
| `views` | view definitions | cheap | no views in use |
| `gridfs` | metadata (Phase 1) + content hash (Phase 3 — reads every file's bytes once per side) | cheap (Phase 1) / scales with file size (Phase 3) | no GridFS in use |
| `data` | document content — count, hash, field-level diff. This is the actual point of the tool | scales with data volume, parallelized across `max_workers` | only for a connectivity/structure-only sanity check |
| `sharding` | shard count + shard key | cheap | **off by default** — never validated against a real sharded cluster (see docs/TESTING.md); turn on only for sharded source/target, and read results with that caveat in mind |
| `bidirectional` | reverse scan for documents in target that don't exist in source (split-brain detection) | roughly +20–30% runtime (only pulls `_id`, not full documents) | only if you're certain nothing else ever writes to target |

`hash_options` (under `data`) controls how documents are normalized
*before* hashing — these absorb CDC distortion (timezone shifts, float
precision, etc.), not version differences. The defaults are deliberately
conservative; see
[`docs/DESIGN.md`](docs/DESIGN.md#6-hash-normalization-design) for the
rationale behind each one. Leave them as-is unless you know your specific
pipeline introduces a distortion they don't already cover.

## Recommended default

This is what [`config.yaml`](config.yaml) ships with — and deliberately
the *same* recommendation whether this is a first run or a cross-version
migration. There's no separate "cross-version mode": every check that
specifically matters for a version gap (version/FCV info, replica set
topology, real GridFS content hashing) is now either always-on or purely
informational, not something you'd toggle differently per scenario.

```yaml
verify:
  auth: true
  cluster: true
  schema: true
  index: true
  data: true
  gridfs: true
  views: true
  sharding: false   # turn on only if source/target are sharded
  bidirectional: true

skip_dbs:
  - local
  - config
  - admin   # admin's meaningful content (users, roles, replset config) is
            # already covered by the auth/cluster checks above; raw-comparing
            # the rest of admin always false-positives on user credentials
            # (SCRAM salt is fresh per createUser call, even on a correct
            # migration) — see docs/TESTING.md section 9.5
```

Everything else (`batch_size`, `max_workers`, `rate_limit_ms`,
`sample_rate`, `range_split_threshold_docs`, `range_workers_per_collection`,
`hash_options`) ships with safe defaults already tuned for reading a live
production source without disrupting it — see [`config.yaml`](config.yaml)
for the full annotated list, [`docs/DESIGN.md`](docs/DESIGN.md#9-performance--memory-design)
for the reasoning behind the performance-related ones, and "Large
collections" above for what the two `range_*` settings do.

## Usage

```bash
# All three phases, with prompts between them
./mongogate --phase all

# Just the structural check, right after initial sync
./mongogate --phase 1

# Sampled check while the migration is still running
./mongogate --phase 2

# Full exact check after pausing writes on source — this is the cutover gate
./mongogate --phase 3

# Resume a Phase 3 run that was interrupted
./mongogate --phase 3 --resume

# Restrict to one namespace, or a whole database with a wildcard
./mongogate --phase 3 --include-ns mydb.users
./mongogate --phase 3 --include-ns "mydb.*"

# Scan without comparing (sanity-check connectivity/counts only)
./mongogate --dry-run --phase all
```

### Auto-repair

`--auto-repair` **writes to the target cluster** — after Phase 3 runs, it
copies the diffs it found (missing/different documents, capped at 50
sampled docs per type) from source to target. Run a plain `--phase 3`
first and read the report; only add `--auto-repair` once you've confirmed
the diffs it would copy are genuinely missing data, not a symptom of
something else (e.g. a bug feeding the migration, or a structural drift
Phase 1 should have caught first).

```bash
# 1. Review first
./mongogate --phase 3

# 2. Only then, if the diffs above are what you expect
./mongogate --phase 3 --auto-repair
```

Note also: `--sample` only affects Phase 2. Phase 3 always does a full,
unsampled scan regardless of `--sample` or `sample_rate` in config.yaml —
that's what makes it the cutover gate.

## Scoping a run to one database or collection

`--include-ns` / `--exclude-ns` (or the `include_ns` / `exclude_ns` lists in
`config.yaml`) narrow a run down to specific namespaces. Two patterns are
supported:

- `"mydb.users"` — exactly one collection
- `"mydb.*"` — every collection in `mydb`

```bash
# Only verify mydb.users
./mongogate --phase 3 --include-ns mydb.users

# Only verify the whole mydb database
./mongogate --phase 3 --include-ns "mydb.*"

# Verify everything except one noisy/huge collection
./mongogate --phase 3 --exclude-ns mydb.event_logs
```

`config.yaml` accepts a list, so you can combine multiple patterns:

```yaml
include_ns:
  - mydb.users
  - otherdb.*
exclude_ns:
  - mydb.event_logs
```

The scope applies consistently across **every** Phase 1 structural check —
database list, collection list/options, indexes, views, and GridFS — as well
as Phase 2/3 data verification. Excluding `mydb.*` means mongogate won't
report on, or even read from, that database at all; excluding a single
collection leaves the rest of its database in scope. `include_ns` is a
whitelist: once it has any entries, only matching namespaces are verified and
everything else is skipped, regardless of `exclude_ns`; with `include_ns`
empty (the default), `exclude_ns` acts as a blacklist on top of "verify
everything." The two compose — `include_ns: [mydb.*]` plus
`exclude_ns: [mydb.logs]` verifies all of `mydb` except `logs`. The CLI flags
(`--include-ns`/`--exclude-ns`) only take a single namespace each — for more
than one, list them in `config.yaml` instead.

**One subtlety:** views and GridFS are checked per-*database*, not
per-collection, so scoping `include_ns` to a single collection (e.g.
`mydb.users`) does not silence views or GridFS buckets elsewhere in `mydb` —
they're still verified as long as any pattern puts that database in scope.
To exclude those too, exclude the whole database instead.

## CI/CD integration

```bash
./mongogate --phase 3
if [ $? -eq 0 ]; then
  ./cutover.sh
else
  echo "verification failed, blocking cutover"
  exit 1
fi
```

or poll `GET /passed` on the HTTP API (`http_port` in config) while a run is
in progress — `200` once everything has passed, `500` if anything failed.

## Testing

```bash
go test ./... -v
```

Unit tests cover hash normalization and `DeepCompare` classification in
isolation. The harder questions — does this hold up against a real 3-node
replica set under continuous writes, network partitions, process crashes, and
primary failover — are answered with a real `kind` cluster; see
[`docs/TESTING.md`](docs/TESTING.md) for the full scenario matrix, how to
reproduce it, and the measured results.

Two things this tool **cannot do at all**, by design, not by oversight: see
[Known limitations](docs/TESTING.md#known-limitations) before relying on it
for a migration that uses Vector/Atlas Search indexes or Queryable
Encryption.

Every push and PR runs `go build`/`go vet`/`go test`/`golangci-lint` via
[`.github/workflows/ci.yml`](.github/workflows/ci.yml). Every merge to `main`
builds and pushes a Docker image to `ghcr.io/null-ptr-exception/mongogate`
via [`.github/workflows/release.yml`](.github/workflows/release.yml).

## Deployment

See [`docs/DESIGN.md`](docs/DESIGN.md#18-deployment) for running directly,
via Docker Compose (with Prometheus + Grafana), or as a systemd service for
long-running `--resume` jobs.

## License

No license file is currently included; treat as all-rights-reserved until one is added.
