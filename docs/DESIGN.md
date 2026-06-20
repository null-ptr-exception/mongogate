# mongogate — MongoDB Migration Verification System
## Full Design & Feature Specification

**Version:** 2.0
**Language:** Go 1.22
**Lines of code:** ~3,000
**File count:** 21 (+ test/e2e tooling)

---

## Table of Contents

1. [Background & Problem](#1-background--problem)
2. [Design Principles](#2-design-principles)
3. [Architecture Overview](#3-architecture-overview)
4. [Three-Phase Verification Strategy](#4-three-phase-verification-strategy)
5. [Full Verification Scope](#5-full-verification-scope)
6. [Hash Normalization Design](#6-hash-normalization-design)
7. [Deep Compare](#7-deep-compare)
8. [Bidirectional Comparison](#8-bidirectional-comparison)
9. [Performance & Memory Design](#9-performance--memory-design)
10. [Monitoring Design](#10-monitoring-design)
11. [Alerting Design](#11-alerting-design)
12. [HTTP API](#12-http-api)
13. [Prometheus Integration](#13-prometheus-integration)
14. [Grafana Dashboard](#14-grafana-dashboard)
15. [Diff Handling](#15-diff-handling)
16. [Execution Control](#16-execution-control)
17. [Usage](#17-usage)
18. [Deployment](#18-deployment)
19. [Full Feature List](#19-full-feature-list)
20. [Design Decision Q&A](#20-design-decision-qa)

---

## 1. Background & Problem

During a MongoDB migration — whether via mongomirror, mongosync, Debezium, or a
homegrown CDC pipeline — you run into the same set of problems:

```
Problem 1: You don't know whether the data actually finished moving
  → You need to verify count, content, and structure

Problem 2: The verification itself affects the production database
  → You need to read from secondaries, and rate-limit yourself

Problem 3: Data keeps changing while the migration is in flight
  → You can't just diff blindly; you need a staged strategy

Problem 4: Off-the-shelf tools (mongosync) use a lot of memory
  → You need a streaming design with constant memory

Problem 5: Data can be distorted in transit through CDC/Kafka
  → datetime timezones shift, float precision drifts, ObjectIds degrade
  → You need normalized hashing, not a raw byte comparison

Problem 6: When something doesn't match, you don't know which field is wrong
  → You need a deep compare that tells you the exact field and issue type
```

### Why not just use mongosync's built-in verification?

| | mongosync built-in | mongogate |
|---|---|---|
| Memory | 8–16 GB | ~50 MB (constant) |
| Verification scope | Count + basics | 27 distinct checks |
| Diff detail | None | Field-level + issue type |
| Hash normalization | None | 6 normalization rules |
| Bidirectional comparison | None | ✅ |
| Alerting | None | Slack + Email |
| CI/CD integration | None | HTTP API + exit code |
| Auto-repair | None | ✅ |

---

## 2. Design Principles

### Principle 1: Constant memory, independent of data volume

```
❌ Naive approach: load every document into memory before comparing
   1,000,000 docs × 1KB = 1GB of memory

✅ Our approach: cursor streaming
   Pull 500 docs → compare → discard → pull the next 500
   Memory stays ≈ 50MB regardless of total document count
```

### Principle 2: Zero impact on the source database

```
1. readPreference=secondaryPreferred  → reads hit secondaries, not primary
2. Rate limit of 10ms per batch        → actively yields I/O
3. Independent connection pools        → doesn't compete with migration traffic
4. Monitoring uses its own connection  → monitoring writes don't affect verification
```

### Principle 3: Distortion must be explicitly labeled

```
While migration is in flight, every number is an "estimate."
Only Phase 3 (after the source stops accepting writes) is "exact."

Every monitoring sample carries an is_exact field.
Count carries count_is_exact: false.
Hash carries hash_is_exact: true/false.
```

### Principle 4: Interruptible, resumable

```
A checkpoint (checkpoints.json) is saved every 1,000 documents.
If the process crashes mid-run, --resume picks up from the last checkpoint.
No need to start over from scratch.
```

### Principle 5: Only deep-compare when hashes differ

```
Normal documents (99%): hash matches → pass immediately, O(1) cost
Anomalous documents (1%): hash differs → fall into DeepCompare
                                        → pinpoint the exact field and issue
```

---

## 3. Architecture Overview

### Project layout

```
mongogate/
├── cmd/
│   └── mongogate/main.go          # Entry point, phase control, CLI flags
├── internal/
│   ├── config/
│   │   └── config.go              # Config loading, NS filtering, validation
│   ├── verifier/
│   │   ├── types.go               # Shared CollectionTask type
│   │   ├── auth_verifier.go       # Users, roles, LDAP, auth mechanism
│   │   ├── cluster_verifier.go    # Replica set, sharding, server params
│   │   ├── schema_verifier.go     # DB, collection, options, validator
│   │   ├── index_verifier.go      # Every index type
│   │   ├── view_verifier.go       # View definitions
│   │   ├── gridfs_verifier.go     # GridFS metadata + MD5
│   │   └── data_verifier.go       # Document count + hash, multi-threaded
│   ├── utils/
│   │   ├── hasher.go              # SHA256 hash + normalization + DeepCompare
│   │   └── checkpoint.go          # Resume support
│   ├── monitor/
│   │   └── metrics_writer.go      # Time series + TTL event log
│   ├── alert/
│   │   └── alert.go               # Slack + Email alerting
│   ├── progress/
│   │   └── bar.go                 # Terminal progress bar + ETA
│   ├── repair/
│   │   └── repairer.go            # Automatic diff repair
│   ├── export/
│   │   └── csv_exporter.go        # Diff export to CSV
│   ├── httpapi/
│   │   └── server.go              # HTTP API server
│   ├── prometheus/
│   │   └── metrics.go             # Prometheus /metrics endpoint
│   └── report/
│       └── report.go              # Report struct, thread-safe, output
├── test/
│   ├── hasher_test.go             # Unit tests
│   └── e2e/                       # kind-based end-to-end test environment
├── deploy/
│   ├── Dockerfile                 # Multi-stage build
│   ├── docker-compose.yml         # One-command full stack
│   └── prometheus.yml             # Prometheus scrape config
├── docs/
│   ├── PLAN.md                    # Project plan
│   ├── DESIGN.md                  # This document
│   ├── TESTING.md                 # E2E test plan and results
│   └── grafana_dashboard.json     # Grafana dashboard
├── config.yaml                    # Full config file (every option)
├── go.mod
└── README.md
```

### Data flow

```
CLI flags + config.yaml
        ↓
    cmd/mongogate
        ├── Phase 1 ──→ auth / cluster / schema / index / view / gridfs verifier
        │                       ↓
        │               report.SetXxx()
        │
        ├── Phase 2 ──→ data_verifier (sampled)
        │                       ↓
        │         ┌─────────────────────────────┐
        │         │  source cursor (streaming)   │
        │         │  → DocHash quick compare     │
        │         │  → DeepCompare on mismatch   │
        │         │  → progress bar update       │
        │         │  → checkpoint save           │
        │         │  → metrics_writer.Write()    │
        │         │  → alert.Check()             │
        │         │  → prometheus.Set()          │
        │         └─────────────────────────────┘
        │
        └── Phase 3 ──→ data_verifier (full + bidirectional)
                                ↓
                    auto_repair (optional)
                                ↓
                    export CSV (optional)
                                ↓
                    report.Print() + report.Save()
                                ↓
                    alert.Fire() (final result)
                                ↓
                    os.Exit(0/1) (for CI/CD)
```

---

## 4. Three-Phase Verification Strategy

### Why three phases?

```
Data keeps changing throughout the migration:
  Source is still accepting writes
  Target is still catching up on the oplog

→ You can't do an exact comparison while data is still moving
→ Stage it: do the parts that can't be distorted first, save the exact
  comparison for last
```

### Timeline

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
        ~5 min          ~30 min        depends on volume
```

### Phase 1: Structural verification (run right after initial sync completes)

**What's checked:** every static structure that can't be distorted by ongoing writes.

- Users / roles / auth mechanism / LDAP
- Replica set / sharding / server parameters
- Database list
- Collection list, options, validator, collation, time series, TTL, capped
- Every index type
- View definitions
- GridFS metadata (excluding MD5)

**Typical duration:** 1–5 minutes

### Phase 2: Sampled verification (run once oplog lag < 10s)

**What's checked:** a 10% sampled hash comparison.

**Note:** slightly distorted (data is still changing) — results are directional,
confirming the migration is broadly on track.

**Typical duration:** 10–30 minutes

### Phase 3: Full exact verification (run after the source stops accepting writes)

**What's checked:**
- Full, exact count comparison
- Full hash comparison (with all 6 normalization rules)
- Field-level DeepCompare diffs
- Bidirectional comparison (finds documents that only exist in target)
- GridFS content MD5

**This is the final gate — only proceed to cutover once this passes.**

---

## 5. Full Verification Scope

### Security layer

| Item | Description | Phase |
|------|-------------|-------|
| Users | Account list, password hashes (not compared directly) | 1 |
| Roles | Each user's role list | 1 |
| Custom roles | Custom role definitions | 1 |
| Auth mechanism | SCRAM-SHA-256, etc. | 1 |
| LDAP servers | LDAP server settings | 1 |

### Cluster layer

| Item | Description | Phase |
|------|-------------|-------|
| Replica set protocolVersion | RS protocol version | 1 |
| writeConcernMajorityJournalDefault | Write-acknowledgment setting | 1 |
| slowOpThresholdMs | Slow-query threshold | 1 |
| maxIncomingConnections | Max connection count | 1 |
| Shard count | (requires verify.sharding) | 1 |
| Shard key | Per-collection shard key | 1 |

### Database layer

| Item | Description | Phase |
|------|-------------|-------|
| DB count | | 1 |
| DB names | | 1 |

### Collection layer

| Item | Description | Phase |
|------|-------------|-------|
| Collection count | | 1 |
| Collection names | | 1 |
| capped / size / max | Capped collection settings | 1 |
| validator | Schema validation rules | 1 |
| validationLevel / validationAction | Validation behavior | 1 |
| collation | Sort-order rules (matters a lot for non-Latin scripts) | 1 |
| timeseries | Time series settings | 1 |
| expireAfterSeconds | Collection TTL | 1 |
| changeStreamPreAndPostImages | Change stream setting | 1 |
| clusteredIndex | Clustered collection setting | 1 |

### Index layer

| Item | Description | Phase |
|------|-------------|-------|
| Index name | | 1 |
| key | Indexed fields | 1 |
| unique | Unique index | 1 |
| sparse | Sparse index | 1 |
| hidden | Hidden index | 1 |
| expireAfterSeconds | TTL index | 1 |
| partialFilterExpression | Partial index condition | 1 |
| weights | Text index weights | 1 |
| collation | Index collation | 1 |
| wildcardProjection | Wildcard index | 1 |
| 2dsphereIndexVersion | Geo index version | 1 |
| default_language / language_override | Text index language | 1 |

### View layer

| Item | Description | Phase |
|------|-------------|-------|
| viewOn | Source collection for the view | 1 |
| pipeline | View aggregation pipeline | 1 |
| collation | View collation | 1 |

### GridFS

| Item | Description | Phase |
|------|-------------|-------|
| File count | | 1 |
| filename | | 1 |
| length | File size | 1 |
| md5 | Content MD5 (Phase 3 only) | 3 |

### Data layer

| Item | Description | Phase | Precision |
|------|-------------|-------|-----------|
| Count | Document count | 2/3 | estimate/exact |
| Hash comparison | Full SHA256 | 2/3 | sampled/exact |
| Field types | Each field's BSON type | 3 | exact |
| datetime timezone | Normalized to UTC | 3 | exact |
| Float precision | Fixed to 10 decimal places | 3 | exact |
| Decimal128 trailing zeros | Normalized away | 3 | exact |
| Binary subtype | BinData type tag | 3 | exact |
| ObjectId degradation | OID turned into a string | 3 | exact |
| Array order | Optional sorting | 3 | configurable |
| Bidirectional comparison | Extra docs in target | 3 | exact |

---

## 6. Hash Normalization Design

### Why normalize?

Hashing documents directly produces a lot of false positives after a CDC migration:

```
Scenario                       Cause                          Our fix
─────────────────────────────────────────────────────────────────────────
datetime timezone differs      CDC through Kafka shifts tz     Normalize to UTC
0.1+0.2 ≠ 0.3                  float precision error           Fix to 10 decimal places
"123.45" ≠ "123.4500"          Decimal128 trailing zeros        Strip trailing zeros
BinData subtype differs        CDC doesn't preserve subtype     Tag with subtype
ObjectId becomes a string      CDC degrades it via JSON         OBJECTID_DEGRADED
Array order differs            Replay order may differ          Optional sorting
BSON key order is unstable     Marshal order isn't guaranteed   Sort keys before hashing
```

### Type-to-normalization mapping

| BSON type | Normalization | Config switch |
|-----------|----------------|---------------|
| ObjectID | prefixed `"oid:abc123"` | strict_objectid |
| DateTime | UTC + RFC3339Nano | normalize_datetime |
| Decimal128 | big.Float, trailing zeros stripped | normalize_decimal128 |
| Binary | `"bin:subtype=X,data=hex"` | check_binary_subtype |
| float64 | `"f:0.3000000000"` fixed precision | normalize_float_precision |
| int32 | prefixed `"i32:123"` | always |
| int64 | prefixed `"i64:123"` | always |
| Array | optional sort | sort_arrays |
| document | keys sorted, recursive | always |
| Timestamp | `"ts:T,I"` | always |
| Regex | `"regex:/pat/opts"` | always |

### Config

```yaml
hash_options:
  normalize_datetime: true         # required when CDC goes through Kafka
  normalize_float_precision: 10    # 0 = disabled
  normalize_decimal128: true       # strip trailing zeros
  sort_arrays: false               # set false if array order is business-meaningful
  check_binary_subtype: true       # CDC may not preserve subtype
  strict_objectid: true            # ObjectId must not degrade
```

---

## 7. Deep Compare

### Two-stage design

```
Stage 1: DocHash (SHA256) quick compare
  match → return true immediately (zero extra cost)
  mismatch → fall through to stage 2

Stage 2: deepDiff recursively finds the cause
  walks every field
  returns []DiffDetail
```

### DiffDetail structure

```go
type DiffDetail struct {
  Path      string  // field path, e.g. "address.city"
  SrcType   string  // source BSON type
  TgtType   string  // target BSON type
  SrcValue  string  // source value (truncated to 120 chars)
  TgtValue  string  // target value (truncated to 120 chars)
  IssueType string  // see table below
}
```

### IssueType classification

| IssueType | Description | Common cause |
|-----------|-------------|---------------|
| `OBJECTID_DEGRADED` | ObjectId turned into a string | CDC through JSON serialization |
| `TYPE_MISMATCH` | Types differ | int32 vs int64, string vs ObjectId |
| `VALUE_DIFF` | Same type, different value | a genuine data difference |
| `MISSING_FIELD` | Target is missing a field | CDC dropped a field |
| `EXTRA_FIELD` | Target has an extra field | CDC added a field |
| `MISSING_DOC` | Target is missing the whole document | migration never copied it |
| `ARRAY_LENGTH_MISMATCH` | An array of documents has a different element count | replay missed/duplicated an array element |

Arrays of plain values (strings, numbers) are compared as a single
normalized value, same as any other field. Arrays containing at least one
document get recursed into element-by-element, with paths like
`items[2].name` — so a difference nested inside one array element is
reported precisely instead of as one opaque "the whole array differs."

---

## 8. Bidirectional Comparison

### The problem

A one-directional comparison (source → target) only finds documents that
exist in source but not in target.

But during a migration, the opposite can also happen — documents existing in
target but not in source:

```
Case 1: A bug in the migration script wrote extra documents into target
Case 2: Something else is writing into target independently
Case 3: A delete wasn't replicated correctly (a gap in the oplog)
```

### Design

```
Forward (src → tgt): finds MISSING_DOC
  for each doc in source:
    if not in target: → MissingCount++

Reverse (tgt → src): finds EXTRA_IN_TARGET
  for each _id in target:   ← only pulls _id, to save bandwidth
    if not in source: → ExtraInTarget++
```

### Switch

```yaml
verify:
  bidirectional: true  # run the reverse scan during Phase 3
```

---

## 9. Performance & Memory Design

### Memory

```
Batch size: 500 documents
Average doc size: 1 KB
Single worker: 500 KB

4 parallel workers:
  Data:          4 × 500 KB = 2 MB
  Go runtime:    ~20 MB
  Other overhead: ~30 MB
  ─────────────────────
  Total: < 60 MB (constant, independent of data volume)

Compare to mongosync: 8–16 GB
Compare to Debezium:  4–8 GB
```

This holds for the assumption it's built on — many small documents (~1KB),
with batching keeping the concurrently-held set fixed. It does **not** hold
per individual document: a single document is fully decoded, re-normalized,
and JSON-marshaled for hashing (for both source and target), so memory
scales with the size of the largest document encountered, not the documented
baseline. Measured, not assumed: see `docs/TESTING.md`'s large-document
findings (a single 12MB document drove peak RSS to ~917MB).

### Throughput (estimated, 4 workers)

| Data volume | Estimated time |
|-------------|-----------------|
| 1,000,000 docs | 3–8 minutes |
| 10,000,000 docs | 30–60 minutes |
| 100,000,000 docs | 5–10 hours |

These are design-time estimates; see `docs/TESTING.md` for actual measurements
taken in a real cluster.

### Throttling

```
rate_limit_ms: 10     # pause 10ms between every 500-doc batch
                      # reduces pressure on the database
                      # 0 = full speed

max_workers: 4        # parallel collections
                      # more workers = faster but more DB pressure
```

### Retry behavior

```
retry_count: 3        # retry up to 3 times on failure
retry_wait_ms: 500    # wait 500ms between retries
timeout_secs: 30      # per-query timeout

Useful for:
  - brief network blips
  - MongoDB primary elections
  - momentary connection pool exhaustion
```

---

## 10. Monitoring Design

### Time series collection

```
DB: migration_monitor
Collection: verify_metrics (time series)
TTL: 7 days, auto-expired

Write frequency: every 1,000 documents
```

### Monitoring fields

| Field | Description | Precision |
|-------|-------------|-----------|
| timestamp | write time | UTC |
| ns | namespace (db.col) | |
| phase | phase2_sample / phase3_full | |
| processed_docs | documents processed so far | exact |
| total_docs | total document count | exact |
| progress_pct | progress % | exact |
| missing_count | missing document count | exact |
| different_count | different document count | exact |
| src_count | source count | **estimate** |
| tgt_count | target count | **estimate** |
| count_diff | count gap | **estimate** |
| is_exact | whether this comparison is exact | |
| duration_seconds | elapsed seconds | |

### Event log

```
DB: migration_monitor
Collection: verify_events (TTL 30 days)

Milestones logged:
  INFO:  Phase 1/2/3 complete, verification passed
  WARN:  oplog lag exceeded threshold
  ERROR: verification failed, diffs found
```

---

## 11. Alerting Design

### Trigger conditions

| Condition | Description | Severity |
|-----------|-------------|----------|
| missing_count > threshold | missing documents exceed the configured value | CRITICAL |
| different_count > threshold | different documents exceed the configured value | CRITICAL |
| progress unchanged for N minutes | verification appears stuck | WARNING |
| overall verification passed | cutover notification | INFO |
| overall verification failed | blocks cutover | CRITICAL |

### De-duplication

The same key (e.g. `missing:mydb.users`) won't fire again within a 10-minute
window, to avoid an alert storm. This cooldown is currently hardcoded, not
configurable — see `docs/TESTING.md` for the implications.

### Slack message format

```
[CRITICAL] 🚨 Missing document count exceeded threshold
  NS:      mydb.users
  Message: [mydb.users] missing=15 threshold=0
  Level:   CRITICAL
  Time:    2026-06-17 14:30:00
```

### Config

```yaml
alert:
  enabled: true
  slack_webhook: "https://hooks.slack.com/services/xxx/yyy/zzz"
  email_smtp: "smtp.gmail.com:587"
  email_from: "alert@company.com"
  email_to:
    - "dba@company.com"
    - "devops@company.com"
  missing_threshold: 0      # 0 = alert on the first missing doc
  different_threshold: 0
  stuck_minutes: 10
```

---

## 12. HTTP API

### Startup

```yaml
http_port: 8080   # 0 = disabled
```

### Endpoints

| Endpoint | Description | Use case |
|----------|-------------|----------|
| `GET /health` | service health | load balancer health check |
| `GET /status` | verification summary (per-section pass/fail) | dashboards |
| `GET /progress` | per-namespace detailed progress | polling |
| `GET /report` | full JSON report | downloading complete results |
| `GET /passed` | 200=passed / 500=failed | CI/CD gating |

### CI/CD integration examples

```bash
# Option 1: exit code
./mongogate --phase 3
if [ $? -eq 0 ]; then
  ./cutover.sh
else
  echo "Verification failed, blocking cutover"
  exit 1
fi

# Option 2: HTTP polling (while verification is still running)
while true; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/passed)
  if [ "$STATUS" = "200" ]; then
    echo "Passed!"
    break
  elif [ "$STATUS" = "500" ]; then
    echo "Failed!"
    exit 1
  fi
  sleep 30
done
```

---

## 13. Prometheus Integration

### Startup

```yaml
prometheus_port: 9090  # 0 = disabled
```

### Metrics format (`/metrics`)

```
# Per-namespace metrics
mongogate_ns_passed{ns="mydb_users"} 1
mongogate_ns_missing{ns="mydb_users"} 0
mongogate_ns_different{ns="mydb_users"} 0
mongogate_ns_src_count{ns="mydb_users"} 150000
mongogate_ns_tgt_count{ns="mydb_users"} 150000
mongogate_ns_progress_pct{ns="mydb_users"} 100

# Overall
mongogate_all_passed 1
```

### Suggested Prometheus alert rules

```yaml
groups:
  - name: mongogate
    rules:
      - alert: MissingDocs
        expr: mongogate_ns_missing > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.ns }} has {{ $value }} missing documents"

      - alert: DifferentDocs
        expr: mongogate_ns_different > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.ns }} has {{ $value }} different documents"

      - alert: VerifyStuck
        expr: increase(mongogate_ns_progress_pct[10m]) == 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Verification progress is stuck"
```

---

## 14. Grafana Dashboard

### Import steps

```bash
# 1. Install the MongoDB datasource plugin
grafana-cli plugins install grafana-mongodb-datasource

# 2. Restart Grafana
systemctl restart grafana-server

# 3. Add a datasource
#    Type: MongoDB
#    URL: mongodb://monitor:27017
#    Database: migration_monitor

# 4. Import the dashboard
#    Import → Upload JSON → docs/grafana_dashboard.json
```

### Dashboard layout

```
┌──────────┬──────────┬──────────┬──────────┐
│ Overall  │ Current  │ Missing  │Different │
│ Progress │ Phase    │ Count    │ Count    │
│ Gauge    │ Stat     │ Stat     │ Stat     │
│ 0–100%   │ phase3   │ 0 (green)│ 0 (green)│
├──────────┴──────────┴──────────┴──────────┤
│        Verification Progress Trend         │
│        processed_docs / total * 100        │
├─────────────────────┬─────────────────────┤
│  Count Diff Trend    │  Missing/Different  │
│  expected to → 0     │  trend (expect 0)   │
├─────────────────────┴─────────────────────┤
│      Per-Collection Verification Status    │
│  NS | Phase | progress bar | Missing | Diff│
│  mydb.users | phase3_full | ████ 100% | 0 | 0 │
└───────────────────────────────────────────┘
```

### Panel color rules

```
Progress gauge:
  0–50%   → red
  50–99%  → yellow
  100%    → green

Missing / Different:
  0      → green
  > 0    → red

Collection table rows:
  passed  → green row background
  failed  → red row background
```

---

## 15. Diff Handling

### Auto-repair

```yaml
auto_repair: true   # automatically repair diffs after Phase 3
```

**Repair logic:**
1. Take the doc_id from MissingSample / DiffSample
2. Look up the latest version in source
3. ReplaceOne with upsert=true into target
4. Print fixed/failed counts

**Note:** only the sample (up to 50 documents) is repaired. A full repair
requires re-running the migration.

### CSV diff export

```yaml
export_csv: "auto"   # auto-names the file diff_20260617_150405.csv
```

**CSV columns:**
```
ns, issue_type, doc_id, path, src_type, tgt_type, src_value, tgt_value, detected_at
mydb.users, OBJECTID_DEGRADED, 64a1b2c3..., ref, ObjectID, string, oid:64a1b2c3, 64a1b2c3, 2026-06-17 15:04:05
mydb.orders, VALUE_DIFF, 64a1b2c4..., price, float64, float64, f:99.9000000000, f:100.0000000000, ...
```

---

## 16. Execution Control

### CLI flags

| Flag | Description | Default |
|------|-------------|---------|
| `--phase` | 1 / 2 / 3 / all | all |
| `--resume` | resume from checkpoint | false |
| `--config` | path to config file | config.yaml |
| `--sample` | sample rate (0.1=10%) | 0 (full scan) |
| `--dry-run` | scan only, no comparison | false |
| `--include-ns` | only verify this NS | (all) |
| `--exclude-ns` | exclude this NS | (none) |

### Namespace filter syntax

```
"mydb.users"    → only verify mydb.users
"mydb.*"        → verify every collection in mydb
```

The filter applies consistently across every Phase 1 structural check
(database list, collection list/options, indexes, views, GridFS) and Phase
2/3 data verification — not just data verification. A whole-database exclude
(`"mydb.*"`) takes that database out of scope entirely; excluding a single
collection leaves the rest of its database in scope. See `config.DBInScope`
and `config.NSFilter` in `internal/config/config.go`.

### Checkpoint/resume mechanism

```
Every 1,000 documents, a checkpoint is saved to checkpoints.json:
{
  "mydb.users": "64a1b2c3d4e5f6a7b8c9d0e1",
  "mydb.orders": "64a1b2c3d4e5f6a7b8c9d0e2"
}

The next --resume run continues from that _id.
Each collection's checkpoint is independent, so they can resume in parallel.
```

---

## 17. Usage

### Install

```bash
# Option 1: build from source
git clone <repo>
cd mongogate
go mod tidy
go build -o mongogate ./cmd/mongogate
./mongogate --phase all

# Option 2: Docker
cd deploy
docker-compose up
```

### Configure config.yaml

The minimum required settings:
```yaml
source_uri: "mongodb://source:27017/?readPreference=secondaryPreferred"
target_uri: "mongodb://target:27017/?readPreference=secondaryPreferred"
monitor_uri: "mongodb://monitor:27017/"
```

### Full workflow

```bash
# Step 1: after initial sync completes, run the structural check
./mongogate --phase 1
# confirm ✅ everything passed

# Step 2: once oplog has caught up, run the sampled check
./mongogate --phase 2
# confirm ✅ broadly on track (slightly distorted, directional only)

# Step 3: pause writes on source, run the full exact check
./mongogate --phase 3
# passed → cutover ✅
# failed → check diff_xxx.csv, repair, and re-run

# Or run everything in one go (auto-waits on oplog, prompts before pausing writes)
./mongogate --phase all
```

### Tests

```bash
go test ./... -v

# Sample output:
# --- PASS: TestDocHash_SameDocSameHash (0.00s)
# --- PASS: TestDocHash_DatetimeUTC (0.00s)
# --- PASS: TestDocHash_FloatPrecision (0.00s)
# --- PASS: TestDeepCompare_ObjectIDDegraded (0.00s)
# ... 12 tests passed
```

---

## 18. Deployment

### Option 1: run directly (simplest)

```bash
./mongogate --config /etc/mongogate/config.yaml --phase all
```

### Option 2: Docker (recommended)

```bash
cd deploy
docker-compose up -d

# View logs
docker-compose logs -f mongogate

# Service endpoints
# Grafana:    http://localhost:3000  (admin/admin)
# Prometheus: http://localhost:9090
# HTTP API:   http://localhost:8080
```

### Option 3: systemd service (long-running)

```ini
# /etc/systemd/system/mongogate.service
[Unit]
Description=MongoDB Migration Verifier

[Service]
ExecStart=/usr/local/bin/mongogate --phase 3 --resume
WorkingDirectory=/var/lib/mongogate
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable mongogate
systemctl start mongogate
journalctl -u mongogate -f
```

---

## 19. Full Feature List

### Verification

| # | Feature | Description |
|---|---------|--------------|
| 1 | Users | account list, roles |
| 2 | Custom roles | custom role definitions |
| 3 | Auth mechanism | SCRAM / x.509 |
| 4 | LDAP | LDAP server settings |
| 5 | Replica set | protocolVersion, writeConcern |
| 6 | Sharding | shard count, shard key |
| 7 | Server parameters | slowOp, maxConnections |
| 8 | Database list | count, names |
| 9 | Collection list | count, names |
| 10 | Collection options | capped, validator, collation |
| 11 | Time series | settings |
| 12 | TTL collection | expireAfterSeconds |
| 13 | Change stream | preAndPostImages |
| 14 | Clustered index | settings |
| 15 | Index types | all 12 index attributes |
| 16 | TTL index | exact-second comparison |
| 17 | Partial index | filter condition |
| 18 | Text index | weights, language settings |
| 19 | Geo index | 2dsphere version |
| 20 | Wildcard index | projection settings |
| 21 | View definitions | viewOn, pipeline, collation |
| 22 | GridFS metadata | count, filename, length |
| 23 | GridFS MD5 | content integrity (Phase 3) |
| 24 | Count comparison | tagged estimate/exact |
| 25 | Hash comparison | full SHA256 |
| 26 | Field type verification | BSON types |
| 27 | datetime timezone | normalized to UTC |
| 28 | float precision | fixed to 10 places |
| 29 | Decimal128 | trailing zeros stripped |
| 30 | Binary subtype | BinData type |
| 31 | ObjectId degradation detection | OBJECTID_DEGRADED |
| 32 | Array order | optional sorting |
| 33 | Bidirectional comparison | finds extra docs in target |

### Execution control

| # | Feature | Description |
|---|---------|--------------|
| 34 | Three-phase execution | 1/2/3/all |
| 35 | Multi-threaded | max_workers control |
| 36 | Resume support | --resume |
| 37 | Sampling mode | --sample |
| 38 | Dry run | --dry-run |
| 39 | Namespace filtering | --include-ns / --exclude-ns |
| 40 | Automatic retries | retry_count + timeout |
| 41 | Rate limiting | protects the source DB |
| 42 | Secondary-preferred reads | doesn't touch primary |

### Monitoring & output

| # | Feature | Description |
|---|---------|--------------|
| 43 | Terminal progress bar | with ETA |
| 44 | Time series monitoring | stored in MongoDB |
| 45 | Event log | 30-day TTL |
| 46 | JSON report | detailed results file |
| 47 | CSV export | diff detail for DBAs |
| 48 | Slack alerts | de-duplicated, 10-minute cooldown |
| 49 | Email alerts | SMTP |
| 50 | Stuck-progress alert | fires after N minutes idle |
| 51 | HTTP API | 5 endpoints |
| 52 | Prometheus | /metrics text format |
| 53 | Grafana dashboard | 8 panels |
| 54 | Grafana annotations | milestone markers |

### Diff handling & engineering

| # | Feature | Description |
|---|---------|--------------|
| 55 | DeepCompare | field-level diff classification |
| 56 | Auto-repair | auto_repair |
| 57 | Unit tests | hasher/DeepCompare coverage |
| 58 | Docker | multi-stage build |
| 59 | docker-compose | one-command full stack |
| 60 | CI/CD integration | exit code + HTTP API |
| 61 | Config validation | clear errors on bad config |

---

## 20. Design Decision Q&A

### Q1: Why SHA256 instead of MD5?

MD5 has known collision risks. More importantly: the choice of hash algorithm
isn't what makes this accurate — the normalization that happens *before*
hashing is. Our 6 normalization rules are what eliminate false positives;
MD5 without normalization would misreport just as much as SHA256 without it.

### Q2: Why not hash the raw BSON bytes?

```
Raw BSON bytes have several problems:
1. BSON key order isn't guaranteed → the same document can produce different bytes
2. int32 vs int64 bytes differ → you can't tell if it's a type issue or a value issue
3. You can't apply type-specific normalization

→ We hash a normalized, serialized representation instead
```

### Q3: Why mark monitoring data as is_exact = false?

A count query against source and a count query against target run at two
different points in time, with writes potentially happening in between — so
the two counts were never a consistent snapshot to begin with. Marking it as
an estimate tells the user that "count diff = 5" might just be a timing
artifact, not necessarily a real discrepancy.

### Q4: How much slower does bidirectional comparison make things?

The reverse scan only pulls `_id` values (not full documents), so it's much
cheaper than the forward pass. For a target with a document count similar to
source, expect roughly a 20–30% increase in total runtime.

### Q5: Is auto_repair safe?

`auto_repair` only repairs the documents captured in MissingSample and
DiffSample (up to 50 each). If there are more diffs than that, you need to
investigate and re-run the migration manually. This cap is a deliberate
safety limit.

### Q6: Can this run against production?

Yes, because it:
- reads from secondaries (`readPreference=secondaryPreferred`)
- rate-limits itself (10ms per batch)
- uses an independent connection pool
- only reads from source, never writes to it

Recommended starting point: `max_workers` 2–4, `rate_limit_ms` 10–50, tuned to
your production load.
