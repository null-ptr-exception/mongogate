# MongoDB Migration Verification System — Project Plan

Version: 1.0
Date: 2026-06-17

This is the original pre-implementation project plan, kept as written -
not updated to track what was actually built. For current architecture see
[`docs/DESIGN.md`](DESIGN.md); for what's actually been confirmed against
a real cluster (including several things this plan assumed that turned
out not to hold, like there ever having been a working binary) see
[`docs/TESTING.md`](TESTING.md).

---

## 1. Background & Goals

During a MongoDB migration, you need to ensure:
- Source and target data are fully consistent
- Accounts, permissions, structure, indexes, and data content are all verified
- The verification process doesn't degrade the original database's performance
- Verification progress and results are visualized

---

## 2. Verification Scope Overview

### 2.1 Checklist

| Layer | Item | Priority | Distortion risk |
|-------|------|----------|------------------|
| Security | Users / Roles | 🔴 required | none |
| Security | Auth mechanism / LDAP | 🔴 required | none |
| Security | Field-level encryption | 🟡 optional | none |
| Cluster | Replica set settings | 🔴 required | none |
| Cluster | Sharding / shard key | 🟡 optional | none |
| Cluster | Server parameters | 🟡 optional | none |
| DB | DB count / names | 🔴 required | none |
| DB | DB collation | 🔴 required | none |
| DB | readConcern / writeConcern | 🟡 optional | none |
| Collection | Collection count / names | 🔴 required | none |
| Collection | Capped size limit | 🔴 required | none |
| Collection | Validator (schema) | 🔴 required | none |
| Collection | Collation | 🔴 required | none |
| Collection | Time series settings | 🔴 required | none |
| Collection | TTL settings | 🔴 required | none |
| Collection | Change stream preAndPostImages | 🟡 optional | none |
| Index | Index name / key | 🔴 required | none |
| Index | Unique / sparse / hidden | 🔴 required | none |
| Index | Partial filter | 🔴 required | none |
| Index | TTL expireAfterSeconds | 🔴 required | none |
| Index | Text index weights | 🔴 required | none |
| Index | 2dsphere / wildcard | 🔴 required | none |
| Index | Collation | 🔴 required | none |
| Data | Count | 🔴 required | low (estimate) |
| Data | Content hash (SHA256) | 🔴 required | exact in Phase 3 |
| Data | Field type check | 🔴 required | exact in Phase 3 |
| Data | datetime timezone | 🔴 required | exact in Phase 3 |
| Data | Decimal128 precision | 🔴 required | exact in Phase 3 |
| Data | Array order | 🟡 optional | exact in Phase 3 |
| Data | Binary / BinData | 🔴 required | exact in Phase 3 |
| GridFS | File count / metadata | 🔴 required | low |
| GridFS | Content MD5 | 🔴 required | exact in Phase 3 |

---

## 3. Three-Phase Verification Strategy

### Timeline

```
T0          T1              T2              T3          T4
│           │               │               │           │
Migration   Initial         Oplog lag       Source      Cutover
starts      sync done       < 10s           write-stop
│           │               │               │           │
            ▼               ▼               ▼
        Phase 1         Phase 2         Phase 3
        Structural      Sampled         Full exact
        (no distortion) (slight         (no distortion)
                          distortion)
        ~5 min          ~30 min        depends on volume
```

### Phase 1: Structural verification (run right after initial sync completes)

**What's checked:**
- Users & roles & auth mechanism
- Cluster / replica set settings
- DB list
- Collection list, options, validator, collation
- Every index (all types)
- GridFS metadata

**Characteristics:** static data, no distortion, completes quickly

### Phase 2: Sampled verification (run once oplog lag < 10s)

**What's checked:**
- 10% sampled hash comparison
- Count trend monitoring

**Characteristics:** slightly distorted, directional only, confirms the
migration is broadly on track

### Phase 3: Full exact verification (run after source stops accepting writes)

**What's checked:**
- Full, exact count comparison
- Full hash comparison
- Field-level type verification
- datetime timezone verification
- GridFS content MD5 comparison

**Characteristics:** no distortion — this is the final gate

---

## 4. Distortion Safeguards

| Problem | Safeguard |
|---------|-----------|
| Count queries run at two different points in time | Tag is_exact=false; only Phase 3 is exact |
| Comparing data while the oplog is still catching up | Phase 2 only samples; Phase 3 does the full scan |
| datetime timezone offsets | Normalize to UTC before comparing |
| Float precision issues | Format to a fixed decimal precision before hashing |
| Inconsistent array order | Optional: sort before hashing |
| Monitoring writes affecting the migration | Independent connection + secondaryPreferred |

---

## 5. Performance Specification

| Metric | Value |
|--------|-------|
| Memory usage | < 100MB (constant, independent of data volume) |
| Batch size | 500 docs/batch |
| Parallel workers | 4 (configurable) |
| Rate limit | 10ms/batch (protects the DB) |
| Read source | Secondary (protects primary) |
| Estimated time for 100M docs | 2–3 hours (4 workers) |
| Resume support | ✅ supported, continues after interruption |

See `docs/TESTING.md` for actual measured numbers from the kind test
environment, rather than these design-time estimates.

---

## 6. Monitoring Metrics (written to time series)

| Metric name | Description | Distortion tag |
|-------------|-------------|-----------------|
| progress_pct | verification progress % | exact |
| processed_docs | documents processed so far | exact |
| missing_count | missing document count | exact |
| different_count | different document count | exact |
| src_count | source count | estimate |
| tgt_count | target count | estimate |
| count_diff | count gap | estimate |
| phase | current phase | exact |
| duration_seconds | elapsed seconds | exact |

---

## 7. Grafana Monitoring Design

### Dashboard layout

```
┌─────────────────────────────────────────────────────┐
│  MongoDB Migration Verification Monitor              │
├──────────┬──────────┬──────────┬────────────────────┤
│ Overall  │ Current  │ Missing  │ Different doc       │
│ progress │ phase    │ count    │ count               │
│  78%     │ Phase 3  │   0      │    0               │
├──────────┴──────────┴──────────┴────────────────────┤
│  Verification progress trend (line chart)            │
│  processed_docs over time                           │
├─────────────────────┬───────────────────────────────┤
│  Count diff trend    │  Missing / different trend   │
│  (src - tgt)         │                               │
├─────────────────────┴───────────────────────────────┤
│  Per-collection verification status table            │
│  ns | status | count | missing | different | time   │
└─────────────────────────────────────────────────────┘
```

### Panel configuration

**Panel 1: Overall progress (Gauge)**
```
Query: db.verify_metrics.aggregate([
  {$group: {_id: null, avg_progress: {$avg: "$progress_pct"}}}
])
Threshold: 0 (red) → 50 (yellow) → 100 (green)
```

**Panel 2: Current phase (Stat)**
```
Query: the `phase` field of the most recent document
```

**Panel 3: Missing count (Stat)**
```
Query: the most recent missing_count
Color: 0=green, >0=red
```

**Panel 4: Progress trend (Time series)**
```
Query: processed_docs / total * 100 over time
```

**Panel 5: Count diff (Time series)**
```
Query: count_diff over time
Expected trend: gradually converges to 0
```

**Panel 6: Collection status table (Table)**
```
Query: grouped by ns, latest status
Columns: ns, phase, progress_pct, missing_count, different_count
Row color: passed=green, failed=red
```

### Alert rules

| Alert | Condition | Severity |
|-------|-----------|----------|
| Missing increasing | missing_count > 0 for 5 minutes | 🔴 Critical |
| Different increasing | different_count > 0 for 5 minutes | 🔴 Critical |
| Verification stuck | progress_pct unchanged for 10 minutes | 🟡 Warning |
| Count gap too large | count_diff > 10000 | 🟡 Warning |

---

## 8. Usage

### Install

```bash
git clone <repo>
cd mongogate
go mod tidy
go build -o mongogate ./cmd/mongogate
```

### Configure config.yaml

```yaml
source_uri: "mongodb://source:27017/?readPreference=secondaryPreferred"
target_uri: "mongodb://target:27017/?readPreference=secondaryPreferred"
monitor_uri: "mongodb://monitor:27017/"

batch_size: 500
max_workers: 4
rate_limit_ms: 10

skip_dbs:
  - local
  - config

verify:
  auth: true
  cluster: true
  schema: true
  index: true
  data: true
  gridfs: true
  sharding: false
  encryption: false

phase2_lag_threshold_seconds: 10
sample_rate: 0.1
```

### Running it

```bash
# Run all three phases automatically
./mongogate --phase all

# Run only Phase 1 (structural verification)
./mongogate --phase 1

# Run only Phase 2 (sampled)
./mongogate --phase 2

# Run only Phase 3 (full, resumable)
./mongogate --phase 3

# Resume from a checkpoint
./mongogate --phase 3 --resume

# Restrict to a specific collection
./mongogate --phase 3 --include-ns mydb.users

# Sampled mode for a quick check
./mongogate --phase 3 --sample 0.1
```

### Output

```
Report file: verify_report_20260617_120000.json
Monitoring data: MongoDB time series (migration_monitor.verify_metrics)
TTL: auto-cleared after 7 days
```

---

## 9. Grafana Setup Steps

1. Install the Grafana MongoDB datasource plugin
   ```bash
   grafana-cli plugins install grafana-mongodb-datasource
   ```

2. Add a datasource pointing at the monitor MongoDB instance

3. Import `docs/grafana_dashboard.json`

4. Configure an alert notification channel (Slack / Email)

5. Configure dashboard variables:
   - `$job_id`: filter to a specific migration job
   - `$ns`: filter to a specific collection

---

## 10. Risks & Considerations

| Risk | Description | Mitigation |
|------|-------------|------------|
| Phase 2 distortion | counts are inaccurate while the oplog is still catching up | tag is_exact=false |
| Source load | heavy find() traffic can affect the primary | secondaryPreferred + rate limiting |
| Memory blowup | large documents consume memory | batched cursor streaming |
| Verification interrupted | network blips or process crashes | checkpoint/resume |
| Large GridFS files | MD5 computation takes time | deferred to Phase 3, run in parallel |
| Array order | business logic may rely on array order | array-sort option can be disabled |
