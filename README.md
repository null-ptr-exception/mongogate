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
  every GridFS file's content MD5 is checked. This is the actual cutover gate.

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

## Feature list

| Area | Features |
|---|---|
| Security | Users, roles, custom roles, auth mechanism, LDAP |
| Cluster | Replica set config, sharding, shard keys, server parameters |
| Schema | DB/collection lists, capped/validator/collation/TTL/time-series/change-stream/clustered-index options |
| Indexes | Every index attribute: unique, sparse, hidden, TTL, partial filter, text weights, 2dsphere version, wildcard projection, collation |
| Views | `viewOn`, pipeline, collation |
| GridFS | Metadata + content MD5 |
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
collection leaves the rest of its database in scope. The CLI flags
(`--include-ns`/`--exclude-ns`) only take a single namespace each — for more
than one, list them in `config.yaml` instead.

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
go test ./test/... -v
```

Unit tests cover hash normalization and `DeepCompare` classification in
isolation. The harder questions — does this hold up against a real 3-node
replica set under continuous writes, network partitions, process crashes, and
primary failover — are answered with a real `kind` cluster; see
[`docs/TESTING.md`](docs/TESTING.md) for the full scenario matrix, how to
reproduce it, and the measured results.

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
