# Loadtest stack: live verification under write load, watched via Prometheus

A self-contained docker compose environment that answers one question:
**what does mongogate's continuous verification look like on a dashboard
while a source database is being hammered and a lossy CDC pipeline is
struggling to keep the target in sync?**

## Architecture

```
loadgen write ──▶ source (mongo:7.0, single-node replica set rs0)
                    │ change stream
                    ▼
              loadgen mirror ──(lag + drop rate)──▶ target (mongo:7.0)
                    ▲                                  ▲
                    │          mongogate ──────────────┘
                    │          --phase 2 --loop-interval 5s
                    │          (compares source vs target forever)
                    │                │
                    │                ├── :9090 /metrics ◀── Prometheus (:9091 on host)
                    │                └── :8080 HTTP API
                    └── monitor (mongo:7.0, time series progress metrics)
```

- **loadgen write** generates steady insert/update/delete traffic against
  `source.migtest.live_col`.
- **loadgen mirror** tails source's change stream and replays each event to
  target after `MIRROR_LAG`, dropping `MIRROR_DROP_RATE` of them - a stand-in
  for a real CDC pipeline (mongosync/Debezium/etc.) with controllable lag and
  loss, so the verifier always has real drift to report.
- **mongogate** runs in `--loop-interval` mode: one long-lived process
  repeating sampled Phase 2 passes, keeping `/metrics` and the HTTP API up
  continuously between passes (a shell-level restart loop would tear the
  listener down every pass and give Prometheus almost nothing to scrape).
- **source** runs as a single-node replica set because the mirror's change
  stream requires one; `source-init` initiates it idempotently on `up`.

## Quickstart

```bash
cd test/e2e/loadtest
docker compose up --build -d
```

Give it ~30s for the first Phase 2 pass, then open:

| URL | What |
|-----|------|
| http://localhost:9091 | Prometheus UI - query and graph the gauges below |
| http://localhost:9090/metrics | mongogate's raw Prometheus endpoint |
| http://localhost:8080/progress | mongogate HTTP API (also `/health`, `/status`, `/report`, `/passed`) |

Useful queries to graph in Prometheus:

```
mongogate_ns_missing            # docs in source but not target (dropped inserts)
mongogate_ns_different          # docs whose content mismatches (dropped updates)
mongogate_ns_extra_in_target    # docs in target but not source (dropped deletes)
mongogate_ns_src_count          # both sides' doc counts, overlaid
mongogate_ns_tgt_count
mongogate_all_passed            # 0/1 overall verdict
```

With the default 2% drop rate, all three diff gauges climb slowly over time -
that's the deliberately lossy mirror, not a mongogate bug. Set
`MIRROR_DROP_RATE=0` for a stack where drift stays transient (mirror lag only)
and passes eventually go green once writes stop.

Teardown (including data volumes):

```bash
docker compose down -v
```

## Knobs

All optional, set as environment variables on `docker compose up`:

| Variable | Default | Meaning |
|----------|---------|---------|
| `LOAD_RATE` | `30` | writer ops/sec against source |
| `LOAD_DURATION` | `24h` | how long writer and mirror run |
| `MIRROR_LAG` | `20ms` | delay before each change is replayed to target |
| `MIRROR_DROP_RATE` | `0.02` | fraction of changes silently dropped |
| `LOOP_INTERVAL` | `5s` | pause between mongogate Phase 2 passes |

**Constraint: keep `LOAD_RATE` below `1/MIRROR_LAG`.** The mirror replays
serially, sleeping `MIRROR_LAG` per event, so its throughput ceiling is
~`1/lag` events/sec. Exceed it and target falls behind without bound -
verification then correctly reports ever-growing missing counts, which is a
statement about these knobs, not about mongogate. (Confirmed live: 800 ops/s
against a 300ms lag left target 92% behind within minutes.)

## Reading the results honestly

Phase 2 here is a **sampled snapshot of a moving target** - `exact=false` in
every report is the signal for that. Non-zero gauges while writes are flowing
mean "this much drift right now", not "the migration is broken"; see
`docs/TESTING.md` section 1 for why only a quiesced Phase 3 pass is a valid
cutover signal. This stack is for watching the monitoring surface behave, not
for judging a migration.

Two verifier behaviors this stack specifically exercises (both were real bugs
found by running it - see git history):

- a target that has fallen far behind must not turn the pass into a
  per-missing-doc retry storm; absent ids are rechecked in whole-batch `$in`
  queries with the normal retry budget, so lag costs queries, not minutes,
- lookup timeouts are recorded as errors (failing the collection), never
  counted as missing documents.
