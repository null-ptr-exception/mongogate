# Plan: cross-version source/target testing

**Status: executed.** Everything proposed below was carried out for real
against a kind cluster, and then widened to a full 4.4/5.0/6.0/7.0 → 8.0
matrix (this plan only proposed one pair). See `docs/TESTING.md` sections
8-11 for the actual results, evidence, and fixes - this file is kept as
the original pre-execution proposal, not updated to match.

## Context

Every E2E pass so far ran source and target on the **same** MongoDB version
(both `mongo:7.0`, then both `mongo:8.0` for the data-format pass). That's
the easy case. The common real-world case is the opposite: migrations
usually exist specifically *to* move to a different version (e.g. 7.0 → 8.0),
so source and target are version-mismatched by design for the entire
duration of the cutover window. This was never tested. This plan covers what
could go wrong, how to verify it for real, and what (if anything) to change
in the tool as a result.

## Risks identified (not yet confirmed against a real cluster)

| # | Risk | Mechanism | Currently detected by mongogate? |
|---|------|-----------|-----------------------------------|
| 1 | Built-in role privileges differ across major versions | MongoDB sometimes adds new privilege actions to built-in roles (e.g. `readWrite`, `dbAdmin`) between releases | **Yes — and this is new.** The recent fix that made `getRoles()` request `showPrivileges: true` (see `docs/TESTING.md` section 6) means a version-driven privilege difference on a stock built-in role will now surface as a reported diff, indistinguishable from a real migration bug. Before that fix this was invisible; after it, it's a likely new false-positive source specifically for cross-version runs. |
| 2 | Index version fields differ by server default | `2dsphereIndexVersion`, text index version, etc. can default differently depending on which version created the index | Yes, reported as a diff — but it may not indicate an actual problem, just a version-driven default. |
| 3 | Collation behavior drifts silently | Each MongoDB release bundles a specific ICU (Unicode) library version; the same `{locale, strength}` spec can sort/compare certain characters differently between ICU versions | **No.** `VerifyCollections` only compares the collation *option string*, never actual sort/compare behavior. Same spec on both sides reads as "match" even if real-world sort order differs. |
| 4 | FCV (Feature Compatibility Version) mismatch | A cluster can run newer binaries with an older FCV set (common during staged rollouts) - this is a real operational difference, separate from binary version | **No.** `cluster_verifier.go` never reads or compares FCV at all - not even enough to flag that source and target are at different FCVs. |
| 5 | Feature availability asymmetry | Time series (5.0+), clustered collections (5.3+), Queryable Encryption (6.0+/7.0+ for range), Vector Search (Atlas-only, various) | Indirectly, via existing structural checks (e.g. "missing collection") - mainly relevant for downgrade/rollback scenarios, not forward migrations. |

Risk #1 is the most concrete and the most likely to actually fire in
practice - directly created by a fix landed in this same testing pass.

## Test plan

1. Bring up the kind environment as before, but seed it cross-version:
   `mongo-source` StatefulSet on `mongo:7.0` (or `mongo:6.0` to widen the
   gap), `mongo-target` on `mongo:8.0`.
2. Run the existing `loadgen seed-baseline` + `seed-users` unchanged (same
   fixtures used throughout this testing pass, so any new diff is
   attributable to the version gap, not to a different dataset).
3. Run Phase 1 and capture *every* diff that appears. For each one,
   determine: is this a real difference a human should care about before
   cutover, or purely a version-driven artifact (built-in role privileges,
   index version numbers)?
4. Manually compare `buildInfo` and `getParameter({featureCompatibilityVersion:1})`
   on both sides via `mongosh` (mongogate doesn't check this today - see
   risk #4) to confirm whether FCV itself was also mismatched in the test,
   and whether that correlates with any of the diffs found in step 3.
5. Best-effort: see if a collation edge case can be constructed that
   demonstrably sorts differently between the two ICU versions bundled
   with 7.0 vs 8.0. This may not be conclusively reproducible without
   deeper research into which characters' collation weights actually
   changed between those specific ICU versions - treat as exploratory,
   not a hard requirement of this plan.
6. Document actual findings in `docs/TESTING.md` (new section), following
   the same evidence-first style as the rest of that file - real command
   output, not predictions.

## Possible follow-up changes (decide after step 6, not before)

- Add an informational (non-blocking) line to the Phase 1 report showing
  source/target `buildInfo.version` and FCV side by side, so a human
  reading the report has the context to correctly interpret any
  version-driven diffs instead of being surprised by them.
- Open design question, needs a decision before implementing: should
  built-in-role-privilege diffs and index-version-only diffs be
  downgradable to a non-blocking warning (e.g. via a config flag), given
  they may be expected noise specifically in cross-version migrations? Or
  is surfacing them as-is the more honest default, leaving the
  interpretation to the human? No change should land here without
  explicitly deciding this rather than guessing.

## Verification

- Actually run the steps above against a real kind cluster - this entire
  plan exists because the conceptual risk table above is unconfirmed.
- Any new diffs found get attributed to a specific, named cause (not left
  as "something differs"), the same standard applied throughout
  `docs/TESTING.md` so far.
- If any follow-up change is implemented, it ships with `go build && go vet
  && go test ./... && golangci-lint run` clean, consistent with every other
  change in this repo.
