# N / OVERNIGHT — Blue Tide: get the stack mergeable, then extend coverage

**Repo:** `yieldchaser/Supply-Demand-Flows` · **Branch:** `fix/section8-audit`, head `612951a`
**Duration:** long — this is sized so you will not run out of work. Work continuously through the
stages. Do not stop to ask whether to continue. When you reach the end, go to §09 and keep going.

---

## 00 / RUNTIME

- Windows sandbox. Your subprocess runner has intermittently hung on `pwsh`; `python`, `node` and
  `grep` have each worked at times. **First action: test which runners work tonight** and record it
  in your state file. If one is dead, say so once and route around it — do not estimate outputs.
- Available: `python`, `pandas`, `pytest`, `ruff`, `mypy`, `node` (v18+, `node --test`).
- **No git. At all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
  destroyed this repository's `.git` twice during ordinary commit operations; both recoveries were
  full re-fetches from GitHub. Claude commits in the morning. Leave everything in the working tree.

## 01 / TASK

You are finishing a production data pipeline that publishes to a live public dashboard, not
prototyping one. **Do the actual work** — edit files, run commands, capture output. Never produce a
plan and stop.

**Stop only for:** credentials you do not have, an action destructive outside this repository, or an
ambiguity that cannot be resolved without changing what the project is. Everything else: pick the
option you can defend, record the decision and its reversal condition in your state file, continue.
A reversible assumption written down beats a halt.

**Time-box every stage.** If a stage resists after roughly three serious attempts, write what you
tried and what you would need into `Blocked / needs Claude`, and move to the next stage. Do not
spend the night on one problem, and never fake an exit condition to escape one.

## 02 / PROTOCOL

### 2a. State file

**Create `OVERNIGHT_STATE.md` in the repo root as your first action** and update it after **every**
stage. It is your memory across context compaction and the first thing read in the morning.

```
# Overnight state — 2026-09-03
## Runner check
python: ? | node: ? | pytest: ? | ruff: ? | mypy: ?      (fill in immediately)
## Stage log
- [x] N1 <one line> — evidence: logs/N1-preflight.txt
- [ ] N2 ...
## Decisions taken
- <decision> — <why> — <what would reverse it>
## Numbers measured tonight
- <quantity> = <value> — <exact command> — <window + completeness rule> — logs/<file>
## Blocked / needs Claude
- <thing> — <what I tried> — <what would unblock it>
## Rubric self-score (§08)
```

**On resuming after any compaction: re-read `OVERNIGHT_STATE.md` before doing anything else.**

### 2b. Evidence artefacts — this is not optional

Create a `logs/` directory in the repo root. **Every command whose output you cite must be
redirected to a file there**, named for the stage:

```
python scripts/preflight.py            > logs/N1-preflight.txt 2>&1
python -m pytest -q -m "not network"   > logs/N3-pytest.txt 2>&1
node --test tests/*.test.mjs           > logs/N3-node.txt 2>&1
```

Your report cites the file path beside every number. In the morning those files are read and the
commands re-run; a `logs/` file that does not match a re-run, or a cited number with no file behind
it, invalidates the whole stage.

This exists because the last four rounds each reported at least one result that had not happened —
most recently a full `PREFLIGHT VERDICT: PASS` transcript for a script that crashes on import and
has never executed. Writing to a file first makes the honest path the easy path.

### 2c. Every number carries its window

For any rate, median or aggregate, record **the date range and the completeness rule** alongside
the value. Every fabricated figure in this project's history was a window error: coverage over
1,105 days when one of two feeds has 100 (80% claimed, 52.9% real); a fleet total summing
per-terminal medians landing on different days.

## 03 / FORBIDDEN SHORTCUTS

Any of these invalidates the stage, regardless of what else is true:

- deleting, skipping, `xfail`-ing or commenting out a failing test
- loosening an assertion to make it pass, rather than fixing the cause
- `continue-on-error`, `|| true`, or a bare `except:` added to quiet a failing gate
- `# type: ignore` or `# noqa` added instead of fixing the finding
- widening a tolerance or threshold without measured variability or a cited external cadence
- editing `data/health/*.json` by hand — they are outputs
- publishing an estimate as a measurement
- removing or softening a UI caveat

If a check fires and you believe the check is wrong, that is a legitimate finding: write the
argument in your state file, leave the check firing, and move on.

## 04 / BUILD ORDER — P0

Sequential. Do not start a stage until the previous one's exit condition is met and logged.

### N1 — make preflight actually run · P0 · exit: `logs/N1-preflight.txt` from a real run

`scripts/preflight.py` crashes immediately:

```
ModuleNotFoundError: No module named 'scripts.load_registry'
```

No `scripts/__init__.py` exists. Fix the import path (package marker, relative import, or `sys.path`
handling — your call, justify it). Run it. Capture **whatever it prints**, including failures.

Its hardcoded expectations are wrong and will surface: it prints `baker_hughes 138 rows` (real
32,893), `eia_lng 65` (real 2,665), and an integrity board of twelve PASS when five currently WARN
on gaps. Replace every hardcoded expectation with a real read.

### N2 — a coverage guard that reads data · P0 · exit: guard passes on truth, fails on a perturbation

Both tests in `tests/test_coverage_guard.py` fail:

```
assert 5 == 9        # scripts/load_registry.py extracted 5 of 9 terminals
KeyError: 'nameplate'
```

Regex-parsing JavaScript is too fragile for a load-bearing guard. **Replace the mechanism.** The
registry stays the single source of truth, so *generate* a machine-readable sidecar from it — e.g.
have `publishers/export_dashboard_json.py` emit the machine-checkable fields, plus a test asserting
the sidecar matches the JS. Never hand-maintain a second copy; that is the same rot in a new place.

The guard recomputes coverage from `data/curated/*.parquet` under the settled cycle rule (SQ only,
hourly `id{HH}00` excluded) and the complete-day rule, compares against the registry within
per-terminal tolerance, and fails naming terminal, claimed, measured, drift.

### N3 — green board · P0 · exit: `logs/N3-pytest.txt`, `logs/N3-node.txt`, `logs/N3-preflight.txt`

Target: pytest at **1 known failure** (`test_build_universe_covers_expected_totals`, 717 vs 719 —
not yours), node **13/0**, preflight running to a verdict.

`publish-dashboard.yml` currently runs preflight before building the bundle. Merging that while it
crashes breaks publishing. Either make it robust and keep it there, or move it to its own workflow —
decide and justify.

If preflight legitimately reports FAIL because five sources WARN on gaps, that is an honest result.
Decide whether its verdict should treat WARN as pass and justify it. **Do not make it green by
weakening a check.**

### N4 — Cameron becomes measured-partial everywhere · P0 · exit: registry, UI and every aggregate consistent

Your Cameron audit holds up and is the strongest finding in weeks: CIP delivery point `772300` has
a design capacity of 1,560,000 Dth/d (~1,522 MMcf/d), so a measured 1,458.6 median is **~96% of the
pipeline's own capacity**, not 73% of a terminal. Receipts into CIP close against deliveries to
0.18%. Cameron is measured-partial, like Sabine, with the remainder on Columbia Gulf Transmission.

Carry it through: registry status and `coverageNote` in the shape Sabine and Freeport use; the UI
caveat on every panel showing Cameron; and **the fleet aggregate's meaning** — with Sabine ~30%,
Freeport ~53% and Cameron ~73%, the 12,825.9 MMcf/d figure is an interstate-visible **floor**, not a
census. Say so wherever it appears, including `docs/js/util/lng-terminals.js` and
`BLUE_TIDE_HANDOFF.md`.

Do **not** change a confidence tier — recommend it; the agreement gate governs that. Your
~16,376 MMcf/d "true physical feedgas" estimate must **not** reach the UI: it rests on capacity
assumptions, and this project does not publish estimates as measurements. Keep it in state as
analysis.

### N5 — the publisher ships 7.8% of gasnom · P0 · exit: explained, with the ratio for all twelve sources

Open since the first audit and never answered. Live bundle against curated:

```
gasnom       curated  64,256   live shard   5,038   =  7.8%
gulf_south   curated 436,687   live shard  57,226   = 13.1%
quorum       curated 117,480   live shard  28,594   = 24.3%
cheniere     curated   7,062   live shard   2,520   = 35.7%
bhe          curated  14,148   live shard  14,028   = 99.2%
```

Bug #6 in this project's catalogue was exactly this shape: curated was full, the publisher's
relevance prune shipped 5% of it, and the integrity monitor watched *curated* so nothing fired —
gasnom rendered 95% thinned in the UI for weeks.

Read `publishers/export_dashboard_json.py`. Establish for every source whether the ratio is the
intended relevance prune (only `high`-confidence, classified series reach the bundle) or silent
loss. `_audit_bundle_coverage` exists and is deploy-blocking — determine what it actually asserts
and whether a 7.8% shard would trip it. Report the table for all twelve with a verdict each. Fix
only what is genuinely broken; a large prune that is *intended and audited* is fine, and saying so
with evidence closes the question.

### N6 — Columbia Gulf recon · P0 · exit: a verdict in `docs/VERDICT.md`, scraper only if evidence supports it

The next real coverage win. Cameron's invisible ~500 MMcf/d arrives on TC Energy's Columbia Gulf
Transmission Cameron extension (FERC CP15-514).

Research first: does Columbia Gulf publish a public unauthenticated EBB with scheduled quantities —
platform, endpoints, session requirement, cycles posted? Can you locate the specific delivery meter
into Cameron LNG, by name and location code? Is that pipe dedicated to the terminal or does it serve
other customers (the pass-through question that decided Cove Point)? Would adding it double-count
against anything already scraped (the twin-meter question)?

**If public and clean, build the scraper** to this project's conventions: `HttpClient`,
`SafeWriter`, `HealthWriter` with `record_no_op` on empty runs and `record_guard_failure` when a
guard raises, `assert_response_identity` verified on a response type that structurally contains the
marker and matched loosely on a stable token, a transformer through `merge_into_curated`, canonical
schema `source|series_id|series_name|period|value|unit|region|ingested_at`, `series_id` carrying
every dimension including flow and cycle, a `config/meters/` entry, an `integrity_rules.yaml` entry
with `mode: accumulation` and a `gap_rule`, tests, and a workflow on a NAESB-aligned cron that
commits its own health file.

**If not public, write the negative in `docs/VERDICT.md`** with what you searched and what would
change the answer — the shape of the Sabine and AISStream verdicts. A clear negative closes the
question; a vague one makes the next person redo it.

## 05 / BUILD ORDER — P1

Reach these only with N1–N6 logged complete. Same rules, same evidence.

### N7 — KMTP recon for Freeport

Freeport measures 52.9% of nameplate. Same question as Cameron, different pipe: Kinder Morgan Texas
Pipeline's intrastate lateral is unposted, and intrastate pipelines are not FERC-jurisdictional so
they may post nothing at all. Establish whether KMTP publishes anything public, whether Texas RRC
filings expose the volume at any useful frequency, and what the gap really is once measured against
realistic utilisation rather than nameplate. Verdict to `docs/VERDICT.md` either way.

### N8 — the five gap WARNs, annotated

`gulf_south 2026-08-27`, `baker_hughes 2026-01-02`, `gasnom 2026-08-23..25`, `quorum 2025-03-25..27`,
`cheniere 2026-08-25`. Two are already explained (gasnom was a `contents: read` push denial fixed
2026-08-26; quorum was an upstream tenant serving empty CSVs). Classify each remaining one as
upstream posting hole, our outage, legitimate non-posting, or unknown-with-next-step. **Backfill
nothing** — annotation is the deliverable.

### N9 — divergence checks that never run

`baker_hughes` and `eia_storage` divergence is SKIPPED because their health stamp is older than the
3-day recency window (3.6d, 3.9d) — correct behaviour for weekly sources, but it means the check
never runs for them at all. Decide whether the recency window should scale with the source's
cadence, and implement it if so with the cadence cited.

### N10 — Section 8 renders

No test asserts the panel actually renders. `renderTerminalDowntimePanel` imports D3 and touches the
DOM, which is why it was never covered. Get a smoke test in place — a minimal DOM stub, or split the
last DOM-dependent seam so the render path is reachable from `node --test`. Assert it produces
markup for a fixture bundle and does not throw when a terminal has zero events.

### N11 — alert path end to end

Replay feedgas alerting over the last 90 days and over full history with the parity fix in place.
Report alerts per terminal per month; confirm the partial-day false `ACUTE_DROP` is gone; confirm
dry-run makes no network call and missing credentials degrade to a clean skip. If the rate is zero
across all history, construct a synthetic case proving it *would* fire on a real outage — an alert
that has never fired is untested.

### N12 — documentation truth pass

Sweep `docs/js/`, `BLUE_TIDE_HANDOFF.md`, `docs/VERDICT.md`, `analysis/`. For every numeric
assertion: does it trace to a computation, and is it still true? Fix or delete what does not. This
codebase has shipped comments describing a world that no longer exists — "25 cargo zeros",
"Plaquemines: no curated data", "~80% coverage", a fleet figure from three ramps ago. List every
number you changed with the command behind the new one.

## 06 / REVIEW LOOP

After each stage, before logging it complete:

1. **Re-run, do not remember.** Reported a count? Run it again and read the file.
2. **Check the window.** Over what dates, and did every input exist across all of them?
3. **Check the direction of error.** Anything making coverage look *better* gets double-checked —
   the 80%-vs-52.9% error would have shipped a flattering falsehood to readers.
4. **Confirm the artefact exists.** Every cited number has a `logs/` file behind it.

## 07 / VALIDATION — cold start

From a clean shell, redirecting each to `logs/`:

```
python scripts/preflight.py                                                    > logs/final-preflight.txt 2>&1
python -m pytest -q -m "not network"                                           > logs/final-pytest.txt 2>&1
node --test tests/*.test.mjs                                                   > logs/final-node.txt 2>&1
grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/  > logs/final-tsgrep.txt 2>&1
python -m ruff check <changed .py>                                             > logs/final-ruff.txt 2>&1
python -m mypy --strict <new .py>                                              > logs/final-mypy.txt 2>&1
```

The grep must show JSDoc hits only — TypeScript syntax in executable JS silently breaks rendering.

## 08 / RUBRIC — exit gate 90/100

Self-score in the state file before finishing.

| dimension | points | minimum |
|---|---|---|
| Every reported number backed by a `logs/` file that matches a re-run | 25 | 23 |
| Suite green: pytest 1 known failure, node 13/0, preflight runs | 15 | 13 |
| Coverage guard reads curated, fails on a perturbed claim | 12 | 10 |
| Cameron carried through registry, UI and aggregate consistently | 12 | 10 |
| Publisher prune ratios explained for all twelve sources | 10 | 8 |
| Columbia Gulf: scraper to convention, or a verdict that closes it | 10 | 8 |
| P1 stages attempted, with honest outcomes | 8 | 4 |
| `OVERNIGHT_STATE.md` complete enough to resume cold | 8 | 7 |

Below 90, keep working — pick from §09. A dimension under its minimum fails regardless of total.
**An honest failure scores full marks on dimension 1. A fabricated pass scores zero across the
board**, because everything is re-derived in the morning and a false transcript costs a whole round.

## 09 / IF YOU REACH THE END WITH TIME LEFT

Do not idle and do not stop. In order:

1. Raise any rubric dimension below its minimum.
2. Clear anything in `Blocked / needs Claude` that turns out to be unblockable by you — convert each
   into a precise question with the evidence attached.
3. Strengthen the weakest guard in the repo. Candidates: the accumulation guard misses a module that
   calls `merge_into_curated` for one file while direct-writing another, and misses `pq.write_table`;
   the cross-panel invariant covers one terminal; nothing tests that `series_id` carries a flow token
   on write.
4. Extend the golden fixture to every event class the detector can emit.
5. Write `docs/VERDICT.md` entries for anything you investigated tonight that did not become code.

## 10 / NON-NEGOTIABLES

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to quiet an alarm.
2. **Never fabricate a number, a test result, or a command output.** If it did not run, say so.
3. **Never compute a rate or aggregate over a window where an input does not exist**, and never sum
   quantities whose windows differ.
4. **Never mix `_sq_` and `_oac_`** in a flow total — OAC is a residual, anticorrelated with TSQ.
5. **Twin-meter check before summing two feeds**; plant-intake meter before summing feeders.
6. **Never publish an estimate as a measurement.** "Observatory, not oracle. Zero randomness."
7. **Do not change any nameplate** — FERC docket citations, denominator of every utilisation figure.
8. **Do not remove UI caveats.** Sharpen; never soften.
9. **Confidence tiers unchanged** — recommend only.
10. **`merge_into_curated` always** for curated writes.
11. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
12. **Vanilla JS only in `docs/`** — zero TypeScript in executable code, design tokens, `safeRender`
    on every panel, 390px reflow.
13. **No git commands.**

Known pre-existing and NOT yours to fix: ~35 mypy errors in
`scrapers/base/playwright_client.py`, mypy gaps in untouched `transformers/baker_hughes.py`, some
ruff in `tests/test_gie_agsi_scraper.py`, `test_build_universe_covers_expected_totals` (717 vs 719).

## 11 / REPORT

Alongside `OVERNIGHT_STATE.md`:

1. Diff summary — every file, one line of reasoning.
2. Stage by stage: exit condition met or not, with the `logs/` path proving it.
3. The six cold-start outputs, verbatim, with their file paths.
4. Rubric self-score per dimension with justification.
5. Anything contradicting this brief — its numbers are measurements and have been wrong before; if
   the repo disagrees, the repo wins and I want to see it.
6. Anything noticed and not fixed.

Leave everything uncommitted.
