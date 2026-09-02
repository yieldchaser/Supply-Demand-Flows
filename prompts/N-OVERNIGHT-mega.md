# N / OVERNIGHT — Blue Tide: get the stack mergeable, then extend coverage

**Repo:** `yieldchaser/Supply-Demand-Flows` · **Branch:** `fix/section8-audit`, head `612951a`
**Expected duration:** long. Work continuously through the stages below. Do not stop to ask
whether to continue.

---

## 00 / RUNTIME

- Windows sandbox. Your subprocess runner has intermittently hung on `pwsh`; `python`, `node` and
  `grep` have all worked at times. **Test early which of them work tonight** and record the result
  in your state file. If a runner is dead, say so once and hand over scripts instead of estimating.
- `python`, `pandas`, `pytest`, `ruff`, `mypy`, `node` (v18+, `node --test`) are available.
- **No git. At all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
  destroyed this repository's `.git` twice during ordinary commit operations and the recovery both
  times was a full re-fetch from GitHub. Claude commits in the morning. Leave everything in the
  working tree.

## 01 / TASK

You are finishing a production data pipeline, not prototyping one. **Perform the actual work** —
edit files, run commands, paste output. Do not produce a plan and stop.

The stack on this branch is one working session from being merged into a live public dashboard.
Three things currently block it, and beyond them there is real coverage work that has never been
attempted. Both are specified below.

**Stop only for:** credentials you do not have, an action that would be destructive outside this
repository, or an ambiguity that cannot be resolved without changing what the project is. Anything
else — pick the option you can defend, record the decision and the reasoning in your state file,
and keep moving. A reversible assumption recorded in writing is always better than a halt.

## 02 / PROTOCOL

**Maintain `OVERNIGHT_STATE.md` in the repo root** from your first action. It is your memory across
context compaction and the thing that will be read first in the morning. Structure:

```
# Overnight state — <date>
## Runner check
python: works|hangs   node: works|hangs   pytest: works|hangs   (record once, early)
## Stage log
- [x] N1 ... one line, with the evidence that closed it
- [ ] N2 ...
## Decisions taken
- <decision> — <why> — <what would reverse it>
## Numbers measured tonight
- <quantity> = <value> — <command that produced it> — <window and completeness rule>
## Blocked / needs Claude
- <thing> — <why>
```

Update it after **every** stage, not at the end. If you are compacted mid-run, that file is what
lets you resume without redoing work.

**Every number you record must carry the command that produced it and the window it covers.** This
is the single discipline that has failed most often here: coverage computed over a window where one
feed did not exist, aggregates summed across misaligned windows, and — last round — a
`PREFLIGHT VERDICT: PASS` transcript for a script that crashes on import and has never run once.

## 03 / BUILD ORDER

Sequential. Do not start a stage until the previous one's exit condition is met and logged.

### N1 — make preflight actually run *(exit: pasted output of a real run)*

`scripts/preflight.py` crashes immediately:

```
ModuleNotFoundError: No module named 'scripts.load_registry'
```

There is no `scripts/__init__.py`. Fix the import path (package marker, or drop the `scripts.`
prefix, or `sys.path` handling — your call, justify it). Then run it and paste **whatever it
prints**, including failures. Its current hardcoded expectations are wrong and will surface as
mismatches; that is the point.

Known-wrong values inside it that must come from real reads: it prints `baker_hughes 138 rows`
(real: 32,893), `eia_lng 65` (real: 2,665), and an integrity board with all twelve sources PASS
when five currently WARN on gaps.

### N2 — the coverage guard, working *(exit: guard passes on truth, fails on a perturbed claim)*

Both tests in `tests/test_coverage_guard.py` fail:

```
assert 5 == 9        # scripts/load_registry.py extracted 5 of 9 terminals
KeyError: 'nameplate'
```

The regex parser over `lng-terminals.js` is too fragile for this job. **Replace the mechanism.**
The registry is the single source of truth and must stay so, so generate a machine-readable
sidecar from it rather than hand-maintaining a copy — for example have
`publishers/export_dashboard_json.py` emit the machine-checkable fields, or add a small generator
with a test asserting the sidecar is in sync with the JS. State the trade-off you accepted.

The guard must recompute coverage from `data/curated/*.parquet` using the settled cycle rule
(SQ only, hourly `id{HH}00` excluded) and the complete-day rule, compare against the registry
within per-terminal tolerance, and fail loudly naming terminal, claimed, measured and drift.

### N3 — a green board *(exit: `pytest` at 431+ passed / 1 known failure, `node --test` 13/0, preflight PASS or an honest FAIL)*

Get the suite back to a single known failure (`test_build_universe_covers_expected_totals`, 717 vs
719 — not yours to fix). If preflight legitimately reports FAIL because the integrity board has
WARNs, that is an honest result: decide whether preflight's verdict should treat WARN as pass, and
justify it. **Do not make it green by weakening a check.**

Also: `publish-dashboard.yml` now runs preflight before building the bundle. Merging that while it
crashes would break publishing. Either make it robust and keep it, or move it to its own workflow.

### N4 — Cameron becomes measured-partial *(exit: registry, UI caveats and every aggregate claim consistent)*

Your Cameron audit is the strongest finding in weeks and it holds up: CIP delivery point `772300`
has a design capacity of 1,560,000 Dth/d (~1,522 MMcf/d), so at a measured median of 1,458.6 it is
running at ~96% of *its own* capacity, not 73% of a terminal. Cameron is measured-partial, like
Sabine, with the remainder on Columbia Gulf Transmission's Cameron extension.

Carry that through everywhere:

- registry status and `coverageNote`, matching how Sabine and Freeport are described,
- the UI caveat on every panel that shows Cameron,
- **the fleet aggregate's meaning.** With Sabine ~30%, Freeport ~53% and now Cameron ~73%, the
  12,825.9 MMcf/d figure is an interstate-visible **floor**, not a census. Say that wherever the
  number appears, including `docs/js/util/lng-terminals.js` and `BLUE_TIDE_HANDOFF.md`.
- Do **not** change a confidence tier — recommend it; the agreement gate governs that.

Your ~16,376 MMcf/d "true physical feedgas" estimate must **not** appear in the UI. It is an
estimate built on capacity assumptions, and this project does not publish estimates as
measurements. Keep it in your state file as analysis.

### N5 — Columbia Gulf recon *(exit: a verdict in `docs/VERDICT.md`, scraper only if the evidence supports it)*

This is the next real coverage win and nobody has looked. Cameron's invisible ~500 MMcf/d arrives
on TC Energy's Columbia Gulf Transmission (Cameron extension, FERC CP15-514).

Establish, as research first:

1. Does Columbia Gulf publish a public, unauthenticated EBB with scheduled quantities? TC Energy's
   platform, its endpoints, whether it needs a session, what cycles it posts.
2. Can you locate the specific delivery meter into Cameron LNG? Name and location code.
3. Is it a dedicated delivery or does that pipe serve other customers (the pass-through question
   that decided Cove Point)?
4. Would adding it double-count against anything already scraped (the twin-meter question)?

**If it is public and clean, build the scraper** to this project's conventions: `HttpClient`,
`SafeWriter`, `HealthWriter` with `record_no_op` on empty runs and `record_guard_failure` when a
guard raises, `assert_response_identity` on a response type that structurally contains the marker,
a transformer routing through `merge_into_curated`, canonical schema
`source|series_id|series_name|period|value|unit|region|ingested_at`, `series_id` carrying every
dimension including flow and cycle, `config/meters/` entry, `integrity_rules.yaml` entry with
`mode: accumulation` and a `gap_rule`, tests, and a workflow on a NAESB-aligned cron that commits
its own health file.

**If it is not public, write the negative in `docs/VERDICT.md`** with what you searched and what
would change the answer — the same shape as the Sabine and AISStream verdicts. A clear negative
ends the question for the next person; a vague one makes them redo it.

### N6 — the alert path, end to end *(exit: replay output pasted, counts per terminal)*

With the parity fix in place, replay feedgas alerting over the last 90 days and over full history.
Report alerts per terminal per month. Confirm the partial-day false `ACUTE_DROP` is gone. Confirm
dry-run makes no network call and missing credentials degrade to a clean skip.

If the rate is zero everywhere across full history, say so — an alert that never fires is untested,
and you should construct a synthetic case proving it *would* fire on a real outage.

### N7 — documentation truth pass *(exit: every asserted number traces to a command)*

Sweep `docs/js/`, `BLUE_TIDE_HANDOFF.md`, `docs/VERDICT.md` and `analysis/` for numeric assertions.
For each: does it trace to a computation, and is it still true? Fix or delete what does not. This
codebase has repeatedly shipped comments describing a world that no longer exists — "25 cargo
zeros", "Plaquemines: no curated data", "~80% coverage", a fleet figure from three ramps ago.

Record in your state file a list of every number you changed, with the command behind the new one.

## 04 / REVIEW LOOP

After each stage, before logging it complete, re-derive your own claims:

1. **Re-run, do not remember.** If you reported a count, run the command again and read it.
2. **Check the window.** For any rate or aggregate: over what dates, and did every input exist
   across all of them? Every fabrication in this project's history has been a window error.
3. **Check the direction of error.** A number that makes coverage look better than it is gets
   double-checked. The 80%-versus-52.9% error would have shipped a flattering falsehood to readers.
4. **Paste, do not summarise.** Unedited output, warnings and failures included.

If a stage's exit condition cannot be met, log why in `Blocked / needs Claude` and move to the next
stage. Do not loop on it and do not fake the exit.

## 05 / RUBRIC — exit gate 90/100

Score yourself honestly in the state file before you finish.

| dimension | points | minimum |
|---|---|---|
| Every reported number reproduced by a pasted command | 30 | 27 |
| Suite green: pytest 1 known failure, node 13/0, preflight runs | 20 | 18 |
| Coverage guard reads curated and fails on a perturbed claim | 15 | 13 |
| Cameron carried through registry, UI and aggregate consistently | 15 | 13 |
| Columbia Gulf: scraper to convention, or a verdict that closes the question | 10 | 8 |
| `OVERNIGHT_STATE.md` complete enough to resume from cold | 10 | 9 |

Below 90, keep working. A dimension below its minimum is a fail regardless of total. **Reporting an
honest failure scores full marks on dimension 1; a fabricated pass scores zero across the board**,
because everything gets re-derived in the morning and a false transcript costs a whole round.

## 06 / VALIDATION — cold start

Last, from a clean shell:

```
python scripts/preflight.py
python -m pytest -q -m "not network"
node --test tests/*.test.mjs
grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/
python -m ruff check <your changed .py files>
python -m mypy --strict <your new .py files>
```

Paste all six outputs verbatim at the end of your report. The grep must show JSDoc hits only —
TypeScript syntax in executable JS silently breaks rendering here.

## 07 / NON-NEGOTIABLES

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to quiet an alarm. Threshold changes need evidence — measured variability or a cited
   external cadence.
2. **Never fabricate a number, a test result, or a command output.** If it did not run, say so.
3. **Never compute a rate or aggregate over a window where an input does not exist**, and never sum
   quantities whose windows differ. State the window and completeness rule before computing.
4. **Never mix `_sq_` and `_oac_`** in a flow total. OAC is a residual, anticorrelated with TSQ.
5. **Twin-meter check before summing any two feeds**; look for a plant-intake meter before summing
   feeders. Both rules exist because violating them shipped wrong numbers.
6. **Never publish an estimate as a measurement.** "Observatory, not oracle. Zero randomness."
7. **Do not change any nameplate** — FERC docket citations, denominator of every utilisation figure.
8. **Do not remove UI caveats.** Sharpen them; never soften.
9. **Confidence tiers unchanged** — recommend only.
10. **`merge_into_curated` always** for curated writes; a test enforces it.
11. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
12. **Vanilla JS only in `docs/`** — zero TypeScript in executable code, design tokens only,
    `safeRender` on every panel, 390px reflow.
13. **No git commands.**

Known pre-existing and NOT yours to fix: ~35 mypy errors in
`scrapers/base/playwright_client.py`, mypy gaps in untouched `transformers/baker_hughes.py`, some
ruff in `tests/test_gie_agsi_scraper.py`, and `test_build_universe_covers_expected_totals`
(717 vs 719).

## 08 / REPORT

At the end, in addition to `OVERNIGHT_STATE.md`:

1. Diff summary — every file, one line of reasoning.
2. Stage-by-stage: exit condition met or not, with the evidence.
3. The six cold-start outputs, verbatim.
4. Your rubric score per dimension with justification.
5. Anything contradicting this brief — its numbers are measurements and have been wrong before; if
   the repo disagrees, the repo wins and I want to see it.
6. Anything noticed and not fixed.

Leave everything uncommitted.
