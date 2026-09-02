# Implementation task: make the coverage guard real, then audit the last two unexamined terminals (P0, five parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/section8-audit`, head `37a3ded`. Verified state: **python 430 passed / 1 known
failure**, **node 12 passed / 1 failed** (a float comparison, Part 1a). The coverage caveats are
corrected, the partial-day trap is closed, the registry is reconciled against measurement for all
nine terminals, and the cross-panel invariant holds.

This is the last stretch before the whole stack merges. Parts 1–3 make it mergeable and prove it
stays correct. Parts 4–5 go after the last two terminals nobody has looked at properly, which is
where the remaining measurement risk lives.

---

## How your report is used

Every number is re-derived before commit. Last round most of it held: the parity rule, the
corrected caveats, the nine-terminal reconciliation and python's 430/1 all checked out, and the
Freeport distribution matched exactly. That is why it is committed.

Three things did not:

1. **node was 12/1, reported as 13/0.** The failure is trivial —
   `strictEqual(1000.0000000000001, 1000)` — but it was reported as passing.
2. **The coverage guard does not read any data.** It imports `LNG_TERMINALS` and asserts internal
   consistency plus hardcoded constants (`strictEqual(fp.expectedCoveragePct, 52.9)`). It cannot
   detect drift between a claim and reality, which was the entire purpose, and it must be
   hand-edited whenever the data legitimately moves.
3. **The fleet aggregate is not reproducible.** Reported peak 13,413.2 MMcf/d on 2026-09-01 with
   Freeport at 1,450.0. Freeport that day is **1,622.0**, and the fleet total on days where every
   terminal posted is **13,913.8**. The 60-day median of complete-day sums is **12,825.9** — close
   to the reported 12,805.6, but reached by summing per-terminal medians from different windows,
   which is not the same quantity.

The pattern across rounds is consistent and worth naming: **your code is good and your arithmetic
over multi-window data is not.** Every fabrication that has survived to a report has been a
number computed over a window where one input did not exist, or summed across windows that do not
align. When you are about to report an aggregate, state the window and the completeness rule
first, then compute inside it.

**If a computation cannot run in your sandbox, hand over the script.** That is always acceptable.
Paste unedited output. If your output disagrees with a number here, the output wins.

---

## Ground rules

**No git commands at all.** Claude commits. This sandbox has destroyed the repository's `.git`
twice during ordinary commit/checkout operations.

**Wide remit**: `docs/js/`, `publishers/`, `scrapers/`, `scripts/`, `config/`, `tests/`,
`.github/workflows/`. `docs/` rules apply — vanilla JS only, zero TypeScript in executable code,
design tokens, `safeRender`, 390px reflow, and paste the `docs/js/` TypeScript grep before
finishing.

**`BLUE_TIDE_HANDOFF.md` is not authoritative.** It has been wrong three times (a skill that never
existed, "Section 8 unshipped" when it was live, a Cove Point zero-day count with no basis). You
may update it, but every number you put in it must come from a computation you ran, and it is
never a source to cite.

Parts 1–3 must land. Parts 4–5 are the valuable tail; stop cleanly and say where you stopped.

---

# PART 1 — the three defects

**1a.** `tests/test_lng_cross_panel_invariant.test.mjs:150` — the genuinely-zero-feed test compares
`1000.0000000000001` to `1000` with `strictEqual`. The behaviour is correct; the assertion needs a
tolerance like its neighbours use. Node must be **13 passed / 0 failed**.

**1b.** Correct the fleet aggregate wherever it appears, using the complete-day rule from Part 3 of
the previous brief. Measured here:

```
days where every terminal posted, latest three:
  2026-08-30   13,770.7 MMcf/d   (72.3% of 19,050)
  2026-08-31   13,644.8 MMcf/d   (71.6%)
  2026-09-01   13,913.8 MMcf/d   (73.0%)

per-terminal on 2026-09-01:
  Plaquemines 3884.7   Calcasieu 1660.3   Corpus 2647.1   Sabine 1466.8
  Cameron     1426.2   GoldenPass 358.8   CovePoint 847.8  Freeport 1622.0

60-day median of complete-day sums: 12,825.9 MMcf/d
```

Note the correct construction: **sum the terminals per day first, then take the median of those
daily sums.** Summing each terminal's own median is a different and wrong quantity — the medians
land on different days.

**1c.** Fix the `docs/js/` header comment in `lng-terminals.js` and anywhere else carrying the old
aggregate, so the figure, its basis, and its date travel together.

# PART 2 — a coverage guard that actually reads the data

This is the durable piece and it is currently missing. The guard must **recompute each terminal's
coverage from `data/curated/*.parquet` and fail when the registry's documented figure drifts
beyond tolerance.** A test that only checks the registry against itself would not have caught the
80%-versus-52.9% error that made this necessary.

The obstacle is that the registry is JavaScript and the data is parquet, so the natural home is a
**pytest** test that reads both. Decide how Python gets at the registry values and justify it —
options include exporting the machine-checkable fields to a JSON sidecar as part of the build,
having `publishers/export_dashboard_json.py` emit them into the bundle, or parsing the registry.
Pick one, state the trade-off, and make the sidecar (if you choose one) generated rather than
hand-maintained, because a hand-maintained copy is the same rot in a new place.

The guard must:

- recompute per-terminal coverage from curated, using the settled cycle rule (SQ only, hourly
  `id{HH}00` excluded) and the complete-day rule from Part 1b,
- compare against the registry's `expectedCoveragePct` within `coverageTolerancePct`,
- **fail loudly with the terminal name, claimed value, measured value and drift** when they part,
- and prove itself: a test that deliberately perturbs a claim and asserts the guard rejects it.

Reconsider the ±15% tolerance too. It was chosen without analysis. Look at the actual day-to-day
and month-to-month variability per terminal and set it from that — Golden Pass at 12.7% and
climbing has different natural movement than Cove Point at 97%. A single global tolerance may not
be right; per-terminal is fine if you justify it.

# PART 3 — prove the stack is mergeable

Before this merges I need one artefact: a single script that runs every check and prints a verdict.

Write `scripts/preflight.py` (name it as you like) that runs and reports, in one pass:

- the integrity board (`python -m validators.run_integrity`) and its overall verdict,
- `scripts/task3_validate.py` and whether every case reports correct,
- the alert replay, printing how many alerts would fire over the last 90 days per terminal,
- the coverage guard from Part 2,
- curated row counts and latest period per source,
- and a final PASS/FAIL line.

This is the thing I will run before merging, and the thing that should run in CI afterwards.
Wire it into a workflow if that is straightforward; say so if it is not.

Then run it and paste the entire output, whatever it says. If it prints FAIL, that is a useful
result and I want to see it rather than a fixed-up version.

# PART 4 — Cameron: 72.9% of nameplate, and nobody has checked why

Cameron LNG measures **1,458.6 MMcf/d against a 2,000 MMcf/d nameplate — 72.9%**, via
`cameron_interstate_sq_772300_d` on the gasnom source. That number has never been audited. Three
possibilities and they have very different consequences for the dashboard:

1. **Cameron genuinely runs at ~73%** — plausible for a 3-train facility with one train down or in
   maintenance, in which case the number is right and should be documented as measured-complete.
2. **We are seeing one pipeline of several.** Cameron is fed by Cameron Interstate Pipeline, and
   possibly by other interconnects we do not scrape. If so it is **measured-partial** like Sabine
   and the UI is currently overstating what it knows.
3. **The headline meter is a feeder rather than the plant intake** — the Cove Point error, where
   summing or picking the wrong meter gives pipeline throughput rather than feedgas.

Apply the discipline that resolved Cove Point and Sabine: enumerate every location the gasnom
source publishes for Cameron, look for a consolidated plant-intake or plant-gate meter, run the
twin-meter check (correlation and mean difference) before treating any two as summable, and ask
whether the pipe is dedicated to the terminal or carries pass-through to other customers.

Report the verdict and update the registry's status and caveat to match. **Do not promote or demote
a confidence tier** — that is gated by the agreement audit and wants its own review — but say what
tier you believe is correct and why.

# PART 5 — Golden Pass: 12.7% and rising

Golden Pass measures **330.4 MMcf/d against 2,600 nameplate — 12.7%**, and is described as a
commissioning ramp. Both parts of that need checking now that the tooling exists.

- Is 12.7% consistent with commissioning, or does it look like partial visibility? Plot the
  trajectory over the available history and say whether it is a ramp, a plateau, or noise.
- Same meter audit as Part 4: is `golden_pass_sq_1097217_d` the plant intake, one feeder among
  several, or something else?
- Golden Pass is the terminal most likely to move materially in the next months. Whatever
  tolerance Part 2 sets for it must survive a genuine ramp without either crying wolf or going
  blind. Say what you chose and what it would tolerate.

If either terminal turns out to be measured-partial, that changes the fleet aggregate's meaning —
the ~12,826 MMcf/d figure would be a floor, not a measurement. Say so plainly if that is what you
find; it is exactly the kind of honest downgrade this project has taken before.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm. Part 2 invites a tolerance change — with variability analysis behind it.
2. **Never fabricate a number, a test result, or a command output.** If you cannot run it, hand
   over the script.
3. **Never compute a rate or an aggregate over a window where an input does not exist**, and never
   sum quantities whose windows differ. State the window and completeness rule before computing.
4. **Never mix `_sq_` and `_oac_` in a flow total.**
5. **Twin-meter check before summing any two feeds**; plant-intake meter before summing feeders.
6. **Do not change any nameplate** — FERC docket citations, denominator of every utilisation figure.
7. **Do not remove UI caveats.** Parts 4 and 5 may add them.
8. **Confidence tiers unchanged in this brief** — recommend, do not change.
9. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
10. **No git commands at all.**
11. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
    `docs/js/` TypeScript grep, `node --test tests/*.test.mjs`. Known pre-existing and NOT yours to
    fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps in
    untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and
    `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — node at 13/0 with the run pasted; the corrected fleet aggregate and everywhere it
   now appears.
3. **Part 2** — how Python reaches the registry values and why; the guard failing on a deliberately
   perturbed claim; the variability analysis behind your tolerances.
4. **Part 3** — the entire preflight output, verbatim, PASS or FAIL.
5. **Part 4** — Cameron's location enumeration, twin-meter and pass-through findings, the verdict,
   the registry change, and the tier you would recommend.
6. **Part 5** — Golden Pass's trajectory, meter audit, and the tolerance that survives its ramp.
7. **The `docs/js/` TypeScript grep output.**
8. **Test output** — `pytest`, `ruff`, `mypy`, `node --test`. Baseline **430 passed, 1 failed,
   16 deselected**; node **12 passed, 1 failed** until Part 1a. Derive any difference.
9. **Anything contradicting this brief.** Its numbers are measurements; if the repo disagrees, the
   repo is right.
10. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews, commits, runs preflight, merges the stack, and
verifies the live deploy.
