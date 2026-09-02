# Implementation task: settle cycle semantics, then close the two biggest coverage holes (P0, five parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Work on branch `fix/section8-audit` (head `6ee2d2b`, tagged `[UNRESOLVED]`). **Build on it.** Real
progress is banked there: the classifier is extracted to `docs/js/util/lng-downtime.js` free of D3
and the DOM, `node --test` runs six passing JS tests where previously nothing in CI ran any
JavaScript, and feedgas alerting exists with dry-run rendering and a fixed trigger list.

Parts 1–3 finish Section 8. Parts 4–5 are the two biggest untouched holes in the observatory.
**Work them in order.** If you run out of room, stop cleanly after Part 3 and say so — a correct
Section 8 is worth more than a half-built Part 5.

---

## Correction to the previous brief — read before Part 1

Brief H told you the "Freeport 2026-07-15 dip to 142 MMcf/d" case was fiction, on the strength of
a direct parquet read showing 1,955,166 Dth that day. **That read was wrong.** It matched
`24329` and `_d_` without `_sq_`, so it pulled OAC rows into a scheduled-quantity total.

Correct, SQ-only, latest cycle per feed:

```
2026-07-12  1,076,889 Dth  1050.6 MMcf/d   gs=957,568   tetco=119,321
2026-07-13  1,078,474 Dth  1052.2 MMcf/d   gs=959,153   tetco=119,321
2026-07-14  1,062,590 Dth  1036.7 MMcf/d   gs=1,062,590 tetco=0
2026-07-15    265,284 Dth   258.8 MMcf/d   gs=145,963   tetco=119,321
2026-07-16    636,021 Dth   620.5 MMcf/d   gs=600,282   tetco=35,739
2026-07-17    762,173 Dth   743.6 MMcf/d   gs=642,852   tetco=119,321
2026-07-18  1,026,198 Dth  1001.2 MMcf/d   gs=906,877   tetco=119,321
30-day median before 07-15: 1,621,803 Dth
```

`gs=145,963 Dth` is **142.4 MMcf/d** — precisely the handoff's figure. **The dip is real and the
original case was right.** The 16× discrepancy chased in brief H was largely an artefact of that
bad read, so Part 1's premise was wrong and any change made to satisfy it needs revisiting.

The lesson to carry: **never mix `_sq_` and `_oac_` in one total.** OAC is a residual — on Gulf
South, `Operationally Available Capacity = posted capacity − scheduled quantity`, correlated −1.0
with TSQ. Summing them produces a number that means nothing.

---

## Ground rules

**No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
destroyed the repository's `.git` twice during ordinary commit/checkout operations. Claude commits.

**Wide remit** inside these parts: pick designs and proceed, edit `docs/js/`, `scripts/`,
`publishers/`, `config/`, `.github/workflows/`, `tests/`, add modules, fix adjacent bugs (report
each). `docs/` rules apply: vanilla JS only, zero TypeScript in executable code, design tokens
only, `safeRender`, 390px reflow, and paste the `docs/js/` TypeScript grep before you finish.

### Evidence

Three rounds running, the code has been fine and the transcript has not. Last round the report
recorded case 1 as `2024-11-28 dur=239` where the script prints `2024-12-11 dur=251`, case 2 as
`dur=7` where it prints `dur=10`, case 4's value as `322,000` where it prints `1,664,994`, and
case 3 as "OK CORRECT" **where the script prints `MISFIRE`**. The reconciliation table gave Gulf
South 24329 as `1,447,820 Dth` on 2026-07-10 when the largest value across every cycle that day is
`955,727`, so no cycle rule could produce it.

**Paste the command and its unedited output, byte-for-byte** — warnings, encoding artefacts,
failures and all. If you reformat, show the raw form too. Any number not backed by a pasted
transcript is unverified. A failing test reported honestly is worth more to me than a passing one
I have to disprove.

---

# PART 1 — settle what a cycle means, with evidence

This is the question everything else rests on, and it is genuinely unsettled.

TETCO meter 79999 on gas day 2026-07-15 posts:

```
timely=119,321   late=117,996   latec=148,887
id0000..id2300 = 0   (all twenty-four hourly cycles)
```

Under "latest cycle wins" — the rule adopted last round — TETCO reads **zero** for that day.
Under "timely wins" it reads 119,321. Both are defensible from the names alone, and the choice
changes every event the detector emits.

**Answer it from the data, not from the naming convention.** Suggested lines of attack:

- Do those hourly `id{HH}00` cycles carry real zeros, or are they placeholder rows for cycles that
  were never nominated? Compare days where the terminal was certainly flowing: if the hourly
  cycles read zero on days when timely/evening/id1-3 all show strong flow, they are placeholders
  and must be excluded from cycle selection entirely.
- Does every source use the same cycle vocabulary? Gulf South posts only `id1/id2/id3`; TETCO adds
  `timely`, `late`, `latec` and the hourly set. A single global precedence may not fit both.
- NAESB semantics: each cycle revises the prior one for the same gas day, so the last *genuinely
  nominated* cycle is the best estimate of scheduled flow. The operative word is *nominated*.

Decide the rule, implement it identically in `scripts/task3_validate.py` and
`docs/js/util/lng-downtime.js`, and document it where both can be read. Then produce a
**reproducible** reconciliation: ≥10 gas days per terminal, detector total beside a direct
SQ-only parquet read, with the query you used pasted so I can run it myself.

Also confirm Sections 5, 7 and 8 apply the same rule — if the fleet panel and the downtime panel
disagree about what a terminal flowed, the dashboard contradicts itself.

# PART 2 — make the two implementations actually agree

`tests/test_lng_downtime.py::test_golden_fixture_agreement` **fails**. The fixture exists to prove
Python and JS produce identical events; JS passes and Python does not, so they disagree. Current
suite: **424 passed, 2 failed** (the golden fixture, plus the known `test_build_universe_covers_expected_totals`).

Fix the disagreement — do not weaken the fixture or relax the assertion to make it pass. Once
green, extend the fixture to cover every event class the detector emits (`OFFLINE`, `DEPRESSED`,
`RAMPING`, `NOT_YET_OPERATIONAL`, and any cargo-idle state you keep), so the agreement guarantee
covers the whole classifier rather than two of its branches.

# PART 3 — re-ground the validation set on Part 1's rule

With the cycle rule settled, the case set needs rebuilding on true numbers:

- **Freeport 2026-07-15 is a real dip** — restore it as a positive case. Decide from the data
  whether it should classify as `DEPRESSED` and say why: it runs 258.8 → 620.5 → 743.6 MMcf/d on
  15/16/17 July against a ~1,582 MMcf/d baseline, i.e. three consecutive days under 60%, against a
  5-day rule. If the rule says no event, that is an acceptable answer — but say so explicitly and
  consider whether a 3-day deep excursion should have its own class.
- **Case 3 currently prints `MISFIRE`.** Resolve it honestly.
- **Case 2's duration moved** from `dur=7` to `dur=10` when the cycle rule changed. Establish what
  TETCO's April 2024 outage actually was from the data and make the expected value match reality.
- Keep the negative cases: Cove Point plant intake has no zero days; that remains valuable.
- Re-derive per-terminal per-year event rates and give a credibility verdict for each.

# PART 4 — Sabine: find out whether the biggest hole can be closed

Sabine Pass is the largest US LNG terminal and this observatory measures roughly **31% of it**
(`creole_trail_sq_CT200111_d`, ~4,500 MMcf/d nameplate). The rest arrives on feeds we do not see —
Transco Zone 3 among them. That single gap is the biggest analytical weakness in the project.

There is a known precedent for fixing exactly this shape of problem: **Cove Point.** Summing
visible receipt meters gave pipeline throughput rather than feedgas, and the honest number came
from the terminal's own consolidated **plant-intake** meter, `cpl_sq_10001_d`. Nobody has checked
whether Cheniere's API exposes the same thing for Sabine.

**This part is research, not a panel.** Deliver findings, not features:

1. Enumerate what Cheniere's API actually returns for Creole Trail and Corpus Christi — every
   location code, name and flow direction, not just the ones already in `config/meters/cheniere.json`.
   Look specifically for anything resembling a consolidated plant intake, a plant gate, or a
   terminal-total meter.
2. For each candidate, quantify: what does it read, over what history, and how does it compare to
   the 4,500 MMcf/d nameplate and to the ~31% currently measured?
3. Apply the project's own discipline before believing any of it — the twin-meter check
   (correlation and mean-difference before summing any two feeds, since operators on both sides of
   a handoff both publish it) and the pass-through question (is this pipe dedicated to the
   terminal, or does it serve other customers too?).
4. If a plant-intake meter exists, say what promoting it would do to Sabine's coverage and what
   would need to change in `config/meters/cheniere.json` and `docs/js/util/lng-terminals.js`.
   **Do not promote it in this brief** — confidence-tier changes are gated by the registry↔config
   agreement audit and want their own review.
5. If nothing exists, say so plainly and record it in `docs/VERDICT.md` with what you searched, so
   nobody re-runs this hunt in three months.

A clear negative is a good outcome here. Do not manufacture coverage.

# PART 5 — power burn, if there is room

`config/meters/classification.json` already classifies **24 meters as `power_burn`** and nothing
in the dashboard uses them. The data is collected, curated and sitting idle.

Before building anything, establish whether those 24 meters are worth a panel:

- What do they actually cover — which pipelines, which regions, how much volume, how much history?
- Is the coverage coherent enough to mean something, or is it 24 scattered meters that cannot be
  summed into any interpretable quantity?
- What would a reader learn that Sections 1–8 do not already tell them?

**If the answer is that the coverage is too thin to interpret, say so and stop.** This project has
killed two of its own analytical ideas for exactly that reason, and that is a feature. If it is
worth building, build it to the same standard as the other panels — `safeRender`, design tokens,
390px reflow, JSDoc, no TypeScript, and a caveat naming what it does not see.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm. Threshold changes need evidence.
2. **Never fabricate a number, a test result, or a command output.** Negative results are valid.
3. **Never mix `_sq_` and `_oac_` in a flow total** — OAC is a residual, anticorrelated with TSQ.
4. **Twin-meter check before summing any two feeds**; look for a plant-intake meter before summing
   feeders.
5. **Do not remove the UI caveats** about invisible gas (Freeport's KMTP lateral, Sabine's non-CTPL
   feeds). Part 4 may sharpen them; it may not delete them.
6. **Nameplates need FERC docket citations** in JSDoc.
7. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
8. **Confidence tiers:** only `high` survives publisher pruning, and a registry headline must be
   `high` in config — the agreement gate enforces it. Do not change a tier in this brief.
9. **No git commands at all.**
10. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
    `docs/js/` TypeScript grep, and `node --test tests/*.test.mjs`. Known pre-existing and NOT
    yours to fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps
    in untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`,
    and `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — the cycle rule, the evidence that settled it (especially what the hourly `id{HH}00`
   zeros mean), the reconciliation table with the query pasted, and confirmation Sections 5/7/8 agree.
3. **Part 2** — the golden fixture passing in both languages, with both transcripts, and the
   extended class coverage.
4. **Part 3** — every case with its expected and actual classification, the Freeport 2026-07-15
   verdict with reasoning, and per-terminal per-year event rates.
5. **Part 4** — the full Cheniere location enumeration, candidate analysis, twin-meter and
   pass-through findings, and the coverage verdict. `docs/VERDICT.md` updated either way.
6. **Part 5** — the power-burn assessment and your build/don't-build decision with reasoning.
7. **The `docs/js/` TypeScript grep output.**
8. **Test output** — `pytest`, `ruff`, `mypy`, `node --test`. Full-suite counts from an actual run;
   baseline on this branch is **424 passed, 2 failed, 16 deselected**. Derive any difference.
9. **Anything contradicting this brief** — including the correction at the top. The numbers there
   are measurements; if the repo disagrees, the repo is right and I want to know.
10. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
