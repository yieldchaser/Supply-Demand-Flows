# AA — The Fleet Has No Single Depth

Branch: cut a new one from `main` at `ffb36cc`. I commit and I merge.

Five items. The first is a consequence of Z's success and is the one that matters. Everything below
is measured on the host — you are deciding and implementing, not investigating.

**Scoring, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty. A fabricated number forfeits the section *and*
> Stage 0.
>
> **A new test that has not been proven red forfeits its section.**
>
> **Parse every file you touch, JS and Python.**
>
> **The collected test count must not fall.** Record it before you start and again at the end. A
> drop without an explicit deletion in this brief means you destroyed a test, and that forfeits
> Stage 0. §05 makes this mechanical so it stops depending on anyone remembering.

---

## 00 / WHY THAT LAST RULE EXISTS

Z was a real success — 64,430 → 865,730 rows, three years of gasnom, Golden Pass's 738-day
pre-service span rendering correctly. But it also deleted this line:

```python
def test_workflow_run_steps_have_valid_syntax() -> None:
```

The body was absorbed into the function above it, **the file still parsed**, and pytest quietly went
448 → 447. Your report listed 447 as the *before* number, so the drop was invisible.

That is the third consecutive brief with a destructive edit — V removed a `} catch (err) {`, W
replaced a docstring's closing `"""`, Z removed a `def`. The parse check I added after W catches
the first two shapes and cannot catch the third. Hence the count rule, and hence §05.

Z also softened three assertions, which ground rule 7 forbids: the WSL bash guard I had already
reverted once in Y came back; `counts["gasnom"] in (61, 65)`; and
`round(coverage, 1) in (30.3, 30.5)`. **An enumeration of two acceptable values is the tell** — it
hides which one is right. Both had real causes and both are now fixed at the cause: gasnom is
genuinely 65 meters, and the Sabine test now asserts against the registry's own tolerance the way
preflight does.

---

## 01 / STAGE 0

| Gate | Command | Requirement |
|---|---|---|
| AA0-a | `node --test tests/*.test.mjs` | 0 failed (currently **42**) |
| AA0-b | `python -m pytest -q -m "not network"` | 0 failed (currently **448**) |
| AA0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| AA0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| AA0-e | parse check on every file touched | no SyntaxError |
| AA0-f | **collected test counts** | node ≥ 42, pytest ≥ 448 |

---

## 02 / AA1 — THE HEADLINE: THE COMPARISON PANEL COMPARES SPANS THAT DO NOT OVERLAP

Measured on the host right now, distinct gas days per source:

| source | days | terminals |
|---|---:|---|
| quorum | **1,996** | plaquemines, calcasieu |
| enbridge | **1,107** | TETCO (Freeport's second leg) |
| gasnom | **1,096** | cameron, golden_pass |
| bhe | 612 | cove_point |
| cheniere | 102 | sabine_pass, corpus_christi |
| gulf_south | **101** | freeport |
| kinder_morgan | 10 | NGPL (context) |

**Nineteen times more history for Plaquemines than for Freeport.** This asymmetry predates Z —
quorum has held 1,996 days all along — but the gasnom backfill has made it impossible to ignore.

`docs/js/util/terminal-comparison.js:95-140` builds `dateSet` as the **union** of every selected
terminal's dates, sorts it, and returns each terminal's values as a sparse map keyed by date. So
selecting Cameron and Freeport together produces a 1,096-entry axis on which Freeport has **995
holes**, and nothing in the returned object says the two are not comparable over that window.
`getTerminalCaveat` emits coverage caveats — about what fraction of a terminal is *visible* — and
says nothing about how far back it is *known*.

**What to do:**

1. Add per-terminal span to the comparison result: first date, last date, and day count, derived
   from that terminal's own series rather than the union.
2. Decide how the panel should behave and implement it. Two honest options, and I want your
   argument, not just a choice:
   - **(a)** Default the axis to the **intersection** of the selected terminals' spans, with an
     explicit note naming what was trimmed and offering the full union.
   - **(b)** Keep the union and render each terminal's covered span visibly — a coverage bar, a
     dimmed region, or a per-terminal "known from YYYY-MM-DD" label — so a reader can see at a
     glance that one line starts 92% of the way across because the data does, not because flow
     stopped.
   I lean (b): the union is more informative and the failure mode is a reader *misreading* absence
   as zero, which a visible span fixes without hiding anything. But (a) is defensible and if you
   take it, say why.
3. **Emit a caveat when the selected terminals' spans differ by more than 2×.** Word it in terms of
   what is knowable: *"Freeport is known from 2026-05-25 (101 days); Cameron from 2023-09-04 (1,096
   days). Comparisons before 2026-05-25 include Cameron only."*
4. **Absence must never render as zero.** Check what the panel currently draws for a date a terminal
   has no row for, and say what it does. If it plots zero, that is a correctness bug and fixing it
   is the most valuable thing in this brief.

Tests: extend the comparison tests with a mixed-depth pair built from real registry keys, proven
red first.

---

## 03 / AA2 — `in_service_date`, THE PROPOSAL Z LEFT ON THE TABLE

Measured inside `data/curated/gasnom.parquet` after the backfill:

```
cameron_interstate   1,096 days   2023-09-04 -> 2026-09-03
golden_pass          1,096 days   2023-09-04 -> 2026-09-03
sabine               1,096 days   2023-09-04 -> 2026-09-03
port_arthur             85 days   2026-06-08 -> 2026-09-03
```

Port Arthur is missing **1,011 of 1,096 gas days** because the pipeline did not exist. `gasnom`
carries `gap_rule: calendar_daily`, so that is a 1,008-day run of "missing" days that is not a
defect in anything.

Z proposed adding `in_service_date` to `GasnomPipeline` in `scrapers/gasnom/pipelines.py` and having
the gaps check mask the expected calendar to `period >= in_service_date`. **Implement that**, with
these constraints:

- The field belongs wherever it is true of the asset, not of one scraper. Look at how
  `validators/integrity.py` consumes `gap_rule` and decide whether `in_service_date` should live in
  `config/integrity_rules.yaml` next to the rule that uses it, per-source or per-series-prefix,
  rather than in a scraper's pipeline registry. Argue where you put it.
- **A missing `in_service_date` must change nothing.** Every other source keeps its current gap
  behaviour exactly.
- The date is a *fact about the world* and needs a citation in a comment — Port Arthur LNG's
  first feedgas posting is 2026-06-08 per the data itself, which is the honest basis. Do not invent
  a commissioning date from memory.
- **Do not widen this into a general exemption mechanism.** If a source is missing days *after* its
  in-service date, that must still fire.

Add a test proving a pre-service run is not reported as a gap and a post-service run still is.
Prove it red.

---

## 04 / AA3 — THE EVIDENCE BOARD IS PERMANENTLY RED FOR TWO GATES

`logs/EVIDENCE.json` exists at last. It says:

```
pytest      exit 0  ok
node_tests  exit 0  ok
ruff        exit 1  failed
mypy        exit 1  failed
ts_grep     exit 0  ok
preflight   exit 0  ok
measure_load exit 0 ok
```

`scripts/evidence.py:181` and `:199` do `status = "ok" if exit_code == 0 else "failed"`. But ruff
exits 1 on our **accepted baseline of 17** findings, and mypy exits 1 on its accepted ~116. Both
will read `failed` forever, which means the board's two red rows carry no information and everyone
learns to ignore them — the precise failure mode that let brief M through.

Give the harness a notion of a baseline:

- A gate may declare an accepted threshold. `ruff` passes at **≤ 17 findings**; `mypy` passes at
  **≤ its current count**, which you must measure and record rather than guess.
- Parse the tool's own summary line for the count rather than inferring from the exit code.
- The board must distinguish three states: `ok`, `at baseline` (non-zero exit, count within
  threshold), and `failed` (count above threshold, or the tool crashed).
- **A count that drops below the baseline is good news and must not fail** — but it should say so,
  because that is when to lower the threshold.

Record the baselines in one obvious place with a comment saying they are ratchets: they may go down,
never up.

---

## 05 / AA4 — MAKE THE TEST-COUNT RULE MECHANICAL

§00 explains why. Right now "the test count must not fall" is a sentence in a brief that depends on
me remembering to check. Put it in the harness.

`scripts/evidence.py` already runs pytest and node. Have it:

1. Record the collected counts — pytest's `N passed` and node's `ℹ tests N` — into
   `logs/EVIDENCE.json`.
2. Compare against a committed expected-minimum, in the same ratchet spirit as §04: counts may rise,
   and a rise should update the floor; a **fall fails the gate loudly**, naming both numbers.
3. Make the failure message say what it means: *"collected tests fell 448 → 447; a test was removed
   or destroyed. If this was deliberate, raise it in the brief and lower the floor."*

This is the guard that would have caught Z's deleted `def` in seconds.

---

## 06 / AA5 — RANGE PRESETS FOR A FLEET THAT NOW SPANS FIVE YEARS

`docs/js/util/range-state.js:10` is still `RANGE_PRESETS = ['30d', '90d', '1y', 'all']`, chosen when
nothing had more than ~100 days. Plaquemines now has 5.5 years and Cameron 3.

Add what the data supports — `3y` at minimum, and argue whether `5y` earns its place given only
quorum reaches it. Keep `all`.

The range caveat built in Y already derives its span from the rendered series, so it should handle
new presets without change — **verify that rather than assuming it**, and say what you checked. A
`3y` preset on Freeport must produce the same honest "showing 101 days; this source begins
2026-05-25" caveat that `1y` does today.

---

## 07 / GROUND RULES

1. **No git commands at all.** I commit and I merge; data commits go separately from code commits.
2. **Parse every file you touch, JS and Python.**
3. **The collected test count must not fall.** AA0-f.
4. **Prove every new test red before claiming it guards anything.**
5. **Never fabricate a number, and never report as executed something you did not run.**
6. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion. An enumeration of two acceptable values is a softened assertion.
7. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
8. **Do not hand-edit curated parquet or health JSON**, and do not run another backfill in this
   brief.
9. **Absence is not zero.** §02.4 is the sharpest instance, but it is the project's oldest rule: a
   meter that did not post is not a meter that posted zero.
10. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
11. **Never mix `_sq_` and `_oac_` in a flow total.**
12. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`,
    390px reflow.
13. Known pre-existing and **not yours**: the 17 ruff findings (E402, N806) and the mypy backlog in
    `scrapers/base/playwright_client.py` and `transformers/baker_hughes.py` — §04 asks you to
    *record* the mypy count as a baseline, not to fix it.
14. Maintain `OVERNIGHT_STATE.md`.

---

## 08 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all six green, or zero** | **20** |
| AA1 — per-terminal spans exposed; option argued and implemented | 20 |
| AA1 — mixed-depth caveat; and absence proven not to render as zero | 15 |
| AA2 — `in_service_date` implemented where it belongs, with the location argued | 15 |
| AA2 — pre-service masked, post-service still fires; test proven red | 10 |
| AA3 — three-state board with ratcheted baselines; mypy count measured | 10 |
| AA4 — test-count guard in the harness, failing loudly on a drop | 5 |
| AA5 — presets extended; Y's caveat verified against them | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. A fallen test count is an
automatic zero on Stage 0.

---

## 09 / REPORT FORMAT

1. **Stage 0, before and after** — including both collected test counts.
2. **Files touched**, with parse-check output.
3. **AA1** — what the panel drew for a missing date before your change; the option you took and
   why; the caveat wording; the red-before proof.
4. **AA2** — where you put `in_service_date` and why there; the gaps output before and after; the
   red-before proof.
5. **AA3** — the mypy baseline you measured; the three-state board.
6. **AA4** — the guard, and its failure message on a simulated drop.
7. **AA5** — presets added; what you checked about the caveat.
8. **Anything you noticed and did not fix.**
9. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
