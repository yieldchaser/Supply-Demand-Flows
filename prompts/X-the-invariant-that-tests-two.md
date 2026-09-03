# X — The Invariant That Tests Two

Branch: cut a new one from `main` at `f21042d`. I commit and I merge.

Focused brief. One theme, five items. Everything below is measured on the host — you are not asked
to investigate, only to decide and implement.

**How this brief is scored, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty and keeps the section's code weight. A
> fabricated number forfeits the section *and* Stage 0.
>
> **A new test that has not been proven red forfeits its section.** Revert the fix in a temporary
> copy, run the test, paste the failing output, restore byte-for-byte, confirm the restore.
>
> **Every file you touch must be parsed before you report** — JS *and* Python:
> ```
> node --input-type=module -e "await import('./docs/js/<f>.js')"
> python -c "import ast,io; ast.parse(io.open('<f>.py',encoding='utf-8').read())"
> ```
> A file that does not parse forfeits Stage 0 outright.

---

## 00 / WHY THAT THIRD RULE NOW COVERS PYTHON

In V you shipped a `docs/js` module with a deleted `catch` clause. In W you did the identical thing
in Python: the §08 typing edit replaced the module docstring's closing `"""` in
`scripts/task3_validate.py` with `from __future__ import annotations`, so the docstring never
closed. **`pytest` could not collect at all** — three collection errors, zero tests run — and ruff
went 17 to 53. Self-scored 100/100, including a mypy figure for a file that has no mypy result
because it does not parse.

W also reported §06's migration as executed. It was not; the parquet still held `evng`/`itrd1-3`
and the "after" table was a prediction. Your predicted numbers were right, but running it for real
surfaced two things a prediction could not: `safe_write_parquet` was called with its arguments
swapped, and an in-flight edit to `transformers/kinder_morgan.py` had deleted the `tsq_dth`
extraction while still referencing it. Five tests, `NameError`.

**What W got right, and it was the substance.** The cycle rule is now defined once and imported.
The JS learned the KM vocabulary and the parity test is real. §03's FERC analysis was decisive —
`DOWNTIME_CONF`'s own honesty string already read "2,100 MMcf/d" three lines under
`nameplate: 2140`, and 1111.5/2100 = 52.93% matches the registry's 52.9. **I took the decision:
2140 → 2100 in `DOWNTIME_CONF` only.** Preflight now reads
`freeport | 2100 | 52.9% claimed | 52.9% measured | 0.0% drift`.

And §04's honest answer is what this brief is built on. You reported that
`tests/test_lng_cross_panel_invariant.test.mjs` hardcodes `freeport` and `cove_point` and silently
skips everything else. That was correct, and it turned out to be hiding something.

---

## 01 / STAGE 0 — THE GATE

| Gate | Command | Requirement |
|---|---|---|
| X0-a | `node --test tests/*.test.mjs` | 0 failed (currently 27 passed) |
| X0-b | `python -m pytest -q -m "not network"` | 0 failed, **no collection errors** (currently 448 passed) |
| X0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| X0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| X0-e | parse check on every file you touched, JS and Python | no SyntaxError |

---

## 02 / X1 — THE HEADLINE: SECTION 5 RENDERS NOTHING FOR HALF THE FLEET

I built a synthetic bundle from each terminal's own `DOWNTIME_CONF` stems — two gas days, a
`_timely` and an `_id3` cycle each — and ran all three panel builders over it. Days returned:

| terminal | S5 `buildMultiFeedData` | S7 `terminalSummary` | S8 `buildDailyTotal` |
|---|---|---|---|
| freeport | 2 | 2 | 2 |
| cove_point | 2 | 2 | 2 |
| sabine_pass | 2 | 2 | 2 |
| corpus_christi | 2 | 2 | 2 |
| **plaquemines** | **0** | 2 | 2 |
| **cameron** | **0** | 2 | 2 |
| **calcasieu** | **0** | 2 | 2 |
| **golden_pass** | **0** | 2 | 2 |

**Section 5 produces an empty series for four of the eight operational terminals.**

The cause is that `LNG_TERMINALS` carries two mutually exclusive shapes and each panel reads only
one of them:

- **Multi-feed shape** — `feeds: [{ source, series, label, kind }]`, no `seriesPrefix`/`loc`.
  Carried by `freeport`, `cove_point`, `sabine_pass`, `corpus_christi`.
- **Single-meter shape** — `seriesPrefix`, `loc`, `flow`, and `feeds: []`.
  Carried by `plaquemines`, `cameron`, `calcasieu`, `golden_pass`.

`docs/js/util/lng-feedgas-data.js:48` iterates `t.feeds`, so on the single-meter shape it iterates
nothing. `docs/js/util/lng-fleet-data.js:27` builds its prefix from `seriesPrefix`/`loc`/`flow` and
handles both, and `buildDailyTotal` reads `DOWNTIME_CONF` which is uniform.

**Decide and justify:**

- **(a)** Give the four single-meter terminals an explicit `feeds` array derived from their own
  `seriesPrefix`/`loc`/`flow`, so `LNG_TERMINALS` has one shape. Cleanest, but it duplicates the
  identity in two fields unless you also drop the old ones — and other code reads them, so grep
  first.
- **(b)** Make `buildMultiFeedData` fall back to the `seriesPrefix`/`loc`/`flow` identity when
  `feeds` is empty, leaving the registry as it is.

I lean **(a)**, because the second shape exists only for historical reasons and the divergence is
the bug. But grep every reader of `seriesPrefix`, `loc` and `flow` before you commit to it and say
what you found. If (a) would break a reader you cannot cleanly update, take (b) and say why.

**Do not change any `series` id, `nameplate`, `expectedCoveragePct` or `coverageTolerancePct`.**
This is a shape change, not a data change.

---

## 03 / X2 — MAKE THE INVARIANT TEST COVER WHAT IT CLAIMS

`tests/test_lng_cross_panel_invariant.test.mjs` opens with:

> Proves mathematically that Section 5 (Hero Feedgas), Section 7 (Fleet Overview), and Section 8
> (Terminal Downtime) produce IDENTICAL daily flow totals **for any terminal**.

It tests two. Its fixture, `createTestBundle()`, hardcodes Freeport's two series ids, and the two
`test(...)` blocks name `LNG_TERMINALS.freeport` and `LNG_TERMINALS.cove_point` directly.

Rewrite it to be **parametric over `DOWNTIME_CONF`**, so a terminal added there is covered
automatically and cannot be added silently again:

1. Generate the fixture per terminal from that terminal's own feed identity — several gas days,
   more than one cycle per day, including a later cycle that must supersede an earlier one.
2. Keep every existing assertion for Freeport. **The hourly-`id{HH}00` case and the multi-feed
   routing case are the most valuable tests in this file — they must survive intact.** If they only
   make sense for a multi-feed terminal, keep them as named Freeport-specific tests alongside the
   parametric loop rather than deleting them to fit the new shape.
3. The parametric loop asserts, per terminal: identical date sets across S5/S7/S8, and per-date
   agreement within `1e-6`.
4. Skip `port_arthur` — it is `operational: false` and absent from `DOWNTIME_CONF`.

**Prove it red properly.** With X1 unfixed this test must fail for the four single-meter terminals.
Do X2 first, watch it go red on exactly those four, paste that output, then do X1 and watch it go
green. That ordering *is* the red-before proof for both sections — say so explicitly in your report.

Report how many terminals the file covered before and after, as exact integers.

---

## 04 / X3 — TWO REGISTRIES, ONE TRUTH

`LNG_TERMINALS[k].feeds[].series` with `kind`, and `DOWNTIME_CONF[k].feeds[].stem` with `context`,
are two spellings of the same fact. Filtering `kind` of `context`/`comparison` on one side and
`context: true` on the other, the two agree today for all four multi-feed terminals — I checked:

```
freeport        S5 [gulf_south_sq_24329_d, tetco_sq_79999_d]  S8 [same two]
cove_point      S5 [cpl_sq_10001_d]                           S8 [cpl_sq_10001_d]
sabine_pass     S5 [creole_trail_sq_CT200111_d]               S8 [creole_trail_sq_ct200111_d]
corpus_christi  S5 [corpus_christi_sq_CC200221_d]             S8 [corpus_christi_sq_CC200221_d]
```

They agree *now*. Nothing stops them diverging, and if they do, the panels disagree by
construction and X2's invariant will fail without pointing at the cause.

Add a test that asserts, for every terminal in `DOWNTIME_CONF`: the set of counted feed stems
derived from `LNG_TERMINALS` equals the set derived from `DOWNTIME_CONF`, compared
case-insensitively. Prove it red by adding a stem to one side in a temporary copy.

**Do not merge the two registries in this brief.** They serve different panels and consolidating
them is a bigger change than X should carry. A test that pins them together is the deliverable.

---

## 05 / X4 — A CASE INCONSISTENCY THAT IS NOT A BUG

`DOWNTIME_CONF.sabine_pass` spells its stem `creole_trail_sq_ct200111_d` (lowercase `ct`) while
`LNG_TERMINALS` and the curated parquet both use `creole_trail_sq_CT200111_d` (uppercase `CT`).
`corpus_christi_sq_CC200221_d` is uppercase on both sides.

**This is not currently a bug and you must not report it as one.** All three readers lowercase both
sides before comparing — `lng-downtime.js:172/176`, `lng-feedgas-data.js:18/21`,
`lng-fleet-data.js:30/63/141`. I verified each.

Normalise the one odd stem to match curated for consistency, and add a one-line comment at the
`DOWNTIME_CONF` definition noting that stem matching is case-insensitive by design so nobody
"fixes" this into a case-sensitive comparison later. That is the whole item.

---

## 06 / X5 — RUN THE HARNESS. SEVENTH BRIEF.

`scripts/evidence.py` has still never executed and there is still no `logs/EVIDENCE.json`.

Run it if you can. If you cannot, say so — full marks — and leave `logs/` containing only what a run
produced. **Absent, not a tombstone.** Sixth time of asking.

---

## 07 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`, not `gc`. This sandbox has
   destroyed this repository's `.git` twice. I commit and I merge.
2. **Parse every file you touch, JS and Python.** X0-e. Forfeits Stage 0.
3. **Prove every new test red before claiming it guards anything.** Scored. §03 tells you the
   ordering that gives you this for free.
4. **Never fabricate a number**, and never report as executed something you did not run. W's
   migration table was a prediction printed as a result.
5. **A negative result is a valid result.** If (a) in §02 turns out to break a reader, saying so and
   taking (b) scores full marks.
6. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion. In particular, if X2 fails for a terminal after X1, **do not exclude that
   terminal from the loop** — report it.
7. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
   §02 is a shape change and §05 is a spelling change; neither alters a number.
8. **Do not merge `LNG_TERMINALS` and `DOWNTIME_CONF`.** §04 is a pinning test, not a refactor.
9. **Do not hand-edit curated parquet or health JSON.**
10. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
11. **Never mix `_sq_` and `_oac_` in a flow total.** OAC is a residual (`capacity − TSQ`).
12. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`
    on every panel, 390px reflow.
13. Known pre-existing and **not yours**: the 17 residual ruff findings (E402, N806), ~35 mypy
    errors in `scrapers/base/playwright_client.py`, mypy gaps in `transformers/baker_hughes.py`.
14. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what ran or why it could not.

---

## 08 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all five green, or zero** | **20** |
| X1 — option chosen with the grep behind it; Section 5 returns data for all eight | 25 |
| X2 — invariant parametric over `DOWNTIME_CONF`; red on exactly the four, then green | 25 |
| X2 — the hourly-`id{HH}00` and multi-feed routing tests survive intact | 10 |
| X3 — registry pinning test, proven red | 10 |
| X4 — stem normalised, comment added, correctly **not** reported as a bug | 5 |
| X5 — `evidence.py` run, or honestly declared with `logs/` clean | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. A file that does not parse is an
automatic zero on Stage 0.

---

## 09 / REPORT FORMAT

1. **Stage 0 table** — five rows: gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **Files touched**, with the parse-check output for each. JS and Python.
3. **X1** — the grep of every `seriesPrefix`/`loc`/`flow` reader; the option you took and why; the
   before/after version of the eight-terminal table in §02.
4. **X2** — terminals covered before and after; the red output showing exactly four failures; the
   green output after X1; confirmation the two named Freeport tests still exist and pass.
5. **X3** — the pinning test and its red-before proof.
6. **X4** — the diff and the comment.
7. **X5** — did it run; the state of `logs/`.
8. **Anything you noticed and did not fix.**
9. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
