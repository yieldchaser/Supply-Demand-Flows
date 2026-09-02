# Implementation task: close Section 8, then cargo timing and the VG study (P0, four parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Work on branch `fix/section8-audit` (head `5017f7a`). **Build on it.** The cycle rule is settled and
correct, the Sabine verdict and the power-burn decision are recorded, and the Python suite is at
**425 passed / 1 known failure**.

---

## Read this first — how your report is actually used

Every number in your report gets independently re-derived before anything is committed. The
commands are re-run, the parquets re-read, the tests re-executed. That has happened on every round
so far and it will happen on this one.

That means a report which reconstructs what the output *should* have been costs you a round rather
than saving one. On the last five briefs the code arrived in good shape and the transcript did
not, and each time the mismatch was found within minutes:

- reported `OFFLINE 2024-04-20 dur=10`; the script prints `2024-04-17 dur=7`
- reported case 3 total `265,284 Dth`; the script prints `294,850` — `265,284` was the number in
  the *brief*, not in the output
- reported case 4 `119,321 Dth, feeds 1/2`; the script prints `1,644,078 Dth, feeds 2/2`
- reported case 6 as one RAMPING event; the script prints two
- reported node tests "6 passed, 0 failed"; the actual run is **5 passed, 1 failed**
- an earlier round reported a case "OK CORRECT" where the script printed `MISFIRE`

None of that was a coding failure. In every instance the underlying work was sound or nearly so,
and the round was rejected on the transcript alone. A failing test pasted honestly gets your work
merged with a note; a passing test that isn't real gets the whole branch held back.

So: **paste what the terminal printed, unedited — including the encoding artefacts, the warnings,
the failures, and any output that contradicts what you expected.** If you did not run something,
say so; that is always acceptable. If your output disagrees with a number in this brief, the
output wins and I want to see it, because this brief's numbers are measurements too and they can
be wrong — brief H's central premise was, and brief I opened by correcting it.

---

## Ground rules

**No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
destroyed the repository's `.git` twice during ordinary commit/checkout operations. Claude commits.

**Wide remit** inside these parts: pick designs and proceed, edit `docs/js/`, `scripts/`,
`publishers/`, `scrapers/`, `config/`, `.github/workflows/`, `tests/`, add modules, fix adjacent
bugs (report each). `docs/` rules apply: vanilla JS only, zero TypeScript in executable code,
design tokens, `safeRender`, 390px reflow, and paste the `docs/js/` TypeScript grep before finishing.

Work the parts in order. **Part 1 is small and must land.** If you run out of room in Part 3 or 4,
stop cleanly and say where you stopped.

---

# PART 1 — finish Section 8 (small, do this first)

**1a. A stale test asserts the old cycle rule.** `tests/test_lng_downtime.test.mjs` line ~184,
*"Cycle priority order: later cycle supersedes earlier cycle"*, fails:

```
AssertionError [ERR_ASSERTION]: Expected values to be strictly equal:
  0 !== 109
  actual: 0, expected: 109
```

It expects `id0900 → 109` under the superseded rule. The code is right — hourly `id{HH}00` cycles
are placeholders and now return 0 — so **update the assertion to encode the new rule**, and add a
case proving an hourly placeholder cannot win over a genuinely nominated cycle. Node must be
6 passed / 0 failed.

**1b. The documented rule and the implementation disagree.** Your Part 1 write-up says `timely`
wins for TETCO on 2026-07-15, giving a Freeport total of `265,284 Dth`. The code picks `latec`
(148,887) and prints `294,850 Dth`. Both cannot be right.

Settle it from NAESB semantics and the data: `latec` is a late correction cycle, posted after
`timely` for the same gas day, so on the "last genuinely nominated cycle wins" rule it should
supersede. If that is right, the documentation is stale; if `latec` means something else on this
platform, the code is wrong. Decide, make code and docs agree, and say which you changed.

**1c. Case 2's duration moved.** The TETCO April 2024 outage now prints `2024-04-17 dur=7`, having
printed `2024-04-20 dur=10` under the previous rule. Establish from `data/curated/enbridge.parquet`
what that outage actually was — how many consecutive days meter 79999 posted zero — and make the
expected value match the data rather than either previous answer.

**1d.** Re-run the full validator afterwards and paste it whole.

# PART 2 — AISStream vessel tracking: cargo timing

This is the last major unbuilt idea, and it is greenfield — the only mentions of AIS in the repo
are in `README.md`, no code exists.

It was originally conceived as a *proxy* for feedgas, which is now measured directly, so **that is
no longer the reason to build it.** The reason is cargo timing: feedgas tells you a terminal is
liquefying; vessel movements tell you when the product leaves and where it goes. Loading cadence
against feedgas is a genuinely new signal, and nobody publishes it free.

The prior spec, for reference: Gulf geofence roughly **lat 27–30.5, lon −98 to −88**, filtering
`ship_type=80` (tankers), hourly push, memory-only state, hosted off-GitHub-Actions.

**Do the feasibility work before the plumbing**, and report it as findings:

1. What does AISStream actually offer on a free tier — message types, rate limits, whether a
   persistent websocket is required, what an API key needs. If a persistent connection is
   mandatory, GitHub Actions is the wrong host and you should say so rather than forcing it.
2. Can LNG carriers be distinguished from other tankers? `ship_type=80` is broad. Look at what the
   feed carries — IMO numbers, vessel names, dimensions — and say how a Sabine or Plaquemines
   loading would be identified with acceptable precision.
3. What state must persist between runs, and where can it live given this repo publishes from
   GitHub Pages and commits data to git? A design that needs a always-on server is a legitimate
   finding — say so plainly rather than building something that cannot run.

**Then build only what the findings support.** A scraper following this project's conventions
(`HttpClient`, `SafeWriter`, `HealthWriter`, `record_no_op` on empty runs, `merge_into_curated`,
canonical schema, `series_id` carrying every dimension) — or, if the findings say the free tier
cannot support it, a written verdict in `docs/VERDICT.md` and no code. **A clear negative is a
good outcome.** Do not build a scraper that will silently return nothing.

If you do build it, the data lands in curated and gets integrity rules like every other source. No
panel in this brief.

# PART 3 — the Venture Global leading-indicator study

The full-fleet version of this is blocked — fleet history starts ~2026-05 and the EIA LNG export
series ends 2026-05-01, so there are no overlapping months. **The narrow version is testable now**
and has more data than previously assumed:

```
quorum   (Gator Express + TransCameron)  2021-03-15 -> 2026-09-02
eia_lng_exports                          2021-01-01 -> 2026-05-01, 45 series
```

Calcasieu Pass took first gas in 2022 and Plaquemines in December 2024, so the usable overlap is
substantially more than the ~20 monthly points the original note assumed. Establish the real n.

**This is research, not a panel.** The question: does Venture Global daily nominated feedgas lead
EIA's monthly LNG export figures, and by how much?

Write it up as `analysis/vg_leading_indicator.md` following the shape of
`analysis/storage_nowcast_research.md` — method, n, the actual statistics, and an honest verdict.
Aggregate daily feedgas to monthly, align to the EIA series for the right destination aggregation,
and report correlation at several lags with confidence intervals.

**Be as willing to kill this as the storage nowcast was killed.** That study died because headline
R²=0.53 dissolved to R²=0.01 on the surprise, and a naive persistence model beat it 11.7× on MSE.
Test against the *surprise*, not the headline — a feedgas series that merely tracks a slow-moving
level will correlate impressively and predict nothing. If it does not survive that, say so and
record why.

# PART 4 — NGPL 3592: confirm it stays demoted

Small and mechanical. `km_ngpl_sq_3592_*` was demoted to `kind: 'context'` at 0.0 after posting a
briefly coherent reading (TSQ 472,702 + OAC 27,298 = exactly 500,000 design capacity on
2026-08-24), then going idle. The standing instruction was to promote it back to `measured` if it
returns non-zero, citing the dated evidence.

Current state, which I measured:

```
km_ngpl_sq_3592_d_{best,evng,itrd1,itrd2,itrd3,timely}
29 rows, 9 days, nonzero = 0, max = 0.0
```

It is still idle. **Confirm this yourself and leave it demoted.** Do not promote a meter reading
zero. Note whether anything in the registry↔config agreement gate would fire if it were promoted,
so the path is known for when it does return.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm. Threshold changes need cited evidence.
2. **Never fabricate a number, a test result, or a command output.** Negative results are valid and
   this brief expects at least one.
3. **Never mix `_sq_` and `_oac_` in a flow total** — OAC is a residual, anticorrelated with TSQ.
4. **Twin-meter check before summing any two feeds**; plant-intake meter before summing feeders.
5. **`merge_into_curated` always** for curated writes; every transformer routes through it and a
   test enforces that.
6. **`series_id` carries every dimension** — `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`,
   `flow ∈ {r, d}`. Omitting one causes silent overwrites.
7. **RAW `Dth/d` in Python; convert only in frontend JS.**
8. **Confidence tiers:** only `high` survives publisher pruning; a registry headline must be `high`
   in config. Do not change a tier in this brief.
9. **`record_no_op` on zero-record runs**, `record_guard_failure` when a guard raises.
10. **No git commands at all.**
11. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
    `docs/js/` TypeScript grep, and `node --test tests/*.test.mjs`. Known pre-existing and NOT
    yours to fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps
    in untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`,
    and `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — node at 6/0 with the run pasted; the `timely` vs `latec` decision and which side you
   changed; what TETCO's April 2024 outage actually was from the data; the full validator output.
3. **Part 2** — the AISStream feasibility findings; the vessel-identification approach; the state
   and hosting verdict; then either the scraper with its tests, or the written negative and what
   you recorded in `docs/VERDICT.md`.
4. **Part 3** — `analysis/vg_leading_indicator.md`, the real n, correlations at each lag tested
   against the surprise rather than the level, and your verdict including "this does not work" if
   that is what the data says.
5. **Part 4** — the 3592 confirmation and the agreement-gate note.
6. **The `docs/js/` TypeScript grep output.**
7. **Test output** — `pytest`, `ruff`, `mypy`, `node --test`. Full-suite counts from an actual run;
   baseline on this branch is **425 passed, 1 failed, 16 deselected**, and node is **5 passed,
   1 failed** until Part 1a. Derive any difference.
8. **Anything contradicting this brief.** Its numbers are measurements; if the repo disagrees, the
   repo is right.
9. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
