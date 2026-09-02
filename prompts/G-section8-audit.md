# Implementation task: audit and re-validate Section 8, which is already live (P0)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`. Work from current
`main` (the three fix branches were merged; the board is clean apart from known WARNs).

The handoff says Section 8 (Terminal Downtime / Turnaround Indicator) is "specced, validation
never run, nothing shipped." **That is false.** `docs/js/panels/lng-terminal-downtime.js` is 486
lines, imported and rendered by `docs/js/main.js:22,178`, and live on the published site. It
carries a header claiming all four validation cases pass.

Some of those claims do not survive contact with the curated data. Section 8 is the first panel
that tells a reader *what to think* rather than what was measured, so it needs the strictest
verification in the project — and it is currently telling readers things this repo's own data
does not support.

**This is an audit, not a rewrite.** Establish what is true, then fix what is wrong.

---

## Ground rules

**No git commands at all** — not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox
has destroyed the repository's `.git` twice during ordinary commit/checkout operations, once while
the agent was correctly following a documented allowlist. It is a property of the environment,
not a judgement about you. Claude commits. If you need history, ask in your report.

**Wide remit inside the parts below.** Pick designs and proceed; edit `docs/js/`, `scripts/`,
`analysis/`, `tests/` as the work requires; fix adjacent bugs you find, reporting each separately.

**`docs/` is in scope for this brief** — it is the only brief where that is true, because the
defect is in a panel. Two hard rules there:

- **Vanilla JS only. Zero TypeScript syntax in executable code** (JSDoc is fine). Before you
  finish, run and paste the output of:
  ```
  grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/
  ```
  Only JSDoc hits are acceptable. TypeScript syntax in these files silently breaks rendering and
  is a recurring failure mode here.
- Design tokens only in CSS; every panel stays wrapped in `safeRender`; mobile reflow at 390px.

**Paste raw command output for every claim.** If a command could not run, say so — a missing
result is fine, an invented one is not.

---

# PART 1 — Cove Point is summing feeders, which the project already learned not to do

The panel configures Cove Point as:

```js
  cove_point: {
    label: 'Cove Point',
    feeds: [
      { source: 'bhe', stem: 'cpl_sq_45001_d', label: 'Transco PV' },
      { source: 'bhe', stem: 'cpl_sq_37001_d', label: 'Columbia Loudoun' },
    ],
```

Those are **receipt meters on the pipeline**, not plant intake. The handoff's domain notes are
explicit about this exact terminal: Cove Point's pipeline (CPL) is not a dedicated feedgas pipe —
roughly **37% of its throughput goes to LDC and power customers and never reaches the plant**.
Summing receipts gives *pipeline throughput*, not feedgas. The honest number came from the
terminal's own consolidated plant-intake meter, **`cpl_sq_10001_d`**, and the lesson recorded was
"look for a plant-intake meter before summing feeders."

Section 8 sums the feeders anyway. Measured from `data/curated/bhe.parquet`:

```
cpl_sq_10001_d_* : 982 rows, 100 days (2026-05-26 -> 2026-09-02), median 751,590 Dth/d, ZERO zero-values
cpl_sq_37001_d_* : ~73-76 zero rows per cycle series
```

So the panel's headline claim — *"Case 4 Cove Point 25 cargo zeros: 5 CARGO_IDLE, 0 OFFLINE"* —
is measuring zeros on a **feeder that idles while the plant keeps running**. The plant-intake
meter has **no zero days at all** in curated history.

Decide what Cove Point's downtime signal should be built on, implement it, and justify it against
the pass-through finding. If plant intake is the right basis, the CARGO_IDLE logic and its
thresholds need rederiving from that series rather than carried over. **Report what the corrected
Cove Point event list actually is** — it may well be zero events, and that is a valid result.

# PART 2 — the validation header does not match the data

The panel's header records:

```
 *   Case 3 Plaquemines pre-gas: no curated data (loc 24301). Logic correct:
 *     gap-only dates are NOT posted-zeros, so pre-operational periods are silent.
```

Plaquemines data exists. From `data/curated/quorum.parquet`:

```
gator_express_sq_vgpqd_d_* / gator_express_oac_vgpqd_d_*
range 2024-04-05 -> 2026-09-02, 878 days, 2,700 rows dated before 2025-01-01
```

Plaquemines took first gas around December 2024, so that 2024 history is exactly the
pre-operational period case 3 was written to test — and it is present. The panel also has **no
`plaquemines` entry in its terminal config at all**, so the case is not merely mis-recorded, it is
uncovered.

Add Plaquemines and run case 3 for real: the pre-first-gas period must classify as
**NOT-YET-OPERATIONAL**, never as an outage. Report the classification the detector actually
produces, before and after any change you make.

Then re-run the other three cases against current data and report what the detector actually
outputs for each — not what the header says:

1. **Freeport 2026-07-15** dip to 142 MMcf/d → expected DEPRESSED. The header claims it is
   correctly NOT flagged as routing. Both cannot be right; determine which is, and note that
   `gulf_south` curated only reaches back to 2026-05-25.
2. **TETCO 2024-04-11** zero-day → expected OFFLINE. Header claims OFFLINE dur=7. Verify the
   duration against `data/curated/enbridge.parquet` (the meter has 54 rows on that date).
3. **Plaquemines pre-first-gas** → NOT-YET-OPERATIONAL (above).
4. **Cove Point** → see Part 1; the "446 zero-days" figure in the handoff is not supported by
   curated data for any `cpl_` series (the highest zero-count on any of them is ~76). Establish
   what the real figure is and say plainly if the handoff's number is wrong.

`scripts/task3_validate.py` exists and is claimed to be the validator. Run it. If its results
disagree with the panel header, say which is stale.

# PART 3 — event counts and threshold sanity

The header claims full-history event counts:

```
 *   Freeport: 7 events (3 OFFLINE + 4 RAMPING) over 1100 days
 *   Cove Point: 7 events (5 CARGO_IDLE + 1 DEPRESSED + 1 RAMPING) over 93 days
 *   Sabine: 0 events over 94 days
```

Reproduce these from current curated data and paste the real numbers. Note that Freeport's
"1100 days" cannot come from `gulf_south` (99 days of history) — establish which feed supplies it
and whether mixing feeds of very different history lengths distorts the baseline.

The rule of thumb from the spec: **dozens of events per terminal per year means the thresholds are
wrong.** Report the per-terminal, per-year event rate for every configured terminal and say
whether each is credible. Tune before shipping, and show the before/after counts for any threshold
you change — with the reasoning.

# PART 4 — the multi-feed routing suppression

The design says a single feed dropping while the total holds is routing, not downtime, and that
posted-zero must be distinguished from did-not-post (a posting-gap-as-zero produced eight fake
events in Section 7 before a both-active guard killed them).

Verify both behaviours hold in the current implementation, with tests:

- a feed at zero while its sibling covers the total → **not** an outage,
- a day where a feed simply did not post → **never** counted as a zero,
- both feeds posting zero → a real outage.

Freeport is the multi-feed case (`gulf_south` 24329 + `tetco` 79999). Cove Point becomes
single-feed if Part 1 moves it to plant intake — say what that does to the routing logic there.

# PART 5 — leftovers from the previous round

Small, unrelated, and worth clearing while you are in here:

1. `tests/test_freshness_observability.py` has two unused imports (`pytest`, and
   `build_source_state` from `validators.integrity`) — ruff F401. Remove them.
2. `eia_lng_exports` keeps `staleness: warn_days 45` while its own cited cadence is a
   Census-derived 90–115 day lag, so it warns chronically and sits ~41 days from a hard FAIL at
   135. `eia_supply` was recalibrated to 75 with its cadence cited; do the same here **with the
   real publication cadence cited in a comment**, or state why it should differ.
3. The gasnom health merge runs inside each matrix leg, so a leg merges its own fresh stamp with
   its siblings' last-committed ones and concurrent legs race on the canonical file. Better than
   watching one slug, not yet correct. Fix it properly (a dependent job after the matrix, as
   quorum does) or explain why the current shape is acceptable.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing
   a check to stop an alarm. Threshold changes need cited evidence.
2. **Never fabricate a number, a test result, or a command output.** A negative result is a valid
   result — this project has killed two of its own analytical ideas because the data did not
   support them, and "Cove Point has no downtime events" is an acceptable answer if it is true.
3. **Do not remove the UI caveats** about known invisible gas (Freeport's KMTP intrastate lateral,
   Sabine's non-CTPL feeds at ~69% of that terminal). They are load-bearing.
4. **Nameplates need FERC docket citations** in JSDoc — do not add or change one without.
5. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
6. **No git commands at all.**
7. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, plus the
   `docs/js/` TypeScript grep above. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, pre-existing mypy gaps in untouched
   `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and a failing
   `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — what Cove Point's signal is now built on and why; the corrected event list.
3. **Part 2** — all four validation cases with the detector's *actual* output, before and after;
   `scripts/task3_validate.py` output pasted; which of panel header / handoff / data is stale
   wherever they disagree.
4. **Part 3** — real event counts per terminal per year, with a credibility verdict each, and
   before/after for any threshold you tuned.
5. **Part 4** — the three routing/posting-gap behaviours, with the tests that prove them.
6. **Part 5** — the three leftovers.
7. **The `docs/js/` TypeScript grep output**, pasted.
8. **Test output** — real `pytest`, `ruff`, `mypy`. Full-suite counts from an actual run; the
   current baseline on `main` is **417 passed, 1 failed** (the known one), 16 deselected. Derive
   any difference rather than attributing it to environment variance.
9. **Anything contradicting this brief.** The measurements above came from the current tree; if
   the repo disagrees, the repo is right and I want to know.
10. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
