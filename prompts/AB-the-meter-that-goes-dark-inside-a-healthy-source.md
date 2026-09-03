# AB — The Meter That Goes Dark Inside a Healthy Source

Branch: cut a new one from `main` at `1795601`. I commit and I merge.

This closes the gap AA left open, and it turns out to be bigger than the one AA found. Four items,
all measured on the host.

**Scoring, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty. A fabricated number forfeits the section *and*
> Stage 0.
>
> **A new test that has not been proven red forfeits its section.**
>
> **Parse every file you touch, JS and Python.**
>
> **The collected test count must not fall.** `scripts/evidence.py` now records this and fails
> loudly if it drops — run it and let it tell you, do not trust a manual count.

---

## 00 / WHAT AA FOUND, AND WHAT I FOUND LOOKING PAST IT

AA wired `in_service_dates` into `check_gaps` to mask Port Arthur's pre-service window inside
gasnom's combined dataframe. It doesn't work: `check_gaps` has exactly one call site
(`validators/integrity.py:753`, inside `run_source_checks`), and it always receives the **entire**
source's dataframe. The masking guard requires every row in that dataframe to share one prefix,
which can never be true when a source mixes pipelines.

I looked past the masking bug at what `check_gaps` actually verifies, and it is narrower than
anyone building on top of it has assumed: **it checks whether *any* row exists for a calendar
day, not whether *each meter* does.** I proved the consequence directly. Take the real
`gasnom.parquet`, delete every `golden_pass` row for the trailing 90 days — 18,606 rows, one of
four pipelines gone dark for three months — and rerun the checks that are supposed to catch this:

```
check_gaps:      PASS - calendar complete: 1096 consecutive days 2023-09-04..2026-09-03
check_coverage:  PASS
```

Both pass, because Cameron, SABINE and Port Arthur keep posting and that is enough to keep every
calendar day "present" and keep the series count above the 20% shrinkage floor. **A terminal going
completely dark inside a healthy multi-pipeline source is invisible to the integrity board.**

This is not a gasnom-specific defect. Six of the seven daily EBB sources mix more than one prefix:

| source | prefixes |
|---|---|
| gasnom | `cameron_interstate`, `golden_pass`, `port_arthur_pipeline`, `sabine_pipe_line` |
| quorum | `gator_express`, `trans_cameron` |
| cheniere | `corpus_christi`, `creole_trail` |
| bhe | `cpl`, `egts` |
| kinder_morgan | `km_ngpl`, `km_tgp` |
| enbridge | `tetco` (single, unaffected) |
| gulf_south | `gulf_south` (single, unaffected) |

Every one of the first five has this blind spot today.

**One thing already exists that could have caught this and doesn't get used**:
`data/health/gasnom.json` already carries per-pipeline scraper status —
`"pipelines": {"SABINE": "ok", "cameron": "ok", "goldenpass": "ok", "portarthurpipeline": "ok"}` —
written by the scraper on every run. The integrity validator never reads it. Whether that signal
belongs in this brief or is a distraction from it is your call to make and defend in §02.

---

## 01 / STAGE 0

| Gate | Command | Requirement |
|---|---|---|
| AB0-a | `node --test tests/*.test.mjs` | 0 failed (currently **44**) |
| AB0-b | `python -m pytest -q -m "not network"` | 0 failed (currently **449**) |
| AB0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| AB0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 (at baseline) |
| AB0-e | `python -m mypy scripts/preflight.py scripts/evidence.py scripts/prune_bundles.py publishers/export_dashboard_json.py tests/test_bundle_retention.py tests/test_bundle_coverage_audit.py` | ≤ 53 (at baseline) |
| AB0-f | parse check on every file touched | no SyntaxError |
| AB0-g | `python scripts/evidence.py`, check `logs/EVIDENCE.json`'s test-count fields | no drop |

---

## 02 / AB1 — DESIGN THE CHECK. THIS IS THE DECISION THAT MATTERS.

**Ground rule for the whole brief: additive, not a rewrite of `check_gaps`.** `check_gaps` runs for
every source in `config/integrity_rules.yaml` and I am not re-verifying twelve sources' worth of
gap behaviour in one brief. Write a **new** check function — `check_prefix_gaps` or similar name —
that:

1. Groups the source dataframe by a **prefix key** derived from `series_id`, using the same split
   logic I used to measure the table above (`_sq_` / `_oac_` / `_design_` / `_opcap_` as the
   delimiter — read `scripts/task3_validate.py`'s `resolve_series` and the transformer modules for
   the canonical way this project already derives a prefix from a series id, and reuse it rather
   than inventing a second parser).
2. Applies the *same* `gap_rule` semantics `check_gaps` already has (`calendar_daily`,
   `weekly_friday`, `monthly`) **per prefix group**, not once for the whole source.
3. Is **opt-in**, wired only into sources you explicitly enable in
   `config/integrity_rules.yaml` — start with the five multi-prefix EBB sources in the table above.
   A source with no opt-in key runs exactly as it does today, unchanged, unverified-by-you and
   correctly so.
4. **Severity is WARN, never FAIL** — matching the project's stated norm for gap detection
   elsewhere (`config/integrity_rules.yaml:137`, quorum's comment: *"upstream retention holes emit
   WARN (never FAIL) for operator visibility"*). A dark pipeline is an operator signal, not an
   automatic page.
5. Correctly consumes `in_service_date` **per prefix**, closing AA's actual gap: with this check,
   `port_arthur_pipeline: "2026-06-08"` finally masks the right thing, because the check now
   operates on Port Arthur's rows alone rather than the whole source's calendar union.

**Decide where `in_service_dates` lives.** It is currently under `sources.gasnom.in_service_dates`
in the integrity config, which is the right neighbourhood — keep it there, and have the new check
read it, rather than duplicating it elsewhere.

**Decide whether to also wire in the per-pipeline health signal from §00.** Argue it either way. My
lean: not in this brief — the health file's per-pipeline status answers "did the scraper run
successfully," the new check answers "did data land," and those are genuinely different signals
worth keeping separate rather than conflating on day one. But if you find a reason the health
signal is cheap to add now and materially strengthens this, make the case and do it.

**Prove the fix against the exact scenario I measured.** Reconstruct the 90-day golden_pass
blackout the way I did, run the new check against it, and show it now reports WARN with the
specific dates and the specific prefix named. Then restore and confirm the untouched parquet is
clean. This is your primary red-before proof; build a synthetic fixture test from it for
`tests/test_integrity.py` as well.

---

## 03 / AB2 — WIRE IT IN, ONE SOURCE AT A TIME, VERIFIED EACH TIME

Enable the new check for `gasnom` first — it's the one I measured and it's the one carrying the
`in_service_dates` config already. Run `python -m validators.run_integrity` and confirm:

- `gasnom` still reads `PASS` on real, current data (no new false positives from four legitimately
  healthy pipelines).
- Port Arthur's pre-2026-06-08 window is masked, not flagged.
- Re-run the 90-day golden_pass blackout scenario end to end through `run_source_checks`, not just
  the new function in isolation, and confirm the source-level verdict changes from PASS to WARN.

Then enable it for `quorum`, `cheniere`, `bhe`, `kinder_morgan` **one at a time**, running
`python -m validators.run_integrity` after each and reporting the real verdict. If any of them goes
red on data that is not actually a defect — a pipeline with genuinely sparse legitimate posting,
say — **stop, report it, and leave that source unopted-in** rather than tuning the threshold to
force a PASS. That is a real finding, not friction to route around.

Report the final per-source verdict table.

---

## 04 / AB3 — EVIDENCE, AND THE COUNTERS THAT NOW EXIST

`scripts/evidence.py` gained a ratcheted board and test-count tracking in AA. Use them as designed:
run it, confirm `logs/EVIDENCE.json` shows the collected test counts rising (not just not-falling)
by exactly the number of new tests you added, and paste the board.

If ruff or mypy's count changes from this brief's own edits, that is a real ratchet move — report
the new number and whether the baseline in `scripts/evidence.py` needs updating. Do not let it
silently drift past the recorded baseline either direction without saying so.

---

## 05 / GROUND RULES

1. **No git commands at all.** I commit and I merge.
2. **Additive only — do not modify `check_gaps` itself.** New function, opt-in wiring, twelve
   sources' existing behaviour must be provably untouched.
3. **Parse every file you touch, JS and Python.**
4. **The collected test count must not fall**, and `evidence.py` will tell you if it did.
5. **Prove every new test red before claiming it guards anything**, against the real 90-day
   blackout scenario, not only a synthetic one.
6. **Never fabricate a number, and never report as executed something you did not run.**
7. **Severity is WARN, never FAIL**, matching the existing project norm for gap detection.
8. **A source with no opt-in key is unaffected. Verify that, don't assume it.**
9. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
10. **Do not hand-edit curated parquet or health JSON.**
11. Known pre-existing and **not yours**: the 17 ruff findings, the 53 mypy findings on the
    targeted files, the wider mypy backlog.
12. Maintain `OVERNIGHT_STATE.md`.

---

## 06 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all seven green, or zero** | **20** |
| AB1 — check designed additively, opt-in, WARN severity, reuses the project's own prefix parser | 20 |
| AB1 — `in_service_date` genuinely reachable now; proven against the real blackout scenario | 20 |
| AB2 — five sources enabled one at a time, each verified, any real finding reported not routed around | 25 |
| AB3 — evidence board run, test count rose by the right number, ratchets reported honestly | 15 |

Below 85 is not done. One fabricated number caps the brief at 50. A modification to `check_gaps`
itself (rather than a new additive check) forfeits AB1 and AB2 regardless of how well it works.

---

## 07 / REPORT FORMAT

1. **Stage 0, before and after** — including both collected test counts.
2. **Files touched**, with parse-check output.
3. **AB1** — the design, where `in_service_dates` was read from, the health-signal decision and why,
   the 90-day blackout proof (red, then green after the fix).
4. **AB2** — per-source verdict table, in order enabled, any source left out and why.
5. **AB3** — the evidence board, test-count delta, ratchet report.
6. **Anything you noticed and did not fix.**
7. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
