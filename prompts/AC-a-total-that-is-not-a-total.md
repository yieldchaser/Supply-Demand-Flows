# AC — A Total That Is Not a Total

Branch: cut a new one from `main` at `b3d2da34`. **Work in the main checkout
`C:\Users\Dell\Github\Supply-Demand-Flows`, not in any `.claude/worktrees/` directory** — the
worktrees hold other branches and `scripts/task3_validate.py` there is a stale pre-V copy with only
three terminals. Confirm you are on `main` with nine terminals in `TERMINALS` before you start.

One defect. It is live, it is visible on the dashboard, and it is the project's cardinal sin.

**Scoring, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores zero for its
> section. `NOT RUN` scores full marks for honesty. A fabricated number forfeits the section.
>
> **Parse every file you touch.** **The collected test count must not fall** (currently **453**;
> `scripts/evidence.py` records and checks this — run it and let it tell you).
> **Prove every new test red before claiming it guards anything.**

---

## 00 / THE DEFECT, MEASURED

`scripts/task3_validate.py::load_terminal_history` sums a terminal's configured feeds into a daily
total. Freeport has two:

```
['gulf_south_sq_24329_d', 'tetco_sq_79999_d']
```

`enbridge` (TETCO) curated history runs from 2023-08-24. `gulf_south` starts 2026-05-25, because
that is when we began scraping it — brief Y established Gulf South's EBB only serves a ~90-day
rolling window, so this is permanent, not a backfill we forgot.

Measured on `main` on 2026-09-19:

```
freeport span: 2023-08-24 -> 2026-09-18, 1122 days

  pre-2026-05-25  (TETCO leg only) : n=1005 days, median  216.6 MMcf/d
  post-2026-05-25 (both legs)      : n= 117 days, median 1170.6 MMcf/d

  2026-05-22   187.9 MMcf/d  feeds_posted=1
  2026-05-23   121.0 MMcf/d  feeds_posted=1
  2026-05-24   183.2 MMcf/d  feeds_posted=1
  2026-05-25  1027.3 MMcf/d  feeds_posted=2   <-- 5.4x step, not physical
```

**1,005 of 1,122 days present a one-leg number as the terminal's total**, and the series carries a
5.4× discontinuity that is an artifact of our scraper start date. A reader sees Freeport running at
~217 MMcf/d for nearly three years and then quintupling overnight.

**Root cause is one line —** `scripts/task3_validate.py:194`:

```python
expected_feeds = sum(1 for feed, min_d in feed_min_dates.items() if d >= min_d)
if feeds_posted < expected_feeds:
```

`expected_feeds` counts feeds that have *already appeared in the data*, using each feed's own first
posting date. Before Gulf South's first row exists, only TETCO is "expected", so a day with 1 of 2
configured feeds is marked complete. The parity rule was written to tolerate a feed that has not
reported *today*; it accidentally also tolerates a feed that did not exist for three years.

This is the rule the project states everywhere and breaks here: **a meter that did not post is not
a meter that posted zero**, and a sum missing a component is not that component's total.

---

## 01 / STAGE 0

| Gate | Command | Requirement |
|---|---|---|
| AC0-a | `node --test tests/*.test.mjs` | 0 failed (currently **44**) |
| AC0-b | `python -m pytest -q -m "not network"` | 0 failed, **453** collected, must not fall |
| AC0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| AC0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| AC0-e | parse check on every file touched | no SyntaxError |

---

## 02 / AC1 — THE FIX

A day belongs in a terminal's **total** only when every **configured** feed reported. A feed
posting a genuine `0` counts as reporting — zero is data, absence is not.

1. Change the parity rule so `expected_feeds` is `len(conf["feeds"])`, not the count of
   already-seen feeds.
2. **Do not silently destroy the excluded days.** 1,005 days of real TETCO flow still exist and are
   worth keeping. Return them separately — a `load_terminal_history_with_partials(term_key)`
   returning `(history, partial_days, conf)`, with `load_terminal_history` delegating and keeping
   its current two-value signature, is the shape I would pick. Existing callers must not break.
3. Grep every caller before finishing and name them in your report. At minimum
   `scripts/preflight.py` and `tests/test_coverage_guard.py`
   (`compute_terminal_coverage_from_curated`) call this. There will be others.

**Expected outcome, stated so you cannot inflate it:** Freeport drops to ~117 days and the 5.4×
step disappears. Single-feed and already-aligned terminals — Cameron, Golden Pass, Sabine Pass,
Corpus Christi, Plaquemines, Calcasieu — must be **unchanged** in day count. Report before/after
for all nine.

**The coverage guard samples the trailing 60 days**, which is entirely inside Freeport's two-leg
period, so Freeport's claimed 52.9% must **not** move. Confirm it does not. If it moves, stop and
report — do not touch the registry.

---

## 03 / AC2 — WHAT THIS DOES TO SECTION 8

`detect_events` derives a 30-day rolling baseline and a first-operation date from this history.
Before the fix, Freeport reported **7 events (3 OFFLINE, 4 RAMPING)** across 1,122 days — computed
across a 5.4× step change, so some of those are near-certainly artifacts of the composition change
rather than physical events.

Re-run `detect_events` for all nine terminals after the fix and report the before/after event
counts by type. Say plainly which Freeport events survive and whether the ones that disappear were
clustered around 2026-05-25. **If the count goes to zero because the history is now short, say
so** — that is an honest consequence worth stating, not a regression to hide.

Do **not** re-tune any detector threshold to restore the old event count.

---

## 04 / AC3 — THE STALE COMMENT

`docs/js/panels/lng-terminal-downtime.js` carries a header comment citing Freeport's event count
and day span from a pre-fix run. After AC1 it is wrong. Correct it to what the code now produces,
or delete the specific numbers rather than leaving a figure that will drift again.

Brief Y's lesson applies: **a wrong number in a comment is invisible — no test fails, no guard
fires, and everyone downstream believes it.**

---

## 05 / GROUND RULES

1. **No git commands at all.** I commit and I merge.
2. **Work in the main checkout on `main`**, never in `.claude/worktrees/`. Verify `TERMINALS` has
   nine entries before starting; if it has three you are in the wrong tree — stop.
3. **Parse every file you touch.** **Test count must not fall.**
4. **Prove every new test red.**
5. **Never fabricate a number; never report as executed something you did not run.** If a file or
   command named in this brief does not exist where I said, **stop and report that** rather than
   improvising — that is a full-marks outcome and it means I mis-specified something.
6. **When a guard fires, fix the cause** — never a threshold, a nameplate, or a softened assertion.
7. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
8. **Do not hand-edit curated parquet or health JSON.**
9. Known pre-existing and not yours: the 17 ruff findings, the 53 mypy findings on the targeted set.
10. Maintain `OVERNIGHT_STATE.md`.

---

## 06 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all five green, or zero** | **20** |
| AC1 — parity rule fixed at the cause; partial days preserved, not destroyed | 30 |
| AC1 — all nine terminals' before/after reported; only Freeport moves; coverage unmoved | 20 |
| AC2 — event counts before/after by type; artifact events identified honestly | 20 |
| AC3 — stale comment corrected | 10 |

Below 85 is not done. One fabricated number caps the brief at 50.

---

## 07 / REPORT FORMAT

1. **Stage 0, before and after**, including collected test counts.
2. **Files touched**, with parse-check output.
3. **AC1** — the diff, every caller found, the nine-terminal before/after table, the coverage-guard
   rows for Freeport and Cameron.
4. **AC2** — event counts before/after by type; which Freeport events were artifacts.
5. **AC3** — the corrected comment.
6. **Anything you noticed and did not fix.**
7. **Rubric self-score**, honest.
