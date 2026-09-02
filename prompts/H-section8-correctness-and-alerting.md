# Implementation task: make Section 8 correct, then make it useful (P0, five parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Work on branch `fix/section8-audit` (commit `ae7c224`, tagged `[VALIDATION UNRESOLVED]`).
**Build on it — the code changes there are good and verified.** Cove Point now reads the
consolidated plant-intake meter, Plaquemines is configured, gasnom's workflow race is gone,
`eia_lng_exports` is recalibrated, and the suite is green at **422 passed / 1 known failure**.

What is not resolved is whether the detector's numbers are right. They are not.

This is the panel that tells a reader *what to think* — the first interpretive layer over an
otherwise purely observational dashboard. That is the whole point of the project's next phase, and
it is worth nothing if the arithmetic underneath is wrong. Parts 1–3 make it correct. Parts 4–5
make it trustworthy and make it reach someone.

---

## Ground rules

**No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
destroyed the repository's `.git` twice during ordinary commit/checkout operations. It is a
property of the environment, not a judgement about you. Claude commits.

**Wide remit.** Inside these five parts: pick designs and proceed, edit `docs/js/`, `scripts/`,
`publishers/`, `.github/workflows/`, `config/`, `tests/` as needed, add modules, fix adjacent bugs
you find (report each separately).

**`docs/` is in scope, with its rules:** vanilla JS only, zero TypeScript syntax in executable
code (JSDoc fine), design tokens only in CSS, every panel wrapped in `safeRender`, mobile reflow
at 390px. Paste the output of this before you finish:

```
grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/
```

### Evidence — read this twice

The last two rounds failed at the transcript, not the code. One invented a gap table; the previous
one **ran `scripts/task3_validate.py` and then rewrote its output** into a passing result — case 1
reported as `1,624,374 Dth` where the script prints `119,321`, case 3 as one event where the
script prints twelve, and two terminals reported "plausible" where the script prints
`TOO QUIET - check sensitivity`.

So: **paste the command and its unedited output.** Byte-for-byte, including warnings, ugly
formatting and failures. If you reformat anything, say so and show the raw form too. If a command
cannot run, say it cannot run — a missing result is fine, a polished one is not. Any number in
your report that did not come from a pasted transcript will be treated as unverified.

---

# PART 1 — the daily total is wrong, and everything downstream depends on it

`scripts/task3_validate.py` reports Freeport's 2026-07-15 total as **119,321 Dth**. Reading the
curated parquets directly — `gulf_south` meter 24329 and `tetco` 79999, taking one value per feed
per gas day and summing the feeds — gives **1,955,166 Dth (1,907 MMcf/d)**:

```
  2026-07-14     1,384,590    1350.8 MMcf/d   gs=1,062,590 tetco=322,000
  2026-07-15     1,955,166    1907.5 MMcf/d   gs=1,633,166 tetco=322,000
  2026-07-16     1,465,108    1429.4 MMcf/d   gs=1,178,847 tetco=286,261
```

A detector that is 16× under the underlying data cannot classify anything correctly, and every
event count, baseline and threshold in the panel inherits the error.

**Find the root cause.** The likely area is cycle handling: each feed publishes the same gas day
under several NAESB cycles (`timely`, `evening`, `id1`, `id2`, `id3`), so a correct daily value
picks **one** cycle per (feed, gas day) rather than summing them, averaging them, or taking
whichever row sorts last. `cyclePriority` in `docs/js/panels/lng-fleet-overview.js` already
encodes a precedence order — read it and reuse it rather than inventing a second convention.

Fix it in both implementations (`scripts/task3_validate.py` and
`docs/js/panels/lng-terminal-downtime.js`), then **prove it**: a reconciliation table of at least
ten gas days per terminal showing the detector's daily total beside a direct read of the curated
parquet, with the two agreeing. State explicitly which cycle wins and why.

While you are there, confirm the same convention is used by the fleet panel and the feedgas panel
— if Section 5 and Section 8 disagree about what a terminal flowed on a given day, the dashboard
contradicts itself. Report what you find; fix it if it is in these files.

# PART 2 — the event model

With correct totals, rerun and fix these:

**2a. Pre-operational periods fragment.** Plaquemines' pre-first-gas window produces seven
separate `NOT_YET_OPERATIONAL` runs, four `RAMPING` events and a `DEPRESSED` event:

```
[('NOT_YET_OPERATIONAL', '2024-06-26', 83), ('NOT_YET_OPERATIONAL', '2024-07-13', 13),
 ('NOT_YET_OPERATIONAL', '2024-07-29', 14), ('RAMPING', '2024-08-16', 10),
 ('NOT_YET_OPERATIONAL', '2024-08-26', 3), ('NOT_YET_OPERATIONAL', '2024-09-02', 3),
 ('RAMPING', '2024-09-03', 8), ('NOT_YET_OPERATIONAL', '2024-09-16', 7),
 ('DEPRESSED', '2024-11-12', 7), ('NOT_YET_OPERATIONAL', '2024-11-18', 3),
 ('RAMPING', '2024-11-26', 8), ('RAMPING', '2024-12-29', 17)]
```

A terminal that has not yet taken first gas is in **one** state, for one continuous span, until it
starts. `DEPRESSED` and `OFFLINE` must not fire inside it — you cannot be depressed relative to a
baseline you have never had. Decide how first gas is detected from the data (not a hardcoded date)
and say how you chose. Commissioning `RAMPING` after first gas is legitimate; distinguish it.

**2b. Two terminals report `TOO QUIET - check sensitivity`.** Cove Point (0 events / 100 days) and
Sabine (0 events / 101 days). Resolve honestly: either these terminals genuinely ran without
interruption over that window — in which case zero events is the right answer and the *message* is
wrong for a short history — or the thresholds are too slack to see anything. Decide with data, and
**do not tune a threshold merely to produce events**. Zero is an acceptable answer; the project has
killed two of its own analytical ideas for less.

**2c. Event rates.** After the Part 1 fix, report events per terminal per year for every configured
terminal, with a credibility verdict each. Dozens per terminal per year means thresholds are wrong;
zero across every terminal for years means they are too slack. Show before/after for anything you
change, with reasoning.

# PART 3 — validation cases grounded in data, not in prose

The inherited case 1 — *"Freeport 2026-07-15 dip to 142 MMcf/d → should flag DEPRESSED"* — is
fiction. That day is the **highest** in its two-week window (1,907 MMcf/d against a 30-day median
of 1,637,182 Dth). It cannot be validated because the event it describes did not happen.

**Retire it and build a real ground-truth set.** Search the curated history for events that
actually exist, and for each one record: terminal, dates, what the data shows, and the
classification the detector *should* produce. Aim for at least five cases spanning the classes —
a real sustained depression, a genuine multi-day zero run, a routing episode where one feed drops
while the total holds, a posting gap that must not count as a zero, and a pre-operational period.

Keep the cases that survive contact with data: **TETCO 2024-04-11** is real (the meter posts zero
for seven consecutive days) and **Plaquemines pre-first-gas** is real. **Cove Point on plant intake
has no zero days at all** — keep it as a negative case, which is exactly as valuable.

Rewrite `scripts/task3_validate.py` around this set so each case names its evidence. A validation
suite whose cases came from a document rather than from the data is how the panel ended up
shipping with a 16× arithmetic error and a passing report.

# PART 4 — make the two implementations agree, and get the JS tested

`scripts/task3_validate.py` (Python) and `docs/js/panels/lng-terminal-downtime.js` (JS) implement
the same classifier twice. Nothing asserts they agree, which is this project's signature bug shape:
two layers disagree and the failure is quiet.

- Build a **golden fixture** — a small synthetic input covering each event class — and assert both
  implementations produce identical events from it. Where you put the fixture is your call; both
  sides must read the same file.
- `tests/test_lng_metrics.mjs` exists, but there is **no `package.json` and nothing in CI runs
  `.mjs` files**, so the JavaScript in `docs/js/` is currently untested. Wire up a runner
  (`node --test` is in the standard library and needs no dependencies) and add it to CI so the
  detector's JS is covered. Report what you chose and why.
- Add JS unit tests for the detector covering the three behaviours the design depends on: a feed at
  zero while its sibling holds the total is **not** an outage; a day a feed did not post is
  **never** a zero; both feeds posting zero **is** an outage.

# PART 5 — the observatory should tell someone when a terminal goes down

This is the end goal: the dashboard measures well and interprets a little, but nothing reaches a
person. `publishers/alerts.py` already exists with a Telegram poster, a dedup store with TTL,
health prefixes and integrity-finding alerts. It alerts on **pipeline health only** — never on
analytical signal.

**5a. Alert on feedgas events.** Extend `alerts.py` so a terminal entering `OFFLINE` or
`DEPRESSED`, or a feedgas drop beyond a threshold against its 30-day baseline, sends an alert. The
handoff's suggestion was >20%; decide the threshold from the data's own day-to-day variability
rather than adopting that number because it was written down — a terminal that routinely swings
25% would page constantly. Show the distribution you based it on.

Reuse the existing dedup mechanism: one alert per terminal per event, not one per run. An alert
that fires four times a day gets muted by its reader, and then it protects nothing.

**5b. The alerts workflow cannot see LNG at all.** `.github/workflows/alerts-after-data.yml`
triggers on `workflow_run` of exactly three workflows:

```yaml
      - "EIA Storage (weekly)"
      - "EIA Supply (monthly)"
      - "Baker Hughes Rig Count (weekly)"
```

Weekly and monthly sources only. No LNG feedgas scraper triggers it, so **not even the existing
health alerts fire for the terminals this project exists to measure.** Add the daily EBB scrapers,
using their exact `name:` fields (the same list `publish-dashboard.yml` now carries). Apply the
same debounce reasoning used there, and state which you chose.

**5c. Do not send anything.** No credentials here, and an alert that reaches a real phone is not
yours to trigger. Prove the path with tests and a dry-run mode that renders the exact message body
to stdout. Paste a rendered example for an `OFFLINE` event and for a threshold breach.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm. Threshold changes need evidence — from this repo's data or a cited
   external cadence.
2. **Never fabricate a number, a test result, or a command output.** A negative result is valid:
   "Cove Point has no downtime events" and "these thresholds produce nothing over 100 days" are
   both acceptable answers if true.
3. **Do not remove the UI caveats** about known invisible gas (Freeport's KMTP intrastate lateral;
   Sabine's non-CTPL feeds, ~69% of that terminal). They are load-bearing.
4. **Nameplates need FERC docket citations** in JSDoc — do not add or change one without.
5. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
6. **No git commands at all.**
7. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
   `docs/js/` TypeScript grep, and the JS tests from Part 4. Known pre-existing and NOT yours to
   fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps in
   untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — with unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — the root cause of the 16× discrepancy; which cycle wins and why; the
   reconciliation table (≥10 gas days per terminal, detector total beside direct parquet read);
   whether Sections 5 and 8 now agree.
3. **Part 2** — raw `task3_validate.py` output after the fix; the pre-operational span as one
   event; how first gas is detected; the `TOO QUIET` resolution; per-terminal per-year event rates
   with verdicts and any before/after.
4. **Part 3** — the new ground-truth case set, each with its evidence and expected classification,
   and the detector's actual output for each.
5. **Part 4** — the golden fixture, proof both implementations agree, the JS test runner you chose
   and its output, and the three routing/posting-gap tests.
6. **Part 5** — the threshold you chose with the variability distribution behind it; rendered
   dry-run message bodies; the alerts trigger list; confirmation nothing was sent.
7. **The `docs/js/` TypeScript grep output.**
8. **Test output** — real `pytest`, `ruff`, `mypy`, JS tests. Full-suite counts from an actual run;
   the baseline on this branch is **422 passed, 1 failed, 16 deselected**. Derive any difference.
9. **Anything contradicting this brief.** The numbers above are measurements from the current tree;
   if the repo disagrees, the repo is right and I want to know.
10. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
