# Y — How Deep Does the Well Go

Branch: cut a new one from `main` at `e00c777`. I commit and I merge.

**This brief is a change of altitude.** T through X were internal-consistency work — cycle
vocabularies, registry parity, invariant coverage. All real bugs, all worth fixing, and the board
is now green: node 36/0, pytest 448/0, preflight PASS, ruff 17. But none of it made the observatory
observe more.

Y is about the product. Four items, and the first one is a measurement, not an implementation.

**Scoring, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty. A fabricated number forfeits the section *and*
> Stage 0.
>
> **A new test that has not been proven red forfeits its section.**
>
> **Every file you touch must be parsed before you report** — JS *and* Python:
> `node --input-type=module -e "await import('./docs/js/<f>.js')"` /
> `python -c "import ast,io; ast.parse(io.open('<f>.py',encoding='utf-8').read())"`.
> A file that does not parse forfeits Stage 0 outright.
>
> **§02 is a research question with a real chance of a negative answer.** "The endpoint will not
> serve more than N days" is a complete, full-marks deliverable. Do not implement a backfill you
> have not proved is possible.

---

## 00 / THE FOREST

Blue Tide's own README calls it *"the physical observatory of North American natural gas"* and says
the goal is *"to make every molecule of natural gas visible."* The flagship is the LNG Feedgas
Observatory. Here is what that observatory can actually see, measured on the host today:

| source | rows | distinct gas days | first | last |
|---|---:|---:|---|---|
| quorum | 117,534 | **1,996** | 2021-03-15 | 2026-09-03 |
| enbridge | 238,432 | **1,107** | 2023-08-24 | 2026-09-03 |
| bhe | 14,268 | **612** | 2024-12-31 | 2026-09-03 |
| **cheniere** | 7,128 | **101** | 2026-05-24 | 2026-09-02 |
| **gulf_south** | 441,115 | **101** | 2026-05-25 | 2026-09-02 |
| **gasnom** | 64,430 | **99** | 2026-05-25 | 2026-09-03 |
| **kinder_morgan** | 135 | **10** | 2026-08-25 | 2026-09-03 |

`gulf_south`, `gasnom` and `cheniere` all begin within a day of each other. That is not an upstream
limit — **that is the day we started scraping.** Meanwhile `quorum` has five and a half years and
`enbridge` has three.

The consequence is the thing that matters. For Freeport, Cameron, Golden Pass, Sabine Pass and
Corpus Christi — **five of the eight terminals the observatory covers** — nobody can ask:

- Is this outage unusual, or does it happen every August?
- What did feedgas do last winter?
- Is 52.9% coverage normal for September, or is a pipeline down?
- How does this year compare to last year?

Every one of those is the ordinary question a reader brings to a gas observatory, and the answer is
currently "we started watching in May." A hundred days of history is a demo. **Depth of history is
worth more to this project right now than any further internal consistency work**, and finding out
whether we can get it is the first thing to do.

---

## 01 / STAGE 0 — DO NOT REGRESS

| Gate | Command | Requirement |
|---|---|---|
| Y0-a | `node --test tests/*.test.mjs` | 0 failed (currently 36 passed) |
| Y0-b | `python -m pytest -q -m "not network"` | 0 failed, no collection errors (currently 448) |
| Y0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| Y0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| Y0-e | parse check on every file touched, JS and Python | no SyntaxError |

---

## 02 / Y1 — HOW FAR BACK WILL EACH SOURCE SERVE? (MEASURE. DO NOT BUILD.)

This is the whole point of the brief. I want a number per source, obtained by asking the source.

**`gulf_south` — the one that matters most, because Freeport is the flagship.**
`scrapers/energy_transfer/gulf_south.py` does not query by date. It pulls a **postings listing** and
walks it. `POSTINGS_PAGE_SIZE = 100` today, and the comment above it already records real
measurements taken 2026-09-02:

```
page_size -> postings / OAC-CSV postings / gas days
    20   ->  20 /  4 /  2
    50   ->  50 / 11 /  4
   100   -> 100 / 23 /  8
   200   -> 200 / 45 / 15
```

That is ~13 postings per gas day and **nobody has ever tested beyond 200.** Establish the ceiling:

1. Extend the table — 500, 1000, 5000, and whatever the endpoint tolerates. Record, for each:
   postings returned, OAC-CSV postings, distinct gas days, oldest gas day reached, wall-clock, and
   any error or silent truncation.
2. Determine whether the endpoint **paginates** (an offset/cursor/page parameter, or a
   `Link`/`next` field). If it does, pagination — not page size — is the route to real depth, and
   that changes the answer entirely. Say which.
3. Report the **oldest gas day you could actually reach**, as a date.

**`gasnom` and `cheniere`:** same question, appropriate to each API. Read the scraper first and say
how the endpoint is addressed — date parameter, rolling listing, or something else — then measure
the reachable floor. `kinder_morgan` too if it is cheap; 10 days of history is thin even by the
standards above.

**Be a good citizen while you do this.** These are public FERC-mandated endpoints, but they are
somebody's servers. Use the existing rate-limited client rather than a raw loop, do not run the
large probes more than once each, and if you see a 429 or an error that looks like a limit, **stop
probing that source and report it** — do not retry into it. If a probe would take more than a
couple of minutes, say so and stop rather than hammering.

**Deliverable:** a table — source, current depth in gas days, reachable depth in gas days, oldest
date reachable, method (page size / pagination / date param / not possible), and a one-line
confidence note. Plus a recommendation: which source to backfill first and roughly what it costs.

**Implement nothing in this section.** No backfill, no `POSTINGS_PAGE_SIZE` change committed. If
the answer is "these endpoints serve a rolling ~15-day window and history is unavailable," that is
a complete and valuable answer, and it closes the question for good.

---

## 03 / Y2 — THE FRONT DOOR IS DESCRIBING A DIFFERENT PROJECT

`README.md` is what every new reader sees first, and it is badly out of date. Measured against the
repository as it stands:

| README says | reality |
|---|---|
| `status-6 live sources` | **12** curated sources with data |
| "LNG Feedgas Observatory is shipped and live (Freeport LNG, first terminal)" | **8 terminals** in `DOWNTIME_CONF`, all 8 coverage-guarded and passing |
| Coverage table lists **Freeport only** as ✅ Live | freeport, cove_point, sabine_pass, plaquemines, cameron, calcasieu, golden_pass, corpus_christi |
| Data Sources table lists 4 live platforms | gulf_south, gasnom, quorum, cheniere, bhe, enbridge, kinder_morgan, plus the EIA/BH/GIE feeds |
| "Freeport is additionally fed by TETCO (KM); full multi-pipeline per-terminal coverage is a later wave" | multi-feed is implemented and cross-panel-tested |

Bring it up to date, and **derive every number from the repository rather than from this table** —
I compiled the right-hand column by hand and you should check me. There are 18 dashboard panels in
`docs/index.html`; the README mentions none of them.

Two rules for this section:

- **Do not overstate.** Every terminal listed as live must have a `DOWNTIME_CONF` entry *and*
  curated rows. If a terminal is guarded but thin — Corpus Christi has 101 gas days, Kinder Morgan
  10 — say the depth next to it. The project's stated philosophy is *"an observatory, not an
  oracle... transparency over prediction"*, and a README that oversells is the first place that
  breaks.
- **Add a "History depth" line to the coverage table** using the real per-source gas-day counts from
  §00, so a reader knows before they click that the LNG panels hold ~100 days.

Update the badge count. Move anything from "Planned" to "Live" only if it is genuinely live.

---

## 04 / Y3 — THE UI OFFERS A YEAR OF DATA WE DO NOT HAVE

`docs/js/util/range-state.js:10` exports `RANGE_PRESETS = ['30d', '90d', '1y', 'all']` and
`'1y'` resolves to 365 days. The LNG feedgas sources hold ~101.

Brief Q added a test named *"Range exceeding history surfaces honest caveat rather than silent
clipping"*, so the machinery may already exist. **Find out whether that caveat actually fires on
the LNG panels** — Section 5, 7 and 8 — when `1y` is selected against 101 days of data, and report
what a reader sees today. Screenshot-free is fine; describe the rendered state precisely.

If it fires: say so, and this section is done in a paragraph.

If it does not: make it. The caveat must state the actual span the data covers, not merely that the
range was clipped — *"showing 101 days; this source begins 2026-05-25"* is useful, *"insufficient
data"* is not. Follow the existing caveat pattern rather than inventing a second one, and keep it
inside `safeRender`.

Add a test either way, proven red.

---

## 05 / Y4 — EVIDENCE. EIGHTH BRIEF.

`scripts/evidence.py` has never executed and `logs/EVIDENCE.json` has never existed. Run it if you
can; declare `NOT RUN` if you cannot. Either is fine. What is not fine is `logs/` containing
anything a run did not produce — **absent, not a tombstone.** Seventh time.

---

## 06 / GROUND RULES

1. **No git commands at all.** This sandbox has destroyed this repository's `.git` twice. I commit.
2. **Parse every file you touch, JS and Python.** Forfeits Stage 0.
3. **Prove every new test red before claiming it guards anything.**
4. **Never fabricate a number, and never report as executed something you did not run.**
5. **A negative result is a valid result** — §02 especially. "Not possible" closes a question worth
   closing.
6. **Implement nothing in §02.** Measurement and a recommendation only.
7. **Do not overstate in the README.** §03 is the public description of a project whose stated
   philosophy is transparency; getting it wrong there costs more than a bug.
8. **Be gentle with the upstream endpoints.** Rate-limited client, no repeated large probes, stop on
   any sign of a limit and report it.
9. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
10. **Do not hand-edit curated parquet or health JSON.**
11. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
12. **Never mix `_sq_` and `_oac_` in a flow total.**
13. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`,
    390px reflow.
14. Known pre-existing and **not yours**: the 17 residual ruff findings (E402, N806), the mypy
    errors in `scrapers/base/playwright_client.py` and `transformers/baker_hughes.py`.
15. Maintain `OVERNIGHT_STATE.md`.

---

## 07 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all five green, or zero** | **20** |
| Y1 — depth table for gulf_south, gasnom, cheniere; method identified per source | 30 |
| Y1 — pagination question answered, and a first-backfill recommendation with a cost | 15 |
| Y2 — README accurate, numbers re-derived, depth stated, nothing overstated | 20 |
| Y3 — what a reader sees today on `1y`; caveat verified or built; test proven red | 10 |
| Y4 — `evidence.py` run or honestly declared, `logs/` clean | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. An implemented backfill in §02
scores zero for that section however well it works — the question there is whether we *can*, not
whether you did.

---

## 08 / REPORT FORMAT

1. **Stage 0 table** — gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **Files touched**, with parse-check output for each.
3. **Y1** — the extended page-size table with real observations; the pagination finding; the depth
   table across sources; the recommendation and its cost. If a source refused, say exactly how.
4. **Y2** — the README diff, and where each number came from.
5. **Y3** — what a reader sees today, and what you changed if anything.
6. **Y4** — did it run; the state of `logs/`.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
