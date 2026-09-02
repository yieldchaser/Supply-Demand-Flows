# Q / CLOSE THEN BUILD — finish the branch, then make the dashboard fast and interactive

**Repo:** `yieldchaser/Supply-Demand-Flows` · **Branch:** `fix/section8-audit`, head `3bc6dc5`

Long session, one continuous run. **Stage 0 is a gate: four failing tests and a crashing script.**
Nothing else may start until it is green, because the branch has been one round from merging three
times running. After the gate, three arcs: make it fast, find what is broken, make it interactive.

Do not stop to ask whether to continue. When you reach the end, go to the final section and keep
going.

---

## 00 / RUNTIME AND EVIDENCE

- `python`, `pandas`, `pytest`, `ruff`, `mypy`, `node` (v18+, `node --test`) available. **First
  action: test which runners work tonight** and record it. If one is dead, say so once and route
  around it — never estimate an output.
- **No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
  destroyed this repository's `.git` twice during ordinary commit operations; both recoveries were
  full re-fetches from GitHub. Claude commits. Leave everything in the working tree.
- **Maintain `OVERNIGHT_STATE.md`** in the repo root, updated after **every** stage: runner check,
  stage log with the evidence file that closed each one, decisions with their reversal conditions,
  every number with the command and window behind it, and `Blocked / needs Claude`. On resuming
  after a compaction, read it before doing anything else.
- **Every cited command redirects into `logs/`**, e.g.
  `python -m pytest -q -m "not network" > logs/Q3-pytest.txt 2>&1`. Then read the file you just
  wrote and quote from that.
- **Time-box each stage** to roughly three serious attempts, then write what you tried into
  `Blocked / needs Claude` and move on. Never fake an exit to escape a stage.

### Why the evidence rules are mechanical now

The previous session reported pytest 434/1, node 16/0, a preflight PASS and a 100/100 self-score.
Re-run, the truth was **pytest 431/4, node 13/1, and preflight crashing in its first section.** The
`logs/` files had been typed by hand rather than produced by redirection — `logs/final-pytest.txt`
cited `tests/test_universe.py`, which does not exist in this repo, and reported `eia_supply` at
7,238 rows against a real 468. Those files were not committed; fabricated evidence in a repository
is worse than none.

That took ninety seconds to catch, and it cost a whole round. **Run the command, read the file it
wrote, quote from the file.** If a command cannot run in your sandbox, say so — that is always
acceptable and costs nothing. If your output disagrees with a number in this brief, the output wins
and I want to see it.

## 01 / FORBIDDEN SHORTCUTS

Any of these invalidates the stage: deleting, skipping or `xfail`-ing a failing test; loosening an
assertion instead of fixing the cause; `continue-on-error`, `|| true` or a bare `except:` to quiet a
gate; `# type: ignore` or `# noqa` instead of a fix; widening a threshold without measured evidence;
hand-editing `data/health/*.json`; publishing an estimate as a measurement; removing or softening a
UI caveat.

If a check fires and you believe the check is wrong, that is a finding: write the argument, leave
the check firing, move on.

---

# STAGE 0 — THE GATE

Current state, verified:

```
pytest: 431 passed, 4 failed, 16 deselected
  tests/test_classify_meters.py::test_build_universe_covers_expected_totals   (known, NOT yours)
  tests/test_coverage_guard.py::test_terminal_coverage_guard_against_curated_parquets
  tests/test_coverage_guard.py::test_coverage_guard_rejects_perturbed_flattering_claim
  tests/test_integrity.py::TestDivergence::test_cadence_scaled_health_recency

node --test tests/*.test.mjs: 13 passed, 1 failed
  tests/test_lng_downtime_render.test.mjs — fails to load
```


Current, verified by me:

```
pytest: 431 passed, 4 failed, 16 deselected
  tests/test_classify_meters.py::test_build_universe_covers_expected_totals   (known, NOT yours)
  tests/test_coverage_guard.py::test_terminal_coverage_guard_against_curated_parquets
  tests/test_coverage_guard.py::test_coverage_guard_rejects_perturbed_flattering_claim
  tests/test_integrity.py::TestDivergence::test_cadence_scaled_health_recency

node --test tests/*.test.mjs: 13 passed, 1 failed
  tests/test_lng_downtime_render.test.mjs — fails to load
```

### O1 — the sidecar is missing a field

Both coverage-guard tests die on `KeyError: 'nameplate'`.

`scripts/load_registry.py` now correctly extracts all **9** terminals into
`config/terminals_registry.json` — that part is a real improvement over the 5 it managed before.
But the emitted records omit `nameplate`, which the guard needs as the denominator of every
coverage figure.

Add it, along with anything else the guard reads. Then make the sidecar's completeness *testable*:
a test asserting every field the guard consumes is present for all nine terminals, so the next
missing field fails loudly at the source rather than as a `KeyError` three layers away.

### O2 — preflight still crashes

```
UnicodeEncodeError: 'charmap' codec can't encode character '→' in position 56
```

It prints `→` into a cp1252 console. Fix it so it runs on this machine — reconfigure the stream
encoding, or use ASCII in the output. Your call; it must survive a plain `python scripts/preflight.py`
with no environment tweaks, because that is how it will be run.

Then run it and capture the real output. Expect the integrity board to show **five WARNs** on
documented historical gaps (`gulf_south 2026-08-27`, `baker_hughes 2026-01-02`,
`gasnom 2026-08-23..25`, `quorum 2025-03-25..27`, `cheniere 2026-08-25`) — those are honest and
already annotated. Decide whether preflight's verdict treats WARN as passing, and justify it in one
line. **Do not make it green by weakening a check.**

### O3 — the render test does not load

`tests/test_lng_downtime_render.test.mjs` fails at load, not at assertion. Find out why — most
likely an import that reaches D3 or the DOM through `lng-terminal-downtime.js` — and fix it so the
render seam is genuinely reachable from `node --test`. If the seam is not clean enough to test
headlessly, split it further rather than stubbing the DOM into existence.

### O4 — the divergence test

`test_cadence_scaled_health_recency` is your own new test and it fails. Either the per-source
`health_recency_days` implementation in `validators/integrity.py` is wrong or the test's expectation
is. Work out which, fix that one, and say which it was.

### O5 — the workflow question

`publish-dashboard.yml` runs preflight before building the bundle. Once O2 lands, decide: does a
preflight failure deserve to block publishing? An argument exists both ways — blocking protects
readers from a bad bundle, and not blocking keeps a stale dashboard from freezing on an unrelated
WARN. Pick one, implement it, state the reasoning in one line.

---


### Gate exit condition

```
python -m pytest -q -m "not network"   > logs/Q0-pytest.txt 2>&1     # 1 known failure only
node --test tests/*.test.mjs           > logs/Q0-node.txt 2>&1       # 14 passed, 0 failed
python scripts/preflight.py            > logs/Q0-preflight.txt 2>&1  # runs to a verdict
```

Quote all three from the files. **Only then continue to Arc One.** If the gate cannot be closed
after three serious attempts, record why in `Blocked / needs Claude` and continue anyway — but say
loudly at the top of your report that the branch is still red.

---

# ARC ONE — MAKE IT FAST

## P1 — 1.5 GB of dead artifacts · exit: measured before/after, with a retention policy and a test

`docs/data/` is **1.5 GB across 156 files**, and all but one hash is dead:

```
bundle.*.json    11 files, ~65 MB each   (~715 MB)
index.*.json     11 files
src.*.json      132 files                 (12 sources x 11 hashes)
manifest hash currently live: 66c9d2c6    (1 of the 11)
```

Every clone of this repository pays for all of it, GitHub Pages stores all of it, and CI checks it
out on every scraper run — which is most of the wall-clock time in a job that only needs to append a
few thousand rows.

Note before deleting anything: `docs/js/data/bundle-loader.js:114` falls back to the monolithic
`bundle.json`, described there as "always tracked + served". So `bundle.json` and the current hash's
files are load-bearing. Establish exactly which files the live site can request — read the loader,
do not assume — then remove the rest.

Then make it not recur: a retention policy in `publishers/export_dashboard_json.py` that prunes
superseded hashes on publish, keeping the current one plus a small, stated number of previous
generations for rollback. Add a test asserting the policy holds, so `docs/data/` cannot silently
grow back to a gigabyte.

**Measure and report**: file count and total size before and after, and what a fresh clone now costs.

## P2 — measure the real load, then improve it · exit: numbers before and after, from measurement

The project's own note says the bundle is "~3.2 MB gzipped over the wire, boot parse ~123 ms" and
warns that an earlier session invented a size cap and nearly pruned real history over it. **So
measure; do not assume, and do not prune history.**

Establish, for the live site or a local serve:

- transfer size gzipped of each thing the page actually fetches on boot,
- the number of requests to first render,
- parse time for the boot payload, and for a panel's lazy shard fetch,
- which panels block first paint and which are genuinely deferred.

Then improve what the measurements justify, and re-measure. Candidates, in the order I would look:
shards that could be narrower for a first render; whether the boot set is really the minimum three
sources; whether panels below the fold defer their fetch until visible; whether any panel parses the
whole bundle when it needs one source.

**Report a before/after table.** Any change without a measurement behind it does not count.

## P3 — the bug sweep · exit: a written finding per class, with severity

Hunt deliberately rather than reading around hoping. This codebase's failure signature is
consistent — *two layers disagree and nothing asserts they agree, so the failure is silent.* Ask of
each area: **what would this look like if it silently did nothing?**

Sweep at minimum:

- **Panels vs data**: a panel whose series no longer exists in the bundle renders empty rather than
  loudly. Which panels can do that, and does anything catch it?
- **The publisher's prune**: `gasnom` ships 7.8% of its curated rows to the bundle, `gulf_south`
  13.1%, `bhe` 99.2%. That has been asserted to be the intended relevance prune. Verify it against
  `_audit_bundle_coverage` and say whether that audit would actually catch a regression to 5%.
- **Cycle handling outside Section 8**: the settled rule (SQ only, hourly `id{HH}00` excluded,
  latest genuinely nominated cycle wins) is enforced in the LNG panels. Do Sections 1–4 and 6 use
  the same convention where they touch cycle data, or do they have their own?
- **Error paths**: what does each panel do with an empty series, a single data point, a NaN, a
  future-dated period? `safeRender` catches a throw, but a panel that silently renders nothing is
  the thing this project keeps shipping.
- **Timezone and gas-day boundaries** in the frontend: `new Date('2026-09-01')` parses as UTC
  midnight and can render as the previous day in a negative-offset timezone. Check every date
  construction in `docs/js/`.

For each finding: severity, the mechanism, whether it is live today, and a test that would have
caught it. **Fix the ones that are live and wrong; write up the rest.** Do not fix by hiding.

---

# ARC TWO — MAKE IT INTERACTIVE

The dashboard measures well and interprets a little. Every panel is currently a fixed view: it shows
what it shows. The goal of this arc is that a reader can **ask a question and get it answered
without leaving the page** — while keeping the project's honesty rules intact.

Build in this order, and treat each as shippable on its own.

## P4 — a shared time range · exit: one control drives every time-series panel

Add a date-range control that all time-series panels obey — a brush on a summary strip, or a
compact range selector with sensible presets (30d, 90d, 1y, all, plus custom).

Requirements:

- **State lives in the URL** so a range is linkable and survives reload. A reader sending someone
  "look at Freeport in July" should be able to send a URL.
- Panels re-render from data already in memory where possible rather than refetching.
- Ranges that exceed a source's history must show the caveat, not silently clip. Sources have wildly
  different spans — `quorum` reaches 2021, `gulf_south` starts 2026-05-25 — and a chart that quietly
  shortens is exactly the kind of dishonesty this project exists not to commit.

## P5 — Section 8 events on the feedgas chart · exit: events visible in context, click-through both ways

This is the highest-value interaction available and the data already exists. Section 8 computes
`OFFLINE`, `DEPRESSED`, `RAMPING` and `NOT_YET_OPERATIONAL` events per terminal. Section 5 draws
feedgas over time. They do not talk to each other.

Overlay the events on the feedgas chart as shaded spans or markers, so a reader sees *the dip* and
*the classification of the dip* together. Clicking an event focuses that period; clicking a chart
region surfaces any event covering it.

Honesty constraints that must survive: an incomplete newest day stays suppressed (feed parity —
see `docs/js/util/lng-downtime.js`); a posting gap must be visually distinct from a posted zero, not
merged into one; and event classifications carry their basis on hover — a reader should be able to
see *why* a span is `DEPRESSED` (below 60% of a 30-day baseline for ≥5 days) without reading source.

## P6 — terminal comparison · exit: any two or more terminals on one normalised axis

Let a reader select several terminals and compare them — absolute MMcf/d and as a percentage of
nameplate, since those answer different questions.

The trap: **coverage differs per terminal and comparing them naively lies.** Sabine is measured at
~30%, Cameron ~73%, Freeport ~53%, Cove Point ~97%. A chart showing Sabine below Cove Point without
saying why is actively misleading. Surface each terminal's coverage inline — a badge, a hatched
band, whatever reads clearly — so the comparison carries its own caveat.

## P7 — export and deep links · exit: a reader can take the data and cite the view

- CSV export of what a panel is currently showing, with the series ids and the cycle rule in a
  header comment so an exported file is self-describing.
- PNG or SVG export of a chart for pasting into a note.
- Deep-linkable panel state — terminal selection, range, comparison set — so a specific view is a
  URL. This turns the dashboard into something citable, which is most of the difference between a
  page people visit and a page people reference.

## P8 — the things that make it feel finished · exit: verified at 390px and by keyboard

- Loading skeletons rather than layout jump while a lazy shard fetches.
- Keyboard navigation across interactive controls, visible focus states, ARIA labels on charts, and
  a text alternative conveying the same numbers for a screen reader.
- Mobile reflow verified at **390px** — that is a stated project requirement and every new control
  must meet it.
- Empty and error states that say what is missing and why, in the voice the caveats already use.

Design constraints: **vanilla JS only, zero TypeScript syntax in executable code** (JSDoc is fine),
**design tokens only in CSS**, every panel wrapped in `safeRender`. Paste the TypeScript grep before
finishing:

```
grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/ > logs/P-tsgrep.txt 2>&1
```

Extract new logic into `docs/js/util/` modules free of D3 and the DOM so `node --test` can reach it,
the way `lng-downtime.js` was extracted. **Every new interactive behaviour gets a test.**

---

## 02 / RUBRIC — exit gate 90/100

| dimension | points | minimum |
|---|---|---|
| Stage 0 gate closed: pytest 1 known failure, node 14/0, preflight runs | 15 | 15 |
| Every reported number backed by a `logs/` file that matches a re-run | 20 | 18 |
| `docs/data/` pruned, retention policy tested, before/after measured | 12 | 10 |
| Load measured before and after, improvements justified by numbers | 10 | 8 |
| Bug sweep: findings written with severity, live ones fixed | 11 | 9 |
| Shared time range with URL state, honest about differing histories | 9 | 7 |
| Section 8 events on the feedgas chart, gaps distinct from zeros | 9 | 7 |
| Comparison and export carry their coverage caveats | 7 | 5 |
| Suite green, 390px verified, TypeScript grep clean | 7 | 6 |

**Stage 0 is worth its full 15 or nothing — a red gate is a red gate.**

Below 90, keep working. A dimension below its minimum fails regardless of total. **An honest failure
scores full marks on dimension 1; a fabricated pass scores zero across the board**, because
everything is re-derived before it merges.

## 03 / NON-NEGOTIABLES

1. **When a guard fires, fix the cause.**
2. **Never fabricate a number, a test result, or a command output.** If it did not run, say so.
3. **Never publish an estimate as a measurement.** "Observatory, not oracle. Zero randomness."
4. **Do not remove UI caveats** — the new views must carry them, not escape them.
5. **Do not prune curated history.** P1 removes superseded *published artifacts* only. The parquets
   are the archive and short-retention EBB sources cannot be re-fetched.
6. **Do not change any nameplate** — FERC docket citations.
7. **Never mix `_sq_` and `_oac_`** in a flow total.
8. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
9. **Vanilla JS only in `docs/`**, design tokens, `safeRender`, 390px.
10. **No git commands.**

Known pre-existing and NOT yours: ~35 mypy errors in `scrapers/base/playwright_client.py`, mypy gaps
in `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and
`test_build_universe_covers_expected_totals` (717 vs 719).

## 04 / IF YOU REACH THE END WITH TIME LEFT

Do not idle and do not stop. In order:

1. Raise any rubric dimension below its minimum.
2. Turn each `Blocked / needs Claude` entry into a precise question with its evidence attached.
3. Strengthen the weakest guard in the repo. Known gaps: the accumulation guard misses a module that
   calls `merge_into_curated` for one file while direct-writing another, and misses
   `pq.write_table`; the cross-panel invariant covers one terminal only; nothing asserts a
   `series_id` carries its flow token on write.
4. Extend the golden fixture to every event class the detector emits.
5. Write `docs/VERDICT.md` entries for anything investigated that did not become code.

## 05 / REPORT

1. Diff summary — every file, one line of reasoning.
2. Stage by stage: exit met or not, with the `logs/` path proving it.
3. P1 before/after: file count, total size, fresh-clone cost.
4. P2 before/after table: transfer, requests, parse time.
5. P3: every finding with severity, mechanism, live-or-not, and the test.
6. P4–P8: what a reader can now do that they could not, and how each honesty constraint is met.
7. Rubric self-score per dimension.
8. Anything contradicting this brief — its numbers are measurements and have been wrong before.
9. Anything noticed and not fixed.

Leave everything uncommitted.
