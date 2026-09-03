# W — One Rule, Three Implementations

Branch: cut a new one from `main` at `f35ad45`. I commit and I merge.

This is a big brief. Six items. Every one is measured below — you are not asked to investigate
anything, only to decide and implement. Two items (§03, §08) are explicitly **decide and propose,
do not execute**.

**How this brief is scored, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty and keeps the section's code weight. A
> fabricated number forfeits the section *and* Stage 0.
>
> **A new test that has not been proven red forfeits its section.** Revert the fix in a temporary
> copy, run the test, paste the failing output, restore byte-for-byte, confirm the restore.
>
> **New in W — any file under `docs/js/` you touch must be import-checked before you report:**
> `node --input-type=module -e "await import('./docs/js/<path>.js')"`. A `docs/js` file that does
> not parse forfeits Stage 0 outright, regardless of everything else.

---

## 00 / WHY THAT THIRD RULE EXISTS

Your V report scored itself 100/100. The edit it described as a comment reword in
`docs/js/data/bundle-loader.js` **also deleted the `} catch (err) {` line**, leaving the fallback
body inside the `try` and the `try` with no `catch`:

```
SyntaxError: Missing catch or finally after try
```

`node --test` passed 25/25 anyway, because no test imported any `docs/js` module. That gap is now
closed by `tests/test_module_syntax.test.mjs`, which walks `docs/js` and dynamically imports every
file, failing on `SyntaxError`. **On its first run it found a second, pre-existing SyntaxError** —
`docs/js/panels/lng-feed-substitution.js` declared `N_SHARE_PTS` and `M_TOTAL_PCT` locally *and*
imported the same two names. `main.js:21` imports that module statically, so `main.js` never
parsed and **the deployed dashboard was rendering empty panel skeletons.** I confirmed it in a
browser console before fixing it. Both repaired in `4248ca4`.

Green unit tests are not evidence that the page loads. That is why §W0 now includes an import
check, and why the last line of this brief asks you to say which `docs/js` files you touched.

**What V got right, and it was a lot.** All nine terminals are now coverage-guarded and all nine
PASS — the guard went from `5 passed, 4 skipped (WARN)` to `9 passed, 0 skipped, 0 failed`, and
Calcasieu's claimed 123.5% measured 123.4% against real curated data for the first time. And §04's
answer **contradicted my stated expectation with evidence**, which is the best outcome available:
I predicted `best` should score 0, and you found that on `2026-08-25` `km_ngpl_sq_3592_d_best` is
the sole cycle present, so 0 would drop a real gas day. Ranking it below `timely` is correct. That
is exactly the behaviour the brief asks for.

One correction: **§06's migration numbers were invented.** You reported 67 rows with 49 changing.
Measured: **135 rows across 24 series ids and 4 meters.** §06 of this brief gives you the real
figures.

---

## 01 / STAGE 0 — THE GATE

All-or-nothing. Nothing here is broken today.

| Gate | Command | Requirement |
|---|---|---|
| W0-a | `node --test tests/*.test.mjs` | 0 failed (currently 26 passed) |
| W0-b | `python -m pytest -q -m "not network"` | 0 failed (currently 447 passed) |
| W0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| W0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| W0-e | `node --input-type=module -e "await import('...')"` per changed `docs/js` file | no SyntaxError |

---

## 02 / W1 — THE HEADLINE: THE CYCLE RULE IS IMPLEMENTED THREE TIMES AND THEY DISAGREE

The settled-cycle precedence rule is the single most load-bearing invariant in this project. It
exists in three places:

**1. `docs/js/util/lng-downtime.js:19`** — exported `CYCLE_PRIORITY` + `cyclePriority()`:

```js
{ timely: 1, evening: 2, late: 3, latec: 4, id1: 5, id2: 6, id3: 7 }
```

**2. `docs/js/panels/basin-egress.js:40`** — a *separate copy* named `CYCLE_RANK`, same seven
values, not imported from anywhere:

```js
{ timely: 1, evening: 2, late: 3, latec: 4, id1: 5, id2: 6, id3: 7 }
```

**3. `scripts/task3_validate.py`** — twelve keys, including Kinder Morgan's native tokens and
`best`, with the canonical ranks all shifted up by one to make room:

```python
{ "best": 1, "timely": 2, "evening": 3, "evng": 3, "late": 4, "latec": 5,
  "id1": 6, "itrd1": 6, "id2": 7, "itrd2": 7, "id3": 8, "itrd3": 8 }
```

The differing integers are harmless — only relative order matters. **The differing vocabulary is
not.**

**And it is live.** `docs/js/util/lng-downtime.js` also exports `DOWNTIME_CONF`, and its
`sabine_pass` entry reads:

```js
feeds: [
  { source: 'cheniere',      stem: 'creole_trail_sq_ct200111_d', label: 'Creole Trail' },
  { source: 'kinder_morgan', stem: 'km_ngpl_sq_3592_d',          label: 'NGPL (context)', context: true },
],
```

Every row of `km_ngpl_sq_3592_d` in curated carries a KM-native suffix — `best`, `evng`, `itrd1`,
`itrd2`, `itrd3`, `timely`. The browser's `cyclePriority()` returns **0** for five of those six, so
`lng-downtime.js:140` filters them out. **This is the same bug U fixed on the Python side, still
live in the browser.** It does not currently change a rendered number, because every one of those
values is 0.0 Dth — but the moment NGPL posts real volume, Section 8 and the Python guard will
disagree about Sabine Pass, and the cross-panel invariant test will not catch it because it does
not exercise this feed.

**What to do:**

1. **One definition, imported everywhere.** `docs/js/util/lng-downtime.js` is the right home — it
   already exports both the map and the function, and `lng-fleet-data.js:16` already re-exports
   `cyclePriority` from elsewhere in the chain. Make `basin-egress.js` import `cyclePriority`
   instead of carrying `CYCLE_RANK`. Check for any other local copy before you finish; grep for
   `CYCLE_RANK`, `CYCLE_PRIORITY` and `cyclePriority` across `docs/js` and read every hit.
2. **Teach the JS the same vocabulary the Python knows** — `evng`, `itrd1`, `itrd2`, `itrd3`, and
   `best` ranked below `timely`, matching `scripts/task3_validate.py` exactly. Keep the hourly
   `id{HH}00` exclusion (priority 0) as it is; that rule is correct and separate.
3. **Pin the two implementations to each other with a test.** Add a test that reads
   `CYCLE_PRIORITY` from the JS and `CYCLE_PRIORITY` from `scripts/task3_validate.py` and asserts
   the two produce the **same relative ordering** over the union of their keys. Ordering, not equal
   integers — if a future edit shifts one set of ranks uniformly that is fine, and the test should
   not fail on it. A Python test that parses the JS object literal is acceptable and probably
   simplest; if you do that, parse it robustly rather than with a fragile regex, and say how.
   Prove it red by removing one alias from either side.

`basin-egress.js` is a `docs/js` file — import-check it per W0-e.

---

## 03 / W2 — FREEPORT HAS TWO NAMEPLATES. DECIDE, DO NOT EDIT.

Measured across `docs/js/util/lng-downtime.js`'s `DOWNTIME_CONF` and
`config/terminals_registry.json`:

| terminal | DOWNTIME_CONF | registry | |
|---|---|---|---|
| freeport | **2140** | **2100** | **mismatch** |
| plaquemines | 3400 | 3400 | ok |
| cameron | 2000 | 2000 | ok |
| cove_point | 750 | 750 | ok |
| sabine_pass | 4500 | 4500 | ok |
| calcasieu | absent | 1300 | see §04 |
| golden_pass | absent | 2600 | see §04 |
| corpus_christi | absent | 2400 | see §04 |
| port_arthur | absent | 1900 | non-operational |

Freeport's Section 8 panel divides by **2140**; every coverage figure divides by **2100**. Same
terminal, two denominators, and `expectedCoveragePct: 52.9` was derived against 2100. The
`DOWNTIME_CONF` comment cites `FERC CP12-509 (3 trains x 0.71 Bcf/d)`, which is 2130, not 2140.

**Ground rule 7 forbids you from changing a nameplate, and that rule stands here.** Your job is to
resolve which figure is right and hand me the evidence:

- What does FERC CP12-509 actually authorise, and does the docket support 2100, 2130, or 2140?
- Which figure is each of `expectedCoveragePct`, `expectedMedianMmcf` and the Section 8 utilisation
  band derived against? Show the arithmetic.
- What changes on screen if the losing figure is corrected — recompute both ways and give me the
  delta in percentage points.

Then add a test that asserts `DOWNTIME_CONF[k].nameplate === registry[k].nameplate` for every
terminal present in both. **Write it so it fails today** on Freeport, and say plainly in your report
that it is red pending my decision — do not make it pass by editing either number. A red test I
have been told about is fine; a silently reconciled nameplate is not.

---

## 04 / W3 — SECTION 8 COVERS FIVE OF NINE TERMINALS

`DOWNTIME_CONF` carries `freeport`, `cove_point`, `sabine_pass`, `plaquemines`, `cameron`. It is
missing `calcasieu`, `golden_pass`, `corpus_christi` — the exact three that V just added to the
Python `TERMINALS` and that now measure 123.4%, 12.7% and 99.4% against their registry claims.
`port_arthur` is non-operational and stays out.

The feed ids are known and verified against curated by V:

| terminal | feed | parquet | rows |
|---|---|---|---|
| calcasieu | `trans_cameron_sq_vgcpd_d` | `quorum.parquet` | 9,985 |
| golden_pass | `golden_pass_sq_1097217_d` | `gasnom.parquet` | 505 |
| corpus_christi | `corpus_christi_sq_CC200221_d` | `cheniere.parquet` | 102 |

Add the three `DOWNTIME_CONF` entries. Take `nameplate` from
`config/terminals_registry.json` — **not from your own recollection** — and take the `feeds[].stem`
from the table above. `zeroDaysThreshold` and `cargoZero`: inherit the precedent (3 and `false`)
and say in your report that they are inherited and unvalidated, exactly as V did for the Python
side. That flagging is worth marks; quietly pretending they are derived is not.

Each entry needs an `honesty` string. Write it from what the registry actually records —
`coverageNote` where one exists, `expectedCoveragePct` otherwise. Do not invent a pipeline
relationship. If you cannot state something you can support, say the coverage percentage and stop.

**Then check what breaks.** Section 8 renders every `DOWNTIME_CONF` key. Three new panels will
appear. Confirm `tests/test_lng_downtime_render.test.mjs` and
`tests/test_lng_cross_panel_invariant.test.mjs` still pass, and say whether the invariant test now
covers the new terminals or silently skips them. **If it skips them, say so — do not extend it in
this brief.** Knowing the invariant's true coverage is worth more than widening it blind.

---

## 05 / W4 — RUN THE HARNESS. SIXTH BRIEF.

`scripts/evidence.py` was written in R, hardened in S, audited in U, re-audited in V, and **has
never executed.** There is still no `logs/EVIDENCE.json`.

If you can spawn processes, run it and let it produce this brief's Stage 0 logs.

If you cannot, that remains a full-marks answer — say so and leave `logs/` containing only what a
run produced. **Absent, not a tombstone.** Fifth time of asking.

---

## 06 / W5 — THE KM CANONICAL MIGRATION. EXECUTE IT THIS TIME.

V wrote the plan with invented numbers. Here are the real ones, measured on the host:

`data/curated/kinder_morgan.parquet` — **135 rows, 24 series ids, 4 meters**:

```
km_ngpl_sq_3592_d      km_tgp_sq_47799_d      km_tgp_sq_49524_d      km_tgp_sq_49861_d
```

Cycle-token distribution across all four meters:

```
best      4
evng     35
itrd1    31
itrd2    28
itrd3     5
timely   32
         ---
        135
```

So **99 rows change** (`evng` + `itrd1..3`), 32 `timely` rows are already canonical, and 4 `best`
rows keep their token — `best_available` is a real distinct state, not an alias for a NAESB cycle,
and V established it must survive because it is sometimes the only cycle for a gas day.

**Do the migration:**

1. `transformers/kinder_morgan.py:83` builds `km_{pipeline}_sq_{loc}_d_{cycle}` from a lowercased
   raw token. Add a normalisation map at that point — `evng → evening`, `itrd1 → id1`,
   `itrd2 → id2`, `itrd3 → id3` — leaving `timely` and `best_available` alone. Put the map next to
   the id construction with a comment naming KM's EBB as the source of the non-canonical tokens.
2. Write a one-shot migration script under `scripts/` that rewrites `series_id` **and**
   `series_name` in the existing parquet for the 99 affected rows. It must:
   - refuse to run if the row count before and after differ,
   - refuse to run if the set of `(meter, period)` pairs changes,
   - refuse to run if `value.sum()` moves by more than 1e-6,
   - print the before/after counts per token,
   - and write atomically.
   Run it. Paste the output.
3. **Do not remove the aliases from `CYCLE_PRIORITY` yet** — not in Python and not in the JS you
   just fixed in §02. Old raw files re-transformed would reintroduce the old tokens, and I want one
   full publish cycle of overlap. Leave a comment at both sites saying the aliases are retained for
   backward compatibility and can be dropped after `<date + 14 days>`.
4. `merge_into_curated` dedups on `(series_id, period)`. Say explicitly what happens on the **next
   scrape** after this migration: whether the newly-normalised ids collide correctly with the
   migrated rows, or whether you get parallel series. If you are not certain, say so — that
   uncertainty is worth more than a confident wrong answer, and I will hold the merge.
5. Confirm the shrinkage and accumulation guards do not fire. Run
   `python -m validators.run_integrity` and paste `kinder_morgan`'s line.

`config/meters/classification.json` is keyed on meter id, not series id — confirm that yourself
rather than taking my word, and report anything else keyed on the old tokens.

---

## 07 / W6 — TYPING, NARROWLY

`python -m mypy --strict scripts/ publishers/ validators/` reports **116 errors in 15 files**.
Almost all of it is out of scope. One piece is not:

```
scripts/preflight.py:190: error: Call to untyped function "load_terminal_history" in typed context
scripts/preflight.py:223: error: Call to untyped function "detect_events" in typed context
```

`scripts/task3_validate.py` is entirely untyped, and because `preflight.py` *is* typed, every call
across that boundary is an error. You are touching `task3_validate.py` anyway in §02.

Add type annotations to the functions `preflight.py` calls — at minimum `load_terminal_history`,
`detect_events`, `resolve_series`, `load_feed_daily` and `cycle_priority`. Do not annotate the
whole file, do not restructure anything, and do not add `# type: ignore`.

Report the mypy error count for `scripts/` before and after as exact integers. **Do not report a
number for the full 33-file run unless you actually ran it.**

---

## 08 / W7 — THE HISTORY REWRITE. WRITE IT, DO NOT RUN IT.

`.git` is **605 MB**. **506 objects** across all history reference `docs/data/`. The bleed is
stopped — `docs/data` now has exactly one tracked file, `manifest.json` — but the history is
untouched, so a fresh clone still pays for all of it.

Write me a script I can run outside any sandbox. It must:

- use `git filter-repo` (not `filter-branch`, not BFG), with the exact path globs;
- state precisely what survives — `docs/data/manifest.json` must, and nothing else under
  `docs/data` should;
- include the pre-flight safety steps: a full mirror backup first, and the command to verify the
  backup is complete before anything destructive runs;
- include the verification afterwards — object count, `.git` size, and a check that `main`'s tree
  at HEAD is byte-identical to before except for the removed paths;
- state what every other clone and worktree must do afterwards, and name the specific hazard that
  this repository has several worktrees under `.claude/worktrees/`;
- give me the expected `.git` size afterwards as a range, with your reasoning.

**Run none of it.** Not the backup, not `filter-repo`, not `git gc`. No git commands at all — see
ground rule 1. A script and a runbook, nothing more.

---

## 09 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`, not `gc`. This sandbox has
   destroyed this repository's `.git` twice. I commit and I merge.
2. **Every `docs/js` file you touch must be import-checked.** W0-e. This is not optional and it
   forfeits Stage 0.
3. **Prove every new test red before claiming it guards anything.** Scored.
4. **Never fabricate a number.** V's §06 row counts were invented and were caught in one command.
5. **A negative result is a valid result.** §04 asks whether the invariant test skips the new
   terminals; §06 asks what happens on the next scrape. "I could not determine this" scores.
   A confident wrong answer does not.
6. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion.
7. **Do not change any nameplate.** §03 is explicitly a decision for me, and the test it asks for is
   expected to be red on merge.
8. **Do not hand-edit curated parquet or health JSON** — except via the guarded, atomic migration
   script in §06, which is the one sanctioned exception in this brief and only for `series_id` and
   `series_name`.
9. **When a brief says delete, the artefact must be absent.** Not a tombstone.
10. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
11. **Never mix `_sq_` and `_oac_` in a flow total.** OAC is a residual (`capacity − TSQ`) and is
    anticorrelated with TSQ.
12. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`
    on every panel, 390px reflow.
13. Known pre-existing and **not yours**: the 17 residual ruff findings (E402 from deliberate
    `sys.path` setup, N806 from PascalCase mock classes), the ~35 mypy errors in
    `scrapers/base/playwright_client.py`, and mypy gaps in `transformers/baker_hughes.py`.
14. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what ran or why it could not.

---

## 10 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all five green, or zero** | **20** |
| W1 — one cycle definition, imported everywhere; JS learns the KM vocabulary | 15 |
| W1 — JS/Python ordering-parity test, proven red | 10 |
| W2 — Freeport nameplate evidence and both-ways delta; test written and left red | 10 |
| W3 — three `DOWNTIME_CONF` entries from the registry; inherited defaults flagged | 10 |
| W3 — honest answer on whether the invariant test covers them | 5 |
| W4 — `evidence.py` run, or honestly declared with `logs/` clean | 5 |
| W5 — migration executed with all four guards; integrity line pasted | 15 |
| W5 — next-scrape collision behaviour answered, or uncertainty declared | 5 |
| W6 — `task3_validate.py` annotated; `scripts/` mypy count before and after | 5 |
| W7 — rewrite runbook written, nothing executed | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. A `docs/js` file that does not
parse is an automatic zero on Stage 0.

---

## 11 / REPORT FORMAT

1. **Stage 0 table** — five rows: gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **`docs/js` files touched**, and the import-check output for each. If you touched none, say so.
3. **W1** — the grep of every cycle definition you found; the consolidation diff; the parity test
   and its red-before proof.
4. **W2** — the FERC evidence, the arithmetic both ways, the delta in percentage points, and
   confirmation the new test is red.
5. **W3** — the three entries; which values came from the registry; what the invariant test
   actually covers.
6. **W4** — did it run; the state of `logs/`.
7. **W5** — migration output, per-token before/after, the integrity line, the next-scrape answer.
8. **W6** — mypy `scripts/` count before and after.
9. **W7** — the runbook.
10. **Anything you noticed and did not fix.**
11. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
