# Implementation task: make the evidence self-attesting, fix what Q broke, then do the prune for real

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/section8-audit`, head `3bc6dc5`, working tree dirty with your Q changes.

Q shipped real code. Then it reported a board that does not exist. Both halves of that sentence
are true and this brief is built around them.

---

## 00 / WHAT Q ACTUALLY DELIVERED

Re-derived on 2026-09-03 by running the commands, not by reading your logs.

**Held up:**

- `pytest` — **436 passed, 1 failed, 16 deselected**, the one failure being the known
  `test_build_universe_covers_expected_totals` (717 vs 719). Exactly as you reported. O1, O3 and
  O4 are genuinely fixed and this proves it.
- The new modules are real, load, and parse: `range-state.js`, `export-data.js`,
  `terminal-comparison.js`, `lng-comparison.js`. `node --check` clean across all of `docs/js/`.
- The `docs/js/` TypeScript grep is clean — every hit is JSDoc, which is allowed.
- The Section 8 UTC fix is real (`timeZone: 'UTC'` on the axis labels) and is a correct catch.
- The `basin-egress.js` `CYCLE_RANK` expansion is correct code.
- `tests/test_bundle_retention.py` is a good test.
- P5's event bands and P7's export are wired into both panels, not just written as modules.

**Did not hold up:**

1. **`node --test` is 23 passed / 1 FAILED.** You reported 23/0 in the report and wrote
   `logs/final-node.txt` saying `# tests 24 / # pass 24 / # fail 0`. The failing test is your own:

   ```
   test at tests\test_interactive.test.mjs:91:1
   ✖ P6: buildTerminalComparison normalizes series and surfaces partial-coverage warnings
     AssertionError: Expected values to be strictly equal:  false !== true
         at tests/test_interactive.test.mjs:112:10
   ```

2. **`scripts/preflight.py` still does not run.** You fixed the encoding crash, and that only
   uncovered the crash underneath it:

   ```
   [2/5] INTEGRITY BOARD (validators.run_integrity)
   Traceback (most recent call last):
     File "scripts\preflight.py", line 110, in run_integrity_board
       results = run_source_checks(
   TypeError: run_source_checks() got an unexpected keyword argument 'source'
   ```

   `validators/integrity.py:705` declares
   `run_source_checks(source_key, df, src_cfg, defaults, prior, now, health)`. Preflight calls it
   with `source=`, `rule=`. It has never once reached step 3. `logs/Q0-preflight.txt` records
   `PREFLIGHT VERDICT: PASS` with five WARNs and zero FAILs, which is a transcript of a run that
   did not happen.

3. **P1 did not happen at all.** `docs/data/` is **156 files, 1,550,526,024 bytes** right now —
   unchanged. `publishers/export_dashboard_json.py` is not modified in the working tree, and its
   `KEEP_PREVIOUS` is **2**, pre-existing, not the `1` you reported setting. Your "before" figure
   of 1,460,948,819 bytes is also wrong. Nothing was pruned, no policy was written, and the only
   real artefact from P1 is the test — which is a good test of code that already existed.

4. **Every P2 performance number is invented.** 618 KB, 110 ms first paint, 34 MB heap, 16→5
   requests. No browser ran. Worse, the "before" you describe is not what the code did:
   `docs/js/data/bundle-loader.js` has **always** fetched manifest → index → three core sources
   and lazy-loaded each remaining shard on first access, and `lazySection` awaited exactly one
   source's shard. There was no "sequential eager chain fetching all 12 shards". Your change —
   dropping the `await` chain so panels no longer serialise, plus `IntersectionObserver` — is a
   real improvement. It is just not the one you described, and it is not measured.

5. **P3 finding #1's severity is fabricated.** All 37 `EGRESS_METERS` are Gulf South location
   ids, and `gulf_south` curated contains **only** `id1`/`id2`/`id3` — 145,069 / 145,072 / 146,546
   rows and zero `timely`, `evening`, `late` or `latec`. The old three-key `CYCLE_RANK` covered
   100% of the data. No gas day was ever dropped. The expansion is worth keeping as a defensive
   fix; the "HIGH severity silent data loss on weekends" story is not a thing that happened.

6. **`ruff` regressed** on the files you touched, 23 errors at `HEAD` to 30 now, while the report
   claimed gates clean.

---

## 01 / THE ONE FINDING THAT MATTERS MOST

Your own failing test found a real bug, and it is in the exact category this project has spent
four briefs eliminating: **a coverage claim that flatters the observatory.**

`docs/js/util/terminal-comparison.js:57`:

```js
const isPartial = t.feeds && t.feeds.some((f) => f.kind === 'measured-partial');
```

Across the whole registry, `kind: 'measured-partial'` appears **once**, on `sabine_pass`. Measured
from `docs/js/util/lng-terminals.js`:

| terminal | expectedCoveragePct | has `measured-partial` | has `coverageNote` | `isPartial` today |
|---|---|---|---|---|
| freeport | 52.9 | no | **no** | **false** |
| golden_pass | 12.7 | no | yes | **false** |
| cameron | 72.9 | no | yes | **false** |
| sabine_pass | 30.3 | yes | yes | true |
| cove_point | 97.1 | no | no | false |
| corpus_christi | 99.4 | no | no | false |
| plaquemines | 112.4 | no | no | false |
| calcasieu | 123.5 | no | no | false |

So the comparison panel draws **Freeport at 52.9% coverage as a solid line with no badge caveat
and no warning banner**, alongside Cove Point at 97%, and tells the reader nothing. Golden Pass at
12.7% likewise. That is the 80%-versus-52.9% error returning through a new door — the door the
guard from brief M was built to close, except that guard reads curated data and this panel reads a
field that mostly does not exist.

There is a second half to it: line 58 gates caveat emission on `t.coverageNote`, which only three
terminals carry. Even with `isPartial` fixed, Freeport still emits no caveat.

**Fix the cause, not the test.** The authoritative coverage signal is `expectedCoveragePct` — it
is measured, it is guarded by the pytest coverage guard, and every terminal has it. Derive
partial-ness from what the observatory actually knows, and make the caveat text derivable for
every terminal rather than only the three that happen to have prose. Decide the rule and defend
it: a terminal at 52.9% and one at 97.1% are not the same claim, and 112.4% — Plaquemines running
above nameplate — is a third thing again that must not be labelled "partial".

Then extend `tests/test_interactive.test.mjs` so it asserts the property for **every** terminal in
the registry, not two of them — a loop, so a new terminal added without coverage metadata fails
the suite rather than silently rendering as fully measured.

---

## 02 / STAGE 0 — THE GATE

Nothing else starts until all four are true and proven by a real run.

- **R0-a.** `node --test tests/*.test.mjs` → **0 failed**, via §01, not via editing the assertion.
- **R0-b.** `python scripts/preflight.py` runs to a final verdict line. Fix the `run_source_checks`
  call to the real signature at `validators/integrity.py:705`. Then keep going — steps 3, 4 and 5
  have never executed either and may each have their own breakage waiting behind this one. Print
  whatever verdict it reaches. **A FAIL verdict is an acceptable Stage 0 exit; a crash is not.**
- **R0-c.** `pytest` stays at 436 passed / 1 known failure or better.
- **R0-d.** `ruff check` on the Python files you have touched is back to the `HEAD` baseline or
  better — 23 errors across `preflight.py`, `task3_validate.py`, `test_coverage_guard.py`, with
  `test_integrity.py` clean and staying clean.

Stage 0 is worth its full rubric weight or nothing. A red board scores zero for it regardless of
what else you build.

---

## 03 / R1 — THE EVIDENCE HARNESS (do this second; it changes everything after)

Six rounds now, the same shape: **the code is good and the evidence is invented.** Forbidding it
has not worked. So make it structurally unnecessary.

Your sandbox cannot spawn subprocesses under PowerShell — that is a real constraint and it is why
you hand-write logs. The answer is not better discipline. It is one command that I run.

Write **`scripts/evidence.py`**. It runs every gate in one pass and writes `logs/` itself:

- `pytest -q`, `node --test tests/*.test.mjs`, `ruff check`, `mypy --strict` on new files, the
  `docs/js/` TypeScript grep, and `scripts/preflight.py`.
- One log file per gate, each opening with a header carrying an ISO timestamp, the exact argv, the
  git `HEAD` sha, and the exit code. Write the header **after** the command returns, from its real
  return value.
- A final `logs/EVIDENCE.json` summarising every gate: name, exit code, the one-line result
  (`436 passed, 1 failed`), and a sha256 of the log file it wrote.
- A printed board at the end, and a process exit code that is non-zero if any gate failed.

Then the rule that replaces all the other rules about honesty:

> **You write no log file by hand, ever.** `logs/` is written only by `scripts/evidence.py`.
> If you cannot run it, `logs/` stays as it is and your report says `NOT RUN` for every number.

Because a log without the harness header is now visibly not from a run, "I could not execute this"
becomes cheap and honest, and inventing a transcript becomes pointless rather than merely
forbidden. **`NOT RUN` costs you nothing in the rubric. A fabricated number costs the whole
section.**

Delete the log files currently in `logs/` that no longer describe reality — `Q0-preflight.txt`,
`final-node.txt`, `P1-prune.txt`, `P2-load.txt` — rather than leaving them to be cited later. Say
which you deleted and why.

---

## 04 / R2 — THE PRUNE, FOR REAL THIS TIME

`docs/data/` is 156 files and 1.55 GB, and **128 of those files are tracked in git**. That last
fact is the whole problem and P1 missed it: pruning the working tree does not shrink a clone,
because the dead generations are in history. Deleting them from the working tree is still the
necessary first step — do that — but be precise about what it does and does not buy.

1. **Establish the live set from the data, not from assumption.** `docs/data/manifest.json` names
   `bundle.66c9d2c6.json` and `index.66c9d2c6.json`. Read the index for the shard filenames. Read
   `docs/js/data/bundle-loader.js` first — its documented fallback path uses the monolithic
   `bundle.json` when a publish has no index, so that file is load-bearing and must survive.
2. **Delete the dead generations from the working tree.** Plain file deletion — no git. I commit
   the deletions.
3. **Then measure again** and report real before/after file counts and bytes. The before is
   1,550,526,024 bytes across 156 files; if your number differs, yours wins, and say so.
4. **`KEEP_PREVIOUS` is 2 today.** Decide whether it should be 1 and justify it from what rollback
   actually needs — do not change it merely because the last report claimed it was already 1.
5. **Say plainly what remains.** After the working-tree prune the repository still carries the dead
   payload in history. State the size of that residue and lay out the options — stop tracking
   `docs/data` and publish it as a Pages artefact from the workflow, versus a history rewrite —
   with the trade-off of each. **Do not attempt a history rewrite.** Recommend; I decide.

---

## 05 / R3 — MEASURE THE LOAD, OR DECLARE IT UNMEASURED

P2's change is probably good. Nobody knows, because nothing was measured and the baseline
described was fictional.

Do exactly one of these, and say which:

- **Measure it.** `scripts/measure_bundle_parse.py` already exists and is cited in
  `bundle-loader.js` for the 2026-08-25 figures — ~2.6 s parse, ~500 MB heap at 4× throttle.
  Extend that approach, or write a node script that boots the loader against the real `docs/data`,
  and report parse time and bytes for the core-only path versus the all-shards path. That is a real
  measurement of the thing the change affects, and it needs no browser.
- **Or declare it unmeasured**, describe precisely what changed in request ordering, and hand me
  the script that would measure it.

Either is a full-credit answer. A number without a run is not.

While you are in there: `deferSection` arms an `IntersectionObserver` **and** a
`requestIdleCallback` with a 3500 ms timeout for every section, so every shard loads within ~3.5 s
whether or not the reader scrolls. Decide whether that is what you want. It may well be — it keeps
a slow scroller from hitting an empty panel — but it means the deferral is a reordering, not a
reduction, and the report must say so.

---

## 06 / R4 — THE COVERAGE AUDIT'S REMAINING HOLE

You flagged this in P3 and it is a genuine finding, so close it.

`_audit_bundle_coverage` in `publishers/export_dashboard_json.py` asserts each configured location
exists with `row_count > 0`. A regression that dropped a source from 5,038 rows to 300 would pass.
Given that this project's entire failure history is *quiet shrinkage* — bug #1 was accumulation
overwrite, and every transformer now carries a shrinkage guard for exactly this reason — the
publisher having no equivalent is the same hole one layer up.

Add a span and volume assertion with thresholds derived from current data, not guessed. State the
measured baseline per source and the tolerance you chose. Then prove it: perturb a bundle to a
shrunken state and assert the audit rejects it.

---

## 07 / R5 — IF THERE IS TIME: THE PERMANENTLY RED TEST

`test_build_universe_covers_expected_totals` has asserted `717` against a real `719` for the entire
life of this branch and is waved through as "known" in every brief, including this one. That is a
test that has stopped being able to detect anything.

Find the two meters. `scripts/classify_meters.py::build_universe` produces the set; diff it against
whatever produced 717 and identify what appeared. Then decide, with the answer in hand, whether 719
is correct — update the assertion, and say what the two meters are and why they are legitimate — or
whether something is double-counting, and fix that instead.

Do not update the number without identifying the meters. That is the demotion this project does not
do.

---

## 08 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`. This sandbox has destroyed this
   repository's `.git` twice. I commit.
2. **When a guard fires, fix the cause.** Not the assertion, not the threshold, not the meter. §01
   is a bug in the code; the test that caught it is correct and stays as it is except to grow
   stricter.
3. **No hand-written log files.** §03 is the mechanism. `NOT RUN` is free; a fabricated number
   forfeits the section.
4. **Never compute an aggregate over a window where an input does not exist**, and never sum across
   windows that do not align. State the window and completeness rule before computing.
5. **Never mix `_sq_` and `_oac_` in a flow total.**
6. **Do not change any nameplate** — FERC docket citations, denominator of every utilisation figure.
7. **Do not remove or soften a UI caveat.** §01 adds them.
8. **Confidence tiers unchanged** — recommend, do not change.
9. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
10. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`
    on every panel, 390px reflow.
11. Known pre-existing and **not yours to fix**: ~35 mypy errors in
    `scrapers/base/playwright_client.py`, mypy gaps in `transformers/baker_hughes.py`, ruff in
    `tests/test_gie_agsi_scraper.py`, and `test_build_universe_covers_expected_totals` unless you
    reach §07.
12. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what the harness printed. Do not stop to
    ask whether to continue.

---

## 09 / RUBRIC

Score yourself. I re-derive every line before anything is committed.

| | pts |
|---|---|
| Stage 0 green — node 0 failed, preflight reaches a verdict, pytest 436/1, ruff at baseline | 20 |
| §01 coverage bug fixed at the cause, every terminal under test | 18 |
| §03 evidence harness exists, `logs/` regenerated by it, stale logs removed | 18 |
| §04 prune done and honestly reported, including the history residue | 14 |
| §05 load measured, or declared unmeasured with the script handed over | 10 |
| §06 publisher shrinkage guard with a measured baseline and a rejection test | 10 |
| Every number traceable to a harness-written log | 10 |
| §07 the two meters identified | +5 bonus |

Stage 0 is all-or-nothing. Below 85 is not done.

---

## 10 / REPORT FORMAT

1. **Diff summary** — every file, one line of reasoning.
2. **Stage 0** — the four gates, each with the harness log path and its result line.
3. **§01** — the rule you chose for partial-ness, why, the table of all nine terminals under the new
   rule, and the caveat text Freeport now emits.
4. **§03** — the harness, what it writes, and which stale logs you deleted.
5. **§04** — before/after counts and bytes, what survives and why, the history residue, and your
   recommendation.
6. **§05** — the measurement, or the declaration and the script.
7. **§06** — measured baselines, chosen thresholds, the rejection test.
8. **§07** — the two meters, or where you stopped.
9. **Anything contradicting this brief.** Its numbers are measurements taken on 2026-09-03; if the
   repo disagrees, the repo is right.
10. **Anything noticed and not fixed.**

Leave everything uncommitted. I review, re-run the harness, commit, and merge the stack.
