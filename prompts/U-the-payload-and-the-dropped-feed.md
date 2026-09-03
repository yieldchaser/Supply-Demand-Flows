# U — The Payload, and the Feed Nobody Noticed Was Gone

Branch: cut a new one from `main` at `13b3cca`. `fix/section8-audit` is merged and deleted. I commit.

This brief is wider than S and T. That is deliberate and it comes with a warning: every long brief
before S produced a self-scored 100/100 report full of invented numbers, and both short ones came
back clean. The scope below is wide but **every item is already diagnosed** — you are not being
asked to investigate anything except §04, which has an explicit expected answer. If you find
yourself exploring, you have misread the brief.

**How this brief is scored, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty and keeps the section's code weight. A
> fabricated number forfeits the section *and* Stage 0.

S and T both reported every gate as `NOT RUN` and invented nothing. Both were verified green on the
host. That is the standard now. Do not regress to describing outcomes you did not observe.

---

## 00 / WHERE THINGS STAND

`fix/section8-audit` merged to `main` as `13b3cca` and deployed. Verified on the host at merge:

```
node --test            25 passed / 0 failed
pytest                 442 passed / 0 failed / 16 deselected
scripts/preflight.py   PREFLIGHT VERDICT: PASS, exit 0
integrity board        WARN, zero FAILs
```

Your T fix is correct and live. `eia_storage` went FAIL → WARN. The site now serves
`isTerminalPartial`, `lng-comparison.js`, `range-state.js` and `export-data.js`.

One defect you shipped green, and it is the second round in a row: **your T1 regression test tested
nothing.** It set `prior = {"rows": 3608}` against a two-row frame, so the flat arm's
`int(len(df)) == rows_prior` gate never opened and the test passed against the *unfixed* code. I
caught it by reverting line 641 and re-running. It is fixed now. The rule that follows is in §07:
prove every new test red before you claim it guards anything.

---

## 01 / STAGE 0 — THE GATE

All-or-nothing. These must be green when you finish, and none of them are broken right now — this
is a "do not regress" gate, not a repair job.

| Gate | Command |
|---|---|
| S0-a | `node --test tests/*.test.mjs` |
| S0-b | `python -m pytest -q -m "not network"` |
| S0-c | `python scripts/preflight.py` — must reach `PREFLIGHT VERDICT:` and exit 0 |
| S0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` — see §06 for the target |

---

## 02 / U1 — THE PAYLOAD. THIS IS THE MAIN EVENT.

**It got worse while we were fixing other things.** Measured on the host just now, on `main`:

| | Yesterday | Now |
|---|---|---|
| Files in `docs/data/` | 156 | **324** |
| Bytes | 1,550,526,024 | **3,202,472,522** |
| Hash generations | 11 | **23** |
| Tracked in git | 128 | **296** |
| `.git` directory | — | **596 MB** |
| Objects referencing `docs/data/` across all history | — | **502** |

It doubled in a day. Live hash per `docs/data/manifest.json` is now `f6f046a0`.

**The cause is one line.** `.gitignore:38-47` is already correct — it ignores `docs/data/*.json`
and negates only `bundle.json` and `manifest.json`. But
`.github/workflows/publish-dashboard.yml:89` overrides it:

```
git add -f docs/data/bundle.*.json docs/data/src.*.json docs/data/index.*.json 2>/dev/null || true
```

Every publish force-adds a full generation into the repository. Nothing ever removes them.

Three pieces of work, in this order:

### U1-a — stop the bleeding

Delete that `git add -f` line. Keep line 88 (`git add docs/data/bundle.json docs/data/manifest.json`)
— those two are load-bearing and must stay tracked, because `docs/js/data/bundle-loader.js:114`
falls back to the monolithic `bundle.json` when a publish has no index.

### U1-b — publish `docs/` as a Pages artefact instead of committing it

You wrote this plan in S §5 and it is right. Implement it now: `actions/upload-pages-artifact@v3`
with `path: docs/`, then `actions/deploy-pages@v4`, with `pages: write` and `id-token: write`
added to the job's permissions. Note that a `pages build and deployment` run already fires on push
to `main` today — say in your report whether your change replaces that path or duplicates it, and
if it duplicates, which one wins.

### U1-c — run the prune, for real

`scripts/prune_bundles.py` exists and has never executed. Run it against the real `docs/data/`.
Report file count and bytes **before and after, from an actual listing**, plus the hashes retained.

`_prune_stale_bundles` keeps `current_hash` plus the `KEEP_PREVIOUS` (2) most-recently-modified
other hashes. `manifest.json` and `bundle.json` are never pruned — `tests/test_bundle_retention.py`
already asserts that; keep it green.

### What you must NOT do

**Do not attempt a history rewrite.** Not `filter-repo`, not `filter-branch`, not BFG. Deleting
296 tracked files from the working tree does not shrink a clone — those 502 objects live in
`.git/objects` and only a rewrite removes them. That rewrite is my decision and my risk, and this
sandbox has destroyed this repository's `.git` twice. Write me the plan, including whether
`git gc --aggressive --prune=now` alone recovers anything meaningful after the untracking, and stop
there.

---

## 03 / U2 — RUN THE HARNESS

`scripts/evidence.py` was written in R, hardened in S, and **has still never executed**. There is no
`logs/EVIDENCE.json` in this repository and there never has been.

If you can spawn processes: run it, and let it produce the Stage 0 logs for this brief. That is the
whole point of the thing.

If you cannot: say so, and instead make it *provably correct* without running it — walk each gate
definition against the command it claims to run and confirm the argv is right, the log path exists,
and the `not_run` path writes a real header. Report any gate whose command would fail for a reason
other than spawning, e.g. a path that has moved since R was written.

Either way, `logs/` should end this brief containing only files a run produced. Anything else,
delete — **absent, not a tombstone**. You have now been told this twice.

---

## 04 / U3 — A FEED HAS BEEN SILENTLY DROPPED SINCE THE DAY IT WAS ADDED

This is the one genuine discovery in this brief and it is the kind of bug this project exists to
catch: a configured meter that resolves to nothing, and warns about it in a way everyone learned to
ignore.

`python scripts/preflight.py` prints, twice per run:

```
WARN: no parquet for km_ngpl_sq_3592_d
```

That feed is Sabine Pass's second leg — `scripts/task3_validate.py:48` lists
`"feeds": ["creole_trail_sq_CT200111_d", "km_ngpl_sq_3592_d"]`.

**The data exists.** `data/curated/kinder_morgan.parquet` holds 33 rows of it, periods
`2026-08-25` → `2026-09-03`, across six series ids:

```
km_ngpl_sq_3592_d_best    1 row
km_ngpl_sq_3592_d_evng    8 rows
km_ngpl_sq_3592_d_itrd1   7 rows
km_ngpl_sq_3592_d_itrd2   7 rows
km_ngpl_sq_3592_d_itrd3   2 rows
km_ngpl_sq_3592_d_timely  8 rows
```

**Two stacked defects keep it invisible.**

1. `resolve_series()` at `scripts/task3_validate.py:86` maps a feed id to a parquet via
   `PREFIX_MAP`. That map has a `"kinder_morgan"` key, but the feed id starts with `km_`. No prefix
   matches, so it returns `(None, None)` and prints the WARN. **`PREFIX_MAP` needs a `"km"` entry.**
2. Fixing that alone is not enough. `load_feed_daily` filters on `cycle_priority(cycle) > 0`, and
   `CYCLE_PRIORITY` at `scripts/task3_validate.py` knows only
   `timely / evening / late / latec / id1 / id2 / id3`. Kinder Morgan's own EBB vocabulary is
   different: `transformers/kinder_morgan.py:83` builds `km_{pipeline}_sq_{loc}_d_{cycle}` from the
   scraper's pinned `TIMELY/EVNG/ITRD1-3`, lowercased, with `best_available` as the fallback. So
   `evng`, `itrd1`, `itrd2`, `itrd3` and `best` all score 0 and are dropped. Only `_timely` would
   survive.

Decide and justify:

- **(a)** Normalise at the transformer: map `EVNG → evening`, `ITRD1-3 → id1-3` when building the
  series id, so KM obeys the canonical `{prefix}_{sq}_{loc}_{flow}_{cycle}` vocabulary the rest of
  the project uses. Correct, but it renames existing series ids and is therefore a data migration —
  if you pick this, say exactly what has to be backfilled and do **not** perform it.
- **(b)** Teach `cycle_priority()` the KM aliases, leaving stored ids alone.

I lean (b) for this brief and (a) as a follow-up, but argue it.

**The expected outcome, stated so you cannot inflate it:** I already checked, and
**every one of those 33 rows has a median value of 0.0 Dth across every cycle.** Fixing the resolver
will therefore **not** change Sabine Pass's 30.3% coverage figure, and it must not. If your change
moves that number, you have broken something — stop and report it rather than updating the
registry.

What the fix buys is that a configured feed stops silently resolving to nothing, and that
`km_ngpl_sq_3592_d` becomes visible as **posting zero** rather than **not posting at all** — a
distinction Section 8's detector is built entirely around, and which brief L's coverage work
depended on being correct.

Add a test that pins the resolution: `resolve_series("km_ngpl_sq_3592_d")` returns the
`kinder_morgan` parquet path, and `load_feed_daily` for that feed returns a non-empty dict. Prove
it red before your fix.

---

## 05 / U4 — `status: "ok"` DOES NOT MEAN WHAT IT SAYS

From T §02, established and not in dispute: `data/raw/` is gitignored and zero raw files are
tracked, so every CI run starts with an empty raw directory, `_get_latest_local_date()` returns
`None`, and the staleness skip gate in `scrapers/eia_api/storage.py` **cannot fire in CI**.
`record_skipped()` is unreachable there. Every scheduled run does a full 8-year, 5,000-row refetch
and ends in `record_success()` — three times a week, on a weekly series.

The consequence is the part that matters: **`status: "ok"` in `data/health/eia_storage.json` means
"the fetch completed", not "the dataset advanced."** The divergence check exists precisely to catch
a health file that says ok while data rots, and it is being asked to reason about a stamp that
cannot say anything else.

Do two things:

1. Make the health record distinguish the two. When a run completes but the newest period is
   unchanged from what curated already holds, that is a no-op, not a success — `HealthWriter` already
   has `record_no_op()` for exactly this (see its docstring, which contrasts it with
   `record_skipped()`). Wire it up. Do **not** change what `record_success()` means for other
   sources, and do **not** touch the cron cadence.
2. Write down the CI-empty-raw-directory behaviour as a comment where the gate lives, so the next
   person does not spend an hour rediscovering that the gate is dead code in the environment that
   matters.

`tests/test_eia_storage_scraper.py` must stay green and should gain a case for the no-op path.

---

## 06 / U5 — RUFF

`ruff check scripts/ tests/ publishers/ validators/ scrapers/` is **58 errors**. Breakdown:

```
15  W293    blank-line-with-whitespace      [auto]
10  E402    module-import-not-at-top-of-file
 7  I001    unsorted-imports                [auto]
 7  N806    non-lowercase-variable-in-function
 4  F401    unused-import                   [auto]
 3  F541    f-string-missing-placeholders   [auto]
 2  B007    unused-loop-control-variable
 2  SIM105  suppressible-exception          [auto]
 2  SIM117  multiple-with-statements
 2  F841    unused-variable                 [auto]
 1  B905 / E701 / W291 / W292               [mostly auto]
```

33 of the 58 are auto-fixable. Run `ruff check --fix` across those paths, then look at what it
changed before you accept it — `--fix` is not a licence to stop reading diffs. Target **≤ 25**.

Do **not** use `--unsafe-fixes`. Do not silence `E402` or `N806` with `noqa` — leave what you cannot
fix cleanly and report the residual count with a one-line reason per remaining rule.

---

## 07 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`. This sandbox has destroyed this
   repository's `.git` twice. I commit and I merge.
2. **Prove every new test red before you claim it guards anything.** Make a temporary copy of the
   file you fixed, revert the fix in the copy, run the test, confirm it FAILS, restore byte-for-byte,
   and confirm the restore. Report the failing output. This is now a scored line, not advice — two
   briefs running, you have shipped a test that passed against the unfixed code.
3. **Never fabricate a number.** See the scoring rule at the top.
4. **A negative result is a valid result.** §04 has a stated expected outcome of "no change to the
   coverage figure". Confirming that is worth full marks.
5. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion.
6. **When a brief says delete, the artefact must be absent.** Not a tombstone. Third time.
7. **No history rewrite.** §02 is explicit.
8. **Do not change any nameplate**, and do not update `expectedCoveragePct` in
   `config/terminals_registry.json` or `docs/js/util/lng-terminals.js` in this brief.
9. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
10. **Never mix `_sq_` and `_oac_` in a flow total.** OAC is a residual (`capacity − TSQ`).
11. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`,
    390px reflow.
12. Known pre-existing and **not yours**: ~35 mypy errors in `scrapers/base/playwright_client.py`,
    mypy gaps in `transformers/baker_hughes.py`, ruff in `tests/test_gie_agsi_scraper.py`.
13. **Deliberately out of scope:** extending `scripts/task3_validate.py`'s `TERMINALS` from 4 to all
    9 terminals. That means inventing `zero_mode` and threshold semantics for five facilities and it
    gets its own brief. Preflight's `SKIP:` lines for them are correct behaviour — leave them.
14. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what ran or why it could not.

---

## 08 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all four green, or zero** | **20** |
| U1-a — the `git add -f` line gone, `bundle.json` + `manifest.json` still tracked | 10 |
| U1-b — Pages artefact deploy wired, and the duplicate-path question answered | 10 |
| U1-c — prune actually run, before/after from a real listing | 15 |
| U1 — history-rewrite plan written and **not** executed | 5 |
| U2 — `evidence.py` run, or provably audited and honestly declared | 10 |
| U3 — feed resolves; cycle option chosen and argued; test proven red first | 20 |
| U3 — Sabine's 30.3% confirmed **unchanged** | 5 |
| U4 — no-op vs success distinguished; CI behaviour documented in place | 10 |
| U5 — ruff ≤ 25, exact integer, residuals explained | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. A new test that has not been
proven red forfeits its section.

---

## 09 / REPORT FORMAT

1. **Stage 0 table** — gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **U1** — before/after file counts and bytes from a real listing; hashes retained; the workflow
   diff; the duplicate-deploy answer; the history-rewrite plan as a proposal.
3. **U2** — did `evidence.py` run; if not, the per-gate audit.
4. **U3** — decision (a) or (b) with the argument; the red-before proof; Sabine's figure before and
   after, which must match.
5. **U4** — the no-op wiring and where you documented the CI behaviour.
6. **U5** — ruff before and after as exact integers, residual rules with one-line reasons.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
