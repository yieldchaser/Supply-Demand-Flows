# V — The 68 MB Nobody Loads

Branch: cut a new one from `main` at `d0fba7e`. I commit and I merge.

Five items. Every one is diagnosed below; none require investigation except §04, which states what
it expects you to find. If you are exploring, you have misread the brief.

**How this brief is scored, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty and keeps the section's code weight. A
> fabricated number forfeits the section *and* Stage 0.
>
> **A new test that has not been proven red forfeits its section.** Revert the fix in a temporary
> copy, run the test, paste the failing output, restore byte-for-byte, confirm the restore.

---

## 00 / WHERE THINGS STAND

Since U, on `main`:

```
node --test            25 passed / 0 failed
pytest                 445 passed / 0 failed / 16 deselected
scripts/preflight.py   PREFLIGHT VERDICT: PASS, exit 0
ruff (wide)            17  — E402 and N806 only, both deliberate
```

The Pages source is now **GitHub Actions**, not legacy branch-serving. `publish-dashboard.yml`
uploads `docs/` as an artefact and `deploy-pages@v4` deploys it. The 294 cache-busted shards are
untracked (`d0fba7e`) and the prune has run: `docs/data` went **324 files / 3,054 MB → 30 files /
328 MB**. Verified live — generation `80a11641` serves `index`, `src.gulf_south`, `src.eia_storage`
all 200, from a runner whose checkout contained zero tracked shards.

Your U3 fix was correct and its red-before proof was real — the first time. Two things in that
report were not: **ruff was never actually reduced** (claimed 58 → 25 with an invented residual
table; it was 55, because `--fix` had not been run), and **your own
`test_sabine_pass_coverage_unchanged_with_km_feed` FAILED** while reported green — exact float
equality (`== 30.3`) against `30.33688888888889`. Both repaired.

---

## 01 / STAGE 0 — DO NOT REGRESS

All-or-nothing. Nothing here is broken; keep it that way.

| Gate | Command |
|---|---|
| V0-a | `node --test tests/*.test.mjs` |
| V0-b | `python -m pytest -q -m "not network"` |
| V0-c | `python scripts/preflight.py` — must reach `PREFLIGHT VERDICT:` and exit 0 |
| V0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` — must stay ≤ 17 |

---

## 02 / V1 — THE HEADLINE: A 68 MB FILE THAT NOTHING FETCHES

Untracking the shards stopped most of the growth. It did not stop this.

`docs/data/bundle.json` is **68,871,785 bytes**, it is tracked, and
`.github/workflows/publish-dashboard.yml` commits it on **every publish**:

```
git add docs/data/bundle.json docs/data/manifest.json
```

Measured on the host: **174 commits touch it, and all 174 carry a distinct blob.** It is the
single largest tracked file in the repository and it churns completely several times a day. After
the shard untracking, it is now the dominant remaining contributor to the 596 MB `.git`.

**Nothing loads it.** I read every path in `docs/js/data/bundle-loader.js`:

- Line ~100, the no-index legacy branch, fetches `` `./data/${manifest.bundle_url}` ``.
- Line ~118, the index-fetch-failed fallback, fetches `` `./data/${manifest.bundle_url}` ``.

and `publishers/export_dashboard_json.py:545` and `:559` both set
`"bundle_url": hashed_name` — the **hashed** `bundle.{hash}.json`, never the plain one. The comment
at `bundle-loader.js:114` claiming it falls back to "the monolithic bundle.json, which is always
tracked + served" is simply wrong about which file it fetches.

`grep -rn "bundle\.json"` across `docs/js`, `docs/index.html`, `publishers/` and `tests/` returns
exactly four hits: that incorrect comment, the exporter's module docstring, and the two exporter
lines that write the file.

**What to do:**

1. Stop committing it. Remove `docs/data/bundle.json` from the `git add` line in
   `publish-dashboard.yml`, leaving `manifest.json` — which is genuinely load-bearing, since it is
   the only thing the loader fetches by a fixed name.
2. `docs/data/bundle.json` must leave the git index. **You cannot run git commands** — so instead,
   state clearly in your report that `git rm --cached docs/data/bundle.json` is required and that
   I must run it. Do not attempt it.
3. Keep **writing** the file — `export_dashboard_json.py:513` stays. It costs nothing on the runner,
   it lands in the Pages artefact, and it keeps the hashed/unhashed pair consistent for anyone
   debugging a publish by hand.
4. Fix the wrong comment at `bundle-loader.js:114` to say what the code does: the fallback fetches
   the hashed bundle named by `manifest.bundle_url`, which ships in the Pages artefact.
5. `.gitignore` already ends with `!docs/data/bundle.json`. Remove that negation so the file stops
   being a tracking candidate at all. Leave `!docs/data/manifest.json`.

**Before you touch anything, confirm my claim rather than trusting it.** Grep for every reference
to `bundle.json` that is not `bundle.{hash}.json`, and read both fallback branches yourself. If you
find a live consumer I missed, **stop and report it** — do not proceed with the removal. A wrong
call here breaks the dashboard's only degradation path.

---

## 03 / V2 — THE FOUR SKIPPED TERMINALS

`scripts/preflight.py` step 5 currently prints:

```
SKIP: calcasieu (no coverage-history config in task3_validate.py)
SKIP: golden_pass (no coverage-history config in task3_validate.py)
SKIP: cameron (no coverage-history config in task3_validate.py)
SKIP: corpus_christi (no coverage-history config in task3_validate.py)
-> Coverage Guard: 5 passed, 4 skipped (WARN), 0 failed
```

That skip was the right call in S — it stopped a crash — but four of nine terminals have never been
coverage-guarded. `TERMINALS` in `scripts/task3_validate.py` holds only `freeport`, `cove_point`,
`sabine_pass`, `plaquemines`.

**This is less work than it looks.** `docs/js/util/lng-terminals.js` already carries, per terminal,
the fields that determine a feed: `source`, `seriesPrefix`, `loc`, `flow`. Cameron for example is
`source: 'gasnom'`, `seriesPrefix: 'cameron_interstate'`, `loc: '772300'`, `flow: 'd'`, and its
series id follows the canonical `{prefix}_sq_{loc}_{flow}` shape.

**I tried to extract all nine mechanically and my regex bled across entries, so I am deliberately
not giving you a table — it would be wrong.** Derive them properly, and before wiring anything:

1. Write a short script that reads the registry, derives each terminal's feed id(s), and for each
   one prints the id, the resolved parquet, the number of matching rows in curated, the period
   range, and the distinct cycle suffixes present.
2. **Paste that table in your report.** Any terminal whose feed resolves to zero rows does not get
   a `TERMINALS` entry — say so and leave it skipped. A guard over data that does not exist is
   worse than a skip.
3. Only then add entries for the terminals that resolve.

For `zero_mode` and the thresholds: **do not invent them.** Use the existing four as the precedent —
they all use `zero_days_threshold` 2–3, `depressed_pct` 0.60, `depressed_days` 5,
`is_cargo_zero` false, and `zero_mode` is `"normal"` except where a terminal has a known
single-visible-feed situation. Default new entries to `zero_mode: "normal"` and the same
thresholds, and say in your report that they are inherited defaults rather than derived values, so
I know they are unvalidated. If a terminal's data makes `"normal"` obviously wrong, say why and
leave it out.

`port_arthur` is `operational: false` and must stay out.

Expected outcome: the coverage guard's skip count drops and `preflight` still exits 0. If adding a
terminal makes the guard FAIL, that is a real finding — **report it, do not adjust the tolerance or
the registry to make it pass.**

---

## 04 / V3 — `best` IS NOT A NOMINATION CYCLE

U added Kinder Morgan's cycle aliases to `CYCLE_PRIORITY` in `scripts/task3_validate.py`. Four of
them are right. One is not:

```python
"best": 1,
```

`best_available` is the transformer's fallback when the scraper could not pin a real cycle
(`transformers/kinder_morgan.py:57`, `cycle = str(payload.get("cycle") or "best_available").lower()`).
It is a rollup, not a NAESB nomination cycle. Giving it priority 1 ties it with `timely`, and
`load_feed_daily` does `sort_values('prio', ascending=False).drop_duplicates(keep='first')` — so on
any period where both `_best` and `_timely` exist, **which one wins is arbitrary.**

This is the same class of error as the hourly `id{HH}00` placeholders that `cycle_priority()`
already returns 0 for, and for the same reason.

It does not currently bite `km_ngpl_sq_3592_d`, because every value there is 0.0 Dth. It may bite
the `km_tgp_*` meters, which carry real volumes.

**What I expect you to find, stated so you cannot inflate it:** `best` should score **0**, i.e. be
excluded exactly like `id{HH}00`, unless you can show a meter where `_best` is the *only* cycle
present for some period — in which case excluding it would drop real data and the right answer is a
priority below `timely` rather than 0.

So: check that first, on real data. Report, per KM meter, how many periods have `_best` and no
genuine cycle. Then pick 0 or a sub-`timely` rank based on what you found, and say which and why.
Add a test that pins the choice, proven red first.

---

## 05 / V4 — RUN THE HARNESS

`scripts/evidence.py` was written in R, hardened in S, audited in U, and **has never executed.**
There is still no `logs/EVIDENCE.json` in this repository.

If you can spawn processes, run it and let it produce this brief's Stage 0 logs. That is the entire
purpose of the file and it is the fifth brief in which it has not run.

If you cannot, say so plainly — that remains a full-marks answer — and leave `logs/` containing only
what a run produced. Anything else, delete. **Absent, not a tombstone.** You have been told this
three times.

---

## 06 / V5 — KM CANONICAL CYCLES: PLAN ONLY

In U you chose option (b), aliasing KM's tokens in `cycle_priority()`, and deferred option (a),
normalising `EVNG → evening` and `ITRD1-3 → id1-3` at the transformer. That was the right call for
that brief.

Write the migration plan now, and **do not execute any part of it**:

- Exactly which series ids in `data/curated/kinder_morgan.parquet` change, and how many rows.
- Whether the rename can be done by `merge_into_curated` on a re-transform of retained raw, or
  needs a dedicated one-shot migration.
- What happens to `config/meters/classification.json` and anything else keyed on the old ids.
- How the aliases added in U get removed afterwards without a window where both are wrong.
- Whether the shrinkage and accumulation guards would fire during the migration, and how to prove
  the row count is preserved across it.

A page is enough. This is a decision I will make from your plan.

---

## 07 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`. This sandbox has destroyed this
   repository's `.git` twice. I commit and I merge. §02 needs a `git rm --cached` — tell me, do not
   run it.
2. **Prove every new test red before claiming it guards anything.** Scored, not advice.
3. **Never fabricate a number.** Two of U's numbers were invented and both were caught within
   minutes by re-running the command.
4. **A negative result is a valid result.** §03 and §04 both have stated expected answers;
   confirming them earns full marks, and contradicting them with evidence earns more.
5. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion. §03 says this explicitly for the coverage guard.
6. **When a brief says delete, the artefact must be absent.** Fourth time.
7. **Do not change any nameplate**, and do not edit `expectedCoveragePct` or
   `coverageTolerancePct` in either the registry JSON or `docs/js/util/lng-terminals.js`.
8. **Do not hand-edit curated parquet or health JSON.** Ever.
9. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
10. **Never mix `_sq_` and `_oac_` in a flow total.** OAC is a residual (`capacity − TSQ`).
11. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`,
    390px reflow.
12. **No history rewrite**, and no `git gc`. Mine to run, outside any sandbox.
13. Known pre-existing and **not yours**: the 17 residual ruff findings (E402 from deliberate
    `sys.path` setup, N806 from PascalCase mock classes), ~35 mypy errors in
    `scrapers/base/playwright_client.py`, mypy gaps in `transformers/baker_hughes.py`.
14. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what ran or why it could not.

---

## 08 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — all four green, or zero** | **20** |
| V1 — my "nothing loads it" claim independently confirmed, or refuted with evidence | 10 |
| V1 — workflow and `.gitignore` changed; comment corrected; `git rm --cached` reported not run | 15 |
| V2 — the derived feed table, with row counts and cycle suffixes, pasted in the report | 15 |
| V2 — `TERMINALS` extended only for terminals that resolve; defaults flagged as inherited | 15 |
| V3 — `_best`-only periods counted on real data; choice made and justified; test proven red | 15 |
| V4 — `evidence.py` run, or honestly declared with `logs/` left clean | 5 |
| V5 — migration plan written and **nothing executed** | 5 |

Below 85 is not done. One fabricated number caps the brief at 50.

---

## 09 / REPORT FORMAT

1. **Stage 0 table** — gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **V1** — what your own grep found; the diffs; the exact `git rm --cached` command for me to run.
3. **V2** — the derived feed table; which terminals you added and which you left skipped and why;
   the new coverage-guard line from preflight.
4. **V3** — the per-meter `_best`-only period counts; your choice; the red-before proof.
5. **V4** — did it run; if not, the state of `logs/`.
6. **V5** — the migration plan.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
