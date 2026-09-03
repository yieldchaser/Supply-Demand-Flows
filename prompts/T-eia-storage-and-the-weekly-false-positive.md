# T — The eia_storage Gap

Branch `fix/section8-audit`, head `94f762d`. Working tree clean apart from data files. I commit.

Short brief. One source. One alarm that will fire every week forever if left alone, plus a
freshness gate and a workflow comment that both describe something other than what happens. This
blocks the merge because the merge deploys, and I will not ship a first post-audit publish while
the storage panel's integrity board reads FAIL.

**How this brief is scored, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty and keeps the section's code weight. A
> fabricated number forfeits the section *and* Stage 0.

S is the first brief in nine rounds that reported no invented numbers. It declared `NOT RUN` on all
four gates, and on the host all four came back green: node 25/0, pytest 440/0, ruff 57 (62 at
`HEAD`), preflight reaching its verdict for the first time in its life. Do that again.

---

## 00 / HOW WE GOT HERE

Fixing preflight was worth it immediately. The moment step 2 could run, it surfaced this:

```
eia_storage : FAIL
FAIL divergence — DIVERGENCE: scraper 'ok' 106h ago but dataset degraded:
accumulation row count flat 12 consecutive runs at 3608 rows while 13d stale (warn 9d)
```

I have already done the diagnosis. Everything in §01 and §02 is measured on the host, not inferred.
Your job is the fix, not the investigation. Do not re-derive what is stated here; do challenge it
if the code disagrees with me, and say so plainly if it does.

---

## 01 / T1 — THE DIVERGENCE FALSE POSITIVE

**The alarm is wrong. The data is fine.**

`eia_storage` is EIA's Weekly Natural Gas Storage Report. Periods are **week-ending Fridays**,
published the **following Thursday** at 10:30 ET. `config/integrity_rules.yaml:79` already records
this — `gap_rule: weekly_friday   # weekly data; published Thursdays but period is week-ending Friday`.

Consequence: the newest period's age in calendar days is **never** small. It cycles 6 → 12 days
across each week and only resets on Thursday. Measured now: newest period `2026-08-21` (a Friday),
today `2026-09-03` (a Thursday), age **13 days**. The week-ending `2026-08-28` release lands today
at 14:30 UTC; preflight ran at 03:44 UTC. **`2026-08-21` was the correct latest published period at
the moment of the run.** Nothing is stale.

Now the bug. `validators/integrity.py:624-628` gets this exactly right and says so:

```python
# Fail-level staleness + ok health = degradation (scraper alive,
# dataset rotting). Warn-level = normal publication lag; stagnation
# owns that WARN, so divergence must not double-page on it.
if stale_days > fail_days:
```

Then thirteen lines later the consecutive-flat arm at `validators/integrity.py:641-642` does the
opposite:

```python
flat_is_suspicious = (
    stale_info is not None and stale_days > warn_days
)
if streak >= 3 and flat_is_suspicious:
```

With `warn_days: 9` and a structural period age that reaches 12–13, `stale_days > warn_days` is
true for roughly four days in every seven — and during exactly those days the row count is
*supposed* to be flat, because the next release has not happened. `streak >= 3` is satisfied within
a week. State confirms it: `data/health/_integrity_state.json` has `consecutive_flat: 11` at the
last recorded run and the live run made it 12. **This source will FAIL divergence every week,
permanently, on correct data.**

The staleness arm's own comment states the principle. The flat arm violates it.

**Fix the flat arm, not the threshold.** Do not raise `warn_days` — that would blind the stagnation
check, which is separately doing its job correctly (it is reporting WARN at 13d, which is honest).
Do not add an exception for `eia_storage`. The defect is general: any source whose periods carry a
structural publication lag greater than `warn_days` hits this.

Options, and you choose:

- **(a)** Gate the flat arm on `fail_days` too, matching the staleness arm and its stated reasoning.
  Simple, consistent, one line. Cost: a genuinely wedged accumulation source goes unflagged by
  divergence until it crosses `fail_days` — but stagnation already WARNs throughout that window, so
  the signal is not lost, only its severity.
- **(b)** Make the flat arm calendar-aware: a flat row count is suspicious only when a period that
  the source's `gap_rule` says *should* exist by now is missing. `gap_rule: weekly_friday` already
  encodes the calendar; the gaps check already consumes it. This is the more correct fix and the
  more work.

State which you picked and why. If you pick (b), the rule must be derived from the existing
`gap_rule` machinery — do not write a second, parallel calendar.

Whichever you choose, add a regression test in `tests/test_integrity.py` that pins the real shape:
a `mode: accumulation` source with `warn_days: 9`, `fail_days: 18`, a newest period 13 days old, a
flat row count with `consecutive_flat: 11`, and a fresh `ok` health stamp **must not FAIL
divergence**. Build the fixture from those numbers; they are the live ones.

---

## 02 / T2 — THE SKIP GATE IS DEAD CODE, AND A COMMENT IS LYING

I revised this section after reading `.github/workflows/eia-storage.yml`. My first diagnosis was
wrong and the corrected one is milder — read it as written, not as a production outage.

`.gitignore:24` ignores `data/raw/`, and `git ls-files data/raw/eia_storage` returns **0 files**.
Raw payloads are never committed. So every CI run starts with an empty raw directory, and:

- `_get_latest_local_date()` returns `None`, which can never equal `latest_api_date`.
  **The staleness skip gate cannot fire in CI.** `record_skipped()` is unreachable there.
- Every scheduled run therefore performs a full 8-year, `length=5000` refetch and ends in
  `record_success()`. The workflow runs three times a week (`0 15 * * 4`, `0 3 * * 5`,
  `0 15 * * 6`), so that is three full refetches weekly regardless of whether anything changed.
- Because the stamp is written on every run, **`status: "ok"` in `data/health/eia_storage.json`
  carries no information about whether the dataset advanced.** It means "the fetch completed",
  not "there is new data". This is worth stating plainly somewhere durable.
- The transform step's comment — `Accumulate ALL retained raw files` / "Accumulate EVERY retained
  raw file, oldest first" — is **false in CI**. `sorted(Path('data/raw/eia_storage').rglob('*.json'))`
  finds exactly one file: the one the scraper just wrote. Accumulation works only because
  `merge_into_curated` dedups against the committed curated parquet. The loop is not doing what it
  claims and the comment should say what actually happens.

This also resolves a discrepancy I flagged earlier and have since explained — do not spend time on
it. The `2026-08-29T18:09:40Z` `ok` stamp is the Saturday cron. The single local raw file
(`eia_storage_2026-08-21.json`, mtime 2026-08-25, **contents maxing at `2026-08-14`**, 3,600 rows)
is a local artifact from a local run and never reaches CI.

That local file does expose a genuine code defect, at low severity: `_get_latest_local_date()`
derives freshness from the **filename**, and the filename comes from `client.get_latest_date()` —
a different endpoint from the one supplying the rows. On 2026-08-25 those two disagreed by a week,
so the stored file is named for data it does not contain. On a developer machine with a populated
raw directory, the gate then compares equal and skips forever on a short payload. It cannot bite CI
today, but it is wrong and it is three lines.

**What to do:**

1. Make `_get_latest_local_date()` return the maximum `period` actually present in the newest
   stored payload rather than parsing the filename. `_count_existing_rows()` already opens and
   parses that file, so the read is not new work. Keep the filename convention as it is.
2. Correct the transform step's comment in `.github/workflows/eia-storage.yml` to describe what
   really happens: one raw file per run, with `merge_into_curated` against committed curated
   providing the accumulation and the shrink guard.
3. `tests/test_eia_storage_scraper.py` exists and covers this scraper. Your change must not break
   it, and it should gain a case for the content-derived freshness path.

**Do not** start committing `data/raw/` to make the gate work, **do not** change the cron cadence,
and **do not** hand-edit curated or health JSON.

For the record, so you do not chase it: `2026-08-21` is the correct newest period. Week ending
Friday `2026-08-28` publishes Thursday `2026-09-03` at about 14:30 UTC, and the Thursday
`0 15 * * 4` cron is scheduled to pick it up.

---

## 03 / T3 — PROVE IT ON REAL DATA

After T1 and T2, run:

```
python -m validators.run_integrity
python scripts/preflight.py
```

and report what step 2 says for `eia_storage`. Expected: divergence PASS, stagnation still WARN
(honest — the period genuinely is 13 days old between releases), overall `WARN`, not `FAIL`.

If `EIA_API_KEY` is available, also run the scraper and report whether the week-ending `2026-08-28`
period lands. If it is not available, say so — that is a `NOT RUN`, not a failure.

If either command cannot be spawned, report `NOT RUN` and hand me the exact commands. Do not
describe an outcome you did not observe.

---

## 04 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`. This sandbox has destroyed this
   repository's `.git` twice. I commit.
2. **When a guard fires, fix the cause.** Here the cause is the guard itself — but the correction is
   to the guard's *logic*, never to a threshold chosen to make an alarm quiet. `warn_days: 9` and
   `fail_days: 18` do not change in this brief.
3. **Never fabricate a number.** See the scoring rule at the top.
4. **A negative result is a valid result.** `NOT RUN`, `still failing`, `I could not determine what
   wrote that health stamp` all score. Guesses dressed as measurements do not.
5. **Do not touch the Section 8 / LNG code.** It is committed at `94f762d` and green. This brief is
   `validators/` and `scrapers/eia_api/` only.
6. **Do not edit curated parquet or health JSON by hand.** Ever.
7. **Do not add an `eia_storage` special case.** The defect is general.
8. **When a brief says delete, the artefact must be absent** — not a tombstone. (Nothing in T asks
   you to delete anything.)
9. Known pre-existing and **not yours**: ~35 mypy errors in `scrapers/base/playwright_client.py`,
   mypy gaps in `transformers/baker_hughes.py`, ruff in `tests/test_gie_agsi_scraper.py`, and the
   nine ruff findings in `scripts/task3_validate.py`.
10. Ruff on files you touch must not regress. Current total for
    `ruff check scripts/ tests/ publishers/ validators/ scrapers/` is your baseline — measure it
    first, report both numbers.
11. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what ran or why it could not.

---

## 05 / RUBRIC

| | Points |
|---|---|
| **Stage 0: node, pytest, ruff, preflight all still green after your changes** | **30** |
| T1 — flat arm no longer fires on structural publication lag; option stated and justified | 25 |
| T1 — regression test built from the live numbers, and it fails before your fix | 15 |
| T2 — freshness derived from payload content rather than filename | 15 |
| T2 — workflow comment corrected to describe what CI actually does | 10 |
| T2 — `tests/test_eia_storage_scraper.py` still green, with a case for the new path | 5 |
| Every number traceable to `logs/EVIDENCE.json`, or declared `NOT RUN` | — gate |

Below 85 is not done. One fabricated number caps the brief at 50.

If this lands, `fix/section8-audit` merges and the corrected Section 8 detector finally deploys.
That is the only thing standing between here and a live dashboard that tells the truth.

---

## 06 / REPORT FORMAT

1. **Stage 0 table** — four rows: gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **T1 decision** — (a) or (b), why, and the diff.
3. **T1 test** — the fixture's numbers, and confirmation it is red before the fix and green after
   (or `NOT RUN`).
4. **T2** — what `_get_latest_local_date()` now reads, the corrected workflow comment, and the
   state of `tests/test_eia_storage_scraper.py`.
5. **T3** — what step 2 printed for `eia_storage`, or `NOT RUN`.
6. **Ruff** — before and after, exact integers.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
