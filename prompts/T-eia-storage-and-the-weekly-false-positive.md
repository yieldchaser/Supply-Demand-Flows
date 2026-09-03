# T — The eia_storage Gap

Branch `fix/section8-audit`, head `94f762d`. Working tree clean apart from data files. I commit.

Short brief. One source, two bugs, one of which will fire every week forever if left alone. This
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

## 02 / T2 — THE FRESHNESS GATE READS A FILENAME THAT LIES

This one is a real bug and it is live.

`scrapers/eia_api/storage.py` decides whether to fetch by comparing the EIA API's latest date
against `_get_latest_local_date()`, which derives freshness from **raw filenames**:

```python
return max(p.stem.replace("eia_storage_", "") for p in files)
```

and the file is named from `latest_api_date`, which comes from `client.get_latest_date()` — a
different endpoint from the one that returns the rows.

Measured on the host:

| | |
|---|---|
| Only raw file present | `data/raw/eia_storage/2026/08/eia_storage_2026-08-21.json` |
| Its mtime | 2026-08-25 17:42 |
| Its filename claims | `2026-08-21` |
| **Its contents max at** | **`2026-08-14`** — 3,600 rows, 450 distinct periods |
| Curated parquet | 3,608 rows, 451 periods, max `2026-08-21` |
| Health stamp | `status: "ok"`, `2026-08-29T18:09:40Z`, `metadata.rows: 3608` |

So on 2026-08-25 the latest-date endpoint said `2026-08-21` while the series endpoint returned data
only through `2026-08-14`, and the file was named for the former. The skip gate now evaluates
`latest_local == latest_api_date` as `"2026-08-21" == "2026-08-21"` with `existing_rows` 3,600
≥ 500 → **skip, forever**, even though the stored payload is a week short of its own name.

Two things to establish, in this order:

1. **Derive freshness from content, not the filename.** `_get_latest_local_date()` must return the
   maximum `period` actually present in the newest stored payload. The filename may stay as it is;
   it must stop being the source of truth. Note `_count_existing_rows()` already opens and parses
   that file, so the read is not new work.
2. **Reconcile the health stamp.** `record_success()` runs only after a successful `guarded_write`,
   yet no raw file has been written since 2026-08-25 while health records success on 2026-08-29
   with `rows: 3608` — a count that matches *curated*, not the 3,600 in the raw file. Find out what
   wrote that stamp. If a run can record success without producing an artifact, that is the
   "reports ok while writing nothing" pattern and it is the more serious of the two findings.
   **If you cannot determine this from the code, say so and stop** — do not guess a mechanism.

Do not "fix" this by deleting the raw file or hand-editing curated. Fix the gate.

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
| T2 — freshness derived from payload content rather than filename | 20 |
| T2 — health-stamp discrepancy explained, or honestly declared undetermined | 10 |
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
4. **T2** — what `_get_latest_local_date()` now reads, and what you found about the health stamp.
5. **T3** — what step 2 printed for `eia_storage`, or `NOT RUN`.
6. **Ruff** — before and after, exact integers.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
