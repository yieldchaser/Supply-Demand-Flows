# Implementation task: Gulf South gas-day resolution + commit-gating fix (P0, active data loss)

You already know this repo. Skipping the tour: **Blue Tide**, `yieldchaser/Supply-Demand-Flows`,
a physical-flow observatory for North American natural gas — scrapes public pipeline EBBs,
publishes daily scheduled-quantity data to a static GitHub Pages dashboard.

Work on a new branch off `main`: `fix/gulf-south-gasday`.

Read this entire brief before writing code. The diagnosis is already done and proven from CI
logs — **do not re-diagnose, do not "improve" the diagnosis, implement exactly what is
specified below.** If you believe the diagnosis is wrong, stop and say so with evidence rather
than silently doing something different.

---

## 1. Background: what is broken

The `Gulf South SQ (4× daily)` workflow (`.github/workflows/gulf-south-sq.yml`) fails roughly
50% of the time (20 of the last 40 runs). Each failure **silently discards a correct, already-
computed curated parquet**. One gas day (2026-08-27) has been permanently lost because of it,
and more will be lost every day this persists.

### Proven root cause (from run 33602629628, 2026-09-02T07:15Z)

```
07:15:45  step "Determine gas day"  →  Gas day: 2026-09-02        ← date -u +%Y-%m-%d, WALL CLOCK
07:15:47  scraper  Written 769 rows  data/raw/gulf_south/2026-09-01_ID3.json
07:15:47  scraper  Written 769 rows  data/raw/gulf_south/2026-09-01_ID2.json
07:15:48  scraper  Written 769 rows  data/raw/gulf_south/2026-09-01_ID1.json
07:15:48  scraper  Written 769 rows  data/raw/gulf_south/2026-08-31_ID3.json
07:15:49  scraper  Written 769 rows  data/raw/gulf_south/2026-08-31_ID2.json
07:15:49  scraper  status ok, processed_count 5, skipped_count 0      ← TRUTHFUL
07:15:51  transformer  Accumulated 7380 rows → history now 429322 rows
07:15:51  transformer  4433 series, 2026-05-25 → 2026-09-01           ← TRUTHFUL
07:15:51  meter_inventory  Error: No raw JSON files found for gas day 2026-09-02 in any cycle.
07:15:51  ##[error] Process completed with exit code 1
```

Three separate defects stack here:

**(a) Wall-clock gas day passed to `meter_inventory`.**
The scraper deliberately takes *no* gas-day argument — it syncs whatever the postings listing
offers (a previous fix; the workflow carries a comment explaining it). But the workflow step
`Run meter inventory` is still invoked as:

```yaml
run: python -m scrapers.energy_transfer.meter_inventory ${{ steps.gasday.outputs.gas_day }}
```

where `steps.gasday.outputs.gas_day` is `date -u +%Y-%m-%d`. At 07:15 UTC, Boardwalk has not
posted gas day 2026-09-02 yet — the listing served 09-01 and 08-31. `run_inventory()` looks for
`data/raw/gulf_south/2026-09-02_{cycle}.json` across all five cycles, finds none, and raises
`FileNotFoundError` at `scrapers/energy_transfer/meter_inventory.py:76`. **The wall-clock fix
was applied to the scraper invocation and never to the inventory invocation. It is half a fix.**

**(b) A derived-artifact step gates the primary-artifact commit.**
When `meter_inventory` exits non-zero, GitHub skips all subsequent steps by default. The
`Commit` step never runs — `git add data/curated` appears **zero times** in that run's log. The
transformer's correct 429,322-row parquet, computed on the runner, is thrown away.
`data/raw/` is gitignored and runners are ephemeral, so it evaporates. The repo's curated
parquet is still 427,846 rows; the 1,476-row delta was discarded.

**(c) Health status reflects only the fetch, not the pipeline.**
`data/health/gulf_south.json` was written `"status": "ok"` at 07:15:49 and stayed `ok` even
though the job failed and nothing was committed. Downstream guards read that stamp and assume
it means "the data landed." It does not.

### Why 2026-08-27 is unrecoverable

`fetch_postings_list(client, page_size=20)` fetches only the 20 most recent postings, and not
all of them are OAC CSVs. The observed practical recovery window is **~2 gas days**, not the
~90 days of Boardwalk's archive. Run history shows six consecutive failures spanning ~40 hours
(2026-08-27T02:24Z through 2026-08-28T14:30Z). By the next success, 08-27's postings had rolled
off the listing window.

Failures cluster on runs starting 00:00–09:00 UTC (wall-clock "today" has no posting yet) and
successes on runs starting 19:00–24:00 UTC. Scheduled runs drift heavily — this one fired at
07:15Z for a `30 2` cron.

### What is NOT broken — do not "fix" these

- **Curated `period` values are correct.** `transformers/gulf_south.py:96` reads
  `Effective Gas Day` directly from each CSV row, falling back to the payload date only as a
  last resort. The posting-derived date leaks into *raw filenames only*. Do not touch the
  transformer's period-derivation logic.
- **The scraper is honest.** `status: ok, processed_count: 5` was a true statement about what
  it fetched. The scraper is not the bug. Do not add a gas-day filter back to the scraper — that
  coupling was deliberately removed and re-adding it would reintroduce an older bug.

---

## 2. What to implement

### Change 1 — `meter_inventory` derives its own gas day from what is on disk

**File:** `scrapers/energy_transfer/meter_inventory.py`

The inventory's job is "describe the meters we just pulled," so its input must be *what was
actually pulled*, never a wall-clock guess.

- Add a function that scans `data/raw/gulf_south/` for files matching
  `{YYYY-MM-DD}_{CYCLE}.json` and returns the **newest gas day present on disk** (parse the date
  from the filename; `_raw_path` in `scrapers/energy_transfer/gulf_south.py:42` defines the
  format). Cycles are `_CYCLES = ["TIMELY", "EVENING", "ID1", "ID2", "ID3"]`.
- Change the `__main__` block (currently at `meter_inventory.py:82`, which does
  `_gas_day = date.today()` then optionally `sys.argv[1]`) so that **with no argument it uses the
  newest gas day found on disk**, not `date.today()`. An explicit `sys.argv[1]` must still work
  for manual/backfill use.
- If the raw directory contains no parseable files at all, exit with a clear message. That is a
  genuinely empty state, distinct from "we guessed the wrong day."

**On reusing the Kinder Morgan pattern — read carefully.** The project already solved
served-stamp gas-day anchoring for Kinder Morgan in commit `7d106df`:
`scrapers/kinder_morgan.py:105 parse_posting_stamp()` and `scrapers/kinder_morgan.py:169
derive_gas_day()`. **Reuse the *discipline*, not those functions.** They parse NAESB posting
stamps out of Kinder Morgan HTML (`CycleDesc: EVENING | Post Date: 08/24/2026 | Post Time: 6:45 PM`)
and apply KM's tariff roll-forward rules. Gulf South is a different platform: its served
effective gas day is already present per-row as the `Effective Gas Day` CSV column and in the
raw payload, so there is no stamp to parse. Importing KM's HTML parser here would be wrong.
The shared principle you must honour is: **derive the gas day from what the source served,
never from the wall clock.** Note this deviation explicitly in your report back.

### Change 2 — ungate the commit of the curated parquet

**File:** `.github/workflows/gulf-south-sq.yml`

- Reorder the job so the sequence is: scrape → **transform → commit** → meter inventory.
- Give the `Run meter inventory` step `continue-on-error: true`.
- The meter map (`config/lng_meter_map.json`) is regenerable from any later run; the curated
  parquet on a ~2-day-window source is not. **A derived artifact must never be able to discard a
  primary artifact.**
- If `meter_inventory` produces a changed `config/lng_meter_map.json`, commit that in its own
  step after the inventory runs, so a map change still lands when it succeeds.
- Remove `${{ steps.gasday.outputs.gas_day }}` from the inventory invocation (per Change 1).

**Also fix the decorative-but-wrong commit metadata.** The `Determine cycle` step maps UTC hour
→ cycle (`19`→TIMELY, `02`→EVENING, `17`→ID1, else ID2). Because scheduled runs drift, the hour
usually doesn't match and it falls through to `ID2`. Run 33602629628 logged
`Resolved cycle: ID2` while actually fetching ID1+ID2+ID3 across two gas days, and committed
`data(gulf-south): SQ ID2 2026-09-01`. These commit messages are fiction. Replace the commit
message with one built from **what the run actually processed** — have the scraper's JSON output
(already captured to `/tmp/scraper_output.json`) carry the list of `(cycle, gas_day)` pairs it
wrote, and build the message from that, e.g.
`data(gulf-south): SQ 2026-08-31..2026-09-01 (5 files, ID1/ID2/ID3)`.
Delete the `Determine cycle` and `Determine gas day` steps if nothing else needs them.

### Change 3 — health status must reflect the pipeline outcome

The decision (already made, implement it as stated): **health is recorded at the end of the
pipeline, not by the scraper alone.**

Rationale you should preserve in a code comment: the alternative — having the scraper validate
its own output against the gas day it will later be transformed for — would re-couple the
scraper to a downstream target day, which is exactly the coupling that was deliberately removed.
End-of-pipeline health keeps the scraper honest about fetching while making the health stamp
mean "the data landed," which is what every downstream guard already assumes it means.

- The health stamp for `gulf_south` must not be left as `ok` when the transform/commit stage did
  not land rows.
- Use the existing `HealthWriter` API in `scrapers/base/health_writer.py` — do not invent a new
  status vocabulary. Available: `record_success`, `record_no_op` (streak-escalating: <3 → `warn`,
  ≥3 → `fail`), `record_failure`, `record_guard_failure` (streak-escalating), `record_skipped`.
- A run that fetched files but landed zero curated rows for them is a **no-op**, not a success —
  use `record_no_op` so the existing streak ladder escalates it.

### Change 4 — widen the postings listing window

**File:** `scrapers/energy_transfer/gulf_south.py:47` (`fetch_postings_list`, `page_size=20`)

The 20-posting default is what makes a multi-run outage permanently lossy.

**Measure before choosing a number.** Call the endpoint with progressively larger `page_size`
values, and report: how many postings come back, how many are OAC CSV postings, and how many
distinct gas days that spans, for at least `page_size` ∈ {20, 50, 100, 200}. Then set a default
that spans a comfortable multi-day buffer (target: survive a ~3-day total outage) without
hammering the endpoint. **Report the measurements — do not just pick a number.**

Note the existing staleness gate (`if out_path.exists(): skip`) means a larger window is cheap
in steady state on a persistent disk, but CI runners start with an empty `data/raw/`, so every
run re-fetches everything in the window. Factor that into the choice and say what per-run cost
your chosen size implies (requests × ~0.5s at the client's 2 req/s rate limit).

### Change 5 — regression tests

**Files:** `tests/test_meter_inventory.py`, `tests/test_gulf_south.py` (extend; existing suites
are there), plus a new test module if cleaner.

Required tests:
1. `meter_inventory` with no argument, against a fixture raw dir containing e.g.
   `2026-09-01_ID2.json` and `2026-08-31_ID3.json`, selects **2026-09-01** and does not raise.
2. `meter_inventory` with no argument and an **empty** raw dir fails with a clear message (this
   state must stay loud).
3. **The core guard:** a pipeline run in which the transformer consumes zero files for the target
   gas day **cannot** end with health status `ok`. Assert on the written
   `data/health/gulf_south.json` content, not on a return value.
4. The commit path is not gated on inventory success — assert against the workflow YAML: parse
   `.github/workflows/gulf-south-sq.yml` and assert that the step committing
   `data/curated/gulf_south.parquet` appears **before** the `meter_inventory` step and that the
   inventory step carries `continue-on-error: true`. (A YAML-structure assertion is acceptable
   and expected here; it is the only way to regression-test step ordering.)

### Change 6 — backfill what is still recoverable

- Run the scraper locally and report **which gas days the listing actually offers right now**.
- Backfill every offered day that is missing from `data/curated/gulf_south.parquet`.
- **2026-08-27 is gone. Do not attempt to reconstruct, interpolate, or synthesise it.** If it is
  not in the listing, report it as permanently lost and move on.
- Commit backfilled data as a **separate data commit**, distinct from the code commit.

---

## 3. Project non-negotiables — violating any of these fails the review

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to make an alarm stop. This rule exists because it was violated once and caught.
2. **Never fabricate a number to satisfy a check.** If a meter reads zero, it reads zero. No
   synthetic data, no interpolation, no "restoring" a remembered value. The project's stated
   philosophy is "observatory, not oracle. Zero randomness."
3. **`merge_into_curated` always** (`transformers/base/accumulate.py`). Never
   `safe_write_parquet` directly over a curated file — the shrinkage guard refuses to write a
   smaller frame, and that guard is load-bearing.
4. **Series ID format is non-negotiable:** `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`
   with `flow ∈ {r, d}`. Omitting the flow token once caused a silent overwrite that destroyed
   27,015 rows across five sources.
5. **Values stay RAW `Dth/d` in Python, always.** Conversion happens only in frontend JS
   (`mmcf = dth / 1.025 / 1000`). Do not convert at ingest.
6. **Data commits separate from code commits.**
7. **Gates before you report done:** `pytest`, `ruff check .`, and `mypy --strict` on any new
   file. Known pre-existing failures you must NOT fix and must NOT let mask new ones: ~35 mypy
   errors in `scrapers/base/playwright_client.py`, some ruff in `tests/test_gie_agsi_scraper.py`.
   Full suite is ~391 tests, ~126s; `-m "not network"` excludes network tests.
8. **Do not touch `docs/`** in this task at all.
9. **Do not refactor anything outside the files named above.** No opportunistic cleanups, no
   renames, no reformatting of untouched code. If you spot something else wrong, write it down
   and report it — do not fix it.

---

## 4. What you must report back

Your report will be independently verified against CI logs and the parquet, so make it precise
and do not claim anything you have not observed. "Deployed and verified" is not evidence; a row
count is. Include:

1. **Diff summary** — every file changed, with the reasoning for each change in one line.
2. **`page_size` measurements** — the table asked for in Change 4, and your chosen default with
   the per-run request-count implication.
3. **Test output** — actual terminal output of `pytest` (pass/fail counts), `ruff check .`, and
   `mypy --strict` on new files. Paste the real output, not a summary of it.
4. **Backfill result** — which gas days the listing offered, which were missing from curated,
   which you backfilled, and the curated row count **before and after** (`python -c "import
   pandas as pd; df=pd.read_parquet('data/curated/gulf_south.parquet');
   print(len(df), df.period.nunique(), df.period.min(), df.period.max())"`).
5. **The KM-pattern deviation** — confirm you did not import `parse_posting_stamp`/
   `derive_gas_day`, and state why (Change 1).
6. **Anything you found that contradicts this brief.** Discrepancies are the most valuable thing
   you can surface. If the root cause as described does not match what you observe in the code,
   say so loudly rather than implementing around it.
7. **Anything you noticed but deliberately did not fix**, per non-negotiable 9.

Do not push to `main`. Leave the work on `fix/gulf-south-gasday` and report.
