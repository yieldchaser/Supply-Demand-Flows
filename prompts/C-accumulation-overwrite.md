# Implementation task: bug #1 is still live — three transformers overwrite curated history (P0)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Germany and Poland just lost five years of European storage history on `main`. The cause is the
project's oldest catalogued bug, still present in three transformers. Fix the class, restore the
data, and close the guard gap that let it through silently.

---

## READ THIS FIRST — you must not run git at all

**Do not run any git command in this task. Not `git add`, not `git commit`, not `git checkout`,
not `git status`.** Edit files, run tests, and report. Claude does every commit.

This is not about trust — you followed the rules last time and reported honestly, which is why
little was lost. It is about the environment: this sandbox has now destroyed the repository's
`.git` metadata twice around commit/checkout operations, taking every un-pushed commit with it.
The only reliable protection is that the implementing agent never touches git.

If you need a file's previous contents, ask for it in your report rather than reaching for
`git show`. Everything you need for this task is described below or already on disk.

---

## What happened, proven

Commit `25d79ea` (`data(gie): European storage 2026-08-31`) is on `main` and shipped this:

```
before (e9e889a):  82,720 rows   40 series   2,068 days
after  (25d79ea):  78,208 rows   40 series   2,069 days     ← gained a day, lost 4,512 rows
```

No series disappeared and no dates disappeared, so nothing shallow would notice. What actually
happened is that **868 individual gas days dropped from 40 rows to 32 or 36**. The missing
`(series_id, period)` pairs are exactly:

| series | days lost |
|---|---|
| `gie_storage_de_full_pct`, `_gas_in_storage`, `_injection`, `_withdrawal` | 569 each |
| `gie_storage_pl_full_pct`, `_gas_in_storage`, `_injection`, `_withdrawal` | 568 each |

**4,548 (series, period) rows total — Germany and Poland, back to 2021-01-01.** Germany is the
largest storage system in Europe; the European storage panel is Section 3 of the dashboard.

The live bundle still carries the pre-loss 82,720 rows, so the public dashboard is currently
correct — **but the next publish ships the truncated history.** That is the deadline.

## Root cause

`transformers/gie_agsi.py` calls **`safe_write_parquet` directly** instead of
`merge_into_curated`. It rebuilds the curated file from whatever the scraper fetched on that run.
When the GIE AGSI+ API returned a response without Germany and Poland for that run, the
rebuild wrote a frame missing those countries entirely — and five years of their history was
overwritten in one commit.

This is **bug #1 in the project's own catalogue** ("accumulation overwrite"), the one that cost
three months of Freeport history, verbatim. Non-negotiable #2 exists because of it: *"`merge_into_curated`
always. Never `safe_write_parquet` directly over a curated file. The shrinkage guard refuses to
write a smaller frame."*

**Three transformers still violate it:**

```
transformers/gie_agsi.py          -> safe_write_parquet      (actively losing data)
transformers/eia_lng_exports.py   -> safe_write_parquet      (same latent risk)
transformers/eia_supply.py        -> safe_write_parquet      (same latent risk)
transformers/eia_storage.py       -> uses BOTH — check which path actually writes curated
```

The other nine transformers use `merge_into_curated` correctly. Read one of those
(`transformers/gulf_south.py` or `transformers/quorum.py`) as the reference pattern.

---

## What to implement

### 1. Convert the offending transformers to `merge_into_curated`

`transformers/gie_agsi.py` first — it is the one actively losing data. Then
`eia_lng_exports.py` and `eia_supply.py`. Inspect `eia_storage.py` and determine whether its
`safe_write_parquet` call touches a curated file or something else; convert it only if it does,
and say what you found either way.

Follow `transformers/base/accumulate.py` and match how the working transformers call it. Do not
modify `accumulate.py` itself, and do not weaken the shrinkage guard to make anything pass — if
the guard refuses a write, that refusal is the correct behaviour and the bug is upstream.

Mind the schema rule while you are in there: the canonical columns are
`source | series_id | series_name | period | value | unit | region | ingested_at`, and any
dimension present in the source but absent from `series_id` is a potential silent overwrite.

### 2. Restore the lost Germany and Poland history

The pre-loss data exists in git history at commit `e9e889a` — **but you must not run git.**
Claude has already extracted the pre-loss parquet for you to:

```
C:/Users/Dell/AppData/Local/Temp/claude/verify/gie_before.parquet
```

That file is the 82,720-row version. Restore the 4,548 missing `(series_id, period)` rows for
the eight `gie_storage_de_*` and `gie_storage_pl_*` series into
`data/curated/gie_agsi.parquet`, using the same `merge_into_curated` path you just fixed — not a
hand-written parquet write.

**Restore only rows that are genuinely missing. Do not overwrite any row that currently exists,
and do not invent, interpolate or backfill a single value that is not in that file.** After the
restore, the curated file must contain every row it has now *plus* the missing ones, with the
newest gas day (2026-08-31) intact.

Report the row count before and after, and confirm the day-by-day coverage is back to 40 rows
per day for the affected range.

### 3. Explain and close the guard gap

Two layers should have caught this and neither did. Work out why and report it before changing
anything:

- **The shrinkage guard** in `merge_into_curated` never ran, because this transformer bypasses it
  entirely. Converting the transformer fixes that — but ask whether anything would *detect* a
  transformer that bypasses the accumulator. A grep-based test asserting that every module in
  `transformers/` that writes a `data/curated/*.parquet` does so through `merge_into_curated`
  would make this class of regression impossible to reintroduce. Add it.
- **The integrity monitor** reports `gie_agsi PASS`. Its rules in `config/integrity_rules.yaml`
  are minimal — `staleness` only, no `gap_rule`, no shrinkage/coverage thresholds — while
  `gulf_south` has enough configured that its shrinkage check does fire. Report what `gie_agsi`
  is missing relative to a well-configured source. **Do not add the rules yet** — a separate
  brief will do all twelve sources at once; just tell me what you found.

Also worth checking and reporting: this is a *country-identity* failure, and bug #4 in the
catalogue was also a country-identity failure (EIA's API returning `"DEU"` for both Germany and
Finland). Does the GIE scraper trust country codes from the API response the same way? If the API
can silently omit or rename a country, is there anything that would notice? Report; do not fix.

### 4. Tests

- Every `transformers/*.py` that writes a curated parquet routes through `merge_into_curated`
  (the regression guard above).
- A transformer run whose input is missing a country/series that exists in curated **must not**
  shrink the curated file — assert the guard refuses, using a fixture, not the real data.
- The restored `gie_agsi` curated file has 40 rows on a sampled set of days across the affected
  range, including the earliest (2021-01-01) and a recent one.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to make an alarm stop. Do not hand-edit `data/health/*.json` to a greener status.
2. **Never fabricate a number to satisfy a check.** No synthetic data, no interpolation, no
   "restoring" a remembered value. "Observatory, not oracle. Zero randomness." If a value is not
   in the pre-loss file, it does not get written.
3. **`merge_into_curated` always** — that is the entire point of this task.
4. **Series ID format:** `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`, `flow ∈ {r, d}`.
5. **RAW `Dth/d` in Python; convert only in frontend JS.**
6. **No git commands at all.** See the top of this brief.
7. **Gates:** `pytest`, `ruff check .` on Python files only (ruff cannot parse YAML), `mypy --strict`
   on new files. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (717 vs 719).
8. **Do not touch `docs/`.**
9. **No refactors outside the files named above.** Note anything else; do not fix it.

## What you must report back

Everything is verified independently against the parquet and by executing your code.

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Restore proof** — curated row count and distinct-day count before and after; rows-per-day for
   2021-01-01, a mid-range day, and 2026-08-31; confirmation that no pre-existing row was altered.
3. **Guard-gap findings** (§3) — why both layers stayed silent, what `gie_agsi` lacks in
   `integrity_rules.yaml`, and what you found about GIE country-code trust.
4. **`eia_storage.py` verdict** — does its `safe_write_parquet` touch curated or not?
5. **Test output** — real terminal output of `pytest`, `ruff`, `mypy`.
6. **Full-suite result** — counts, with the known pre-existing failure identified.
7. **Anything contradicting this brief.** Say it loudly.
8. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude will review the working tree and commit.
