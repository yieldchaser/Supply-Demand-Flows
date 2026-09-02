# Implementation task: make the accumulation guard actually catch bug #1 (P0)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

The previous brief (`prompts/C-accumulation-overwrite.md`) converted `gie_agsi`,
`eia_lng_exports` and `eia_supply` to `merge_into_curated`. That work is correct, verified, and
already committed on branch `fix/accumulation-overwrite`. **Do not redo it.**

It also added a regression guard in `tests/test_accumulation_overwrite.py` that is supposed to
make catalogue bug #1 (accumulation overwrite) impossible to reintroduce. **That guard does not
work.** This brief fixes the guard and the one real bypass still in the repo.

---

## READ THIS FIRST — you must not run git at all

**Do not run any git command. Not `git add`, not `git commit`, not `git status`, not `git diff`.**
Edit files, run tests, report. Claude does every commit.

This sandbox has destroyed this repository's `.git` metadata twice around commit/checkout
operations, taking un-pushed commits with it. The protection is that you never touch git. If you
need a file's history or a previous version, ask for it in your report.

Work on the files as they are on disk. You are working on top of the
`fix/accumulation-overwrite` branch state.

---

## The problem, proven

The guard in `test_every_curated_writer_routes_through_merge_into_curated` does substring
matching, and after `baker_hughes.py` tripped an earlier draft it acquired two escape hatches:

```python
calls_bytes = "safe_write_bytes(" in src
uses_merge  = "merge_into_curated(" in src or "merge_into_curated" in src
...
if not calls_direct_write and not calls_bytes and not uses_merge:
    violations.append(...)
```

- `calls_bytes` is used as an **exemption** — any module calling `safe_write_bytes` is never
  flagged.
- `uses_merge`'s second clause makes the first redundant, so a bare **mention in a comment**
  counts as compliance.

That predicate was extracted and run against synthetic reintroductions of bug #1:

```
rebuild via df.to_parquet(buf) + safe_write_bytes(curated_path, ...)  -> PASSES THE GUARD
rebuild with "merge_into_curated" only mentioned in a comment          -> PASSES THE GUARD
the real transformers/baker_hughes.py                                  -> PASSES THE GUARD
```

**A guard that passes the exact bug it exists to catch is worse than no guard**, because it
reports safety that isn't there. This is the same shape as the bugs in the catalogue: two layers
disagree and nothing loud happens.

And the bypass is real, not hypothetical. `transformers/baker_hughes.py:388-389`:

```python
out_df.to_parquet(buf, compression="snappy", index=False)
safe_write_bytes(curated_parquet_path, buf.getvalue())
```

That rebuilds `data/curated/baker_hughes_weekly.parquet` (32,893 rows, 139 periods) from a single
run's spreadsheet, replacing the file wholesale. If Baker Hughes ever publishes a short file, or
the sheet parse drops basins, the accumulated history goes with it — exactly how Germany and
Poland lost five years of storage history.

---

## What to implement

### 1. Rewrite the guard so it detects the behaviour, not a spelling

Substring matching cannot express "this module writes a curated parquet without going through the
accumulator." Use **AST analysis** (`ast` from the standard library) over each module in
`transformers/`.

A module is a **curated writer** if it can reach a `data/curated/*.parquet` path — look for the
string literal, and for parameters/variables named like `curated_parquet_path`. A curated writer
is **compliant** only if it contains an actual **call** to `merge_into_curated` (an `ast.Call`
whose func resolves to that name — not a string, not a comment, not an import alone).

A curated writer is a **violation** if it calls any of these directly:
- `safe_write_parquet(...)`
- `safe_write_bytes(...)`
- `DataFrame.to_parquet(...)` where the destination is a curated path or a buffer later written
  to one

Allow exactly one exception: `transformers/base/accumulate.py`, the atomic writer inside the merge
itself. Encode that as a named constant with a comment saying why, not as an ad-hoc string check.

**Do not add an exemption to make a module pass.** If a module trips the guard, either it is a
real violation (fix the module) or the guard's notion of "curated writer" is wrong (fix the
detection). Widening the exemption list is the failure mode that produced this brief.

**Structure it so it is testable:** put the scanner in a function that takes a directory path
argument (e.g. `find_accumulation_violations(root: Path) -> list[str]`) rather than hard-coding
`transformers/`. Task 3 depends on being able to point it at a fixture directory.

### 2. Convert `transformers/baker_hughes.py`

Replace the `to_parquet` + `safe_write_bytes` rebuild with `merge_into_curated`, matching the
reference pattern in `transformers/gulf_south.py` or the freshly-converted `transformers/gie_agsi.py`.

Its `out_df` already carries the canonical schema (`source | series_id | series_name | period |
value | unit | region | ingested_at`), so this should be a small change. Return stats from the
merged frame, not from `out_df`, as the other converted transformers now do.

**Consider revisions explicitly and say what you concluded.** Baker Hughes republishes a full
history each week and does revise prior weeks. `merge_into_curated` dedups on
`(series_id, period)` keeping the latest `ingested_at`, so a revised week should overwrite the
older value rather than duplicate it. Verify that is what actually happens with a test, and state
in your report whether revisions still land correctly after the conversion. If they do not, say
so rather than shipping it.

### 3. Prove the guard fails on a planted violation

A guard is only trustworthy if you have watched it fire. Add tests that build fixture modules in
`tmp_path` and point the scanner at them:

1. A module writing `data/curated/x.parquet` via `safe_write_parquet(...)` → **must be flagged**.
2. A module writing it via `df.to_parquet(buf)` + `safe_write_bytes(...)` → **must be flagged**
   (the vector the old guard missed).
3. A module that only *mentions* `merge_into_curated` in a comment/docstring while writing
   directly → **must be flagged**.
4. A module that genuinely calls `merge_into_curated(...)` → **must not** be flagged.
5. The real `transformers/` directory → **no violations** (this is the assertion that keeps the
   repo honest, and it must pass only because every transformer is genuinely compliant).

Keep the three existing tests in `tests/test_accumulation_overwrite.py` (the non-shrink guarantee
and the `gie_agsi` coverage check). Update the coverage test's expectation: the curated
`gie_agsi` file now holds **82,756 rows across 2,069 days** after the restore, and gas day
**2026-08-31 legitimately has 36 rows, not 40**, because Poland was genuinely absent from that
source run. Do not "fix" that to 40 — asserting 40 for every day would be asserting a value that
does not exist.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** Never widen an exemption, loosen a threshold, or narrow
   a check to make an alarm stop. This brief exists because that happened.
2. **Never fabricate a number to satisfy a check.** No synthetic data, no interpolation.
   "Observatory, not oracle. Zero randomness."
3. **`merge_into_curated` always** for curated writes. Do not modify
   `transformers/base/accumulate.py` itself.
4. **Series ID format:** `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`, `flow ∈ {r, d}`.
5. **RAW `Dth/d` in Python; convert only in frontend JS.**
6. **No git commands at all.**
7. **Gates:** `pytest`, `ruff check` on Python files only (ruff cannot parse YAML), `mypy --strict`
   on new files. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (717 vs 719).
8. **Do not touch `docs/`.**
9. **No refactors outside the files named above.** Note anything else; do not fix it.

## What you must report back

Your claims are verified by executing your code, including feeding your own scanner deliberately
bad modules. Last round the guard was reported as making bug #1 "impossible to reintroduce" while
passing three separate reintroductions — so this time, show the guard failing.

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Guard proof** — paste real output showing the scanner flagging each of the three planted bad
   modules and clearing the good one. Name the AST node types you match on.
3. **`baker_hughes` conversion** — confirm row count and period count before and after a transform
   run (or explain why you could not run one), and your finding on whether **revisions** still
   overwrite correctly.
4. **Test output** — real terminal output of `pytest tests/test_accumulation_overwrite.py`, plus
   `ruff` and `mypy --strict` on your files.
5. **Full-suite result** — counts, with the known pre-existing failure identified. Note: the
   working tree is on `fix/accumulation-overwrite`, which does **not** contain the gulf-south
   branch's 17 tests; expect roughly 378 collected, not 394. If your number differs from that,
   chase the difference and report what you found rather than calling it environmental variance.
6. **Anything contradicting this brief.** Say it loudly.
7. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
