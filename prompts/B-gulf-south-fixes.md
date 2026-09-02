# Implementation task: fix three defects in the Gulf South gas-day branch (P0)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/gulf-south-gasday` already exists and contains the work from the previous brief
(`prompts/A-gulf-south-gasday.md`). **Check it out and build on it — do not start over, do not
re-implement, do not re-diagnose.** Its two commits are:

- `b982ae6` `data(gulf-south): backfill 2026-08-27 via widened postings window`
- `fb584f9` `fix(gulf-south): gas-day from disk, ungate curated commit [WIP - NOT MERGEABLE]`

The design on that branch is correct and verified: the gas-day-from-disk resolution, the
`page_size=100` measurement, the `pipeline_health.py` reconciler, the workflow reordering, and
the 2026-08-27 backfill are all good work and all confirmed against artifacts. **Do not undo any
of it.** Three defects block the merge. Fix exactly those three, plus one missing test.

---

## READ THIS FIRST — git ground rules, non-negotiable

The previous run of this task destroyed the local git repository: it deleted `refs/heads`,
rewrote `.git/config`, force-reset `main` to the root commit, and ran garbage collection, which
pruned 965 commits of history. It then reported the wreckage as the repo's normal state. The
history was only recoverable because GitHub still had it.

Therefore, on this task you may use **only** these git commands:

```
git status   git diff   git log   git show   git add   git commit   git checkout <branch>
git checkout -b <branch>   git switch
```

**Never run, under any circumstance:** `git gc`, `git prune`, `git fsck`, `git update-ref`,
`git symbolic-ref`, `git read-tree`, `git commit-tree`, `git mktree`, `git reset --hard`,
`git checkout -f`, `git push --force`, `git branch -D`, `git stash`, `git worktree`, or any
direct write to anything inside `.git/`.

**If a git command fails or the repository looks wrong in any way: STOP IMMEDIATELY, change
nothing, and report what you saw.** A confusing git state is never yours to repair. There is no
situation in this task where the correct response to a git error is another git command.

Do not `git push`. Do not touch `main`.

---

## Defect 1 (BLOCKER) — the commit step cannot execute

**File:** `.github/workflows/gulf-south-sq.yml`, step `Commit curated + health`.

The `MSG=$(python - <<'PY' ... PY)` heredoc body is indented 12 spaces inside a `run: |` block
whose own indentation is 10 spaces. YAML strips 10, so Python receives its first line indented
by 2 and dies. Extracted and executed exactly as CI would run it:

```
  File "<stdin>", line 1
    import json
IndentationError: unexpected indent
EXIT=1
```

The step fails, the job fails, and the curated parquet is never committed — **this reintroduces
precisely the data-loss bug the branch exists to fix.** The previous agent reported this heredoc
as tested and working. It was not.

**Fix it, and prefer a shape that cannot silently break again.** A separate committed helper
script invoked as `python scripts/<name>.py < /tmp/scraper_output.json` is more robust than an
inline heredoc, is importable by a test, and removes the YAML-indentation hazard entirely. If
you keep the heredoc instead, the body must sit at column 0 after YAML stripping.

Behaviour must stay as the previous brief specified: build the commit message from the
`files` list the scraper emits (`[{"cycle": ..., "gas_day": ...}, ...]`), e.g.
`data(gulf-south): SQ 2026-08-31..2026-09-01 (5 files, ID1/ID2/ID3)`, and
`data(gulf-south): SQ sync (no new postings)` when the list is empty.

## Defect 2 (BLOCKER) — a normal `skipped` run now fails the job

**File:** `.github/workflows/gulf-south-sq.yml`, step `Record pipeline health`.

The guard only short-circuits when `STATUS` is empty or `failed`. When the staleness gate holds
(`status: skipped`, a routine and healthy outcome), the transform step is skipped, so
`steps.transform.outputs.rows_added` is empty and the step runs:

```
pipeline_health.py: error: argument --rows-added: invalid int value: ''
```

argparse exits 2, the step fails, the job fails. Reproduce it yourself with:

```bash
python -m scrapers.energy_transfer.pipeline_health --rows-added "" --files-fetched 0
```

**Fix:** handle the `skipped` status explicitly, and make the step robust to an unset
`rows_added` regardless of how the path was reached. Decide deliberately what a `skipped` scrape
should stamp — a staleness-gate hold is not a no-op run in the "fetched but landed nothing"
sense, and it must not increment the no-op streak ladder. State your reasoning in the report.

## Defect 3 — mangled function body

**File:** `scrapers/energy_transfer/meter_inventory.py`, roughly lines 136–147.

A botched edit left a dead `for` loop, an orphaned copy of the docstring floating as a no-op
expression statement, and a duplicated comment, immediately before the real loop:

```python
    for cycle in reversed(_CYCLES):
        p = base / f"{gas_day.isoformat()}_{cycle}.json"
    """Orchestrate the inventory run for a given gas day.
    ...
    """
    # Find latest available cycle raw file for this gas day
    target_file = None
    for cycle in reversed(_CYCLES):
        p = base / f"{gas_day.isoformat()}_{cycle}.json"
        if p.exists():
```

It happens to run correctly. Remove the dead loop and the orphaned docstring so the function has
exactly one loop and one docstring, in the right place. No behaviour change.

## Missing test — execute the workflow's shell, don't just parse it

The existing YAML-structure tests in `tests/test_gulf_south_gasday.py` are good and must stay,
but they assert on *structure* and so could not catch Defect 1 or 2. Add tests that catch both:

1. **Every `run:` block in `.github/workflows/gulf-south-sq.yml` is valid shell.** Extract each
   block, apply the same dedent YAML applies, and check it parses (`bash -n`). A syntax or
   indentation error in any step must fail this test.
2. **The commit-message builder produces the expected string** for: multiple files across two
   gas days, a single file, and an empty `files` list. If you extract the builder into a script
   or function (recommended in Defect 1), test it directly.
3. **The health step's `skipped` path** does not crash and does not increment the no-op streak.

---

## Non-negotiables — unchanged from the previous brief

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to make an alarm stop. Related: do not hand-edit `data/health/*.json` to a greener
   status. If a stamp is wrong, fix what produced it. (The previous run wrote `ok` directly over
   a stamp the reconciler had correctly refused to soften. The number happened to be true; the
   method was not acceptable.)
2. **Never fabricate a number to satisfy a check.** No synthetic data, no interpolation.
   "Observatory, not oracle. Zero randomness."
3. **`merge_into_curated` always** (`transformers/base/accumulate.py`). Never
   `safe_write_parquet` directly over a curated file.
4. **Series ID format:** `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`, `flow ∈ {r, d}`.
5. **RAW `Dth/d` in Python; convert only in frontend JS.**
6. **Data commits separate from code commits.**
7. **Gates:** `pytest`, `ruff check .` (Python files only — ruff cannot parse YAML; passing it a
   `.yml` produces hundreds of bogus "SyntaxError" results, which happened last time), and
   `mypy --strict` on new files. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (hardcoded 717 vs 719 meter count).
8. **Do not touch `docs/`.**
9. **No refactors outside the files named above.** Note anything else you spot; do not fix it.

## What you must report back

Everything you claim will be independently verified against the diff, the parquet, and by
executing your code. Last time three claims were checked and found false — a heredoc reported as
tested that raises `IndentationError`, a change to `fetch_postings_list` that does not exist in
the diff, and a description of the repo's git state that was the result of the agent's own
damage. **If you did not run it, do not say you ran it. If something did not work, say so.**

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Proof Defect 1 is fixed** — paste the terminal output of actually executing the commit-message
   build with a sample `files` payload, showing the resulting message string.
3. **Proof Defect 2 is fixed** — paste the output of the `skipped` path running without error,
   and state what status it stamps and why.
4. **Test output** — real terminal output of `pytest tests/test_gulf_south_gasday.py`, plus
   `ruff check` on the `.py` files only, plus `mypy --strict` on your files.
5. **Full-suite result** — pass/fail counts, with the known pre-existing failure identified.
6. **Anything that contradicts this brief.** Say it loudly; discrepancies are the most valuable
   thing you can surface.
7. **Anything you noticed but did not fix**, per non-negotiable 9.

Commit on `fix/gulf-south-gasday`, data separate from code. Do not push. Do not touch `main`.
