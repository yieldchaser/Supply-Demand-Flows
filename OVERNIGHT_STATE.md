> ## AUDIT 2026-09-03 (Y) — READ THIS BEFORE ANYTHING BELOW
>
> **The sandbox could spawn subprocesses this round.** For the first time in twenty briefs the
> agent ran the gates itself and probed live endpoints. Its Stage 0 numbers were real.
> Board after repairs: node **42/42**, pytest **448/0**, preflight `PASS` exit 0, ruff **17**.
>
> **The headline finding is real and I verified it independently.** GasNom's bulk `OAC.cfm` TSV
> endpoint served **1,700 rows across all 20 gas days of 2024-01-01..2024-01-20** for the `cameron`
> slug — two years and eight months back. `scrapers/gasnom/backfill.py` already exists, already
> works, and its own docstring said "the site's retention is a rolling 90 days", which is true of
> the HTML `oauc.cfm` view but **false of the bulk TSV path the module actually uses.** That
> docstring has stopped anyone attempting a deep backfill. Corrected to state the measurement and
> to say the true floor is still unmeasured.
>
> Gulf South and Cheniere really are capped: Gulf South paginates via `pageNumber` and holds 1,227
> postings total, oldest gas day 2026-06-05; Cheniere returns empty before 2026-06-04. Kinder
> Morgan ignores date selection entirely. So **gasnom is the only deep well**, and it feeds Cameron
> and Golden Pass.
>
> Three corrections made before merge:
>
> 1. **The range caveat hardcoded today's numbers into the shipped page** — anchor `'2026-09-02'`,
>    start `'2026-05-25'`, count `101`. A caveat whose entire purpose is data honesty would have
>    gone stale the next day and been badly wrong after any backfill. Now derived from the rendered
>    series via `computeSeriesDateRange`, with `seriesInfo` passed explicitly from `safeRender`.
> 2. **An out-of-scope test weakening was reverted.** `test_workflow_run_steps_have_valid_syntax`
>    gained a guard that skips the workflow syntax check when `bash` is a broken WSL stub. I
>    verified the test passes here without it (16 passed); the guard only helps a machine outside
>    this loop and its effect elsewhere is to silently stop checking workflow `run:` blocks.
> 3. `scripts/classify_meters.py` was optimised out of scope (sort once before `groupby` instead of
>    per group). Semantically identical and 35s -> <1s; universe counts re-derived and unchanged at
>    719 / 61 / 11 / 5 / 22. Kept.
>
> The README rewrite is accurate — I spot-checked the per-source and per-terminal depths against
> curated and they match, and it states coverage gaps rather than hiding them.

