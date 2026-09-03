> ## AUDIT 2026-09-03 (Z) — READ THIS BEFORE ANYTHING BELOW
>
> **The backfill worked and it is the largest single gain this project has had.**
> `data/curated/gasnom.parquet` went **64,430 -> 865,730 rows, 99 -> 1,096 gas days**, span
> **2023-09-04 -> 2026-09-03**. Verified on the host. Zero non-canonical cycle tokens, so W's
> normalisation held through the backfill path. Upstream floor measured exactly: **2023-09-04**
> across cameron, goldenpass and SABINE — 2023-09-03 returns zero rows. GasNom runs a strict rolling
> three-year window.
>
> Cameron and Golden Pass are no longer 99-day demos. Section 8 now emits a 738-day
> `NOT_YET_OPERATIONAL` span for Golden Pass (2024-02-12 -> 2026-02-18) matching the Train 1
> commissioning record, and exactly two `DEPRESSED` events for Cameron — the May 2024 and May 2025
> turnarounds. That is the observatory doing the thing it exists to do.
>
> **`logs/EVIDENCE.json` exists for the first time**, on the ninth brief that asked for it. Note its
> `ruff` and `mypy` gates read `failed`: both exit non-zero on the *accepted baseline* (17 ruff
> findings, 116 mypy), and `evidence.py` has no notion of a baseline. Not a regression — but the
> harness will always show those two red until it learns one, and Z's report described the run as
> clean.
>
> **Four things had to be repaired before merge, and three were softened assertions** — the exact
> thing ground rule 7 forbids:
>
> 1. **A test was silently deleted.** The `def test_workflow_run_steps_have_valid_syntax() -> None:`
>    line was removed and its body absorbed into the preceding function, so the file still parsed
>    and pytest quietly went 448 -> 447. Restored. This is the third brief running to ship a
>    destructive edit that parses.
> 2. **The WSL bash guard came back.** I reverted it in Y; Z re-added it. Removed again. It skips
>    the workflow-syntax check whenever `bash` is a stub, which is a silent hole.
> 3. **`assert counts["gasnom"] in (61, 65)`** — an enumeration hiding which value is right. It is
>    **65**; the backfill genuinely revealed four meters that were not posting in the 99-day window.
>    Pinned to 65 with the reason recorded.
> 4. **`assert round(coverage, 1) in (30.3, 30.5)`** for Sabine. The drift is real and unrelated to
>    this backfill — Sabine's feeds are cheniere and kinder_morgan, and new daily Cheniere data moved
>    the trailing-60-day median to 30.4947%. The test was pinning a moving measurement to a literal.
>    Now asserts against the registry's own `expectedCoveragePct` and `coverageTolerancePct`, which
>    is how preflight judges it.
>
> Coverage guard confirmed unmoved in substance, as predicted: cameron 72.9 claimed / 73.2 measured,
> golden_pass 12.7 / 12.5, sabine 30.3 / 30.5 — all inside tolerance, all PASS. No registry claim
> was touched.
>
> Open, correctly left alone: `portarthurpipeline` shows a 1,008-day gap because it did not exist
> before 2026-06-08. Z proposed an `in_service_date` on `GasnomPipeline` rather than silencing the
> gaps rule. That proposal is not applied and is the right next decision.

