# Z — Fill the Well

Branch: cut a new one from `main` at `fb5aebc`. I commit and I merge.

**This is the first brief that changes production data.** Everything before it changed code, tests
or docs. This one writes years of new rows into `data/curated/gasnom.parquet` and pulls them from
somebody else's public server. Read §01 and §02 before you touch anything.

**Scoring, up front:**

> Any number in your report without a matching entry in `logs/EVIDENCE.json` scores **zero for its
> section**. `NOT RUN` scores full marks for honesty. A fabricated number forfeits the section *and*
> Stage 0.
>
> **A new test that has not been proven red forfeits its section.**
>
> **Parse every file you touch, JS and Python.** A file that does not parse forfeits Stage 0.
>
> **New for Z: stop-and-report beats push-through.** Every stage below has an explicit stop
> condition. Hitting one and stopping is worth full marks for that stage. Continuing past one
> forfeits the brief, however good the result looks.

---

## 00 / WHY THIS BRIEF EXISTS

Y asked how far back each EBB will serve. The answer, verified twice — once by you and once by me
probing the endpoint directly:

| source | reachable | verdict |
|---|---|---|
| **gasnom** | **at least 2024-01, floor unmeasured** | the one deep well |
| gulf_south | 2026-06-05 (1,227 postings, paginated) | capped, ~90 days |
| cheniere | 2026-06-04 (empty before) | capped, ~90 days |
| kinder_morgan | latest cycle only | forward accumulation only |

My probe: `cameron`, window `2024-01-01 .. 2024-01-20`, **1,700 rows across all 20 gas days**.

`scrapers/gasnom/backfill.py` already exists, already works, and already has a CLI:
`python -m scrapers.gasnom.backfill [--since D] [--until D] [--slug S]`. It chunks into ≤89-day
windows, skips existing raw files before any network call, and checkpoints per slug for crash-safe
resume. Nobody has ever run it over a deep window, because its own docstring wrongly said the
retention was 90 days. That sentence is now corrected.

**What this buys:** Cameron (2,000 MMcf/d) and Golden Pass (2,600 MMcf/d) go from 99-day demos to
multi-year series. Year-over-year, winter comparisons, "is this outage unusual" — the questions the
observatory exists to answer — become answerable for two of eight terminals.

---

## 01 / RULES OF ENGAGEMENT FOR THE NETWORK

You are about to pull years of data from a public FERC-mandated endpoint that belongs to somebody
else. It has a WAF. Be a guest.

- **Use `GasnomBackfill` / `GasnomClient` as they are.** They already carry the rate limiting,
  session priming and curl_cffi WAF fallback. Do not write a raw request loop.
- **Do not lower `DOWNLOAD_GAP_SECONDS`.** If anything, raise it.
- **One pass. No retry storms.** The backfill checkpoints; if a slug aborts, report it and stop that
  slug. Do not loop.
- **Stop immediately on any 403, 429, WAF challenge that survives the fallback, or three
  consecutive empty windows that you expected to be populated.** Report what you saw and stop that
  slug. This is a full-marks outcome.
- **Do not run the full three-year pull until §03's staged check has passed.**

---

## 02 / STAGE 0 — DO NOT REGRESS

| Gate | Command | Requirement |
|---|---|---|
| Z0-a | `node --test tests/*.test.mjs` | 0 failed (currently 42 passed) |
| Z0-b | `python -m pytest -q -m "not network"` | 0 failed, no collection errors (currently 448) |
| Z0-c | `python scripts/preflight.py` | reaches `PREFLIGHT VERDICT:`, exits 0 |
| Z0-d | `ruff check scripts/ tests/ publishers/ validators/ scrapers/` | ≤ 17 |
| Z0-e | parse check on every file touched | no SyntaxError |

Run Z0 **before** you start, and record the numbers. You will need the "before" board to prove
nothing broke.

---

## 03 / Z1 — FIND THE FLOOR, THEN PROVE THE PIPELINE ON ONE SLUG

Two things, in this order, and do not skip to §04.

### Z1-a — where does gasnom actually end?

I measured 2024-01 works. Nobody has found the floor. Binary-search it with **single short
windows** (20 days each is plenty — that is one request per probe):

- 2023-01, 2022-01, 2021-01, 2020-01 … until you get empty.
- Then narrow to the month.

Report the **oldest gas day gasnom will serve**, per slug if they differ. Cameron and Golden Pass
are the two that matter; `SABINE` and `portarthurpipeline` are secondary. Port Arthur was not
operational until mid-2026, so empty windows there are expected and are not an error.

Stop condition: any sign of rate limiting. Report the floor you reached and stop.

### Z1-b — one slug, one short window, end to end

Before pulling years, prove the whole chain on something small. Pick `cameron` and a **60-day
window inside existing coverage** — say `2026-06-01 .. 2026-07-30`, which curated already has.

Run the backfill for that window, then `python -m transformers.gasnom` over the new raw files, then:

1. **Row count moved in the right direction only.** Curated gasnom is **64,430 rows / 988 series /
   99 gas days** right now. It may grow. It must not shrink and no existing `(series_id, period)`
   value may change — the backfill recovers *additional cycles* that the live scraper never saw, so
   growth within the existing date range is expected and correct.
2. **No parallel series.** Every new `series_id` must match the canonical
   `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}` shape with a canonical cycle token. If you
   see `evng` or `itrd1-3` appear, the backfill path is bypassing the normalisation added in W —
   **stop and report that**, it is a real bug and it matters more than the backfill.
3. **Spot-check one gas day by hand** against what curated already held: same loc, same cycle, same
   value. Paste the comparison.

Stop condition: if row counts fall, values change, or non-canonical cycle tokens appear — **stop,
restore, report.** Do not proceed to §04.

---

## 04 / Z2 — THE REAL PULL

Only if §03 passed cleanly.

Run `cameron` and `goldenpass` from the floor you established in Z1-a through today. Those are the
two terminals this brief is for. Include `SABINE` if it is cheap; skip `portarthurpipeline` unless
Z1-a showed it has meaningful history.

Then transform, and report:

- rows / series / distinct gas days, before and after
- oldest and newest gas day, before and after
- parquet size on disk, before and after
- wall-clock, and how many HTTP requests it took
- any window that came back empty, and whether that is explicable (pipeline not yet in service)

**Do not commit anything.** Leave the parquet in the working tree. I commit data separately from
code, and I will re-derive these numbers before I do.

Estimate for your own sanity check: curated holds ~651 rows per gas day today. Three years is
~1,095 days, so expect roughly 700k rows and a parquet somewhere near 2 MB. If you land wildly off
that, say so and explain before continuing.

---

## 05 / Z3 — THE SECOND-ORDER EFFECTS, WHICH ARE THE INTERESTING PART

Deepening history changes what several guards see. Work through each and report what actually
happened, not what you expect.

**The coverage anti-rot guard is safe, and I want you to confirm rather than assume.**
`compute_terminal_coverage_from_curated` samples `sorted_dates[-window_days:]` — the *trailing* 60
days. Adding older days cannot move it. Run `python scripts/preflight.py` and confirm Cameron still
reads `72.9% claimed / 72.9% measured` and Golden Pass `12.7% / 12.7%`. **If either moves, stop —
do not touch the registry.** Ground rule 7 stands: nameplates and `expectedCoveragePct` do not
change in this brief.

**The gaps check will light up, and that is a real finding, not noise.** `gasnom` carries
`gap_rule: calendar_daily`. Golden Pass was not delivering in 2024; Port Arthur did not exist. Three
years of history will surface long runs of missing gas days.

Do **not** silence it globally. Report what the gaps check says per slug, then propose — do not
apply — the narrowest honest fix. A per-source in-service date that the gap rule respects is the
shape I would expect; a blanket exemption is not. This is a proposal for me, with the evidence
behind it.

**Section 8's event detector will see a different world.** `detect_events` derives a first
commercial operation date from the data and emits a `NOT_YET_OPERATIONAL` span before it. With
years of pre-service zeros, Golden Pass should now show that span honestly instead of starting
mid-ramp. Check what it produces and say whether the result is right. If it produces something
absurd — hundreds of events, or a first-op date that contradicts the commissioning record — that is
a finding worth more than the backfill itself.

**The published bundle grows.** The gasnom shard is one of twelve. Report the bundle size before
and after, and whether `_audit_bundle_coverage`'s shrinkage baselines need updating — they are
floors, so growth should not trip them, but confirm rather than assume.

---

## 06 / Z4 — MAKE IT REPEATABLE

A backfill nobody can repeat is a one-off. Leave behind:

- A short section in `scripts/` or the module docstring recording the **measured floor per slug**,
  the exact command to reproduce, and the wall-clock it took.
- A note on whether this should ever run in CI. My instinct is **no** — it is a one-time recovery,
  and a scheduled job that pulls three years on every run would be rude to the endpoint and
  pointless. Argue it either way, but argue it.

If you disagree with anything in §05's proposals, say so there rather than implementing your
alternative.

---

## 07 / Z5 — EVIDENCE. NINTH BRIEF.

`scripts/evidence.py` has never executed and `logs/EVIDENCE.json` has never existed. **You could
spawn subprocesses in brief Y**, so this may finally be possible. Run it. If it fails, paste the
failure — that is useful and it is full marks. `logs/` must contain only what a run produced.

---

## 08 / GROUND RULES

1. **No git commands at all.** I commit and I merge. Data commits go separately from code commits.
2. **Stop-and-report beats push-through.** Every stop condition in §03 and §05 is worth full marks.
3. **Parse every file you touch, JS and Python.**
4. **Prove every new test red before claiming it guards anything.**
5. **Never fabricate a number, and never report as executed something you did not run.** In W you
   reported a migration as done while the parquet was untouched.
6. **Be a guest on the endpoint.** §01 is not advisory.
7. **When a guard fires, fix the cause** — never a threshold, a nameplate, a demoted meter, or a
   softened assertion. §05 explicitly forbids silencing the gaps check.
8. **Do not change any nameplate, `series` id, `expectedCoveragePct` or `coverageTolerancePct`.**
9. **The only sanctioned write to curated is via the backfill + transformer path.** No hand edits,
   no ad-hoc parquet surgery.
10. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
11. **Never mix `_sq_` and `_oac_` in a flow total.**
12. Known pre-existing and **not yours**: the 17 residual ruff findings (E402, N806), mypy in
    `scrapers/base/playwright_client.py` and `transformers/baker_hughes.py`.
13. Maintain `OVERNIGHT_STATE.md`.

---

## 09 / RUBRIC

| | Points |
|---|---|
| **Stage 0 — before and after, all five green, or zero** | **15** |
| Z1-a — measured floor per slug, by probing | 15 |
| Z1-b — single-slug dry run verified on all three checks, spot-check pasted | 15 |
| Z2 — the pull, with before/after counts, size, wall-clock, requests | 20 |
| Z3 — coverage guard confirmed unmoved; gaps reported and a narrow fix *proposed* | 20 |
| Z3 — Section 8 event behaviour examined and judged | 5 |
| Z4 — reproducible record, and the CI question argued | 5 |
| Z5 — `evidence.py` run, or the failure pasted | 5 |

Below 85 is not done. One fabricated number caps the brief at 50. Pushing past a stop condition
forfeits the brief.

---

## 10 / REPORT FORMAT

1. **Stage 0, before and after** — gate, command, exact result line.
2. **Files touched**, with parse-check output.
3. **Z1-a** — the probe ladder and the floor per slug.
4. **Z1-b** — the three checks and the hand spot-check.
5. **Z2** — before/after table, wall-clock, request count, empty windows.
6. **Z3** — preflight coverage lines for Cameron and Golden Pass; the gaps output; your proposal;
   the Section 8 event judgement; bundle size before and after.
7. **Z4** — the record and the CI argument.
8. **Z5** — evidence run or failure.
9. **Anything you noticed and did not fix.**
10. **Rubric self-score**, honest. Above 100 means you did not read the top of this brief.
