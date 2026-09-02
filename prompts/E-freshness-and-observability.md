# Implementation task: publish freshness + close every silent-coverage hole (P0/P1, three parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

This brief has three related parts. They ship together because they are the same problem seen
from three angles: **the system cannot currently tell you when it has stopped working.**

1. The dashboard structurally cannot show same-day LNG feedgas.
2. Two sources' health stamps are stale while their data lands fine, which silently disables a guard.
3. Ten of twelve sources have no gap detection at all, and four are missing the accumulation
   checks that would have caught the Germany/Poland loss.

Do all three. They touch overlapping files (`.github/workflows/*`, `config/integrity_rules.yaml`)
and splitting them would mean two agents editing the same surface.

---

## READ THIS FIRST — you must not run git at all

**Do not run any git command. Not `git add`, not `git commit`, not `git status`, not `git diff`.**
Edit files, run tests, report. Claude does every commit.

This sandbox has destroyed this repository's `.git` metadata twice around commit/checkout,
taking un-pushed commits with it. The protection is that you never touch git. If you need a
file's history or a previous version, ask for it in your report.

---

# PART 1 — the publish trigger graph

`.github/workflows/publish-dashboard.yml` rebuilds the dashboard bundle on `workflow_run` of
exactly five workflows:

```yaml
      - "EIA Storage (weekly)"
      - "EIA Supply (monthly)"
      - "Baker Hughes Rig Count (weekly)"
      - "GIE AGSI+ Storage (daily)"
      - "EIA LNG Exports (monthly)"
```

Three are monthly, one is weekly. **No LNG feedgas scraper triggers a rebuild.** In practice the
bundle rebuilds only when GIE AGSI+ happens to run — roughly once a day — so the live dashboard
trails curated by up to a day. Measured on 2026-09-02: live manifest `generated_at`
`2026-09-01T18:18:06Z` while curated already held 2026-09-02 data, and the live `gulf_south`
shard's newest period was `2026-08-31` against curated's `2026-09-01`.

Blue Tide's differentiator is measuring ~12,300 MMcf/d of LNG feedgas daily at meter-point
granularity. A feedgas observatory that publishes yesterday's feedgas has given away the thing
that makes it worth reading.

### What to do

Add every daily EBB scraper to the `workflow_run` trigger list. **Use these exact workflow names**
— `workflow_run` matches on the `name:` field, not the filename:

```
"Gulf South SQ (4× daily)"
"TETCO via Enbridge rtba (5x daily)"
"Quorum IPWS SQ (VG Plaquemines + Calcasieu)"
"GASNom OAC (4 LNG pipelines × 5 cycles)"
"BHE GT&S Cove Point (4× daily)"
"Cheniere OAC (2× daily)"
"Kinder Morgan OpAvail"
```

Keep the five existing entries and `workflow_dispatch`.

### Guard against thrash — and justify your choice

Those seven scrapers run roughly 25–30 times a day between them. Triggering a full bundle rebuild
on each is wasteful and will queue.

The workflow already has `concurrency: {group: publish-dashboard, cancel-in-progress: false}`.
Pick one of these and **state your reasoning in the report**:

- **(a)** Keep `workflow_run` on all twelve, and set `cancel-in-progress: true` so a burst of
  scraper completions collapses into one rebuild — the superseded run is cancelled mid-flight and
  the last one wins. Risk to check: does a cancelled publish leave a partially-committed bundle?
  Read the job's commit step before answering.
- **(b)** Drop `workflow_run` for the high-frequency sources and give the publisher its own
  schedule (e.g. hourly or every two hours), keeping `workflow_run` only for the slow sources.
  Simpler and bounded, but adds up to an hour of latency.

I lean toward **(a)** if and only if a cancelled run cannot commit a partial bundle; otherwise
**(b)**. Verify that before choosing — do not assume.

### Verify

Report the bundle build's actual runtime from a recent `Publish Dashboard Bundle` run and the
implied rebuild count per day under your chosen design. You cannot trigger CI from here, so state
plainly what you verified statically and what needs a live run to confirm.

---

# PART 2 — health stamps that go stale while the data lands

`divergence` checks are SKIPPED on five sources because their health stamp is older than the
3-day recency window — *"health stamp Nd old exceeds recency window 3d — it cannot vouch for
today's dataset."* A stale stamp on a working scraper silently disables a guard.

Measured stamp ages against `data/health/*.json` (all reading `status: ok`):

| source | stamp age | data cadence | verdict |
|---|---|---|---|
| `quorum` | **10.1 d** | daily, landing fine | **broken** |
| `kinder_morgan` | **6.9 d** | daily, landing fine | **broken** |
| `eia_lng` | 27.9 d | monthly | expected |
| `eia_storage` | 3.9 d | weekly | expected |
| `baker_hughes_rigs` | 3.8 d | weekly | expected |

**Both root causes are already found. Implement the fixes; do not re-diagnose.**

### 2a. `quorum` — health is written in one job and committed from another

`.github/workflows/quorum.yml` has two jobs. The `scrape` job (matrix over tenants) writes
`data/health/quorum.json` via `HealthWriter` and uploads **only** `data/raw/quorum/` as an
artifact:

```yaml
        uses: actions/upload-artifact@v4
        with:
          name: quorum-raw-${{ matrix.tsp }}
          path: data/raw/quorum/
```

The separate `publish` job downloads those raw artifacts, transforms, and runs
`git add data/health/quorum.json` — against a file the runner never received. So it is always
unchanged and never commits. Confirmed: the last commit touching `data/health/quorum.json` is
`26ccaf0 chore: quorum health snapshot after backfill`, ten days ago, while
`data/curated/quorum.parquet` has been committed daily throughout.

Fix so the health stamp written by the scrape job actually reaches the commit. Include the health
file in the uploaded artifact (or an equivalent that survives the job boundary). Mind the matrix:
several tenants each write the same path, so decide deliberately how they combine and say what you
chose — the merged result must reflect the whole run, not whichever tenant happened to land last.

### 2b. `kinder_morgan` — the workflow never adds its health file

`.github/workflows/kinder-morgan.yml` commits:

```yaml
          git add data/raw/kinder_morgan/ data/curated/kinder_morgan.parquet || true
```

`data/health/kinder_morgan.json` is absent. It is the **only** one of the twelve scraper
workflows that does not commit its health file — the other eleven all do. Add it, matching how the
neighbouring workflows do it.

### 2c. Audit the rest

Check every scraper workflow for the same class of defect: a health stamp written but not
committed, or written in a job that does not do the committing. Report what you find. Fix only
what is genuinely broken — a monthly source with a 27-day-old stamp is correct behaviour, not a
bug, and must not be "fixed" by touching the stamp.

**Do not hand-edit any `data/health/*.json` to refresh a timestamp.** The stamps are outputs. If
one is stale, the thing that writes it is broken.

---

# PART 3 — gap detection and accumulation checks across all twelve sources

`python -m validators.run_integrity` currently reports `gaps SKIPPED — no gap_rule configured`
for **ten of twelve sources**. Only `gulf_south` has one, which is the sole reason anyone noticed
its missing 2026-08-27. Equivalent holes in every other source are invisible today.

Current state of `config/integrity_rules.yaml`:

| source | mode | gap_rule | staleness (warn/fail days) | bands |
|---|---|---|---|---|
| `gulf_south` | accumulation | calendar_daily | 2 / 4 | yes |
| `gie_agsi` | **—** | — | 3 / 6 | — |
| `baker_hughes` | **—** | — | 9 / 18 | — |
| `eia_storage` | accumulation | — | 9 / 18 | yes |
| `eia_lng_exports` | **—** | — | 45 / 135 | — |
| `eia_supply` | **—** | — | 45 / 135 | — |
| `gasnom` | accumulation | — | 2 / 4 | — |
| `quorum` | accumulation | — | 2 / 4 | yes |
| `bhe` | accumulation | — | 2 / 4 | yes |
| `cheniere` | accumulation | — | 2 / 4 | yes |
| `enbridge` | accumulation | — | 2 / 4 | yes |
| `kinder_morgan` | accumulation | — | 2 / 4 | yes |

### 3a. Add `mode: accumulation` to the four missing it

`gie_agsi`, `baker_hughes`, `eia_lng_exports`, `eia_supply`. This is not cosmetic: **the missing
`mode: accumulation` on `gie_agsi` is why the integrity monitor reported PASS straight through
the loss of 4,548 rows of German and Polish storage history.** Read what `mode: accumulation`
actually enables in `validators/integrity.py` before adding it, and confirm in your report that it
would now catch a comparable drop — ideally by constructing that scenario in a test.

### 3b. Configure `gap_rule` per source, by real posting cadence

Encode each source's actual cadence rather than one blanket rule. Legitimate absences must not
produce noise, or the board becomes something people learn to ignore:

- **Daily EBB sources** — `gulf_south`, `gasnom`, `quorum`, `bhe`, `cheniere`, `enbridge`,
  `kinder_morgan`: a row every calendar day. Pipelines nominate on weekends and holidays, so
  calendar-daily is right — but check the data before asserting it, and say what you found.
- **`gie_agsi`** — daily, but country publication offsets mean a single-day hole for one country
  can be legitimate. Decide whether the rule belongs at source level or per-series and justify it.
- **`baker_hughes`** — weekly (Fridays).
- **`eia_storage`** — weekly (Thursdays).
- **`eia_lng_exports`, `eia_supply`** — monthly.

Read how `gulf_south`'s `calendar_daily` rule is implemented in `validators/integrity.py` and
reuse the mechanism. If a cadence cannot be expressed with what exists, extend the validator
rather than forcing a bad fit — and say so.

### 3c. Run it across full history and report every gap

Run the configured checks over each source's entire curated history and **report every gap found,
per source, with dates.** Expect this to surface holes nobody knows about.

**Report them. Do not backfill anything.** Triage is a separate decision, and some gaps will be
legitimate (a terminal idle, a market holiday, a source that genuinely does not post). A gap list
with an honest explanation per source is the deliverable.

### 3d. Check the `eia_lng_exports` threshold against reality

`eia_lng_exports` sits at `warn 45 / fail 135` days and its newest period is 2026-05-01 — 124 days
old at the time of writing, i.e. **11 days from a hard FAIL**.

Determine whether EIA has genuinely stopped publishing that series or whether the lag is normal
for it. Check EIA's actual publication calendar for the series. If the lag is normal, the
threshold is wrong and should be widened **with the publication cadence cited in a comment**. If
EIA really has stopped, that is a finding worth its own line in the report.

**Do not widen the threshold merely to stop a FAIL.** A threshold change needs a cited reason.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to make an alarm stop. A threshold may only change with cited evidence about the real
   cadence (see 3d).
2. **Never fabricate a number to satisfy a check.** No synthetic data, no interpolation, no
   backfilling in this brief at all.
3. **Never hand-edit `data/health/*.json`.** They are outputs.
4. **`merge_into_curated` always** for curated writes.
5. **RAW `Dth/d` in Python; convert only in frontend JS.**
6. **No git commands at all.**
7. **Gates:** `pytest`, `ruff check` on Python files only (it cannot parse YAML), `mypy --strict`
   on new files. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, pre-existing mypy gaps in untouched
   `transformers/baker_hughes.py` functions, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (717 vs 719).
8. **Do not touch `docs/`.**
9. **No refactors outside what this brief names.** Note anything else; do not fix it.

## Tests

- Every workflow's `run:` blocks stay valid shell (`bash -n` after YAML dedent) — the pattern is
  already in `tests/test_gulf_south_gasday.py` on branch `fix/gulf-south-gasday`; if that file is
  not in your working tree, write the equivalent.
- `publish-dashboard.yml`'s `workflow_run` list contains every daily scraper's exact `name:`,
  asserted by reading the scraper workflows' own `name:` fields rather than a hardcoded list — so
  renaming a workflow cannot silently drop it from the trigger graph.
- Every scraper workflow commits its own `data/health/*.json`.
- A source configured `mode: accumulation` fails integrity when its curated frame loses rows
  (the `gie_agsi` scenario), using a fixture.
- Each configured `gap_rule` behaves on a fixture: a legitimate cadence passes, a real hole fails.

## What you must report back

Claims are verified by executing your code and re-reading the artifacts.

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — chosen debounce design with the reasoning, the partial-bundle-commit finding that
   decided it, measured bundle build time, implied rebuilds/day.
3. **Part 2** — what you changed for `quorum` (including how matrix tenants combine) and
   `kinder_morgan`, plus the audit result for the other ten.
4. **Part 3** — the full gap report per source with dates, your cadence findings, confirmation
   that `mode: accumulation` would now catch a `gie_agsi`-style drop, and your `eia_lng_exports`
   verdict with the citation.
5. **Test output** — real terminal output of `pytest`, `ruff`, `mypy`.
6. **Full-suite result** — counts, with the known pre-existing failure identified. Derive any
   difference from a previous count rather than attributing it to environment variance.
7. **Anything contradicting this brief.** Say it loudly — the tables above are measurements, and
   if the repo disagrees with them, the repo is right and I want to know.
8. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews the working tree and commits.
