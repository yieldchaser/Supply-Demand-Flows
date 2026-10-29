# Blue Tide — Engineering Handoff

**Repo:** `C:\Users\Dell\Github\Supply-Demand-Flows` (GitHub: `yieldchaser/Supply-Demand-Flows`)
**Live:** https://yieldchaser.github.io/Supply-Demand-Flows/
**Handoff date:** 2026-09-02
**Prepared for:** Claude Code, taking over from a chat-based advisory session that ran ~May–Aug 2026.

---

## 0. Read this first

This document is the accumulated state of a project that shipped a lot and broke in
instructive ways. The **Bug Catalog (§6)** is the most valuable section — eight silent
data-loss bugs of the same family, each of which shipped wrong numbers to a public
dashboard for days or months before being caught. Read it before writing any code.

The single most important cultural rule that emerged:

> **When a guard fires, fix the cause. Never demote a meter, loosen a threshold, or
> disable a check to make an alarm stop.**

The second most important:

> **Verify claims against artifacts.** Read the parquet with pandas. Curl the live
> bundle. Check the CI run. A report that says "deployed and verified" is not evidence.
> Roughly a third of agent-reported completions in this project turned out to be
> partially wrong when checked against production.

---

## 1. What Blue Tide is

An open-source **physical flow observatory** for North American natural gas. It scrapes
FERC-mandated public pipeline Electronic Bulletin Boards (EBBs) and publishes daily
scheduled-quantity (nomination) data as a static dashboard on GitHub Pages.

**Philosophy: "observatory, not oracle. Zero randomness."** No stochastic models, no
synthetic data, no interpolation, no fabricated numbers to fill a gap. If the data isn't
there, the panel says so. This principle has been tested repeatedly and held — the
project has killed two of its own analytical ideas (storage nowcast, TSQ/OAC utilization)
because the data didn't support them.

**What makes it differentiated:** it measures ~12,826–13,914 MMcf/d of US LNG export feedgas
daily, per terminal, at meter-point granularity, from public sources, for free. Criterion
Research sells a subset of this to institutions. Enercast covers pipeline *notices* (a
different layer) at $149–$1,399/mo. Nobody publishes the physical flow layer free.

**Related repos by the same owner (context, not in scope):** Weather Desk
(`weather-dd-tracker`), Blue Pulse (`CFTC-Reports-`), Blue Flux (`Nat-Gas-Price-His`),
Blue Meridian (`Nat-Gas-ETFs`), Blue Margin (`mcx-margins`). Cross-project synthesis is a
future idea, not current work.

---

## 2. Current state (verified live 2026-09-02)

### 2.1 Data sources — 12 live

| Source | What | Coverage | Platform |
|---|---|---|---|
| `gulf_south` | Boardwalk / Gulf South Pipeline | 94 days, ~407k rows | public OAC JSON API |
| `enbridge` | Texas Eastern (TETCO) | 1,099 days, ~237k rows | ASP.NET AJAX page-methods |
| `quorum` | Gator Express + TransCameron (Venture Global) | **2021-03-15 →**, ~117k rows | Quorum IPWS `ExportToCSV` |
| `gasnom` | Golden Pass, Cameron, Sabine Pipe Line, Port Arthur | 91 days, ~63k rows | ESG/Latitude ColdFusion |
| `bhe` | EGTS + Cove Point (CPL) | 604 days, ~13k rows | BHE GT&S Next.js REST |
| `cheniere` | Creole Trail + Corpus Christi Pipeline | 94 days, ~6k rows | Cheniere proprietary JSON API |
| `kinder_morgan` | NGPL + TGP + KMLP | new, small | ASP.NET WebForms + Infragistics |
| `eia_storage` | EIA weekly storage, 8 regional series | 450 weeks | EIA v2 API |
| `eia_lng_exports` | EIA monthly LNG exports by country | 65 months | EIA v2 API |
| `eia_supply` | EIA monthly supply | 77 months | EIA v2 API |
| `gie_agsi` | European storage | 2,062 days | GIE AGSI+ API |
| `baker_hughes_weekly` | Rig counts | 138 weeks | Baker Hughes file |

**Shelved:** `williams` (Transco). Williams migrated 1Line to an Angular SPA; the legacy
JSP endpoints return an SPA shell for every path including `/api/definitely-not-real-xyz`.
Proven dead, not blocked. Code + `VERDICT.md` retained in case a public API appears.

### 2.2 LNG terminal coverage — 9 terminals

| Terminal | Headline meter | Nameplate | Status |
|---|---|---|---|
| Plaquemines | `gator_express_sq_vgpqd_d` | 3,400 | measured (~113%, real debottleneck) |
| Corpus Christi | `corpus_christi_sq_CC200221_d` | 2,400 | measured |
| Calcasieu Pass | `trans_cameron_sq_vgcpd_d` | 1,300 | measured (~122%) |
| Cameron | `cameron_interstate_sq_772300_d` | 2,000 | **measured-PARTIAL (~73%)** |
| Sabine Pass | `creole_trail_sq_CT200111_d` | 4,500 | **measured-PARTIAL (~31%)** |
| Freeport | `gulf_south_sq_24329_d` + `tetco_sq_79999_d` | 2,100 | **measured-PARTIAL (~53%)**, multi-feed |
| Cove Point | `cpl_sq_10001_d` (plant intake) | 750 | measured |
| Golden Pass | `golden_pass_sq_1097217_d` | 2,600 | measured (commissioning ramp) |
| Port Arthur | — | 1,900 | not operational (2027) |

Fleet aggregate ≈ **12,825.9 MMcf/d** (60-day median of complete-day sums; latest complete day 2026-09-01 reached 13,913.8 MMcf/d), ~67.3% of 19,050 MMcf/d operational nameplate (61.2% of 20,950 MMcf/d total nameplate including non-operational Port Arthur). This figure is an **interstate-visible floor**, not a full physical census: Sabine Pass (~30%), Freeport (~53%), and Cameron (~73%) are measured-partial at their public interstate delivery meters; unmeasured pipeline corridors feed the remainder. Plaquemines debottlenecked to ~3,885 MMcf/d and Golden Pass commenced commissioning at ~359 MMcf/d.

**Known invisible gas:**
- Freeport's KMTP intrastate lateral (~400–450 MMcf/d capacity, not publicly posted).
- Sabine Pass's non-CTPL feeds (~69% of terminal nameplate — Transco Zone 3 migrated to Williams private SPA; other intrastate).
- Cameron's Columbia Gulf Transmission extension (~500 MMcf/d capacity / ~27% of nameplate, FERC CP15-514, TC Energy EBB not scraped).
All carry prominent UI caveats. **Do not remove those caveats.**

### 2.3 Dashboard sections

1. Storage (EIA + GIE European)
2. Transatlantic storage divergence
3. European storage panel
4. US LNG exports tracker (EIA monthly)
5. **LNG Feedgas Observatory** — 9-terminal fleet grid + hero chart
6. **Basin Egress** — Haynesville corridors, share-in-use
7. **Feed Substitution** — multi-feed routing detection

### 2.4 Infrastructure

- **Publishing:** `publishers/export_dashboard_json.py` → per-source shards + `index.{hash}.json` + `manifest.json`. Lazy loading: boot on 3 core sources, panels fetch shards on demand. Boot parse ~123 ms (was ~1,000 ms).
- **Bundle size:** ~67 MB raw, **~3.2 MB gzipped** over the wire (GitHub Pages compresses). **There is no size problem.** An earlier session invented a "60 MB cap" that doesn't exist and nearly pruned real history over it. If size comes up again, measure gzip transfer and throttled parse time, not raw bytes.
- **Guards, layered:** transformer shrinkage guard → curated integrity monitor → bundle coverage audit → registry↔config↔bundle agreement gate. Each was added after a bug got past the previous layer.
- **CI:** GitHub Actions, per-source scraper workflows on NAESB-aligned crons, `publish-dashboard.yml` rebuilds on `workflow_run`.
- **Tests:** ~391 passing. Full suite ~126 s. `-m "not network"` excludes network tests.

---

## 3. Architecture

```
scrapers/
  base/            # HttpClient, SafeWriter, HealthWriter, identity.py, headers.py — FROZEN, all inherit
  {source}/        # one package per platform
transformers/
  base/accumulate.py   # merge_into_curated + shrinkage guard — MANDATORY for every transformer
  {source}.py
validators/
  integrity.py     # per-source checks: shrinkage, stagnation, coverage, gaps, value sanity, schema, divergence
  collision.py     # (series_id, period) key collision detection
publishers/
  export_dashboard_json.py   # relevance pruning, shards, coverage + agreement audits
config/
  meters/{source}.json       # meter allowlists with confidence tiers
  meters/classification.json # 812 meters classified (storage/basin_egress/power_burn/lng_export/...)
  integrity_rules.yaml       # per-source thresholds
docs/                        # the dashboard — VANILLA JS ONLY
  js/util/lng-terminals.js   # THE terminal registry — single source of truth
  js/util/lng-metrics.js     # LNG_NAMEPLATE_MMCFD with FERC docket citations
  js/panels/*.js
data/
  raw/{source}/    # gitignored — ephemeral runners see it EMPTY
  curated/*.parquet # committed, the real archive
  health/*.json    # committed, per-source status
```

### Canonical data schema

`source | series_id | series_name | period | value | unit | region | ingested_at`

**Series ID format (non-negotiable):**
```
{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}
```
`flow ∈ {r, d}`. Example: `tetco_sq_79999_d_timely`.

Omitting `flow` caused a silent overwrite across five sources that destroyed 27,015 rows.
Any dimension present in the source data but absent from `series_id` is a potential silent
overwrite. `validators/collision.py` now guards this.

**Units:** values stay RAW `Dth/d` (= MMBtu, 1:1) in Python, always. Convert only in the
frontend: `mmcf = dth / 1.025 / 1000`. A KM transformer once converted at ingest and
created a double-conversion hazard.

---

## 4. Non-negotiable conventions

1. **Series keys carry flow direction.** Both legs of bidirectional meters emitted separately. Never arbitrate "delivery wins."
2. **`merge_into_curated` always.** Never `safe_write_parquet` directly over a curated file. The shrinkage guard refuses to write a smaller frame.
3. **RAW Dth/d in Python. Convert in JS only.**
4. **`assert_response_identity` on every multi-tenant response** — but verify on a response type that structurally contains the marker (GET page with `<title>`, not an AJAX delta), and match loosely on a stable token (`"CREOLE TRAIL"`, not `"CTPL"`). Operators rename without notice.
5. **`HealthWriter.record_no_op`** on zero-record runs (WARN, FAIL at 3 consecutive). **`record_guard_failure`** when a guard raises — guards that fail silently are their own bug class.
6. **Vanilla JS only in `docs/`.** Zero TypeScript syntax in executable code (JSDoc fine). Verify before every commit:
   ```
   grep -rnE ": (string|number|boolean|any)\b|interface |\bas (string|number|HTMLElement)" docs/js/
   ```
   Only JSDoc comment hits are acceptable. This is a recurring agent failure mode that silently breaks rendering.
7. **Design tokens only** in CSS. `safeRender` wraps every panel. Mobile reflow at 390px.
8. **Confidence tiers** in `config/meters/*.json`: only `high` survives publisher pruning. If the registry names a series as a headline, it MUST be `high` — the agreement gate enforces this now.
9. **Nameplates need FERC docket citations** in JSDoc. They're the denominator of every utilization figure.
10. **Gates before push:** `pytest`, `ruff check .`, `mypy --strict` on new files. Known pre-existing: 35 mypy errors in `scrapers/base/playwright_client.py`, some ruff in `tests/test_gie_agsi_scraper.py`. Don't fix those; don't let them mask new ones.

---

## 5. Domain knowledge that isn't obvious

- **Gas day / cycles.** NAESB nomination cycles: Timely, Evening, Intraday 1/2/3. Posting timestamps do NOT equal gas day — Timely/Evening/ID1 posted on day P are for gas day P+1; ID2/ID3 are same-day. Anchoring to wall clock instead of the served posting stamp caused a real mislabeling bug.
- **Not every platform posts every cycle.** Boardwalk publishes only ID1/ID2/ID3 as CSV (never Timely/Evening). Cheniere posts no SQ page but *does* return SQ in its API. Platform limits must be surfaced in the UI, not silently smoothed.
- **OAC is usually a residual.** On Gulf South, `Operationally Available Capacity = posted capacity − scheduled quantity`. Pearson(TSQ, OAC) = **−1.0000 exactly** on Lonewa; TSQ+OAC constant to 0.01% CV. **Therefore `TSQ ÷ operating_capacity` is meaningless** — it divides flow by (capacity − flow) and produces >100% readings on 90 of 90 days. Use `share-in-use = TSQ/(TSQ+OAC)`, bounded [0,1] — but know it ranks days identically to volume (Spearman 0.999). The genuinely new signal is the **OAC level itself** (capacity swings = maintenance/constraint events).
- **Twin meters.** When two operators meter the same handoff, both publish it. `cpl_sq_47001_r ≡ egts_sq_40704_d` (mean diff 204 Dth/d, r=0.9991). Summing both double-counts. **Always run a twin check before summing multi-feed terminals.**
- **Pass-through.** Cove Point's pipeline (CPL) is not a dedicated feedgas pipe — ~37% of its throughput goes to LDC/power customers and never reaches the plant. Summing receipts gives *pipeline throughput*, not feedgas. The honest number came from the terminal's own consolidated plant-intake meter (`cpl_sq_10001_d`). **Look for a plant-intake meter before summing feeders.**
- **Zeros are often real.** Cargo-driven terminals idle. Cove Point has 446 zero-days. Turnarounds happen. Never treat zero as missing — but DO distinguish "posted zero" from "did not post" (posting gaps masquerading as zeros produced 8 fake substitution events once).
- **Terminals can exceed nameplate.** Plaquemines runs ~113%, Calcasieu ~122% via optimization debottlenecking. Real, corroborated by cargo schedules. Flag for awareness; don't "correct."
- **EBB retention varies wildly.** Boardwalk ~90 days. Quorum back to 2021. Enbridge 3 years. BHE 39 months. **Every day without a working scraper is history permanently lost** on short-retention platforms.

---

## 6. Bug catalog — the eight silent data-loss bugs

All eight shipped wrong data to production. All eight reported healthy. Read these as
patterns, not history.

| # | Bug | Mechanism | Cost | Guard now |
|---|---|---|---|---|
| 1 | **Accumulation overwrite** | Transformer rebuilt curated parquet from `data/raw/` which is gitignored; ephemeral runners saw it empty → committed a 1-day parquet every run | 3 months of Freeport history, unrecoverable | `merge_into_curated` + shrinkage guard |
| 2 | **Flow-leg collision** | `series_id` omitted `flow_ind`; R and D rows collided, surviving leg varied run-to-run | 27,015 rows across 5 sources | flow token in key + `collision.py` |
| 3 | **Silent no-op** | Scraper requested a cycle/gas-day combo that never matched; logged "skipped", stamped health `ok` | 2 days frozen, undetected | `record_no_op` + streak escalation |
| 4 | **EIA `area-name` corruption** | EIA's own API returns `"DEU"` for both Germany and Finland; scraper trusted it, dropped `duoarea` | Germany's history wrong ~40% of months | `duoarea→ISO3` map from series descriptions |
| 5 | **Double-space header** | Boardwalk CSV header is `Operationally  Available Capacity` (two spaces); exact-match lookup missed it | 205k OAC rows dropped for months | `normalize_header` + `resolve_columns` |
| 6 | **Parquet→bundle gap** | Curated was full; the publisher's relevance prune shipped 5% of it. Integrity monitor watched *curated*, so nothing fired | gasnom 95% thinned in the UI | `_audit_bundle_coverage` (deploy-blocking) |
| 7 | **Confidence-tier disagreement** | Registry named `CT200111-D` as Sabine's headline; `cheniere.json` marked it `comparison`; publisher ships only `high` → panel lost its number | Sabine rendered empty | `_audit_registry_bundle_agreement` |
| 8 | **Guard fails silently** | `assert_response_identity` raised on an AJAX delta (no `<title>` by construction) and on a renamed TSP string → every pull died, looked like a generic failure | KM + Cheniere froze 2 days | `record_guard_failure` + streak ladder |

**The pattern:** two layers disagree, nothing asserts they agree, and the failure is
*quiet*. When adding anything new, ask: what would this look like if it silently did
nothing? Then make that state loud.

**Also relevant:** a "tenant fallback" trap — Kinder Morgan's EBB serves the TGP page
(HTTP 200, wrong entity) for unknown tenant codes. Scraping blind would have produced TGP
data labeled as Elba Express.

### 6.1 Bug #9 — an outage dressed as coverage drift (2026-10-28)

Same pattern as the eight above, caught before it retired the publish job.

**What happened.** Cove Point's plant-intake meter (10001-D) went to its 15,000 Dth/d
floor on 2026-09-19 and stayed there for 23 days. The coverage guard judges every claim
on a rolling 60-day median of feedgas ÷ nameplate. A 60-day window holding 23 dead days
has a median of *operating days*, so it measured **84.8%** against the registry's **97.1%**
claim — 12.3% drift against a 7% tolerance. The guard read "the registry is stale", which
is a FAIL, which blocks publishing. Nothing was structurally wrong: loading the live
bundle shows 725.6–847.8 MMcf/d before the outage and the same kind of level after.

**Why the naive fix is wrong.** "Exclude days at the floor, then take the median of the
window" is right, but *where* the exclusion happens matters. Filtering the whole history
first and then taking the last 60 *operating* days silently shortens the window and does
not match what the claim means. The exclusion belongs **inside** the window:
sample = last `window_days` calendar days, minus floor days. Then:

- `days + outage_days == window_days` is an invariant you can assert (the tests do).
- The median still describes "the last N days the terminal was running", which is the
  structural statement `expectedCoveragePct` actually makes.
- Being at the floor is a *state*, not a value: 5% of the terminal's own operating median
  (`FLOOR_PCT`), floored at 50 MMcf/d absolute. The same 5% that
  `docs/js/util/lng-downtime.js` already uses to classify OFFLINE.

The rule now lives in one place — `compute_terminal_coverage_from_curated` in
`tests/test_coverage_guard.py` — which preflight, the guard tests and the dashboard all
call, so there is no second copy to drift.

**It is not a blanket pass.** Verified before shipping: doubling a terminal's nameplate
(a visible meter lost) still fails on all five terminals measured above 30% of nameplate.
Golden Pass is the exception and stays one: at 12.7% coverage a 15% tolerance cannot fire
in either direction, so the test excludes it rather than pretending the claim is tight.

**The general lesson, stated as a question to ask of any rolling-window guard:** *what
does this window mean when the entity it measures is switched off?* A median over a
window that is part healthy and part dead moves with the outage **length**, not with any
change in the thing you claim to be guarding. Any new guard that divides by a trailing
median inherits this bug.

**Related state in this checkout.** The BHE bundle is frozen at 2026-09-19 (the last
local day carries Cove Point's single floor value). That is a stale-scraper artifact of
this machine, not live data — before treating any outage number here as real, re-pull
the bundle. `scripts/preflight.py Case 5` still asserts 0 OFFLINE events for Cove Point
because the frozen tail is one day and its `zeroDaysThreshold` is 3; on a live bundle
with the outage still running, that case is expected to flip and should be re-pointed at
the historical window instead of deleted.

---

## 7. Things deliberately killed (do not resurrect without new data)

- **EIA weekly storage nowcast.** Daily pipeline storage nominations cover ~4% of the std-dev of the weekly EIA print. Headline regression R²=0.53 dissolved to R²=0.01 on the *surprise*; a naive persistence model beat it 11.7× on MSE. n=11 weeks, injection season only. Full study: `analysis/storage_nowcast_research.md` on branch `research/storage-nowcast`.
- **Storage utilization panel.** Storage TSQ is nomination flow, not inventory. Net flows noise-dominated (Petal −126,540 ± 176,299 Dth/d, CVs 78–949%).
- **`TSQ ÷ operating_capacity` as "utilization."** See §5 — OAC is a residual.
- **Transco / Williams scraper.** Endpoints gone, not blocked.
- **Own-built pipeline notice scraper.** Enercast's free tier covers severity 4–5 notices adequately. Blue Tide is the *flow* layer, not the notice layer. (Ingesting Enercast's free digest as a cross-reference input is still a reasonable future idea.)

---

## 8. Open work

### 8.1 In flight — Section 8: Terminal Downtime / Turnaround Indicator

**Shipped in this working tree, pending commit** (see §6.1 for the coverage-guard half):
- Floor-class rescue in `detectDowntime`: a meter pinned at ≤5% of baseline for N days is
  OFFLINE, not "depressed then ramp" (Cove Point's 10001-D sat at 2% of nameplate).
- Baselines exclude floor days, so a long outage no longer drags its own 30-day median
  onto the floor and makes the recovery read as a ~50× RAMPING event.
- `activeOutage()` in `lng-fleet-data.js` + a status line on the fleet card, so "shut in"
  is not rendered as "running soft".
- Chapter navigation (`docs/js/components/chapter-nav.js`): sections are grouped into
  chapters, and the lazy-load idle path is gated on the visible chapter so opening the
  LNG chapter no longer pulls every other source shard.

### 8.1a In flight — exports that match the screen

- **Chapter-scoped export.** The chapter download used to call `loadBundle()` again, which
  boots a *second* bundle (manifest + index + the three CORE shards) — not the one the page
  renders. A reader exporting Macro refetched storage/supply and never fetched `gie_agsi`,
  the chapter's own source. The export now takes the live bundle handed over on
  `bluetide:bundle` (dispatched by `main.js` before any panel work) and loads only the
  chapter's declared sources through `ensureSource` (`docs/js/util/chapter-sources.js`),
  reusing anything already parsed. Provenance carries `source_load: {fetched, cached,
  failed}` so a missing shard reads as a named failure, never as an empty series.
- **Per-chart CSV.** Panels register plotted points with `registerChartSnapshot`; one
  delegated handler in `main.js` turns any `[data-chart-export]` button into a download of
  the rows that were actually drawn (post-range, post-rollup).
- **`gulf-south-sq.yml` permissions.** Commits push to `main`, but the workflow declared
  `contents: read` and relied on the job-level block, which is not how GitHub resolves it.
  Now `contents: write` at the top level.

Original spec, retained because the validation cases are still the right ones:
- 30-day trailing median baseline per terminal
- `DEPRESSED`: <60% baseline for ≥5 consecutive days
- `OFFLINE`: consecutive posted-zeros (≥2 days; ≥3 for cargo-driven Cove Point)
- `RAMPING`: sustained positive trend in the baseline itself (NOT a spike above it — Golden Pass's rising median makes ratio-based detection fail)
- Multi-feed routing (from Section 7) must suppress downtime flags: one feed drops, total holds = routing, not downtime

**Mandatory validation before shipping** — these four cases have known answers:
1. Freeport 2026-07-15 dip to 142 MMcf/d → should flag as DEPRESSED
2. TETCO 2024-04-11 zero-day (documented real outage) → should flag OFFLINE
3. Plaquemines pre-first-gas zeros in 2024 → must classify NOT-YET-OPERATIONAL, not outage
4. Cove Point's 446 zero-days → must classify cargo-idle, not outage

Also: distinguish "posted zero" from "did not post." Report event counts per terminal
across full history — dozens per terminal per year means thresholds are wrong.

### 8.2 Backlog, roughly prioritized

- **Fleet vs EIA monthly leading-indicator study.** Blocked: full-fleet history starts ~2026-05-25, EIA LNG series ends 2026-05-01 → zero overlapping months. Revisit at 6+ months of overlap (≈ early 2027). A narrow version is testable now: Venture Global (Quorum, back to 2021) vs EIA monthly, ~20 points. Research, not a panel.
- **NGPL 3592.** Currently `kind:'context'` at 0.0. Posted TSQ 472,702 + OAC 27,298 = exactly 500,000 design capacity during recon on 2026-08-24 — an internally consistent real reading. Now idle. If it returns non-zero, the agreement gate flags it; promote back to `measured` citing the dated evidence.
- **Corpus Christi cycle pinning.** Solved for KM (Infragistics `clientState` is a pipe-delimited delta with an embedded JSON change-log, plus `__ASYNCPOST`/`X-MicrosoftAjax: Delta=true` markers). Historical dated pulls are unsupported — the scraper refuses rather than mislabeling.
- **Power burn panel.** 24 meters already classified in `classification.json`, never used.
- **AISStream vessel tracking.** Fully specced months ago (Gulf geofence lat 27–30.5 / lon −98 to −88, `ship_type=80`, Fly.io hourly push, memory-only state). Deferred — was originally a proxy for feedgas, which is now measured directly. Still useful for cargo timing.
- **Cross-project synthesis.** Blue Tide + Weather Desk + Blue Pulse + Blue Flux. Nobody stitches physical flows × weather × positioning × curve. Big idea, no design yet.

---

## 9. Operating notes for Claude Code

- **You are the only agent in this repo.** A parallel-agent phase in August corrupted the working tree (commits on wrong branches, `git stash` eating uncommitted work, two files that silently failed to write). If parallelism is ever needed again: `git worktree`, never multiple clones, never two agents in one checkout.
- **Verify against production, not reports.** Useful commands:
  ```bash
  curl -s https://yieldchaser.github.io/Supply-Demand-Flows/data/manifest.json
  # then fetch index.{hash}.json → per-source shard files
  python -c "import pandas as pd; df=pd.read_parquet('data/curated/gulf_south.parquet'); print(len(df), df.period.min(), df.period.max())"
  python -m validators.run_integrity
  gh run list --limit 20
  ```
- **The `lng-feedgas-audit` skill** (`data-science/lng-feedgas-audit/SKILL.md`) encodes the twin-meter check, mass-balance test, confidence-tier agreement rule, pass-through detection, identity-marker discipline, and the gate-fires-fix-the-cause rule. Use it before promoting any terminal.
- **`docs/VERDICT.md`** records shelving decisions and topology findings. Keep it current.
- **Branch per task, merge, delete.** `feat/transco-williams`, `research/storage-nowcast`, and recon branches are intentionally unmerged.
- **Data commits separate from code commits.**

---

## 10. Honest assessment of what's weak

- **Sabine at ~31% coverage** is the biggest analytical hole — it's the largest US terminal and we see a third of it. Worth periodically re-checking whether Cheniere's API exposes a consolidated plant-intake meter (the Cove Point pattern) that would fix this in one move.
- **Corpus and Sabine occasionally lag** other sources by a day or two. Cheniere's scraper has been the most fragile.
- **Kinder Morgan history is thin** (days, not months) and historical pulls are unsupported by the platform.
- **No alerting on analytical signal** — only on pipeline health. A feedgas drop >20% at a terminal should probably ping Telegram; the bot already exists.
- **The dashboard measures but doesn't yet interpret.** Sections 5–7 are excellent instrumentation. Section 8 (downtime) is the first thing that tells a reader *what to think*, which is also why it needs the strictest validation.
