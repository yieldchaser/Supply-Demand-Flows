# Curation-Integrity Report — EIA regions restored · collision audit · collision guard

Branch: `fix/curation-integrity` (own clone `bt-curation`). Companion to
`analysis/storage_nowcast_research.md` on `research/storage-nowcast`, which
surfaced both bugs.

## TASK 1 — EIA storage regional labels: RESTORED

**Root cause.** The EIA v2 facet `area-name` is literally `"NA"` on every row
of the natural-gas weekly storage dataset (`route=natural-gas/stor/wkly`). The
old transformer read it verbatim, so all 3,600 curated rows collapsed to
`region='NA'`, `series_id='storage'` — eight distinct series silently writing
to one key.

**The real geography lives in two other fields**, confirmed against the one
retained labeled raw file (`eia_storage_2026-04-10.json`, 408 rows / 51 weeks):

| raw `series` | `duoarea` | process | region label | curated `series_id` |
|---|---|---|---|---|
| NW2_EPG0_SWO_R48_BCF | R48 | SWO | Lower 48 (US total) | `storage_lower48` |
| NW2_EPG0_SWO_R31_BCF | R31 | SWO | East | `storage_east` |
| NW2_EPG0_SWO_R32_BCF | R32 | SWO | Midwest | `storage_midwest` |
| NW2_EPG0_SWO_R33_BCF | R33 | SWO | South Central | `storage_south_central` |
| NW2_EPG0_SWO_R34_BCF | R34 | SWO | Mountain | `storage_mountain` |
| NW2_EPG0_SWO_R35_BCF | R35 | SWO | Pacific | `storage_pacific` |
| NW2_EPG0_SNO_R33_BCF | R33 | SNO | South Central Nonsalt | `storage_sc_nonsalt` |
| NW2_EPG0_SSO_R33_BCF | R33 | SSO | South Central Salt | `storage_sc_salt` |

`series_id` is now unique per series, so the eight-way silent collision on
`series_id='storage'` is structurally gone.

### Verification

Re-run over the retained raw file (the only labeled raw in existence — older
raw was never archived; reported as a retention gap):

```
rows: 408 | weeks: 51
Five regions vs Lower 48: max |diff| = 1.0 Bcf over 51 weeks
SC Salt+Nonsalt vs SC:    max |diff| = 1.0 Bcf over 51 weeks
skipped_unlabeled: 0
```

Cross-checks:

- **Rank mapping agrees with the nowcast decode**: joining raw labels to the
  old parquet by (period, value) puts SNO→rank 0, SSO→rank 1, East→rank 2,
  Midwest→rank 3, South Central→rank 4, Mountain→rank 5, Pacific→rank 6,
  Lower48→rank 7 — exactly the empirical mapping from the research doc.
- **Full 450-week internal consistency of the old file** under this mapping:
  ranks {2..6} sum to rank 7 with mean |diff| 0.45 Bcf, max 2.0 Bcf (rounding:
  values are published to 1 decimal; 198 weeks differ by ≤1 Bcf, 4 by 2 Bcf);
  ranks {0,1} sum to rank 4 with max diff 1.0 Bcf.
- The relabeled parquet reproduces known WNGSR scale: e.g. week ending
  2026-04-10 → Lower 48 1,970 = East 283 + Midwest 371 + SC 839 + Mtn 210 +
  Pac 267 (+1 rounding); SC 839 = Nonsalt 596 + Salt 243.

Note: the curated history could not be re-stamped for all 450 weeks — raw
retention only holds one file. The transformer fix applies to every future
run; a one-time full backfill through the fixed pipeline will relabel the
complete 8-year history.

Commit: `fix(transformers): restore EIA storage regional labels from series/duoarea`

## TASK 2 — Dropped-dimension audit per source

Method: rebuild the pre-dedup key space from raw payloads and count rows per
(series_id, period). A key with >1 row and >1 distinct value = guaranteed
silent overwrite at accumulation time (`merge_into_curated` keeps one).

| source | dims in source data | dims encoded in series_id | dropped dims | colliding keys (real data) | keys w/ differing values |
|---|---|---|---|---|---|
| **gulf_south** | loc × cycle × flow_ind × loc_purp × qti × meas_basis × IT | loc + cycle + metric-kind | **flow_ind** (loc_purp mirrors it) | **24,924** | **15,991** across 50 meters |
| quorum | loc × cycle × flow_ind × purpose | pipe + kind + loc + cycle | flow_ind, purpose | 0 in curated (collapsed upstream) | 0 observed |
| gasnom | loc × cycle × flow_ind × purpose | pipe + kind + loc + cycle | flow_ind, purpose | 0 | 0 |
| bhe | loc × interconnect_party × flow_ind × cycle × meas_basis | loc + cycle + metric-kind | interconnect (filter), flow_ind (filter) | 0 (single-meter filter) | 0 |
| cheniere | loc × cycle × flow_ind × cap_type × process | pipeline + kind + loc(incl. -R suffix) + cycle | cap_type, meas_basis, IT | 0 (flow_ind pre-folded into loc id) | 0 |
| eia_storage (old) | series (8 regional) | constant `"storage"` | **region/series** | 8-way × 450 wks = **all rows** | fixed by Task 1 |
| eia_lng_exports | destination × period × units × process | dest + kind | units, process | **43** (`lng_export_deu`) | see below |
| gie_agsi | country × metric | country + metric | none material | 0 | 0 |

### The two live findings besides gulf_south

1. **gulf_south (owned by another agent — not fixed here).** 15,991 keys carry
   differing R/D values into one series_id. Worst examples:
   `gulf_south_sq_23337_id1@2026-05-25 ∈ {0, 25551}`,
   `gulf_south_oac_23337_id1@2026-05-25 ∈ {3000, 54449}`. Affected meters: 50.
   Full named-key table: `analysis/_collision_audit.txt`.
2. **eia_lng_exports — NEW bug found by this audit.** `lng_export_deu` has
   **43 keys with two differing values each** (e.g. 2022-12 ∈ {0.329, 7.112}
   Bcf; 2023-01 ∈ {0.0, 14.314}). Root cause traced to raw: the API returns
   TWO `EVE` rows per DEU-month (both volume-unit, different duoarea facets —
   the scraper's `$`-filter does not separate them and the raw payload drops
   `units`). Dedup kept an arbitrary one, so Germany's export history is
   wrong ~40% of months. Every other destination has exactly 1 row/month;
   totals/regions inherit the error via summation. Fix belongs with the
   lng_exports owner; the new guard now fails this source until then.

Everything else audits clean: zero residual duplicate keys post-dedup.

## TASK 3 — Collision guard shipped

New check `collision` in `validators/collision.py`, wired into
`run_source_checks` (now 8 checks/source):

- FAIL when ≥1 (series_id, period) key maps from multiple rows with differing
  values — message names up to 5 offending keys with their distinct value sets;
  details carry 25 more for dashboards/alert routing.
- PASS/WARN for identical-value repeats (benign re-scrapes), WARN above a
  configurable rate (`collision.identical_dup_warn_pct`).
- Per-source opt-out globs (`collision.ignore_series_globs`) for sources where
  duplicates are expected and handled upstream.

### Guard output against current data

| source | verdict |
|---|---|
| gulf_south, gie_agsi, baker_hughes, eia_storage (new), eia_supply, gasnom, quorum, bhe, cheniere | PASS |
| eia_lng_exports | **FAIL** — 43 of 2,612 keys (1.65%) multi-valued (the DEU bug above) |

And the proof it catches the original bug class — running the guard on the
reconstructed *pre-dedup* Gulf South frame (394,598 rows → 369,674 keys):

```
verdict: FAIL
SILENT-OVERWRITE risk: 15,991 of 369,674 (4.33%) (series_id, period) keys map
from MULTIPLE raw rows with DIFFERING values; 8,933 more keys have benign
identical repeats.
Worst: gulf_south_oac_23337_id1@2026-05-25: [3000.0, 54449.0]; ...
```

(see `analysis/_collision_guard_report.md`; reproducible via
`scripts/collision_guard_report.py`)

Also repaired en route: `tests/test_integrity.py::TestShippedRules` still
asserted the pre-271d8f0 source list and failed on main's own config; updated
to the active ten-source set.

## Gates

- pytest: **262 passed** (full suite)
- ruff: clean on all touched files
- mypy --strict on new/touched modules: clean (pre-existing
  `scrapers/base/playwright_client.py` errors unchanged, forbidden file)
