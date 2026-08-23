# Collision-guard output against current data

## Curated parquets (post-dedup — residual duplicates)

| source | verdict | detail |
|---|---|---|
| gulf_south | **PASS** | no collisions: 192,704 distinct (series_id, period) keys, each from 1 row |
| gie_agsi | **PASS** | no collisions: 81,160 distinct (series_id, period) keys, each from 1 row |
| baker_hughes | **PASS** | no collisions: 32,649 distinct (series_id, period) keys, each from 1 row |
| eia_storage | **PASS** | no collisions: 408 distinct (series_id, period) keys, each from 1 row |
| eia_lng_exports | **FAIL** | SILENT-OVERWRITE risk: 43 of 2,612 (1.65%) (series_id, period) keys map from MULTIPLE raw rows with DIFFERING values; 10 more keys have benign identical repeats |
| eia_supply | **PASS** | no collisions: 462 distinct (series_id, period) keys, each from 1 row |
| gasnom | **PASS** | no collisions: 53,360 distinct (series_id, period) keys, each from 1 row |
| quorum | **PASS** | no collisions: 116,858 distinct (series_id, period) keys, each from 1 row |
| bhe | **PASS** | no collisions: 2,508 distinct (series_id, period) keys, each from 1 row |
| cheniere | **PASS** | no collisions: 6,072 distinct (series_id, period) keys, each from 1 row |

## gulf_south pre-dedup reconstruction (proves the guard catches the original bug)

- pre-dedup rows: 394,598 → keys: 369,674
- verdict: **FAIL**
- SILENT-OVERWRITE risk: 15,991 of 369,674 (4.33%) (series_id, period) keys map from MULTIPLE raw rows with DIFFERING values; 8,933 more keys have benign identical repeats. The accumulator will keep only the last row per key. Worst: gulf_south_oac_22133_id3@2026-08-04: [np.float64(1137997.0), np.float64(1137998.0)]; gulf_south_oac_23337_id1@2026-05-25: [np.float64(3000.0), np.float64(54449.0)]; gulf_south_oac_23337_id1@2026-05-26: [np.float64(2998.0), np.float64(49360.0)]; gulf_south_oac_23337_id1@2026-05-27: [np.float64(3000.0), np.float64(43748.0)]; gulf_south_oac_23337_id1@2026-05-28: [np.float64(3000.0), np.float64(25035.0)]

Named keys (first 25 of the FAIL details):

| series_id | period | distinct values |
|---|---|---|
| gulf_south_oac_22133_id3 | 2026-08-04 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-25 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-26 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-27 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-28 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-29 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-30 | 2 |
| gulf_south_oac_23337_id1 | 2026-05-31 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-01 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-02 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-03 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-04 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-05 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-06 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-07 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-08 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-09 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-10 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-11 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-12 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-13 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-14 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-15 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-16 | 2 |
| gulf_south_oac_23337_id1 | 2026-06-17 | 2 |