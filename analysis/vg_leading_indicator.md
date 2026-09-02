# Venture Global feedgas as a leading indicator for EIA monthly LNG exports

**Status:** killed as a *leading* indicator. Retained as a weak contemporaneous nowcast, worth
re-testing once Venture Global's share of the fleet stabilises.
**Computed:** 2026-09-02, from `data/curated/quorum.parquet` and `data/curated/eia_lng_exports.parquet`.

---

## Question

Venture Global's terminals are the only ones in this observatory with deep history — Quorum's EBB
goes back to 2021-03-15, where the rest of the fleet starts around 2026-05. Does their daily
nominated feedgas lead EIA's monthly US LNG export print, and by how much?

The full-fleet version of this question is blocked: fleet history begins ~2026-05 and the EIA
series ends 2026-05-01, leaving no overlapping months. This narrow version is testable now.

## Method

- **Feedgas:** `vgpqd_d` (Plaquemines, Gator Express) + `vgcpd_d` (TransCameron, Calcasieu Pass),
  scheduled quantity only. One value per feed per gas day, choosing the **latest genuinely
  nominated NAESB cycle** (`timely < evening < late < latec < id1 < id2 < id3`); automated hourly
  `id{HH}00` snapshots are placeholders and excluded. Daily `Dth/d` converted to Bcf
  (`dth / 1.025 / 1e6`) and summed to calendar months.
- **Exports:** `lng_export_total` from the EIA series, already in Bcf. **Not** the sum of the 45
  country series — that set also contains four regional aggregates and the total itself, so
  summing it double-counts (2026-04 sums to 1,307 Bcf against a true total of 436).
- **Overlap:** n = **54 months**, 2021-12 → 2026-05, restricted to months where VG actually flowed.
- Correlations are Pearson with Fisher-z 95% confidence intervals. Forecast comparison is 1-step
  ahead, expanding window, minimum 24 observations, 30 out-of-sample forecasts.

VG's share of US exports over the window: **mean 22.7%, most recent month 41.8%** — this is not a
small corner of the fleet, and the share is growing fast as Plaquemines ramps.

## Results

### Levels — strong, and misleading

| lag | n | r | R² | 95% CI | p |
|---|---|---|---|---|---|
| 0 | 54 | +0.795 | 0.633 | [+0.671, +0.877] | <0.0001 |
| 1 | 53 | +0.774 | 0.600 | [+0.638, +0.864] | <0.0001 |
| 2 | 52 | +0.769 | 0.592 | [+0.628, +0.861] | <0.0001 |

R² above 0.59 at every lag. This is the trap. Both series grew steeply over the window — US
exports from roughly 290 to 436 Bcf/month while Calcasieu Pass commissioned and Plaquemines came
online — so the correlation is measuring shared secular growth, not information.

### Surprise — first differences

| lag | n | r | R² | 95% CI | p |
|---|---|---|---|---|---|
| 0 | 53 | **+0.326** | 0.107 | [+0.062, +0.548] | **0.017** |
| 1 | 52 | −0.225 | 0.051 | [−0.470, +0.051] | 0.108 |
| 2 | 51 | +0.028 | 0.001 | [−0.249, +0.301] | 0.845 |

R² collapses from 0.633 to 0.107 — the same dissolution that killed the storage nowcast (0.53 →
0.01). The contemporaneous relationship is statistically significant but explains only ~11% of
month-to-month variance. **At one month of lead the sign flips negative and significance vanishes.**

### Out-of-sample forecast, 1-step ahead

| model | RMSE | MAE |
|---|---|---|
| naive persistence (`ŷ = y₍ₜ₋₁₎`) | 39.46 Bcf | 29.43 |
| VG-augmented (`ŷ = y₍ₜ₋₁₎ + β̂·Δvg`) | **38.52 Bcf** | 31.20 |

RMSE ratio 0.976 — a 2.4% improvement, while MAE gets *worse* (31.20 vs 29.43). A model that
improves one error metric and degrades another across 30 forecasts is not demonstrating skill.

## Verdict

**Killed as a leading indicator.** There is no lead. The lag-1 correlation is negative and
insignificant; nothing at lag 2. VG feedgas does not tell you what next month's export print will be.

**Retained as a weak contemporaneous signal.** The lag-0 surprise correlation (r = +0.326,
p = 0.017) is real but modest, and it is unsurprising on reflection: feedgas and exports measure
the same month's physical activity, so this is a *nowcast* of a number not yet published, not a
forecast. At 11% of variance explained it is too weak to headline a panel.

**Why it is weaker than it looks like it should be.** US monthly export variance is dominated by
events at terminals this study cannot see — Freeport turnarounds, Sabine maintenance, Corpus
weather delays — plus month-end cargo-loading boundary effects, where a vessel finishing on the 1st
rather than the 31st moves a whole cargo between months. VG's own feedgas is genuinely informative
about VG; it is a minority of the national number.

**Worth re-testing when:** VG's share has stabilised (it has moved from ~0% to 41.8% across this
window, which itself contaminates the differences), or when full-fleet history reaches 6+ months of
overlap with the EIA series — approximately early 2027 — at which point the fleet-wide version
becomes the better question and this one is superseded.

**Do not build a panel on this.** A contemporaneous R² of 0.107 rendered as a leading indicator
would tell readers something the data does not support.

## Reproduction

The figures above were computed directly from the curated parquets with the cycle-selection rule
described under Method. Anyone re-running this must apply the same rule — using every cycle, or
mixing `_oac_` rows into a scheduled-quantity total, produces different and wrong numbers.
