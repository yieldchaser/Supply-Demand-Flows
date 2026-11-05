/**
 * Transatlantic Storage Divergence — interlude card between Section 1 and Section 2.
 *
 * Derived metric: US storage deviation from 5y avg (%) vs EU aggregate deviation (pp).
 * Spread = us_signal − eu_signal → drives gauge needle + regime classification.
 *
 * Data sources:
 *   bundle.sources.eia_storage — US weekly storage (Lower 48 or aggregate)
 *   bundle.sources.gie_agsi   — 10 EU countries daily % full
 */

import * as d3 from 'd3';
import { getSeries } from '../util/series.js';
import { seasonalEnvelope, envelopeLookup, dayOnly } from '../util/season.js';
import { seasonWeekOf } from '../util/storage-analysis.js';
import { parsePeriod, formatPeriodKey } from '../util/period.js';

/* ─────────────────────────────────────────────── */
/*  Constants                                      */
/* ─────────────────────────────────────────────── */

const EU_COUNTRIES = ['de', 'fr', 'it', 'nl', 'es', 'at', 'be', 'cz', 'hu', 'pl'];

// Gauge range in pp spread units
const GAUGE_MIN = -30;
const GAUGE_MAX = 30;

/* ─────────────────────────────────────────────── */
/*  Entry point                                    */
/* ─────────────────────────────────────────────── */

/**
 * Compute the transatlantic divergence view model.
 *
 * Pure: no DOM. Exported so the chapter export can ship the exact numbers the
 * card shows next to the rows that produced them, and so the model can be
 * exercised without a browser.
 *
 * @param {Object} bundle
 * @returns {{usLatest: any, us_signal: (number|null), euLatest: any, eu_signal: (number|null),
 *   spread: number, needlePct: number, regime: any} | null} null when either
 *   side is unavailable
 */
export function buildDivergenceModel(bundle) {
  const usSource = bundle.sources?.eia_storage;
  const euSource = bundle.sources?.gie_agsi;
  if (!usSource?.data?.length || !euSource?.data?.length) return null;

  // Extract Lower 48 total per period (max of 8 region rows = the aggregate).
  // Same workaround as storage.js — see Phase 2 backlog: fix transformer to
  // emit region-tagged rows so we don't need this trick.
  // Keyed on the parsed calendar date, not the raw string: the same week was
  // grouped twice when a source mixed label formats, and `new Date()` read a
  // bare `YYYY-MM-DD` as UTC midnight (a day early west of Greenwich).
  const usByPeriod = d3.rollup(
    usSource.data,
    (g) => d3.max(g, (r) => Number(r.value)),
    (r) => parsePeriod(r.period)?.getTime()
  );
  const usSeries = Array.from(usByPeriod, ([t, value]) => ({
    period: new Date(t),
    value,
  }))
    .filter((r) => Number.isFinite(r.period.getTime()) && Number.isFinite(r.value))
    .sort((a, b) => a.period - b.period);

  // Same week-of-season axis as the storage panel (see util/season.js): the ISO
  // week of a Friday label steps by one more than the mean between neighbouring
  // weeks, so an ISO lookup returned a "+2.5% vs normal" that became "-0.1%" one
  // week later. Season weeks are measured from each year's own first observation.
  const usEnv     = seasonalEnvelope(usSeries, { anchor: dayOnly(usSeries[usSeries.length - 1].period) });
  const usLatest  = usSeries[usSeries.length - 1];
  const usWk      = seasonWeekOf(usSeries, usLatest.period);
  const usNormal  = envelopeLookup(usEnv, usWk);
  const us_signal = usNormal && usNormal.mean > 0
    ? ((usLatest.value - usNormal.mean) / usNormal.mean) * 100
    : null;

  /* ── EU: capacity-weighted aggregate current vs 5y avg ────────── */
  const { euAggSeries, euLatest } = _buildEuAggregate(euSource.data);

  const euEnv     = euLatest ? seasonalEnvelope(euAggSeries, { anchor: dayOnly(euLatest.period) }) : null;
  const euWk      = euLatest ? seasonWeekOf(euAggSeries, euLatest.period) : null;
  const euNormal  = euLatest ? envelopeLookup(euEnv, euWk) : null;

  // eu_signal: pp deviation from seasonal mean (positive = above avg = loose)
  const eu_signal = (euLatest && euNormal) ? euLatest.value - euNormal.mean : null;

  /* ── Spread + gauge ───────────────────────────────────────────── */
  // A missing side is unknown, not zero: pretending EU is "at normal" when its
  // envelope failed turned a data gap into a confident regime call.
  const spread      = (us_signal != null && eu_signal != null) ? us_signal - eu_signal : null;
  const needlePct   = spread == null
    ? 50
    : Math.max(0, Math.min(100, ((spread - GAUGE_MIN) / (GAUGE_MAX - GAUGE_MIN)) * 100));
  const regime      = classifyRegime(us_signal, eu_signal);

  return {
    usLatest, us_signal, euLatest, eu_signal, spread, needlePct, regime,
    usNormal: usNormal ? { weekOfSeason: usWk, n: usNormal.count, mean: usNormal.mean } : null,
    euNormal: euNormal ? { weekOfSeason: euWk, n: euNormal.count, mean: euNormal.mean } : null,
  };
}

/**
 * Compact, JSON-safe projection of the model for downloads.
 *
 * Dates become ISO day strings and the classification pair travels with the
 * numbers, so a reader can tell *why* the card said bullish without reopening
 * the thresholds in source.
 *
 * @param {ReturnType<typeof buildDivergenceModel>} model
 * @returns {Object|null}
 */
export function divergenceExport(model) {
  if (!model) return null;
  // Calendar day, not the UTC instant: `toISOString().slice(0, 10)` reported the
  // previous day for a local-midnight period in any timezone east of Greenwich.
  const day = (d) => formatPeriodKey(d?.period ?? null);
  return {
    basis:
      'US: % deviation of latest weekly total from the same week-of-season mean over the trailing 5 years. ' +
      'EU: capacity-weighted % full (10 AGSI countries) minus the same week-of-season 5y mean, in percentage points. ' +
      'Spread = US% − EUpp. Weeks are counted from each year\'s first observation, not ISO week numbers.',
    thresholds: { us_loose: 3, us_tight: -3, eu_loose: 5, eu_tight: -5, gauge: [GAUGE_MIN, GAUGE_MAX] },
    normal: { us: model.usNormal, eu: model.euNormal },
    us: { period: day(model.usLatest), bcf: model.usLatest?.value ?? null, signal_pct: model.us_signal },
    eu: { period: day(model.euLatest), full_pct: model.euLatest?.value ?? null, signal_pp: model.eu_signal },
    spread_pp: model.spread,
    regime: { label: model.regime.label, kind: model.regime.kind, detail: model.regime.detail },
  };
}

export function renderDivergencePanel(panelEl, bundle) {
  const model = buildDivergenceModel(bundle);

  if (!model) {
    panelEl.innerHTML = '<div class="panel-error">Storage data unavailable for transatlantic comparison.</div>';
    return;
  }

  const { usLatest, us_signal, euLatest, eu_signal, spread, needlePct, regime } = model;

  /* ── Sample size of each seasonal normal ("5y avg" is a window, not a promise) */
  const normalNote = (row, normal) => {
    if (!row) return 'no observation';
    const period = row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    return normal ? `${period} · normal n=${normal.n}` : `${period} · no normal`;
  };

  /* ── Date label (prefer EU daily freshness) ───────────────────── */
  const displayDate = (euLatest ?? usLatest)?.period.toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
  }) ?? '—';

  /* ── Render ───────────────────────────────────────────────────── */
  panelEl.innerHTML = `
    <div class="divergence-card">
      <div class="divergence-card__header">
        <span class="divergence-card__title">Transatlantic Storage Divergence</span>
        <span class="divergence-card__date">${displayDate}</span>
      </div>

      <div class="divergence-card__body">
        <!-- US side -->
        <div class="divergence-side divergence-side--us">
          <div class="divergence-side__label">US</div>
          <div class="divergence-side__value num ${us_signal == null ? '' : us_signal >= 0 ? 'is-loose' : 'is-tight'}">
            ${us_signal != null
              ? (us_signal >= 0 ? '+' : '') + us_signal.toFixed(1) + '%'
              : 'n/a'}
          </div>
          <div class="divergence-side__sub">vs 5y avg</div>
          <div class="divergence-side__note">${normalNote(usLatest, model.usNormal)}</div>
        </div>

        <!-- Gauge -->
        <div class="divergence-gauge">
          <div class="divergence-gauge__bar">
            <div class="divergence-gauge__needle${spread == null ? ' divergence-gauge__needle--unknown' : ''}" style="left: ${needlePct.toFixed(2)}%"></div>
          </div>
          <div class="divergence-gauge__labels">
            <span class="divergence-gauge__edge divergence-gauge__edge--left">US loose</span>
            <span class="divergence-gauge__spread num">${spread == null ? 'n/a' : `${spread.toFixed(1)} pp spread`}</span>
            <span class="divergence-gauge__edge divergence-gauge__edge--right">EU tight</span>
          </div>
        </div>

        <!-- EU side -->
        <div class="divergence-side divergence-side--eu">
          <div class="divergence-side__label">EU</div>
          <div class="divergence-side__value num ${eu_signal == null ? '' : eu_signal >= 0 ? 'is-loose' : 'is-tight'}">
            ${eu_signal != null
              ? (eu_signal >= 0 ? '+' : '') + eu_signal.toFixed(1) + ' pp'
              : 'n/a'}
          </div>
          <div class="divergence-side__sub">vs 5y avg</div>
          <div class="divergence-side__note">${normalNote(euLatest, model.euNormal)}</div>
        </div>
      </div>

      <!-- Regime interpretation -->
      <div class="divergence-card__interpretation divergence-card__interpretation--${regime.kind}">
        <span class="divergence-card__icon">${regime.icon}</span>
        <span class="divergence-card__regime">${regime.label}</span>
        <span class="divergence-card__detail">— ${regime.detail}</span>
      </div>
    </div>
  `;
}


/* ================================================================== */
/*  EU aggregate builder (matches eu-storage.js approach exactly)     */
/* ================================================================== */

function _buildEuAggregate(rows) {
  const countrySeries = EU_COUNTRIES.map((cc) => {
    const fullPct   = getSeries(rows, `gie_storage_${cc}_full_pct`);
    const gasInStor = getSeries(rows, `gie_storage_${cc}_gas_in_storage`);
    const maxStorage = gasInStor.length
      ? Math.max(...gasInStor.map((r) => r.value))
      : 0;
    return { cc, fullPct, maxStorage };
  }).filter((c) => c.fullPct.length > 0 && c.maxStorage > 0);

  const allTimes = [
    ...new Set(countrySeries.flatMap((c) => c.fullPct.map((r) => r.period.getTime()))),
  ].sort((a, b) => a - b);

  const euAggSeries = allTimes.map((t) => {
    let weightedSum = 0;
    let weightSum   = 0;
    for (const c of countrySeries) {
      const match = c.fullPct.find((r) => r.period.getTime() === t);
      if (match) {
        weightedSum += match.value * c.maxStorage;
        weightSum   += c.maxStorage;
      }
    }
    return weightSum > 0 ? { period: new Date(t), value: weightedSum / weightSum } : null;
  }).filter(Boolean);

  const euLatest = euAggSeries.length ? euAggSeries[euAggSeries.length - 1] : null;
  return { euAggSeries, euLatest };
}

/* ================================================================== */
/*  Regime classifier                                                  */
/* ================================================================== */

function classifyRegime(us_signal, eu_signal) {
  // A side with no envelope has no verdict: `null > 3` and `null < -3` are both
  // false, so the fall-through used to print "Neutral transatlantic balance"
  // over a half-missing comparison.
  if (us_signal == null || eu_signal == null) return {
    label: 'Comparison Incomplete',
    detail: 'Seasonal normal unavailable for current period; transatlantic spread undefined.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
    kind: 'neutral',
  };

  const us_loose = us_signal > 3;   // US well above seasonal norm
  const us_tight = us_signal < -3;  // US well below seasonal norm
  const eu_loose = eu_signal > 5;   // EU well above seasonal norm
  const eu_tight = eu_signal < -5;  // EU well below seasonal norm

  if (us_loose && eu_tight) return {
    label: 'Favorable US LNG Export Arbitrage',
    detail: 'US storage in seasonal surplus while European storage entered injection season depressed.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>',
    kind: 'bullish',
  };
  if (us_tight && eu_tight) return {
    label: 'Transatlantic Inventory Deficit',
    detail: 'Depleted storage inventories across both basins; upward pressure on global gas benchmarks.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
    kind: 'warning',
  };
  if (us_loose && eu_loose) return {
    label: 'Global Inventory Surplus',
    detail: 'Inventories elevated across both basins; compressed transatlantic arbitrage and subdued spread volatility.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><line x1="8" y1="12" x2="16" y2="12"></line></svg>',
    kind: 'neutral',
  };
  if (us_tight && eu_loose) return {
    label: 'Inverted Transatlantic Spread',
    detail: 'US storage tight while European storage healthy; heightened sensitivity to export netbacks and curtailment risk.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="1 4 1 10 7 10"></polyline><polyline points="23 20 23 14 17 14"></polyline><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"></path></svg>',
    kind: 'warning',
  };
  return {
    label: 'Balanced Transatlantic Inventories',
    detail: 'Inventories within seasonal norms across both regions; transatlantic spread stable.',
    icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="5" y1="9" x2="19" y2="9"></line><line x1="5" y1="15" x2="19" y2="15"></line></svg>',
    kind: 'neutral',
  };
}
