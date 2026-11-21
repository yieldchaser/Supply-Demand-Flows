/**
 * Transatlantic Storage Divergence — interlude card at the top of the Europe chapter.
 *
 * Both sides are measured in the same unit: the latest stock's % deviation from
 * the average stock at the same point of the year over the prior five years.
 *   US  EIA Lower-48 working gas (Bcf, weekly), same week-of-season mean.
 *   EU  10-country GIE AGSI+ total (TWh, daily), same calendar-day mean.
 * Spread = US % − EU %, in percentage points of those two percentages.
 *
 * The previous card subtracted the EU's deviation in *pp of fill* from the US
 * deviation in *% of stock* and printed the difference as "pp spread". Those
 * are different quantities (at 70 % full, −20 pp of fill is about −22 % of
 * stock, and the ratio changes with the fill level), so the gauge mixed units.
 * Its regime table also fell through to "Balanced … within seasonal norms"
 * whenever only one side was outside its band, so a Europe 20 pp below normal
 * next to a US at normal was labelled balanced. Every combination now has its
 * own reading. The EU total also used to be weighted by each country's maximum
 * stock; it now comes from the same engine as the European storage panel.
 *
 * Data sources:
 *   bundle.sources.eia_storage — US weekly storage (Lower 48)
 *   bundle.sources.gie_agsi   — 10 EU countries, daily
 */

import * as d3 from 'd3';
import { seasonalEnvelope, envelopeLookup, dayOnly } from '../util/season.js';
import { seasonWeekOf, priorSameWeekValues, endOfSeasonAnalog } from '../util/storage-analysis.js';
import { frame, yGrid, tooltip, hitArea, legend, fx, sx, tone } from '../util/eu-chart-kit.js';
import { parsePeriod, formatPeriodKey } from '../util/period.js';
import { getSeries } from '../util/series.js';
import { EU_COUNTRIES, buildCountryViews, buildAggregateView, positionalNormal, analogProjection } from '../util/eu-storage-analysis.js';

/* ─────────────────────────────────────────────── */
/*  Constants                                      */
/* ─────────────────────────────────────────────── */

/** Gauge range, percentage points of spread. */
const GAUGE_MIN = -30;
const GAUGE_MAX = 30;

/** A side is "outside normal" beyond these deviations, % of the average stock. */
export const DIVERGENCE_THRESHOLDS = { us: 3, eu: 5 };

/* ─────────────────────────────────────────────── */
/*  Model                                          */
/* ─────────────────────────────────────────────── */

/**
 * Compute the transatlantic divergence view model.
 *
 * Pure: no DOM. Exported so the chapter export can ship the exact numbers the
 * card shows next to the rows that produced them.
 *
 * @param {Object} bundle
 * @returns {Object|null} null when either side is unavailable
 */
export function buildDivergenceModel(bundle) {
  const usSource = bundle.sources?.eia_storage;
  const euSource = bundle.sources?.gie_agsi;
  if (!usSource?.data?.length || !euSource?.data?.length) return null;

  // Lower 48 total per period (max of the region rows = the aggregate).
  // Keyed on the parsed calendar date, not the raw string.
  const usByPeriod = d3.rollup(
    usSource.data,
    (g) => d3.max(g, (r) => Number(r.value)),
    (r) => parsePeriod(r.period)?.getTime()
  );
  const usSeries = Array.from(usByPeriod, ([t, value]) => ({ period: new Date(t), value }))
    .filter((r) => Number.isFinite(r.period.getTime()) && Number.isFinite(r.value))
    .sort((a, b) => a.period - b.period);
  if (!usSeries.length) return null;

  // Week-of-season axis, as the US storage panel (see util/season.js).
  const usEnv = seasonalEnvelope(usSeries, { anchor: dayOnly(usSeries[usSeries.length - 1].period) });
  const usLatest = usSeries[usSeries.length - 1];
  const usWk = seasonWeekOf(usSeries, usLatest.period);
  const usNormal = envelopeLookup(usEnv, usWk);
  const us_signal = usNormal && usNormal.mean > 0
    ? ((usLatest.value - usNormal.mean) / usNormal.mean) * 100
    : null;

  /* ── EU: 10-country total stock vs same-day 5-year mean stock ─── */
  const eu = buildEuTotal(euSource.data);
  const euLatestRow = eu?.latest ?? null;
  const euLatest = euLatestRow ? { period: euLatestRow.period, value: euLatestRow.pct, twh: euLatestRow.twh } : null;
  // Tolerance covers non-daily inputs; on daily AGSI the exact day is used.
  const euStockNormal = euLatestRow ? positionalNormal(eu.index, euLatestRow.period, { pick: (r) => r.twh, toleranceDays: 3 }) : null;
  const euFillNormal = euLatestRow ? positionalNormal(eu.index, euLatestRow.period, { pick: (r) => r.pct, toleranceDays: 3 }) : null;
  const eu_signal = euLatestRow && euStockNormal?.mean > 0
    ? ((euLatestRow.twh - euStockNormal.mean) / euStockNormal.mean) * 100
    : null;
  const eu_fill_pp = euLatestRow && euFillNormal?.mean != null ? euLatestRow.pct - euFillNormal.mean : null;

  // A missing side is unknown, not zero.
  const spread = (us_signal != null && eu_signal != null) ? us_signal - eu_signal : null;
  const needlePct = spread == null
    ? 50
    : Math.max(0, Math.min(100, ((spread - GAUGE_MIN) / (GAUGE_MAX - GAUGE_MIN)) * 100));
  const regime = classifyRegime(us_signal, eu_signal);

  return {
    usLatest, us_signal, euLatest, eu_signal, eu_fill_pp, spread, needlePct, regime,
    usSeries,
    euTotal: eu,
    usNormal: usNormal ? { weekOfSeason: usWk, n: usNormal.count, mean: usNormal.mean } : null,
    euNormal: euStockNormal?.n ? { n: euStockNormal.n, years: euStockNormal.years, meanTwh: euStockNormal.mean, meanFillPct: euFillNormal?.mean ?? null } : null,
  };
}

/** The 10-country total view from the shared EU engine. */
function buildEuTotal(rows) {
  const per = {};
  for (const c of EU_COUNTRIES) {
    per[c.cc] = {
      pct: getSeries(rows, `gie_storage_${c.cc}_full_pct`),
      gas: getSeries(rows, `gie_storage_${c.cc}_gas_in_storage`),
    };
  }
  const views = buildCountryViews(per);
  if (!views.length) return null;
  const total = buildAggregateView(views);
  return total.rows.length ? total : null;
}

/**
 * Weekly history of both deviations on the same scale (% of the prior-year
 * average stock at the same point of the year), on US report weeks. Each side
 * needs at least three prior years; with AGSI history from 2021 that starts
 * the EU side in 2024.
 *
 * @returns {Array<{period: Date, us: number, eu: number|null, spread: number|null, usN: number, euN: number}>}
 */
export function buildDivergenceHistory(model, { from = new Date(2024, 0, 1), minN = 3 } = {}) {
  if (!model?.usSeries) return [];
  const out = [];
  for (const r of model.usSeries) {
    if (r.period < from) continue;
    const prior = priorSameWeekValues(model.usSeries, r.period);
    if (prior.length < minN) continue;
    const usMean = prior.reduce((a, b) => a + b.value, 0) / prior.length;
    const us = ((r.value - usMean) / usMean) * 100;
    let eu = null;
    let euN = 0;
    if (model.euTotal) {
      // Same Friday on the EU side (or the nearest earlier day within 3 days).
      let row = null;
      for (let k = 0; k <= 3 && !row; k++) {
        row = model.euTotal.index.get(new Date(r.period.getFullYear(), r.period.getMonth(), r.period.getDate() - k).getTime());
      }
      if (row) {
        const n = positionalNormal(model.euTotal.index, row.period, { pick: (x) => x.twh });
        if (n.n >= minN && n.mean > 0) {
          eu = ((row.twh - n.mean) / n.mean) * 100;
          euN = n.n;
        }
      }
    }
    out.push({ period: r.period, us, eu, spread: eu == null ? null : us - eu, usN: prior.length, euN });
  }
  return out;
}

/**
 * End-of-winter analogs on both sides of the Atlantic: the latest stock plus
 * each prior year's change from the same week to 31 March. Only in the
 * withdrawal season (October to March), when 31 March is the season's end.
 */
export function endOfWinterAnalogs(model) {
  const us = model?.usLatest;
  if (!us) return null;
  const m = us.period.getMonth();
  if (m > 2 && m < 9) return null;
  const endYear = m >= 9 ? us.period.getFullYear() + 1 : us.period.getFullYear();
  const end = new Date(endYear, 2, 31);
  // October is still the US injection season; its end-of-March view comes
  // from the same analog only once the US withdrawal season has begun.
  const usA = m === 9 ? null : endOfSeasonAnalog(model.usSeries);
  const euProj = model.euTotal && model.euLatest ? analogProjection(model.euTotal, model.euLatest.period) : null;
  if (!usA && !euProj?.summary) return null;
  return {
    end,
    us: usA ? { min: usA.minBcf, max: usA.maxBcf, median: usA.medianBcf, n: usA.n, normal: usA.normalEndBcf } : null,
    eu: euProj?.summary ? { min: euProj.summary.minPct, max: euProj.summary.maxPct, median: euProj.summary.medianPct, n: euProj.analogs.length, normal: euProj.endNormal.mean } : null,
  };
}

/**
 * Compact, JSON-safe projection of the model for downloads.
 *
 * @param {ReturnType<typeof buildDivergenceModel>} model
 * @returns {Object|null}
 */
export function divergenceExport(model) {
  if (!model) return null;
  // Calendar day, not the UTC instant.
  const day = (d) => formatPeriodKey(d?.period ?? null);
  return {
    basis:
      'Both sides: % deviation of the latest stock from the average stock at the same point of the year over the prior 5 years. ' +
      'US: EIA Lower-48 Bcf, same week-of-season (weeks counted from each year\'s first observation, not ISO weeks). ' +
      'EU: GIE AGSI+ 10-country total TWh, same calendar day. Spread = US% − EU%, in percentage points.',
    thresholds: { us_pct: DIVERGENCE_THRESHOLDS.us, eu_pct: DIVERGENCE_THRESHOLDS.eu, gauge: [GAUGE_MIN, GAUGE_MAX] },
    normal: { us: model.usNormal, eu: model.euNormal },
    us: { period: day(model.usLatest), bcf: model.usLatest?.value ?? null, signal_pct: model.us_signal },
    eu: {
      period: day(model.euLatest),
      full_pct: model.euLatest?.value ?? null,
      stock_twh: model.euLatest?.twh ?? null,
      signal_pct: model.eu_signal,
      fill_vs_avg_pp: model.eu_fill_pp,
    },
    spread_pts: model.spread,
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
          <div class="divergence-side__sub">stock vs 5-yr avg · weekly</div>
          <div class="divergence-side__note">${normalNote(usLatest, model.usNormal)}</div>
        </div>

        <!-- Gauge -->
        <div class="divergence-gauge">
          <div class="divergence-gauge__bar">
            <div class="divergence-gauge__needle${spread == null ? ' divergence-gauge__needle--unknown' : ''}" style="left: ${needlePct.toFixed(2)}%"></div>
          </div>
          <div class="divergence-gauge__labels">
            <span class="divergence-gauge__edge divergence-gauge__edge--left">US loose</span>
            <span class="divergence-gauge__spread num">${spread == null ? 'n/a' : `spread ${spread >= 0 ? '+' : ''}${spread.toFixed(1)} pts`}</span>
            <span class="divergence-gauge__edge divergence-gauge__edge--right">EU tight</span>
          </div>
        </div>

        <!-- EU side -->
        <div class="divergence-side divergence-side--eu">
          <div class="divergence-side__label">EU</div>
          <div class="divergence-side__value num ${eu_signal == null ? '' : eu_signal >= 0 ? 'is-loose' : 'is-tight'}">
            ${eu_signal != null
              ? (eu_signal >= 0 ? '+' : '') + eu_signal.toFixed(1) + '%'
              : 'n/a'}
          </div>
          <div class="divergence-side__sub">stock vs 5-yr avg · daily${model.eu_fill_pp == null ? '' : ` · ${model.eu_fill_pp >= 0 ? '+' : ''}${model.eu_fill_pp.toFixed(1)} pp of fill`}</div>
          <div class="divergence-side__note">${normalNote(euLatest, model.euNormal)}</div>
        </div>
      </div>

      <!-- Regime interpretation -->
      <div class="divergence-card__interpretation divergence-card__interpretation--${regime.kind}">
        <span class="divergence-card__icon">${regime.icon}</span>
        <span class="divergence-card__regime">${regime.label}</span>
        <span class="divergence-card__detail">— ${regime.detail}</span>
      </div>
      ${analogLine(endOfWinterAnalogs(model))}
      <div class="divergence-history eu-panel" data-role="div-history"></div>
    </div>
  `;
  drawHistory(panelEl.querySelector('[data-role="div-history"]'), buildDivergenceHistory(model));
}

function analogLine(a) {
  if (!a) return '';
  const end = a.end.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  const usTxt = a.us
    ? `US ${Math.round(a.us.min).toLocaleString('en-US')}–${Math.round(a.us.max).toLocaleString('en-US')} Bcf (5-yr avg ${Math.round(a.us.normal).toLocaleString('en-US')})`
    : 'US n/a';
  const euTxt = a.eu ? `EU ${fx(a.eu.min, 0)}–${fx(a.eu.max, 0)}% full (5-yr avg ${fx(a.eu.normal, 0)}%)` : 'EU n/a';
  return `<p class="divergence-card__analogs"><b>End of winter (${end}), if the rest of the season repeats each of the last ${Math.max(a.us?.n ?? 0, a.eu?.n ?? 0)} winters:</b> ${usTxt} · ${euTxt}. Analogs, not forecasts.</p>`;
}

function drawHistory(host, rows) {
  if (!host || rows.length < 4) return;
  host.innerHTML = `<div class="divergence-history__head"><span class="divergence-card__title">Stock vs 5-year average, weekly</span>
    <span class="divergence-history__note">same % scale on both sides · from the first week with ≥3 prior years</span></div>`;
  host.insertAdjacentHTML('beforeend', legend([
    { kind: 'line', color: '#7dd3fc', label: 'US (EIA Lower 48)' },
    { kind: 'line', color: '#fbbf24', label: 'EU (10-country total)' },
  ]));
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  const { g, w, h } = frame(chartHost, { height: 200, margin: { top: 8, right: 16, bottom: 24, left: 44 } });
  const x = d3.scaleTime().domain(d3.extent(rows, (r) => r.period)).range([0, w]);
  const vals = rows.flatMap((r) => [r.us, r.eu]).filter(Number.isFinite);
  const lim = Math.max(10, ...vals.map(Math.abs)) * 1.08;
  const y = d3.scaleLinear().domain([-lim, lim]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => `${t}%`, 4, true);
  // Every six months on a wide chart, yearly on a phone: ticks(5) picked
  // yearly marks and left two labels across two-plus years.
  for (const t of x.ticks(d3.timeMonth.every(w > 600 ? 6 : 12))) g.append('text').attr('x', x(t)).attr('y', h + 16).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(d3.timeFormat('%b %y')(t));
  const line = (key) => d3.line().defined((r) => Number.isFinite(r[key])).x((r) => x(r.period)).y((r) => y(r[key]));
  g.append('path').datum(rows).attr('d', line('us')).attr('fill', 'none').attr('stroke', '#7dd3fc').attr('stroke-width', 1.8);
  g.append('path').datum(rows).attr('d', line('eu')).attr('fill', 'none').attr('stroke', '#fbbf24').attr('stroke-width', 1.8);
  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const bis = d3.bisector((r) => r.period).center;
  hitArea(g, w, h, ([mx]) => {
    const r = rows[bis(rows, x.invert(mx))];
    if (!r) return;
    cross.attr('x1', x(r.period)).attr('x2', x(r.period)).style('opacity', 1);
    tt.show(`
      <div class="eu-tt__title">Week ending ${r.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
      <div class="eu-tt__row"><span>US vs avg (n=${r.usN})</span><span class="num ${tone(r.us)}">${sx(r.us)}%</span></div>
      <div class="eu-tt__row"><span>EU vs avg${r.euN ? ` (n=${r.euN})` : ''}</span><span class="num ${tone(r.eu)}">${r.eu == null ? '—' : `${sx(r.eu)}%`}</span></div>
      <div class="eu-tt__row eu-tt__row--accent"><span>Spread (US − EU)</span><span class="num">${r.spread == null ? '—' : `${sx(r.spread)} pts`}</span></div>
    `, x(r.period) + 44, 8);
  }, () => { tt.hide(); cross.style('opacity', 0); });
}


/* ================================================================== */
/*  Regime classifier                                                  */
/* ================================================================== */

const ICONS = {
  up: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>',
  warn: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
  flat: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="5" y1="9" x2="19" y2="9"></line><line x1="5" y1="15" x2="19" y2="15"></line></svg>',
  q: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
};

/**
 * One reading for each of the nine (US, EU) × (below, near, above normal)
 * combinations. Descriptive only: the card states where stocks are, not what
 * prices will do.
 *
 * @param {number|null} us_signal  US stock vs average, %
 * @param {number|null} eu_signal  EU stock vs average, %
 */
export function classifyRegime(us_signal, eu_signal) {
  // A side with no normal has no verdict.
  if (us_signal == null || eu_signal == null) return {
    label: 'Comparison Incomplete',
    detail: 'Seasonal normal unavailable for current period; transatlantic spread undefined.',
    icon: ICONS.q,
    kind: 'neutral',
  };
  const T = DIVERGENCE_THRESHOLDS;
  const us = us_signal > T.us ? 'above' : us_signal < -T.us ? 'below' : 'near';
  const eu = eu_signal > T.eu ? 'above' : eu_signal < -T.eu ? 'below' : 'near';
  const table = {
    'above|below': ['US Surplus, European Deficit', 'US stocks above their seasonal average while European stocks sit well below theirs: the widest transatlantic gap, and the one that pulls US LNG toward Europe.', 'bullish', ICONS.up],
    'near|below': ['Europe-Led Tightness', 'European stocks well below their seasonal average while US stocks are near normal: the gap is a European shortfall, not a US surplus.', 'warning', ICONS.warn],
    'below|below': ['Deficit on Both Sides', 'Stocks below their seasonal averages in both the US and Europe.', 'warning', ICONS.warn],
    'above|near': ['US-Led Surplus', 'US stocks above their seasonal average while European stocks are near normal.', 'neutral', ICONS.flat],
    'near|near': ['Both Near Normal', 'US and European stocks both within their thresholds of the seasonal average.', 'neutral', ICONS.flat],
    'below|near': ['US-Led Tightness', 'US stocks below their seasonal average while European stocks are near normal.', 'warning', ICONS.warn],
    'above|above': ['Surplus on Both Sides', 'Stocks above their seasonal averages in both the US and Europe.', 'neutral', ICONS.flat],
    'near|above': ['Europe-Led Surplus', 'European stocks above their seasonal average while US stocks are near normal.', 'neutral', ICONS.flat],
    'below|above': ['US Deficit, European Surplus', 'US stocks below their seasonal average while European stocks are above theirs: the gap runs against US export pull.', 'warning', ICONS.warn],
  };
  const [label, detail, kind, icon] = table[`${us}|${eu}`];
  return { label, detail, kind, icon, us, eu };
}
