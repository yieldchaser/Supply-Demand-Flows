/**
 * European Gas Storage — GIE AGSI+ daily storage for ten EU member states.
 *
 * One selection drives the whole panel: the 10-country total (default) or any
 * member state, chosen from the toolbar or by clicking a row in the table.
 * Every block below then reads that selection:
 *
 *   1. KPI strip + one-line reading — fill, distance from the same-day normal,
 *      seven-day flow against the usual flow for the week, analog end-of-season
 *      range, year on year.
 *   2. Storage level — three views on one chart:
 *        This season  April-to-March storage year, the prior-year band
 *                     (min–max, median) for every day, last season, and the
 *                     analog paths from today to the end of the current season.
 *        All seasons  every storage year overlaid on the same axis.
 *        Timeline     the full daily history against its same-day normal.
 *      Unit toggle: % of working-gas capacity or TWh.
 *   3. Member-state table — sortable; fill bullet (bar = now, tick = normal),
 *      deviation in pp and TWh, rank, year on year, flow pace, analog outlook.
 *   4. Flow pace — daily reported net flow, its 7-day mean, and the band of the
 *      same 7-day window in prior years.
 *   5. End-of-season outlook — each analog season's remaining net change
 *      applied to today's stock, listed, with the normal end level.
 *   6. Distance from normal over time — the deviation series, only where the
 *      normal has at least three prior years.
 *   7. Who accounts for the gap — additive country attribution of the total's
 *      deviation from the same-day prior-year average.
 *   8. Daily data — the last 14 days behind every number above.
 *
 * All arithmetic lives in util/eu-storage-analysis.js (pure, tested); this
 * module only formats and draws.
 *
 * Data Source: bundle.sources.gie_agsi
 */

import * as d3 from 'd3';
import { renderPanelChrome, appendSection } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries, referenceDate } from '../util/series.js';
import { formatIsoDate } from '../util/format.js';
import { registerChartSnapshot } from '../util/chart-snapshot.js';
import {
  EU_COUNTRIES,
  AGGREGATE_CC,
  FILL_TARGET_PCT,
  buildCountryViews,
  buildAggregateView,
  positionalNormal,
  seasonalBand,
  storageYearTraces,
  storageYearOf,
  storageYearLabel,
  storageYearFraction,
  storageYearStart,
  storageYearEnd,
  countryStorageMetrics,
  flowSeriesWithNormal,
  deviationSeries,
  deviationAttribution,
  flowReconciliation,
  netFlow,
} from '../util/eu-storage-analysis.js';

const CHART_ID = 'eu-storage-level';
const MONTHS_SY = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar'];

/** Baselines the reader can compare against. */
export const EU_BASELINES = {
  '5y': { label: 'Last 5 years', short: '5-yr', minDate: null },
  post2022: { label: 'Since Apr 2022', short: 'post-2022', minDate: new Date(2022, 3, 1) },
};

/** "This season" view: last season in amber against the current blue. */
const LAST_SEASON_COLOR = '#fbbf24';
/** "All seasons" view: one distinguishable hue per prior season, oldest first. */
const SEASON_PALETTE = ['#64748b', '#f472b6', '#34d399', '#a78bfa', '#fbbf24'];

const fmtDay = d3.timeFormat('%-d %b %Y');
const fmtDayShort = d3.timeFormat('%-d %b');

/* ================================================================== */
/*  Model                                                             */
/* ================================================================== */

/**
 * Everything the panel needs, built once per bundle.
 * Exported for the chapter export and the Node tests.
 */
export function buildEuStorageModel(bundle) {
  const source = bundle?.sources?.gie_agsi;
  if (!source?.data?.length) return null;
  const perCountry = {};
  for (const c of EU_COUNTRIES) {
    perCountry[c.cc] = {
      pct: getSeries(source.data, `gie_storage_${c.cc}_full_pct`),
      gas: getSeries(source.data, `gie_storage_${c.cc}_gas_in_storage`),
      injection: getSeries(source.data, `gie_storage_${c.cc}_injection`),
      withdrawal: getSeries(source.data, `gie_storage_${c.cc}_withdrawal`),
    };
  }
  const views = buildCountryViews(perCountry);
  if (!views.length) return null;
  const aggregate = buildAggregateView(views);
  const latestDate = aggregate.latest?.period ?? referenceDate(...views.map((v) => v.rows));

  // GIE's own EU-wide aggregate, when the pipeline has it (scraper `type=eu`).
  // It is shown as its own row, never mixed into the 10-country arithmetic,
  // and it is what lets the panel say how much of the EU the ten cover.
  const euSeries = {
    pct: getSeries(source.data, 'gie_storage_eu_full_pct'),
    gas: getSeries(source.data, 'gie_storage_eu_gas_in_storage'),
    injection: getSeries(source.data, 'gie_storage_eu_injection'),
    withdrawal: getSeries(source.data, 'gie_storage_eu_withdrawal'),
  };
  let euWide = null;
  if (euSeries.pct.length && euSeries.gas.length) {
    euWide = buildCountryViews({ eu: euSeries }, [{ cc: 'eu', label: 'EU total (GIE)' }])[0] ?? null;
    if (euWide) euWide.isEuWide = true;
  }
  const coverage = euWide?.capacityTwh > 0 && aggregate.capacityTwh > 0 ? aggregate.capacityTwh / euWide.capacityTwh : null;
  return {
    source,
    views,
    aggregate,
    euWide,
    coverage,
    entities: [aggregate, ...(euWide ? [euWide] : []), ...views],
    latestDate,
  };
}

/**
 * Compact JSON projection for the chapter download: the per-entity figures
 * the table prints, with their basis.
 */
export function euStorageExport(model, baselineKey = '5y') {
  if (!model) return null;
  const base = EU_BASELINES[baselineKey] ?? EU_BASELINES['5y'];
  const round = (x, d = 2) => (Number.isFinite(x) ? Number(x.toFixed(d)) : null);
  return {
    basis:
      'GIE AGSI+ daily. Capacity per day = gas in storage / % full × 100. Total = Σ stock / Σ capacity over the ten countries. ' +
      `Normal = same calendar day in prior years (${base.label}); pp deviation on the % series, TWh deviation on the TWh series. ` +
      'Pace = trailing 7-day mean reported net flow vs the same window in prior years. ' +
      'Outlook = each prior season\'s remaining net change applied to today\'s stock (analog, not forecast).',
    as_of: model.latestDate ? formatIsoDate(model.latestDate) : null,
    entities: model.entities.map((v) => {
      const m = countryStorageMetrics(v, null, { minDate: base.minDate });
      if (!m) return { cc: v.cc, label: v.label, error: 'no current observation' };
      return {
        cc: v.cc,
        label: v.label,
        fill_pct: round(m.latest.pct),
        stock_twh: round(m.latest.twh),
        capacity_twh: round(m.capacityTwh, 1),
        normal_median_pct: round(m.normal.median),
        normal_years: m.normal.years,
        dev_pp: round(m.deviationPp),
        dev_twh: round(m.deviationTwh),
        rank_low_to_high: m.rank.rank,
        rank_of: m.rank.n + 1,
        yoy_pp: round(m.yoyPp),
        net_7d_gwh_d: round(m.flow7d?.avgNetGwh, 0),
        net_7d_normal_gwh_d: round(m.pace7d?.normal.median, 0),
        outlook_end: m.projection ? formatIsoDate(m.projection.endDate) : null,
        outlook_median_pct: round(m.projection?.summary?.medianPct, 1),
        outlook_min_pct: round(m.projection?.summary?.minPct, 1),
        outlook_max_pct: round(m.projection?.summary?.maxPct, 1),
      };
    }),
  };
}

/* ================================================================== */
/*  Entry point                                                       */
/* ================================================================== */

export function renderEuStoragePanel(panelEl, bundle) {
  const model = buildEuStorageModel(bundle);
  if (!model) {
    panelEl.innerHTML = '<div class="panel-error">GIE AGSI+ data unavailable.</div>';
    return;
  }
  const { views, aggregate, entities, source } = model;

  const state = {
    selected: AGGREGATE_CC,
    baseline: '5y',
    view: 'season',
    unit: 'pct',
    flowDays: 180,
    sort: { key: 'capacity', dir: -1 },
  };

  // Memoised per (entity, baseline): metrics are cheap but the band, deviation
  // and flow series evaluate a normal for every day, and a resize redraw must
  // not redo them.
  const cache = new Map();
  const memo = (key, fn) => {
    if (!cache.has(key)) cache.set(key, fn());
    return cache.get(key);
  };
  const entity = () => entities.find((e) => e.cc === state.selected) ?? aggregate;
  const baseline = () => EU_BASELINES[state.baseline];
  const metricsFor = (v) => memo(`m|${v.cc}|${state.baseline}`, () => countryStorageMetrics(v, null, { minDate: baseline().minDate }));

  // Register before the chrome so the header's CSV button renders.
  registerLevelSnapshot(entity(), metricsFor(entity()), state);

  const drawRef = { current: null };
  const totalCap = aggregate.capacityTwh;
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'European Gas Storage',
    subtitle: `GIE AGSI+ · ${views.length} member states · ${totalCap ? `${Math.round(totalCap).toLocaleString('en-US')} TWh working gas` : ''}${model.coverage ? ` (${Math.round(model.coverage * 100)}% of EU)` : ''} · daily`,
    sourceKey: 'gie_agsi',
    latestPeriod: source.latest_period,
    chartExport: CHART_ID,
    draw: () => drawRef.current?.(),
  });

  // The toolbar sits above the KPI strip it drives (the strip is the panel's
  // sidebar, which the house layout lifts above the chart column).
  const toolbarEl = document.createElement('div');
  toolbarEl.className = 'eu-toolbar';
  toolbarEl.setAttribute('role', 'toolbar');
  toolbarEl.setAttribute('aria-label', 'European storage controls');
  toolbarEl.innerHTML = `
      <label class="eu-field">
        <span class="eu-field__label">Showing</span>
        <select class="eu-select" data-role="entity" aria-label="Country or total">
          ${entities.map((e) => `<option value="${e.cc}">${escapeHtml(e.label)}</option>`).join('')}
        </select>
      </label>
      <div class="eu-seg" role="group" aria-label="Chart view" data-group="view">
        <button type="button" data-value="season" aria-pressed="true">This season</button>
        <button type="button" data-value="years" aria-pressed="false">All seasons</button>
        <button type="button" data-value="timeline" aria-pressed="false">Timeline</button>
      </div>
      <div class="eu-seg" role="group" aria-label="Unit" data-group="unit">
        <button type="button" data-value="pct" aria-pressed="true">% full</button>
        <button type="button" data-value="twh" aria-pressed="false">TWh</button>
      </div>
      <div class="eu-seg" role="group" aria-label="Normal baseline" data-group="baseline">
        ${Object.entries(EU_BASELINES).map(([k, b]) => `<button type="button" data-value="${k}" aria-pressed="${k === state.baseline}">${b.label}</button>`).join('')}
      </div>
  `;
  panelEl.querySelector('.panel-body').before(toolbarEl);

  chartEl.classList.add('eu-panel');
  chartEl.innerHTML = `
    <p class="eu-reading" data-role="reading" aria-live="polite"></p>
    <section class="eu-block" data-role="level"></section>
    <section class="eu-block" data-role="table"></section>
    <div class="eu-pair">
      <section class="eu-block" data-role="flow"></section>
      <section class="eu-block" data-role="outlook"></section>
    </div>
    <div class="eu-pair">
      <section class="eu-block" data-role="deviation"></section>
      <section class="eu-block" data-role="attribution"></section>
    </div>
    <details class="eu-block eu-daily" data-role="daily"></details>
  `;

  const $ = (role) => chartEl.querySelector(`[data-role="${role}"]`);
  const selectEl = toolbarEl.querySelector('[data-role="entity"]');
  const methodHost = document.createElement('div');
  methodHost.className = 'eu-method';
  const methodEl = appendSection(panelEl, 'europe-method', methodHost);

  toolbarEl.addEventListener('click', (e) => {
    const btn = e.target.closest('.eu-seg button');
    if (!btn) return;
    const group = btn.closest('.eu-seg').dataset.group;
    const value = btn.dataset.value;
    if (state[group] === value) return;
    state[group] = value;
    btn.closest('.eu-seg').querySelectorAll('button').forEach((b) => b.setAttribute('aria-pressed', String(b === btn)));
    if (group === 'view') drawLevelOnly();
    else drawAll();
  });
  selectEl.addEventListener('change', () => select(selectEl.value));

  function select(cc) {
    if (!entities.some((e) => e.cc === cc)) return;
    state.selected = cc;
    selectEl.value = cc;
    drawAll();
  }

  function drawLevelOnly() {
    const v = entity();
    const m = metricsFor(v);
    drawLevel($('level'), v, m, state, memo, baseline());
    registerLevelSnapshot(v, m, state);
  }

  function drawAll() {
    const v = entity();
    const m = metricsFor(v);
    selectEl.value = v.cc;
    renderKpis(sidebarEl, v, m, baseline());
    $('reading').innerHTML = readingSentence(v, m, baseline());
    drawLevelOnly();
    drawTable($('table'), entities, metricsFor, state, (cc) => select(cc), (key) => {
      state.sort = state.sort.key === key ? { key, dir: -state.sort.dir } : { key, dir: key === 'label' ? 1 : -1 };
      drawAll();
    });
    drawFlow($('flow'), v, m, state, memo, baseline(), (days) => { state.flowDays = days; drawAll(); });
    drawOutlook($('outlook'), v, m, baseline());
    drawDeviation($('deviation'), v, state, memo, baseline());
    drawAttribution($('attribution'), views, aggregate, model.latestDate, baseline(), memo, state, (cc) => select(cc));
    drawDaily($('daily'), v);
  }

  // Written once: it does not depend on the selection, and main.js folds it
  // behind a disclosure after the first render, which a rewrite would undo.
  methodEl.innerHTML = methodNote(views, aggregate, model);
  drawRef.current = drawAll;
  drawAll();
}

/* ================================================================== */
/*  Formatting                                                        */
/* ================================================================== */

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}
const isNum = (x) => Number.isFinite(x);
function fx(x, d = 1) {
  return isNum(x) ? x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
}
function sx(x, d = 1) {
  if (!isNum(x)) return '—';
  const s = fx(Math.abs(x), d);
  return x > 0 ? `+${s}` : x < 0 ? `−${s}` : s;
}
const tone = (x) => (!isNum(x) ? 'muted' : x > 0 ? 'pos' : x < 0 ? 'neg' : '');
const unitOf = (u) => (u === 'twh' ? 'TWh' : '%');
const pickFor = (u) => (u === 'twh' ? (r) => r.twh : (r) => r.pct);
function yearsSpan(years) {
  if (!years || !years.length) return 'no prior years';
  const lo = Math.min(...years);
  const hi = Math.max(...years);
  return lo === hi ? `${lo}` : `${lo}–${hi}`;
}
function rankText(rank) {
  if (!rank || rank.rank == null) return 'no rank';
  const of = rank.n + 1;
  if (rank.rank === 1) return `lowest of ${of} years`;
  if (rank.rank === of) return `highest of ${of} years`;
  return `${ordinal(rank.rank)} lowest of ${of}`;
}
function ordinal(i) {
  const j = i % 10;
  const k = i % 100;
  if (j === 1 && k !== 11) return `${i}st`;
  if (j === 2 && k !== 12) return `${i}nd`;
  if (j === 3 && k !== 13) return `${i}rd`;
  return `${i}th`;
}
/** Words for a flow-pace comparison, aware of the sign of the season. */
function paceWords(cur, normal) {
  if (!isNum(cur) || !isNum(normal)) return null;
  const diff = cur - normal;
  if (Math.abs(diff) < Math.max(20, Math.abs(normal) * 0.1)) return 'in line with';
  if (normal < 0 || cur < 0) {
    // Withdrawal context: more negative = drawing faster.
    if (cur < normal) return normal >= 0 ? 'drawing while it usually fills' : 'drawing faster than';
    return cur >= 0 && normal < 0 ? 'filling while it usually draws' : 'drawing slower than';
  }
  return cur > normal ? 'filling faster than' : 'filling slower than';
}

/* ================================================================== */
/*  KPI strip + reading                                               */
/* ================================================================== */

function renderKpis(sidebarEl, v, m, base) {
  if (!m) {
    sidebarEl.innerHTML = kpiCardHtml({ label: v.label, value: 'n/a', helpText: 'No current observation' });
    return;
  }
  const rows = v.rows;
  const prev = rows.length > 1 ? rows[rows.length - 2] : null;
  const dod = prev && isNum(prev.pct) ? m.latest.pct - prev.pct : null;
  const pace = m.pace7d;
  const proj = m.projection;
  const words = paceWords(pace?.current, pace?.normal.median);

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: `${v.label} · fill`,
      value: `${fx(m.latest.pct)}%`,
      delta: dod == null ? null : { value: `${sx(dod, 2)} pp on the day`, kind: dod >= 0 ? 'bullish' : 'bearish' },
      helpText: `${fx(m.latest.twh)} of ${fx(m.capacityTwh)} TWh · ${fmtDay(m.latest.period)}`,
    }),
    kpiCardHtml({
      label: `vs same-day median (${base.short})`,
      value: m.deviationPp == null ? 'n/a' : `${sx(m.deviationPp)} pp`,
      delta: m.deviationTwh == null ? null : {
        value: `${sx(m.deviationTwh)} TWh · ${rankText(m.rank)}`,
        kind: m.deviationPp >= 0 ? 'bullish' : 'bearish',
      },
      helpText: m.normal.median == null ? 'No prior-year observation for this date'
        : `Median ${fx(m.normal.median)}% over ${yearsSpan(m.normal.years)} (n=${m.normal.n})`,
    }),
    kpiCardHtml({
      label: 'Net flow · 7-day mean',
      value: pace?.current == null ? 'n/a' : `${sx(pace.current, 0)} GWh/d`,
      delta: pace?.normal.median == null ? null : {
        value: `normal ${sx(pace.normal.median, 0)} · ${sx(pace.diffGwh, 0)}`,
        kind: pace.diffGwh >= 0 ? 'bullish' : 'bearish',
      },
      helpText: words && pace?.normal.n ? `${capitalize(words)} the same week of ${yearsSpan(pace.normal.years)}` : 'Reported injection − withdrawal',
    }),
    kpiCardHtml({
      label: proj ? `Analog level on ${proj.endLabel}` : 'End-of-season analogs',
      value: proj?.summary ? `${fx(proj.summary.minPct, 0)}–${fx(proj.summary.maxPct, 0)}%` : 'n/a',
      delta: proj?.summary ? {
        value: `median ${fx(proj.summary.medianPct, 0)}% vs normal ${fx(proj.endNormal.median, 0)}%`,
        kind: isNum(proj.endNormal.median) && proj.summary.medianPct >= proj.endNormal.median ? 'bullish' : 'bearish',
      } : null,
      helpText: proj?.analogs.length ? `${proj.analogs.length} prior seasons' remaining flows applied to today's stock` : 'No complete analog season',
    }),
    kpiCardHtml({
      label: 'Year on year',
      value: m.yoyPp == null ? 'n/a' : `${sx(m.yoyPp)} pp`,
      delta: m.yoyTwh == null ? null : { value: `${sx(m.yoyTwh)} TWh`, kind: m.yoyTwh >= 0 ? 'bullish' : 'bearish' },
      helpText: 'Same calendar day last year',
    }),
  ].join('');
}

function capitalize(s) {
  return s ? s[0].toUpperCase() + s.slice(1) : s;
}

function readingSentence(v, m, base) {
  if (!m) return '';
  const parts = [];
  const name = v.isAggregate ? `The ${v.label.toLowerCase()}` : v.label;
  parts.push(`${escapeHtml(name)} is <b>${fx(m.latest.pct)}% full</b> (${fx(m.latest.twh)} TWh) on ${fmtDay(m.latest.period)}`);
  if (m.deviationPp != null) {
    const dir = m.deviationPp >= 0 ? 'above' : 'below';
    parts[0] += `, <b class="${tone(m.deviationPp)}">${fx(Math.abs(m.deviationPp))} pp ${dir}</b> the same-day median of ${yearsSpan(m.normal.years)}`;
    if (m.rank.rank != null && (m.rank.rank === 1 || m.rank.rank === m.rank.n + 1)) parts[0] += ` and the ${m.rank.rank === 1 ? 'lowest' : 'highest'} of the ${m.rank.n + 1} years for this date`;
  }
  parts[0] += '.';
  const pace = m.pace7d;
  const words = paceWords(pace?.current, pace?.normal.median);
  if (words && pace.normal.n) {
    parts.push(`Over the last 7 days it averaged ${sx(pace.current, 0)} GWh/d, ${words} the ${sx(pace.normal.median, 0)} GWh/d typical for this week.`);
  }
  const proj = m.projection;
  if (proj?.summary && proj.analogs.length >= 2) {
    parts.push(`Repeating the remaining ${proj.seasonKind} season of each of the last ${proj.analogs.length} years would leave it at ${fx(proj.summary.minPct, 0)}–${fx(proj.summary.maxPct, 0)}% on ${proj.endLabel}${isNum(proj.endNormal.median) ? `, against a normal ${fx(proj.endNormal.median, 0)}%` : ''}.`);
  }
  return parts.join(' ');
}

/* ================================================================== */
/*  Shared chart scaffolding                                          */
/* ================================================================== */

function frame(host, { height, margin }) {
  const width = Math.max(host.getBoundingClientRect().width || 640, 280);
  const w = width - margin.left - margin.right;
  const h = height - margin.top - margin.bottom;
  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('width', width)
    .attr('height', height)
    .attr('class', 'eu-svg');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
  return { svg, g, w, h, width, margin };
}

function yGrid(g, y, w, fmt, ticks = 5, zeroMajor = false) {
  for (const t of y.ticks(ticks)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t))
      .attr('class', zeroMajor && t === 0 ? 'eu-grid eu-grid--zero' : 'eu-grid');
    g.append('text').attr('x', -8).attr('y', y(t)).attr('dy', '0.32em').attr('text-anchor', 'end')
      .attr('class', 'eu-axis').text(fmt(t));
  }
}

function syAxis(g, x, h) {
  // Narrow charts label every other month so the names never run together.
  const every = x.range()[1] < 520 ? 2 : 1;
  for (let m = 0; m < 12; m++) {
    const x0 = x(m / 12);
    g.append('line').attr('x1', x0).attr('x2', x0).attr('y1', h).attr('y2', h + 4).attr('class', 'eu-tick');
    if (m % every) continue;
    g.append('text').attr('x', x(m / 12 + 1 / 24)).attr('y', h + 18).attr('text-anchor', 'middle')
      .attr('class', 'eu-axis eu-axis--sans').text(MONTHS_SY[m]);
  }
  // Season boundary: injection (Apr–Sep) | withdrawal (Oct–Mar)
  g.append('line').attr('x1', x(0.5)).attr('x2', x(0.5)).attr('y1', 0).attr('y2', h).attr('class', 'eu-season-rule');
  g.append('text').attr('x', x(0.25)).attr('y', -6).attr('text-anchor', 'middle').attr('class', 'eu-season-label').text('Injection season');
  g.append('text').attr('x', x(0.75)).attr('y', -6).attr('text-anchor', 'middle').attr('class', 'eu-season-label').text('Withdrawal season');
}

/** Absolute-positioned tooltip inside `host`; positions are host pixels. */
function tooltip(host) {
  const el = document.createElement('div');
  el.className = 'eu-tt';
  el.setAttribute('role', 'status');
  el.style.opacity = '0';
  host.appendChild(el);
  return {
    show(html, px, py) {
      el.innerHTML = html;
      el.style.opacity = '1';
      const hw = host.clientWidth;
      const tw = el.offsetWidth;
      const left = px + 16 + tw > hw ? px - tw - 16 : px + 16;
      el.style.left = `${Math.max(0, left)}px`;
      el.style.top = `${Math.max(0, py)}px`;
    },
    hide() { el.style.opacity = '0'; },
  };
}

function hitArea(g, w, h, onMove, onLeave) {
  g.append('rect').attr('class', 'eu-hit').attr('width', w).attr('height', h)
    .attr('fill', 'transparent')
    .on('pointermove', (ev) => onMove(d3.pointer(ev, g.node())))
    .on('pointerleave', onLeave);
}

function blockHead(title, desc = '', right = '') {
  return `
    <header class="eu-block__head">
      <div>
        <h3 class="eu-block__title">${title}</h3>
        ${desc ? `<p class="eu-block__desc">${desc}</p>` : ''}
      </div>
      ${right}
    </header>`;
}

function legend(items) {
  return `<div class="eu-legend">${items.map((it) => `<span class="eu-legend__item"><span class="eu-key eu-key--${it.kind}"${it.color ? ` style="--c:${it.color}"` : ''}></span>${it.label}</span>`).join('')}</div>`;
}

/* ================================================================== */
/*  Storage level chart                                               */
/* ================================================================== */

function drawLevel(host, v, m, state, memo, base) {
  const unit = state.unit;
  const pick = pickFor(unit);
  const u = unitOf(unit);
  const latest = v.latest;
  if (!latest) {
    host.innerHTML = '<div class="panel-error">No observations for this selection.</div>';
    return;
  }
  const sy = storageYearOf(latest.period);
  const band = memo(`band|${v.cc}|${state.baseline}|${unit}|${sy}`, () => seasonalBand(v.index, sy, { pick, minDate: base.minDate }));
  const traces = memo(`traces|${v.cc}|${unit}`, () => storageYearTraces(v.rows, pick));
  const cap = v.capacityTwh;

  const viewTitle = { season: `Storage year ${storageYearLabel(sy)}`, years: 'Every storage year, April to March', timeline: 'Daily history' }[state.view];
  const descs = {
    season: `Band: min–max of the same day in prior years (${base.label.toLowerCase()}); dashed: their median. Thin dashed lines from today: prior seasons' remaining flows applied to today's stock.`,
    years: `Each line is one storage year (1 Apr – 31 Mar). Band and median as in "This season".`,
    timeline: 'Continuous daily series with its same-day prior-year median and range, drawn where at least three prior years exist.',
  };
  host.innerHTML = blockHead(`Storage level · ${escapeHtml(v.label)} <span class="eu-block__sub">${viewTitle} · ${u === '%' ? '% of working-gas capacity' : 'TWh'}</span>`, descs[state.view]);
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);

  if (state.view === 'timeline') {
    drawTimeline(chartHost, host, v, unit, memo, state, base);
    return;
  }

  const current = traces.find((t) => t.sy === sy);
  const prior = traces.find((t) => t.sy === sy - 1);
  const proj = state.view === 'season' ? m?.projection : null;
  const projPts = proj ? proj.analogs.map((a) => ({
    label: a.label,
    clipped: a.clipped,
    points: a.path.map((p) => ({ x: storageYearFraction(p.period), period: p.period, value: unit === 'twh' ? p.twh : p.pct })),
  })) : [];

  // A storage year with only a few weeks of data (2020/21 is Jan–Mar 2021 in
  // this bundle) is a fragment, not a season; the overlay leaves it out.
  const shown = state.view === 'years'
    ? traces.filter((t) => (t.sy === sy || t.n >= 300) && (!base.minDate || storageYearStart(t.sy) >= base.minDate))
    : [prior, current].filter(Boolean);

  const allVals = [
    ...band.flatMap((b) => [b.min, b.max]),
    ...shown.flatMap((t) => t.points.map((p) => p.value)),
    ...projPts.flatMap((p) => p.points.map((q) => q.value)),
  ].filter(isNum);
  const yMax = unit === 'pct' ? Math.max(100, d3.max(allVals) ?? 100) : Math.max(cap ?? 0, d3.max(allVals) ?? 0) * 1.05;

  const colorOf = (t) => {
    if (t.sy === sy) return 'var(--blue-flame)';
    const idx = sy - 1 - t.sy; // 0 = last season
    if (state.view === 'season') return LAST_SEASON_COLOR;
    return SEASON_PALETTE[Math.max(0, SEASON_PALETTE.length - 1 - idx)];
  };

  const legendItems = [];
  if (state.view === 'season') {
    legendItems.push({ kind: 'line', color: 'var(--blue-flame)', label: storageYearLabel(sy) });
    if (prior) legendItems.push({ kind: 'line', color: colorOf(prior), label: `${prior.label} (last season)` });
    legendItems.push({ kind: 'band', label: 'Prior-year range' }, { kind: 'median', label: 'Prior-year median' });
    if (projPts.length) legendItems.push({ kind: 'proj', label: `Analog paths to ${proj.endLabel}` });
  } else {
    for (const t of shown) legendItems.push({ kind: 'line', color: colorOf(t), label: t.label });
    legendItems.push({ kind: 'band', label: 'Prior-year range' }, { kind: 'median', label: 'Median' });
  }
  if (unit === 'pct') legendItems.push({ kind: 'target', label: `${FILL_TARGET_PCT}% EU reference` });
  chartHost.insertAdjacentHTML('beforeend', legend(legendItems));

  const height = chartHost.getBoundingClientRect().width < 560 ? 300 : 380;
  const { g, w, h } = frame(chartHost, { height, margin: { top: 22, right: 54, bottom: 30, left: 52 } });
  const x = d3.scaleLinear().domain([0, 1]).range([0, w]);
  const y = d3.scaleLinear().domain([0, yMax]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => (unit === 'pct' ? `${t}%` : `${t}`));
  syAxis(g, x, h);

  if (band.length > 1) {
    g.append('path').datum(band).attr('class', 'eu-band')
      .attr('d', d3.area().x((d) => x(d.x)).y0((d) => y(d.min)).y1((d) => y(d.max)).curve(d3.curveLinear));
    g.append('path').datum(band).attr('class', 'eu-median')
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.median)));
  }
  if (unit === 'pct') {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(FILL_TARGET_PCT)).attr('y2', y(FILL_TARGET_PCT)).attr('class', 'eu-target');
    g.append('text').attr('x', 4).attr('y', y(FILL_TARGET_PCT) - 4).attr('class', 'eu-target-label').text(`${FILL_TARGET_PCT}%`);
  }

  const line = d3.line().x((d) => x(d.x)).y((d) => y(d.value));
  for (const t of shown) {
    if (t.sy === sy || t.points.length < 2) continue;
    g.append('path').datum(t.points).attr('class', 'eu-line').style('stroke', colorOf(t))
      .style('stroke-width', state.view === 'season' ? 1.4 : 1.2).attr('d', line);
  }
  for (const p of projPts) {
    if (p.points.length < 2) continue;
    g.append('path').datum(p.points).attr('class', 'eu-proj').attr('d', line);
  }
  if (projPts.length) {
    // End-of-season labels for the analog spread (min / median / max only, so
    // five labels never collide).
    const ends = projPts.map((p) => ({ ...p.points[p.points.length - 1], label: p.label })).sort((a, b) => a.value - b.value);
    const keep = ends.length <= 2 ? ends : [ends[0], ends[Math.floor((ends.length - 1) / 2)], ends[ends.length - 1]];
    for (const e of keep) {
      g.append('circle').attr('cx', x(e.x)).attr('cy', y(e.value)).attr('r', 2.5).attr('class', 'eu-proj-dot');
      g.append('text').attr('x', x(e.x) + 6).attr('y', y(e.value)).attr('dy', '0.32em').attr('class', 'eu-proj-label')
        .text(unit === 'pct' ? `${fx(e.value, 0)}%` : fx(e.value, 0));
    }
  }
  if (current && current.points.length > 1) {
    g.append('path').datum(current.points).attr('class', 'eu-line eu-line--current').attr('d', line);
    const last = current.points[current.points.length - 1];
    g.append('circle').attr('cx', x(last.x)).attr('cy', y(last.value)).attr('r', 4).attr('class', 'eu-dot--current');
    if (!projPts.length) {
      g.append('text').attr('x', x(last.x) + 8).attr('y', y(last.value)).attr('dy', '0.32em').attr('class', 'eu-end-label')
        .text(unit === 'pct' ? `${fx(last.value)}%` : `${fx(last.value)} TWh`);
    }
  }

  // Hover: read every series at the pointer's position in the storage year.
  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const nearest = (pts, xv) => {
    let best = null;
    for (const p of pts) {
      const d = Math.abs(p.x - xv);
      if (!best || d < best.d) best = { d, p };
    }
    return best && best.d < 3 / 365 ? best.p : null;
  };
  hitArea(g, w, h, ([mx, my]) => {
    const xv = x.invert(mx);
    const b = nearest(band, xv);
    if (!b) { tt.hide(); cross.style('opacity', 0); return; }
    cross.attr('x1', x(b.x)).attr('x2', x(b.x)).style('opacity', 1);
    const val = (pt) => (pt == null ? '—' : unit === 'pct' ? `${fx(pt.value)}%` : `${fx(pt.value)} TWh`);
    let rowsHtml = '';
    const lines = state.view === 'years' ? [...shown].reverse() : [current, prior].filter(Boolean);
    for (const t of lines) {
      const pt = nearest(t.points, b.x);
      if (!pt && t.sy !== sy) continue;
      rowsHtml += `<div class="eu-tt__row"><span><i class="eu-tt__sw" style="background:${colorOf(t)}"></i>${t.label}</span><span class="num">${val(pt)}</span></div>`;
    }
    const cur = current ? nearest(current.points, b.x) : null;
    const dev = cur ? cur.value - b.median : null;
    let projHtml = '';
    if (projPts.length && !cur) {
      const vals = projPts.map((p) => nearest(p.points, b.x)).filter(Boolean).map((p) => p.value).sort((a1, a2) => a1 - a2);
      if (vals.length) projHtml = `<div class="eu-tt__row"><span>Analog range</span><span class="num">${fx(vals[0], unit === 'pct' ? 0 : 0)}–${fx(vals[vals.length - 1], 0)}${unit === 'pct' ? '%' : ' TWh'}</span></div>`;
    }
    tt.show(`
      <div class="eu-tt__title">${fmtDayShort(b.period)} · ${escapeHtml(v.label)}</div>
      ${rowsHtml}
      ${projHtml}
      <div class="eu-tt__rule"></div>
      <div class="eu-tt__row"><span>Prior-year median (n=${b.n})</span><span class="num">${val({ value: b.median })}</span></div>
      <div class="eu-tt__row"><span>Prior-year range</span><span class="num">${fx(b.min)}–${fx(b.max)}${unit === 'pct' ? '%' : ''}</span></div>
      ${dev == null ? '' : `<div class="eu-tt__row eu-tt__row--accent"><span>${storageYearLabel(sy)} vs median</span><span class="num ${tone(dev)}">${sx(dev)} ${unit === 'pct' ? 'pp' : 'TWh'}</span></div>`}
    `, x(b.x) + 52, Math.max(0, y(cur ? cur.value : b.median) - 10));
  }, () => { tt.hide(); cross.style('opacity', 0); });
}

function drawTimeline(chartHost, host, v, unit, memo, state, base) {
  const pick = pickFor(unit);
  const series = memo(`timeline|${v.cc}|${state.baseline}|${unit}`, () => v.rows.filter((r) => isNum(pick(r))).map((r) => {
    const n = positionalNormal(v.index, r.period, { pick, minDate: base.minDate });
    return { period: r.period, value: pick(r), median: n.n >= 3 ? n.median : null, min: n.n >= 3 ? n.min : null, max: n.n >= 3 ? n.max : null, n: n.n };
  }));
  chartHost.insertAdjacentHTML('beforeend', legend([
    { kind: 'line', color: 'var(--blue-flame)', label: v.label },
    { kind: 'band', label: 'Prior-year range (n≥3)' },
    { kind: 'median', label: 'Prior-year median' },
    ...(unit === 'pct' ? [{ kind: 'target', label: `${FILL_TARGET_PCT}% EU reference` }] : []),
  ]));
  const height = chartHost.getBoundingClientRect().width < 560 ? 280 : 340;
  const { g, w, h } = frame(chartHost, { height, margin: { top: 12, right: 54, bottom: 30, left: 52 } });
  const x = d3.scaleTime().domain(d3.extent(series, (d) => d.period)).range([0, w]);
  const yMax = unit === 'pct' ? 100 : Math.max(v.capacityTwh ?? 0, d3.max(series, (d) => Math.max(d.value, d.max ?? 0))) * 1.05;
  const y = d3.scaleLinear().domain([0, yMax]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => (unit === 'pct' ? `${t}%` : `${t}`));
  for (const t of x.ticks(d3.timeYear.every(1))) {
    g.append('line').attr('x1', x(t)).attr('x2', x(t)).attr('y1', 0).attr('y2', h).attr('class', 'eu-grid');
    g.append('text').attr('x', x(t) + 4).attr('y', h + 18).attr('class', 'eu-axis').text(t.getFullYear());
  }
  const withN = series.filter((d) => d.median != null);
  const defined = (d) => d.median != null;
  g.append('path').datum(series).attr('class', 'eu-band')
    .attr('d', d3.area().defined(defined).x((d) => x(d.period)).y0((d) => y(d.min)).y1((d) => y(d.max)));
  g.append('path').datum(series).attr('class', 'eu-median')
    .attr('d', d3.line().defined(defined).x((d) => x(d.period)).y((d) => y(d.median)));
  if (unit === 'pct') {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(FILL_TARGET_PCT)).attr('y2', y(FILL_TARGET_PCT)).attr('class', 'eu-target');
  }
  g.append('path').datum(series).attr('class', 'eu-line eu-line--current eu-line--thin')
    .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.value)));
  if (withN.length) {
    const first = withN[0].period;
    g.append('text').attr('x', x(first) + 4).attr('y', 12).attr('class', 'eu-axis').text(`normal from ${fmtDayShort(first)} ${first.getFullYear()}`);
  }
  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const bis = d3.bisector((d) => d.period).center;
  hitArea(g, w, h, ([mx]) => {
    const d = series[bis(series, x.invert(mx))];
    if (!d) return;
    cross.attr('x1', x(d.period)).attr('x2', x(d.period)).style('opacity', 1);
    const dev = d.median != null ? d.value - d.median : null;
    tt.show(`
      <div class="eu-tt__title">${fmtDay(d.period)} · ${escapeHtml(v.label)}</div>
      <div class="eu-tt__row"><span>Level</span><span class="num">${fx(d.value)}${unit === 'pct' ? '%' : ' TWh'}</span></div>
      ${d.median == null ? `<div class="eu-tt__row"><span>Normal</span><span class="muted">n=${d.n}, needs ≥3</span></div>` : `
      <div class="eu-tt__row"><span>Prior-year median (n=${d.n})</span><span class="num">${fx(d.median)}</span></div>
      <div class="eu-tt__row eu-tt__row--accent"><span>Deviation</span><span class="num ${tone(dev)}">${sx(dev)} ${unit === 'pct' ? 'pp' : 'TWh'}</span></div>`}
    `, x(d.period) + 52, Math.max(0, y(d.value) - 10));
  }, () => { tt.hide(); cross.style('opacity', 0); });
}

function registerLevelSnapshot(v, m, state) {
  registerChartSnapshot(CHART_ID, `eu-storage-${v.cc}`, v.rows.slice(-400).map((r) => ({
    date: r.period,
    entity: v.label,
    fill_pct: isNum(r.pct) ? Number(r.pct.toFixed(3)) : null,
    stock_twh: isNum(r.twh) ? Number(r.twh.toFixed(4)) : null,
    capacity_twh: isNum(r.capTwh) ? Number(r.capTwh.toFixed(3)) : null,
    capacity_borrowed: r.capFilled ? 1 : 0,
    injection_gwh_d: r.injectionGwh,
    withdrawal_gwh_d: r.withdrawalGwh,
  })), { baseline: state.baseline, view: state.view, unit: state.unit, latest_dev_pp: m?.deviationPp ?? null });
}

/* ================================================================== */
/*  Member-state table                                                */
/* ================================================================== */

const TABLE_COLS = [
  { key: 'label', label: 'Country', num: false },
  { key: 'pct', label: 'Fill', title: 'Bar: % full today · tick: same-day prior-year median' },
  { key: 'twh', label: 'Stock TWh' },
  { key: 'capacity', label: 'Capacity TWh', title: 'Current working-gas capacity (median of last 30 days’ stock ÷ fill)' },
  { key: 'devPp', label: 'vs normal pp', title: 'Fill minus same-day prior-year median' },
  { key: 'devTwh', label: 'vs normal TWh', title: 'Stock minus same-day prior-year median stock' },
  { key: 'rank', label: 'Rank', title: '1 = lowest for this date among the years on record' },
  { key: 'yoy', label: 'YoY pp' },
  { key: 'net7', label: 'Net 7d GWh/d', title: 'Mean reported injection − withdrawal, last 7 days' },
  { key: 'pace', label: 'Pace vs normal', title: '7-day net flow minus the median of the same 7 days in prior years (GWh/d). Negative = stock falling faster / rising slower than usual.' },
  { key: 'outlook', label: 'Analog end level', title: 'Median (range) of end-of-season fill if each prior season’s remaining flows repeat' },
];

function tableValue(key, v, m) {
  switch (key) {
    case 'label': return v.label;
    case 'pct': return m?.latest.pct;
    case 'twh': return m?.latest.twh;
    case 'capacity': return m?.capacityTwh;
    case 'devPp': return m?.deviationPp;
    case 'devTwh': return m?.deviationTwh;
    case 'rank': return m?.rank.rank;
    case 'yoy': return m?.yoyPp;
    case 'net7': return m?.flow7d?.avgNetGwh;
    case 'pace': return m?.pace7d?.diffGwh;
    case 'outlook': return m?.projection?.summary?.medianPct;
    default: return null;
  }
}

function drawTable(host, entities, metricsFor, state, onSelect, onSort) {
  const pinned = entities.filter((e) => e.isAggregate || e.isEuWide);
  const countries = entities.filter((e) => !e.isAggregate && !e.isEuWide);
  const agg = pinned[0];
  const ms = new Map(entities.map((e) => [e.cc, metricsFor(e)]));
  const { key, dir } = state.sort;
  const sorted = [...countries].sort((a, b) => {
    const va = tableValue(key, a, ms.get(a.cc));
    const vb = tableValue(key, b, ms.get(b.cc));
    if (typeof va === 'string') return dir * va.localeCompare(vb);
    if (!isNum(va)) return 1;
    if (!isNum(vb)) return -1;
    return dir * (va - vb);
  });
  const sample = ms.get(agg.cc);
  const projEnd = sample?.projection?.endLabel ?? 'season end';

  const row = (v) => {
    const m = ms.get(v.cc);
    const sel = v.cc === state.selected;
    if (!m) return `<tr data-cc="${v.cc}"><th scope="row">${escapeHtml(v.label)}</th><td colspan="${TABLE_COLS.length - 1}" class="muted">no current observation</td></tr>`;
    const bulletNorm = isNum(m.normal.median) ? `<span class="eu-bullet__tick" style="left:${Math.min(100, m.normal.median)}%"></span>` : '';
    const p = m.projection;
    const outlook = p?.summary
      ? `<span class="num">${fx(p.summary.medianPct, 0)}%</span> <span class="eu-sub num">${fx(p.summary.minPct, 0)}–${fx(p.summary.maxPct, 0)}${p.analogs.some((a) => a.clipped) ? '*' : ''}</span>`
      : '<span class="muted">—</span>';
    return `
      <tr data-cc="${v.cc}" class="${sel ? 'is-selected' : ''}${v.isAggregate || v.isEuWide ? ' is-total' : ''}" tabindex="0" aria-selected="${sel}">
        <th scope="row"><span class="eu-cc">${v.isAggregate ? 'Σ10' : v.cc.toUpperCase()}</span>${escapeHtml(v.label)}</th>
        <td class="eu-td-fill">
          <span class="num">${fx(m.latest.pct)}%</span>
          <span class="eu-bullet" aria-hidden="true"><span class="eu-bullet__bar" style="width:${Math.min(100, m.latest.pct)}%"></span>${bulletNorm}</span>
        </td>
        <td class="num">${fx(m.latest.twh)}</td>
        <td class="num">${fx(m.capacityTwh)}</td>
        <td class="num ${tone(m.deviationPp)}">${sx(m.deviationPp)}</td>
        <td class="num ${tone(m.deviationTwh)}">${sx(m.deviationTwh)}</td>
        <td class="num">${m.rank.rank == null ? '—' : `${m.rank.rank}/${m.rank.n + 1}`}</td>
        <td class="num ${tone(m.yoyPp)}">${sx(m.yoyPp)}</td>
        <td class="num">${sx(m.flow7d?.avgNetGwh, 0)}</td>
        <td class="num ${tone(m.pace7d?.diffGwh)}">${sx(m.pace7d?.diffGwh, 0)}</td>
        <td class="eu-td-outlook">${outlook}</td>
      </tr>`;
  };

  host.innerHTML = `
    ${blockHead('Member states', `Click a row to read it across the panel. Normals use the selected baseline; outlook is the fill on ${projEnd} under each prior season's remaining flows (* = an analog hit empty or full and was clipped).`)}
    <div class="eu-table-scroll">
      <table class="eu-table">
        <thead><tr>${TABLE_COLS.map((c) => {
          const active = c.key === key;
          const arrow = active ? (dir > 0 ? ' ↑' : ' ↓') : '';
          return `<th scope="col"${c.num === false ? '' : ' class="num"'} aria-sort="${active ? (dir > 0 ? 'ascending' : 'descending') : 'none'}"><button type="button" data-sort="${c.key}"${c.title ? ` title="${escapeHtml(c.title)}"` : ''}>${c.label}${arrow}</button></th>`;
        }).join('')}</tr></thead>
        <tbody>${pinned.map(row).join('')}${sorted.map(row).join('')}</tbody>
      </table>
    </div>`;

  host.querySelector('thead').addEventListener('click', (e) => {
    const b = e.target.closest('[data-sort]');
    if (b) onSort(b.dataset.sort);
  });
  const tbody = host.querySelector('tbody');
  tbody.addEventListener('click', (e) => {
    const tr = e.target.closest('tr[data-cc]');
    if (tr) onSelect(tr.dataset.cc);
  });
  tbody.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const tr = e.target.closest('tr[data-cc]');
    if (!tr) return;
    e.preventDefault();
    onSelect(tr.dataset.cc);
  });
}

/* ================================================================== */
/*  Flow pace                                                         */
/* ================================================================== */

function drawFlow(host, v, m, state, memo, base, onWindow) {
  const latest = v.latest?.period;
  if (!latest) { host.innerHTML = ''; return; }
  const from = new Date(latest.getFullYear(), latest.getMonth(), latest.getDate() - state.flowDays + 1);
  const series = memo(`flow|${v.cc}|${state.baseline}|${state.flowDays}`, () => flowSeriesWithNormal(v, from, latest, { minDate: base.minDate }));
  const opts = [90, 180, 365].map((d) => `<button type="button" data-days="${d}" aria-pressed="${state.flowDays === d}">${d === 365 ? '1Y' : `${d}D`}</button>`).join('');
  host.innerHTML = blockHead(
    'Flow pace',
    'Daily reported net flow (injection − withdrawal), its 7-day mean, and the same 7-day window in prior years.',
    `<div class="eu-seg eu-seg--sm" role="group" aria-label="Flow window">${opts}</div>`,
  );
  host.querySelector('.eu-seg').addEventListener('click', (e) => {
    const b = e.target.closest('[data-days]');
    if (b) onWindow(Number(b.dataset.days));
  });
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  if (series.length < 2) { chartHost.innerHTML = '<p class="muted">Not enough flow data.</p>'; return; }

  chartHost.insertAdjacentHTML('beforeend', legend([
    { kind: 'bar-pos', label: 'Net injection' },
    { kind: 'bar-neg', label: 'Net withdrawal' },
    { kind: 'line', color: 'var(--blue-flame)', label: '7-day mean' },
    { kind: 'band', label: 'Prior years, same 7 days' },
  ]));
  const { g, w, h } = frame(chartHost, { height: 260, margin: { top: 10, right: 16, bottom: 28, left: 56 } });
  const x = d3.scaleTime().domain([from, latest]).range([0, w]);
  const ext = d3.extent(series.flatMap((d) => [d.net, d.avg, d.normMin, d.normMax]).filter(isNum));
  const lim = Math.max(Math.abs(ext[0] ?? 0), Math.abs(ext[1] ?? 0), 10) * 1.08;
  const y = d3.scaleLinear().domain([-lim, lim]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => d3.format(',')(t), 5, true);
  for (const t of x.ticks(w < 420 ? 3 : state.flowDays > 200 ? 6 : 5)) {
    g.append('text').attr('x', x(t)).attr('y', h + 18).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(d3.timeFormat('%b %y')(t));
  }
  const defined = (d) => isNum(d.normMin) && isNum(d.normMax);
  g.append('path').datum(series).attr('class', 'eu-band')
    .attr('d', d3.area().defined(defined).x((d) => x(d.period)).y0((d) => y(d.normMin)).y1((d) => y(d.normMax)));
  g.append('path').datum(series).attr('class', 'eu-median')
    .attr('d', d3.line().defined((d) => isNum(d.normMedian)).x((d) => x(d.period)).y((d) => y(d.normMedian)));
  const bw = Math.max(1, (w / series.length) * 0.7);
  g.append('g').selectAll('rect').data(series.filter((d) => isNum(d.net))).join('rect')
    .attr('x', (d) => x(d.period) - bw / 2)
    .attr('width', bw)
    .attr('y', (d) => (d.net >= 0 ? y(d.net) : y(0)))
    .attr('height', (d) => Math.max(0.5, Math.abs(y(d.net) - y(0))))
    .attr('class', (d) => (d.net >= 0 ? 'eu-bar--pos' : 'eu-bar--neg'));
  g.append('path').datum(series).attr('class', 'eu-line eu-line--current eu-line--thin')
    .attr('d', d3.line().defined((d) => isNum(d.avg)).x((d) => x(d.period)).y((d) => y(d.avg)));

  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const bis = d3.bisector((d) => d.period).center;
  hitArea(g, w, h, ([mx]) => {
    const d = series[bis(series, x.invert(mx))];
    if (!d) return;
    cross.attr('x1', x(d.period)).attr('x2', x(d.period)).style('opacity', 1);
    tt.show(`
      <div class="eu-tt__title">${fmtDay(d.period)} · ${escapeHtml(v.label)}</div>
      <div class="eu-tt__row"><span>Net flow</span><span class="num ${tone(d.net)}">${sx(d.net, 0)} GWh/d</span></div>
      <div class="eu-tt__row"><span>7-day mean</span><span class="num">${sx(d.avg, 0)} GWh/d</span></div>
      ${d.n ? `<div class="eu-tt__rule"></div>
      <div class="eu-tt__row"><span>Prior years median (n=${d.n})</span><span class="num">${sx(d.normMedian, 0)}</span></div>
      <div class="eu-tt__row"><span>Prior years range</span><span class="num">${sx(d.normMin, 0)} to ${sx(d.normMax, 0)}</span></div>` : ''}
    `, x(d.period) + 56, 10);
  }, () => { tt.hide(); cross.style('opacity', 0); });

  const rec = flowReconciliation(v.rows, 30);
  if (rec && Math.abs(rec.meanResidualGwh) > Math.max(25, 0.0002 * (v.capacityTwh ?? 0) * 1000)) {
    host.insertAdjacentHTML('beforeend', `<p class="eu-note">Reported flows and the day-to-day stock change differ by ${sx(rec.meanResidualGwh, 0)} GWh/d on average over the last ${rec.n} days (stock change minus reported net). Levels use the stock series; this chart uses reported flows.</p>`);
  }
}

/* ================================================================== */
/*  End-of-season outlook                                             */
/* ================================================================== */

function drawOutlook(host, v, m, base) {
  const p = m?.projection;
  if (!p || !p.analogs.length) {
    host.innerHTML = `${blockHead('End-of-season outlook')}<p class="muted">No prior season covers both today and the season end${base.minDate ? ' within this baseline' : ''}.</p>`;
    return;
  }
  const rows = [...p.analogs].sort((a, b) => a.endPct - b.endPct);
  const endNormal = p.endNormal;
  const fmtUnit = (twh) => `${fx(twh, 1)}`;
  host.innerHTML = `
    ${blockHead(`End-of-season outlook · ${p.endLabel}`, `Today's stock (${fx(p.startTwh)} TWh, ${fx(p.startPct)}%) plus the net change each prior season recorded from ${fmtDayShort(m.latest.period)} to ${p.endLabel}.`)}
    <div class="eu-table-scroll"><table class="eu-table eu-table--compact">
      <thead><tr><th scope="col">Analog season</th><th scope="col" class="num">Change to ${p.endLabel} TWh</th><th scope="col" class="num">End stock TWh</th><th scope="col" class="num">End fill</th></tr></thead>
      <tbody>
        ${rows.map((a) => `
          <tr>
            <th scope="row">${a.label}${a.clipped ? ' <span class="eu-flag" title="This season’s flows would have emptied or overfilled today’s stock; the path is clipped at the physical limit.">clipped</span>' : ''}</th>
            <td class="num ${tone(a.deltaTwh)}">${sx(a.deltaTwh)}</td>
            <td class="num">${fmtUnit(a.endTwh)}</td>
            <td class="num">${fx(a.endPct)}%</td>
          </tr>`).join('')}
        <tr class="is-ref">
          <th scope="row">Normal level on ${p.endLabel}</th>
          <td class="num muted">—</td>
          <td class="num muted">—</td>
          <td class="num">${isNum(endNormal.median) ? `${fx(endNormal.median)}%` : '—'}</td>
        </tr>
      </tbody>
    </table></div>
    <p class="eu-note">Analogs, not a forecast: each row repeats one season's weather, prices and flows. Deliverability falls as stocks empty, so low starting stock tends to draw less than a high-stock analog did.</p>`;
}

/* ================================================================== */
/*  Deviation history                                                 */
/* ================================================================== */

function drawDeviation(host, v, state, memo, base) {
  const unit = state.unit;
  const series = memo(`dev|${v.cc}|${state.baseline}`, () => deviationSeries(v, { minDate: base.minDate, minN: 3 }));
  const key = unit === 'twh' ? 'devTwh' : 'devPp';
  const u = unit === 'twh' ? 'TWh' : 'pp';
  host.innerHTML = blockHead(
    `Distance from normal · ${u}`,
    series.length ? `Level minus the same-day prior-year median, from ${fmtDay(series[0].period)} (first date with ≥3 prior years in the ${base.label.toLowerCase()} baseline).` : 'Needs at least three prior years.',
  );
  if (series.length < 2) return;
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  const { g, w, h } = frame(chartHost, { height: 230, margin: { top: 10, right: 16, bottom: 28, left: 52 } });
  const x = d3.scaleTime().domain(d3.extent(series, (d) => d.period)).range([0, w]);
  const ext = d3.extent(series, (d) => d[key]);
  const lim = Math.max(Math.abs(ext[0]), Math.abs(ext[1]), 1) * 1.08;
  const y = d3.scaleLinear().domain([-lim, lim]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => d3.format(',')(t), 5, true);
  for (const t of x.ticks(d3.timeYear.every(1))) {
    g.append('text').attr('x', x(t) + 4).attr('y', h + 18).attr('class', 'eu-axis').text(t.getFullYear());
  }
  const uid = `eu-dev-${Math.random().toString(36).slice(2, 8)}`;
  const defs = g.append('defs');
  defs.append('clipPath').attr('id', `${uid}-pos`).append('rect').attr('x', 0).attr('y', 0).attr('width', w).attr('height', y(0));
  defs.append('clipPath').attr('id', `${uid}-neg`).append('rect').attr('x', 0).attr('y', y(0)).attr('width', w).attr('height', h - y(0));
  const area = d3.area().x((d) => x(d.period)).y0(y(0)).y1((d) => y(d[key]));
  g.append('path').datum(series).attr('class', 'eu-area--pos').attr('clip-path', `url(#${uid}-pos)`).attr('d', area);
  g.append('path').datum(series).attr('class', 'eu-area--neg').attr('clip-path', `url(#${uid}-neg)`).attr('d', area);
  g.append('path').datum(series).attr('class', 'eu-line eu-line--dev').attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d[key])));
  const last = series[series.length - 1];
  g.append('circle').attr('cx', x(last.period)).attr('cy', y(last[key])).attr('r', 3).attr('class', 'eu-dot--current');

  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const bis = d3.bisector((d) => d.period).center;
  hitArea(g, w, h, ([mx]) => {
    const d = series[bis(series, x.invert(mx))];
    if (!d) return;
    cross.attr('x1', x(d.period)).attr('x2', x(d.period)).style('opacity', 1);
    tt.show(`
      <div class="eu-tt__title">${fmtDay(d.period)} · ${escapeHtml(v.label)}</div>
      <div class="eu-tt__row"><span>Fill / median</span><span class="num">${fx(d.pct)}% / ${fx(d.normalPct)}%</span></div>
      <div class="eu-tt__row"><span>Stock / median</span><span class="num">${fx(d.twh)} / ${fx(d.normalTwh)} TWh</span></div>
      <div class="eu-tt__row eu-tt__row--accent"><span>Deviation (n=${d.n})</span><span class="num ${tone(d[key])}">${sx(d[key])} ${u}</span></div>
    `, x(d.period) + 52, 10);
  }, () => { tt.hide(); cross.style('opacity', 0); });
}

/* ================================================================== */
/*  Attribution                                                       */
/* ================================================================== */

function drawAttribution(host, views, aggregate, asOf, base, memo, state, onSelect) {
  const att = memo(`attr|${state.baseline}`, () => deviationAttribution(views, aggregate, asOf, { minDate: base.minDate }));
  if (!att.rows.length) { host.innerHTML = ''; return; }
  const residualOk = att.residualTwh == null || Math.abs(att.residualTwh) < 0.05;
  host.innerHTML = blockHead(
    'Who accounts for the gap',
    `Each country's stock minus its same-day average over ${yearsSpan(att.years)} (TWh). Averages, unlike medians, add up: the bars sum to the total's ${sx(att.totalDevTwh)} TWh${residualOk ? '' : ` (residual vs total ${sx(att.residualTwh, 2)} TWh from unequal reporting)`}.`,
  );
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  const rowH = 22;
  const height = att.rows.length * rowH + 30;
  const { g, w } = frame(chartHost, { height, margin: { top: 6, right: 92, bottom: 24, left: 92 } });
  const lim = d3.max(att.rows, (r) => Math.abs(r.devTwh)) * 1.05 || 1;
  const x = d3.scaleLinear().domain([Math.min(0, -lim), Math.max(0, d3.max(att.rows, (r) => r.devTwh) > 0 ? lim : 0)]).nice().range([0, w]);
  for (const t of x.ticks(4)) {
    g.append('line').attr('x1', x(t)).attr('x2', x(t)).attr('y1', 0).attr('y2', att.rows.length * rowH).attr('class', t === 0 ? 'eu-grid eu-grid--zero' : 'eu-grid');
    g.append('text').attr('x', x(t)).attr('y', att.rows.length * rowH + 16).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(d3.format(',')(t));
  }
  att.rows.forEach((r, i) => {
    const yy = i * rowH;
    const sel = r.cc === state.selected;
    const grp = g.append('g').attr('class', `eu-attr${sel ? ' is-selected' : ''}`).attr('transform', `translate(0,${yy})`)
      .attr('role', 'button').attr('tabindex', 0).attr('aria-label', `${r.label}: ${sx(r.devTwh)} TWh`)
      .on('click', () => onSelect(r.cc))
      .on('keydown', (ev) => { if (ev.key === 'Enter') onSelect(r.cc); });
    grp.append('rect').attr('x', -92).attr('width', w + 184).attr('height', rowH).attr('class', 'eu-attr__hit');
    grp.append('text').attr('x', -8).attr('y', rowH / 2).attr('dy', '0.32em').attr('text-anchor', 'end').attr('class', 'eu-attr__label').text(r.label);
    const x0 = x(Math.min(0, r.devTwh));
    grp.append('rect').attr('x', x0).attr('y', 4).attr('height', rowH - 8).attr('width', Math.max(1, Math.abs(x(r.devTwh) - x(0))))
      .attr('class', r.devTwh >= 0 ? 'eu-bar--pos' : 'eu-bar--neg');
    grp.append('text').attr('x', w + 8).attr('y', rowH / 2).attr('dy', '0.32em').attr('class', 'eu-attr__value')
      .text(`${sx(r.devTwh)}${r.sharePct != null && Math.sign(r.devTwh) === Math.sign(att.totalDevTwh) ? ` · ${fx(r.sharePct, 0)}%` : ''}`);
  });
}

/* ================================================================== */
/*  Daily data                                                        */
/* ================================================================== */

function drawDaily(host, v) {
  const open = host.open;
  const rows = v.rows.slice(-15);
  const body = [];
  for (let i = rows.length - 1; i >= 1; i--) {
    const r = rows[i];
    const prev = rows[i - 1];
    const dStock = isNum(r.twh) && isNum(prev.twh) ? (r.twh - prev.twh) * 1000 : null;
    const net = netFlow(r);
    body.push(`
      <tr>
        <th scope="row">${formatIsoDate(r.period)}</th>
        <td class="num">${fx(r.pct, 2)}%</td>
        <td class="num">${fx(r.twh, 2)}</td>
        <td class="num">${fx(r.capTwh, 1)}${r.capFilled ? '*' : ''}</td>
        <td class="num">${fx(r.injectionGwh, 0)}</td>
        <td class="num">${fx(r.withdrawalGwh, 0)}</td>
        <td class="num ${tone(net)}">${sx(net, 0)}</td>
        <td class="num ${tone(dStock)}">${sx(dStock, 0)}</td>
      </tr>`);
  }
  host.innerHTML = `
    <summary>Daily data · ${escapeHtml(v.label)} · last 14 gas days</summary>
    <div class="eu-table-scroll">
      <table class="eu-table eu-table--compact">
        <thead><tr>
          <th scope="col">Gas day</th><th scope="col" class="num">Fill</th><th scope="col" class="num">Stock TWh</th>
          <th scope="col" class="num">Capacity TWh</th><th scope="col" class="num">Injection GWh/d</th><th scope="col" class="num">Withdrawal GWh/d</th>
          <th scope="col" class="num">Reported net</th><th scope="col" class="num">Δ stock GWh</th>
        </tr></thead>
        <tbody>${body.join('')}</tbody>
      </table>
    </div>
    <p class="eu-note">Reported net = GIE injection − withdrawal. Δ stock = change in gas in storage from the previous gas day; the two differ by metering and revisions.${v.isAggregate ? ' Total = sum of the ten countries.' : ''}</p>`;
  host.open = open;
}

/* ================================================================== */
/*  Method note                                                       */
/* ================================================================== */

function methodNote(views, aggregate, model = {}) {
  const capRows = views.map((v) => {
    const moved = isNum(v.capacityFirstTwh) && isNum(v.capacityTwh) ? v.capacityTwh - v.capacityFirstTwh : null;
    return `<tr><th scope="row">${escapeHtml(v.label)}</th><td class="num">${fx(v.capacityFirstTwh)}</td><td class="num">${fx(v.capacityTwh)}</td><td class="num ${Math.abs(moved ?? 0) >= 1 ? tone(moved) : ''}">${sx(moved)}</td></tr>`;
  }).join('');
  const first = aggregate.rows[0]?.period;
  const last = aggregate.latest?.period;
  return `
    <div class="data-provenance eu-method__body">
      <p><b>Source.</b> GIE AGSI+ daily country aggregates (gas in storage, % full, injection, withdrawal) for ${views.length} member states, ${first ? formatIsoDate(first) : '—'} to ${last ? formatIsoDate(last) : '—'}. ${model.coverage
    ? `The 10-country total is these ten countries, ${fx(model.coverage * 100, 0)}% of EU working-gas capacity on the latest day; GIE's own EU-wide aggregate is shown separately as "EU total (GIE)".`
    : 'The total is these ten countries, roughly nine-tenths of EU working-gas capacity; GIE\'s EU-wide aggregate is requested by the pipeline but not yet in this bundle.'} AGSI publishes each gas day the next morning and operators revise recent days.</p>
      <p><b>Capacity.</b> Read per day as gas in storage ÷ % full × 100 (GIE's own definition of % full). Capacities moved during the window, so no single constant is used: the total's % is Σ stock ÷ Σ that day's capacities. Days with fill below 3% borrow the nearest stable day's capacity (marked * in the daily table).</p>
      <p><b>Normal.</b> The same calendar day in each prior year of the baseline (last 5 years, or storage years from April 2022 to exclude the 2021/22 supply crisis), with the sample size printed. pp deviations use the % series and TWh deviations the TWh series, so they agree in sign but not always in proportion when capacity changed.</p>
      <p><b>Storage year.</b> 1 April to 31 March: injection season April–September, withdrawal season October–March. The ${FILL_TARGET_PCT}% line is the EU filling reference (Reg. 2022/1032 as amended in 2025); national obligations differ and are not tested here.</p>
      <p><b>Flow.</b> Reported injection − withdrawal. Regimes in the table use a threshold of ±0.05% of capacity per day, scaled to each country.</p>
      <table class="eu-table eu-table--compact eu-cap-table">
        <caption>Working-gas capacity, first 60 days of data vs last 30 days (TWh)</caption>
        <thead><tr><th scope="col">Country</th><th scope="col" class="num">Earliest</th><th scope="col" class="num">Current</th><th scope="col" class="num">Change</th></tr></thead>
        <tbody>${capRows}</tbody>
      </table>
    </div>`;
}
