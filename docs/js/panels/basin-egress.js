/**
 * Basin Egress Panel — Haynesville take-away capacity and flow (Section 6).
 *
 * The SUPPLY-side counterpart to the LNG Feedgas Observatory. Where the LNG
 * panels watch demand pull, this one watches how much gas is LEAVING the
 * Haynesville through Gulf South's egress meters — a production/takeaway
 * proxy nobody publishes for free.
 *
 * Data contract:
 *   - Registry-driven from util/basin-egress.js (37 basin_egress meters).
 *   - Headline total uses ONLY high-confidence meters; medium-confidence
 *     meters appear in the corridor table flagged, excluded from the
 *     headline, with the confidence split stated in the footnote.
 *   - Congestion = TSQ ÷ operating capacity per corridor. Gulf South's OAC
 *     posting carries zone-level (not meter-level) operating capacity, so
 *     congestion renders as "capacity data unavailable" rather than fake
 *     numbers — until zone OAC lands in curated, only flow trends show.
 *
 * Reuses panel-base chrome, kpi-card, the LNG utilization color ladder,
 * gas-year x-axis, and design tokens. No new visual style.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { dth_to_mmcf, get_utilization_level } from '../util/lng-metrics.js';
import {
  BASIN_SOURCE,
  CORRIDORS,
  EGRESS_METERS,
} from '../util/basin-egress.js';

/** Cycle priority: later cycles are fresher (id3 > id2 > id1). */
const CYCLE_RANK = { id1: 1, id2: 2, id3: 3 };

/** Band colors per corridor index — same family as the LNG feed stack. */
const CORRIDOR_COLORS = [
  { area: 'rgba(14, 165, 233, 0.30)', line: '#38bdf8' }, // sky — Transco
  { area: 'rgba(52, 211, 153, 0.28)', line: '#34d399' }, // emerald — Bennington
  { area: 'rgba(251, 191, 36, 0.25)', line: '#fbbf24' }, // amber — Texas Gas
  { area: 'rgba(167, 139, 250, 0.24)', line: '#a78bfa' }, // violet — Carthage
  { area: 'rgba(244, 114, 182, 0.22)', line: '#f472b6' }, // pink — gathering
  { area: 'rgba(148, 163, 184, 0.20)', line: '#94a3b8' }, // slate — other
];

/**
 * Build daily final-cycle series per meter: dateStr -> Dth value.
 *
 * What:
 *   Strips "{source}_sq_{loc}_" leaving "{flow}_{cycle}" (flow-aware series
 *   keys). Picks the freshest cycle per gas day (id3 > id2 > id1); when both
 *   legs post, prefers the leg matching the meter's declared flow direction,
 *   else the larger value (for "?" meters usually only one leg is active).
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows - bundle source rows
 * @param {number} loc - meter location id
 * @param {'D'|'R'|'?'} declaredFlow - meter's classified flow direction
 * @returns {Object<string, number>} dateStr -> Dth/d
 */
function buildDailyByMeter(rows, loc, declaredFlow) {
  const prefix = `${BASIN_SOURCE}_sq_${loc}_`.toLowerCase();
  // dateStr -> {rank, value}
  const best = {};
  rows.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const parts = sid.slice(prefix.length).split('_');
    if (parts.length !== 2) return;
    const [, cycle] = parts;
    const rank = CYCLE_RANK[cycle];
    if (rank === undefined) return;
    const val = Number(r.value);
    const cur = best[r.period];
    if (!cur || rank > cur.rank || (rank === cur.rank && shouldReplaceLeg(cur, val, declaredFlow, r))) {
      best[r.period] = { rank, value: val };
    }
  });
  const out = {};
  Object.keys(best).forEach((dateStr) => {
    out[dateStr] = best[dateStr].value;
  });
  return out;
}

/**
 * Leg tie-break at equal cycle rank: declared direction wins; otherwise the
 * larger magnitude (inactive legs post 0).
 *
 * @param {{value: number}} cur - current best entry
 * @param {number} newVal - challenger value
 * @param {'D'|'R'|'?'} declaredFlow
 * @param {{series_id: string}} row - challenger row (carries its flow token)
 * @returns {boolean}
 */
function shouldReplaceLeg(cur, newVal, declaredFlow, row) {
  const flowToken = row.series_id.toLowerCase().split('_').slice(-2)[0];
  if (declaredFlow !== '?' && flowToken === declaredFlow.toLowerCase()) return true;
  if (declaredFlow !== '?' && cur.declaredLeg) return false;
  return Math.abs(newVal) > Math.abs(cur.value);
}

/**
 * Compute per-corridor and headline aggregates over common dates.
 *
 * @param {Array<Object>} rows - bundle source rows
 * @returns {{daily: Array<{dateStr: string, date: Date, byCorridor: Object<string, number>, highTotal: number, allTotal: number}>, latest: Object|null}}
 */
function buildEgressSeries(rows) {
  const byMeter = new Map(); // loc -> {daily: Map<dateStr, dth>, meter}
  EGRESS_METERS.forEach((m) => {
    const daily = buildDailyByMeter(rows, m.loc, m.flow);
    if (Object.keys(daily).length > 0) byMeter.set(m.loc, { daily, meter: m });
  });

  const dates = new Set();
  byMeter.forEach(({ daily }) => Object.keys(daily).forEach((d) => dates.add(d)));
  const sortedDates = [...dates].sort();

  const daily = sortedDates.map((dateStr) => {
    const byCorridor = {};
    let highTotal = 0;
    let allTotal = 0;
    byMeter.forEach(({ daily: m, meter }) => {
      const v = m[dateStr];
      if (v === undefined) return;
      byCorridor[meter.corridor] = (byCorridor[meter.corridor] || 0) + v;
      allTotal += v;
      if (meter.inHeadline) highTotal += v;
    });
    return { dateStr, date: new Date(`${dateStr}T00:00:00Z`), byCorridor, highTotal, allTotal };
  });

  const latest = daily.length > 0 ? daily[daily.length - 1] : null;
  return { daily, latest };
}

/**
 * Average of the last `n` values of an array of numbers.
 *
 * @param {number[]} arr
 * @param {number} n
 * @returns {number}
 */
function trailingMean(arr, n) {
  const slice = arr.slice(-n);
  if (slice.length === 0) return NaN;
  return slice.reduce((s, v) => s + v, 0) / slice.length;
}

/**
 * Gas-year info matching lng-feedgas.getGasYearInfo (Feb 1 → Jan 31).
 *
 * @param {Date} date
 * @returns {{gasYear: number, dayIndex: number}}
 */
function getGasYearInfo(date) {
  const y = date.getFullYear();
  const m = date.getMonth();
  let gasYear;
  let start;
  if (m >= 1) {
    gasYear = y;
    start = new Date(y, 1, 1);
  } else {
    gasYear = y - 1;
    start = new Date(y - 1, 1, 1);
  }
  const dayIndex = Math.floor((date.getTime() - start.getTime()) / 86400000);
  return { gasYear, dayIndex };
}

/**
 * Render the Basin Egress panel.
 *
 * @param {HTMLElement} panelEl - container element (#panel-basin-egress)
 * @param {Object} bundle - loaded dashboard bundle
 */
export function renderBasinEgressPanel(panelEl, bundle) {
  panelEl.innerHTML = '';
  const source = bundle.sources?.[BASIN_SOURCE];

  if (!source || !source.data || source.data.length === 0) {
    renderPanelChrome(panelEl, {
      title: 'Haynesville Basin Egress',
      subtitle: 'Take-away flows on Gulf South',
      sourceKey: BASIN_SOURCE,
      latestPeriod: null,
    }).chartEl.innerHTML =
      '<div class="panel-error">No Gulf South data in the bundle yet.</div>';
    return;
  }

  const { daily, latest } = buildEgressSeries(source.data);

  if (!latest || daily.length === 0) {
    renderPanelChrome(panelEl, {
      title: 'Haynesville Basin Egress',
      subtitle: 'Take-away flows on Gulf South',
      sourceKey: BASIN_SOURCE,
      latestPeriod: null,
    }).chartEl.innerHTML =
      '<div class="panel-error">No basin_egress series found in curated data.</div>';
    return;
  }

  const bodyEl = document.createElement('div');
  bodyEl.className = 'basin-egress-wrapper';
  panelEl.appendChild(bodyEl);

  const { chartEl, sidebarEl } = renderPanelChrome(bodyEl, {
    title: 'Haynesville · Basin Egress',
    subtitle: 'Take-away flows via Gulf South · supply-side counterpart to LNG feedgas',
    sourceKey: BASIN_SOURCE,
    latestPeriod: source.latest_period,
  });

  // ── KPI strip ──
  renderKpiStrip(sidebarEl, daily);

  // ── Stacked corridor area chart (chart column) ──
  drawCorridorChart(chartEl, daily);

  // ── Corridor table (full width under chart) ──
  const tableEl = document.createElement('div');
  tableEl.className = 'basin-egress-table-wrap';
  bodyEl.appendChild(tableEl);
  renderCorridorTable(tableEl, source.data, daily);

  // ── Congestion strip + footnote ──
  const congEl = document.createElement('div');
  congEl.className = 'basin-egress-congestion';
  bodyEl.appendChild(congEl);
  renderCongestionStrip(congEl);

  const footEl = document.createElement('p');
  footEl.className = 'basin-egress-footnote';
  const highCount = EGRESS_METERS.filter((m) => m.inHeadline).length;
  const medCount = EGRESS_METERS.length - highCount;
  footEl.textContent =
    `Headline totals use ${highCount} high-confidence meters only. ` +
    `${medCount} medium-confidence meters appear in the table flagged "med" and are excluded from the headline. ` +
    `Classification: config/meters/classification.json (deterministic rule engine, no guessed classes). ` +
    `Congestion needs operating-capacity postings for these zones — not yet in curated; renders when zone OAC lands.`;
  bodyEl.appendChild(footEl);
}

/**
 * KPI sidebar: headline egress (high-confidence), WoW, 30-day trend,
 * all-meter context card.
 *
 * @param {HTMLElement} sidebarEl
 * @param {Array<{dateStr: string, date: Date, highTotal: number, allTotal: number}>} daily
 */
function renderKpiStrip(sidebarEl, daily) {
  const highs = daily.map((d) => d.highTotal);
  const latestHighDth = highs[highs.length - 1];
  const latestMmcf = dth_to_mmcf(latestHighDth);

  // WoW: compare against the value 7 observations back (same weekly slot).
  let wowKpi;
  if (highs.length >= 8) {
    const prev = highs[highs.length - 8];
    const deltaMmcf = dth_to_mmcf(latestHighDth - prev);
    const pct = prev > 0 ? ((latestHighDth - prev) / prev) * 100 : 0;
    wowKpi = kpiCardHtml({
      label: 'WoW Change',
      value: `${deltaMmcf >= 0 ? '+' : ''}${deltaMmcf.toFixed(1)} MMcf/d`,
      delta: {
        value: `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`,
        kind: deltaMmcf > 0 ? 'bullish' : deltaMmcf < 0 ? 'bearish' : 'neutral',
      },
      helpText: 'vs 7 days earlier · high-confidence meters',
    });
  } else {
    wowKpi = kpiCardHtml({
      label: 'WoW Change',
      value: '—',
      helpText: 'Needs 8+ days of history',
    });
  }

  const mean30Dth = trailingMean(highs, 30);
  const mean30Mmcf = dth_to_mmcf(mean30Dth);
  const devPct = mean30Mmcf > 0 ? ((latestMmcf - mean30Mmcf) / mean30Mmcf) * 100 : 0;

  const trendKpi = `
    <div class="kpi-card">
      <div class="kpi-card__label">30-Day Trend</div>
      <div class="kpi-card__value num">${mean30Mmcf.toFixed(0)} MMcf/d</div>
      <div class="kpi-card__delta kpi-card__delta--${
        devPct >= 0 ? 'utilization-green' : 'utilization-red'
      }">${devPct >= 0 ? '+' : ''}${devPct.toFixed(1)}%</div>
      <div class="kpi-card__help">Trailing 30-day mean vs today · high-confidence meters</div>
    </div>`;

  const latestDateLabel = daily[daily.length - 1].date.toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
  });

  const latestKpi = kpiCardHtml({
    label: 'Haynesville Egress',
    value: `${latestMmcf.toFixed(0)} MMcf/d`,
    helpText: `Latest gas day (${latestDateLabel}) · ${EGRESS_METERS.filter((m) => m.inHeadline).length} high-confidence meters`,
  });

  const allLatestDth = daily[daily.length - 1].allTotal;
  const contextKpi = `
    <div class="kpi-card">
      <div class="kpi-card__label">All Meters Incl. Medium</div>
      <div class="kpi-card__value num">${dth_to_mmcf(allLatestDth).toFixed(0)} MMcf/d</div>
      <div class="kpi-card__help">
        Context only — includes ${EGRESS_METERS.filter((m) => !m.inHeadline).length} flagged meters.
        Haynesville produces ~15–17 Bcf/d; Gulf South-visible egress is a subset.
      </div>
    </div>`;

  sidebarEl.innerHTML = [latestKpi, wowKpi, trendKpi, contextKpi].join('');
}

/**
 * Stacked corridor area chart over the current gas year, LNG-panel visual
 * language: gas-year dayIndex x-axis, month ticks, muted gridlines, callout.
 *
 * @param {HTMLElement} container
 * @param {Array<{dateStr: string, date: Date, byCorridor: Object<string, number>, highTotal: number}>} daily
 */
function drawCorridorChart(container, daily) {
  container.innerHTML = '';

  const rect = container.getBoundingClientRect();
  const margin = { top: 24, right: 32, bottom: 40, left: 56 };
  const totalH = 320;
  const width = Math.max(rect.width || 600, 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3
    .select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  daily.forEach((d) => {
    const info = getGasYearInfo(d.date);
    d.gasYear = info.gasYear;
    d.dayIndex = info.dayIndex;
  });
  const currentGasYear = d3.max(daily, (d) => d.gasYear);
  const currentYearRows = daily.filter((d) => d.gasYear === currentGasYear);

  const x = d3.scaleLinear().domain([0, 365]).range([0, width]);
  const maxVal = d3.max(currentYearRows, (d) => dth_to_mmcf(d.highTotal)) || 0;
  const yMax = Math.max(maxVal * 1.15, dth_to_mmcf(1)); // guard empty
  const y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);

  // Gridlines every 500 MMcf/d like the LNG hero.
  const yTicks = [];
  for (let val = 500; val < yMax; val += 500) yTicks.push(val);
  g.selectAll('.gridline')
    .data(yTicks)
    .enter()
    .append('line')
    .attr('x1', 0)
    .attr('x2', width)
    .attr('y1', (dd) => y(dd))
    .attr('y2', (dd) => y(dd))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // One stacked band per corridor (registry order = bottom to top).
  const activeCorridors = CORRIDORS.filter(
    (c) => currentYearRows.some((r) => (r.byCorridor[c.key] || 0) > 0)
  );
  const cum = new Map();
  activeCorridors.forEach((corridor, fi) => {
    const lower = new Map(cum);
    const upper = new Map();
    currentYearRows.forEach((row) => {
      const prev = cum.get(row.dayIndex) || 0;
      const raw = row.byCorridor[corridor.key] || 0;
      upper.set(row.dayIndex, prev + dth_to_mmcf(raw));
    });
    const color = CORRIDOR_COLORS[fi % CORRIDOR_COLORS.length];
    const areaGen = d3
      .area()
      .x((d) => x(d))
      .y0((d) => y(lower.get(d) || 0))
      .y1((d) => y(upper.get(d) || 0))
      .curve(d3.curveMonotoneX);
    g.append('path')
      .datum([...upper.keys()].sort((a, b) => a - b))
      .attr('d', areaGen)
      .attr('fill', color.area)
      .attr('stroke', color.line)
      .attr('stroke-width', 1.5)
      .attr('stroke-opacity', 0.9);
    upper.forEach((v, k) => cum.set(k, v));
  });

  // Total line + latest callout.
  const totalLineGen = d3
    .line()
    .x((d) => x(d.dayIndex))
    .y((d) => y(dth_to_mmcf(d.highTotal)))
    .curve(d3.curveMonotoneX);
  g.append('path')
    .datum(currentYearRows)
    .attr('d', totalLineGen)
    .attr('fill', 'none')
    .style('stroke', 'var(--blue-flame)')
    .attr('stroke-width', 2)
    .attr('stroke-linecap', 'round');

  const lastRow = currentYearRows[currentYearRows.length - 1];
  if (lastRow) {
    const lastY = y(dth_to_mmcf(lastRow.highTotal));
    g.append('circle')
      .attr('cx', x(lastRow.dayIndex))
      .attr('cy', lastY)
      .attr('r', 6)
      .style('fill', 'var(--blue-flame)')
      .style('stroke', '#0c0f16')
      .style('stroke-width', '2px');
    const callout = g
      .append('g')
      .attr('transform', `translate(${x(lastRow.dayIndex) + 12}, ${lastY - 10})`);
    callout.append('rect')
      .attr('x', -4).attr('y', -11).attr('width', 92).attr('height', 15)
      .attr('rx', 3)
      .style('fill', 'rgba(15, 23, 42, 0.85)')
      .style('stroke', 'rgba(125, 211, 252, 0.3)')
      .style('stroke-width', '0.5px');
    callout.append('text')
      .attr('x', 2).attr('y', 0)
      .attr('font-size', '8.5px')
      .attr('font-family', 'var(--font-mono)')
      .style('font-feature-settings', '"tnum"')
      .style('fill', '#fff')
      .style('font-weight', '600')
      .text(`${dth_to_mmcf(lastRow.highTotal).toFixed(0)} MMcf/d egress`);
  }

  // Month ticks (Feb..Jan), mobile-thinned.
  const isMobile = width < 500;
  const months = ['Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan'];
  const start = new Date(currentGasYear, 1, 1);
  months.forEach((m, offset) => {
    if (isMobile && offset % 2 !== 0) return;
    const d = new Date(currentGasYear + (1 + offset >= 12 ? 1 : 0), (1 + offset) % 12, 1);
    const dayIdx = Math.floor((d.getTime() - start.getTime()) / 86400000);
    g.append('text')
      .attr('x', x(dayIdx))
      .attr('y', height + 22)
      .attr('text-anchor', 'middle')
      .attr('font-size', '10px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(m);
  });

  // Y ticks in MMcf/d.
  g.selectAll('.y-tick')
    .data(yTicks)
    .enter()
    .append('text')
    .attr('x', -8)
    .attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', '10px')
    .attr('font-family', 'var(--font-mono)')
    .style('font-feature-settings', '"tnum"')
    .style('fill', 'var(--chart-label)')
    .text((d) => `${d}`);

  // Legend under the chart.
  const legend = svg
    .append('g')
    .attr('transform', `translate(${margin.left}, ${totalH - 6})`);
  activeCorridors.forEach((corridor, fi) => {
    const item = legend
      .append('g')
      .attr('transform', `translate(${fi * 150}, 0)`);
    item.append('rect')
      .attr('width', 8).attr('height', 8).attr('ry', 1)
      .style('fill', CORRIDOR_COLORS[fi % CORRIDOR_COLORS.length].area)
      .style('stroke', CORRIDOR_COLORS[fi % CORRIDOR_COLORS.length].line);
    item.append('text')
      .attr('x', 12).attr('y', 7)
      .attr('font-size', '9px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'rgba(255,255,255,0.65)')
      .text(corridor.label);
  });
}

/**
 * Sortable corridor table: one row per meter — name, corridor, latest MMcf/d,
 * 7d avg, 30d avg, WoW %, share of HIGH-confidence total (medium meters show
 * their share of the all-meter total instead, flagged).
 *
 * @param {HTMLElement} container
 * @param {Array<Object>} rows - bundle source rows
 * @param {Array<{dateStr: string}>} daily
 */
function renderCorridorTable(container, rows, daily) {
  const corridorLabel = new Map(CORRIDORS.map((c) => [c.key, c.label]));

  const metersWithData = EGRESS_METERS.map((m) => {
    const series = buildDailyByMeter(rows, m.loc, m.flow);
    const dateVals = daily.map((d) => series[d.dateStr]).filter((v) => v !== undefined);
    if (dateVals.length === 0) return null;
    const latestDth = dateVals[dateVals.length - 1];
    const avg7Dth = trailingMean(dateVals, 7);
    const avg30Dth = trailingMean(dateVals, 30);
    const prevWeek = dateVals.length >= 8 ? dateVals[dateVals.length - 8] : undefined;
    const wowPct = prevWeek !== undefined && prevWeek > 0
      ? ((latestDth - prevWeek) / prevWeek) * 100
      : null;
    return {
      meter: m,
      latestMmcf: dth_to_mmcf(latestDth),
      avg7Mmcf: dth_to_mmcf(avg7Dth),
      avg30Mmcf: dth_to_mmcf(avg30Dth),
      wowPct,
    };
  }).filter(Boolean);

  const state = { key: 'latestMmcf', dir: -1 };

  function render() {
    const sorted = [...metersWithData].sort((a, b) => {
      const va = a[state.key];
      const vb = b[state.key];
      const na = va == null || Number.isNaN(va) ? -Infinity : va;
      const nb = vb == null || Number.isNaN(vb) ? -Infinity : vb;
      return (na - nb) * state.dir;
    });

    const cols = [
      ['name', 'Meter'],
      ['corridor', 'Corridor'],
      ['latestMmcf', 'Latest'],
      ['avg7Mmcf', '7d Avg'],
      ['avg30Mmcf', '30d Avg'],
      ['wowPct', 'WoW %'],
      ['share', 'Share'],
    ];

    const headHtml = cols
      .map(([key, label]) => `<th data-key="${key}" class="${state.key === key ? 'sorted' : ''}">${label}${state.key === key ? (state.dir === -1 ? ' ↓' : ' ↑') : ''}</th>`)
      .join('');

    const rowsHtml = sorted
      .map((r) => {
        const confBadge = r.meter.inHeadline
          ? ''
          : '<span class="badge">med</span>';
        const wowCell =
          r.wowPct == null
            ? '<td>—</td>'
            : `<td class="${r.wowPct >= 0 ? 'delta-pos' : 'delta-neg'}">${r.wowPct >= 0 ? '+' : ''}${r.wowPct.toFixed(1)}%</td>`;
        const corrLabel = corridorLabel.get(r.meter.corridor) || r.meter.corridor;
        return (
          `<tr><td>${r.meter.name} ${confBadge}</td>` +
          `<td>${corrLabel}</td>` +
          `<td>${r.latestMmcf.toFixed(0)}</td>` +
          `<td>${Number.isNaN(r.avg7Mmcf) ? '—' : r.avg7Mmcf.toFixed(0)}</td>` +
          `<td>${Number.isNaN(r.avg30Mmcf) ? '—' : r.avg30Mmcf.toFixed(0)}</td>` +
          wowCell +
          `<td>${r.sharePct == null ? '—' : `${r.sharePct.toFixed(1)}%`}</td></tr>`
        );
      })
      .join('');

    container.innerHTML = `
      <table class="basin-table basin-egress-table">
        <thead><tr>${headHtml}</tr></thead>
        <tbody>${rowsHtml}</tbody>
      </table>`;

    container.querySelectorAll('th[data-key]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = th.dataset.key;
        if (state.key === key) {
          state.dir = -state.dir;
        } else {
          state.key = key;
          state.dir = key === 'name' || key === 'corridor' ? 1 : -1;
        }
        render();
      });
    });
  }

  // Share of the LATEST-day high-confidence total (headline denominator);
  // medium meters get their share of the all-meter total. latestMmcf and
  // the totals are both converted to MMcf first — same-unit division.
  const lastDay = daily[daily.length - 1];
  const highDenomMmcf = dth_to_mmcf(lastDay.highTotal);
  const allDenomMmcf = dth_to_mmcf(lastDay.allTotal);
  metersWithData.forEach((r) => {
    const denom = r.meter.inHeadline ? highDenomMmcf : allDenomMmcf;
    r.sharePct = denom > 0 ? (r.latestMmcf / denom) * 100 : null;
  });

  render();
}

/**
 * Congestion indicator strip.
 *
 * TSQ ÷ operating capacity needs OAC postings for the egress zones. Gulf
 * South currently ships SQ-only into curated (no _oac_ series), so the strip
 * renders the honest state: methodology + what unlocks it. The color ladder
 * (get_utilization_level) is already wired here — when zone OAC arrives the
 * bars light up with zero further design work.
 *
 * @param {HTMLElement} container
 */
function renderCongestionStrip(container) {
  const levels = [
    { range: '<40%', label: 'low', cls: 'utilization-gray' },
    { range: '40–75%', label: 'normal', cls: 'utilization-green' },
    { range: '75–92%', label: 'high', cls: 'utilization-amber' },
    { range: '>92%', label: 'near saturation', cls: 'utilization-red' },
  ];
  container.innerHTML = `
    <div class="congestion-header">
      <h3>Corridor Congestion — TSQ ÷ Operating Capacity</h3>
      <span class="congestion-status congestion-status--pending">capacity data unavailable</span>
    </div>
    <div class="congestion-ladder">
      ${levels.map((l) => `<span class="congestion-chip ${l.cls}"><b>${l.range}</b> ${l.label}</span>`).join('')}
    </div>
    <p class="congestion-note">
      Utilization per corridor computes once Gulf South posts operationally-available
      capacity for the egress zones into curated (SQ-only today). Near-100% corridors are
      where basis blows out — this strip is the early-warning slot.
    </p>`;
}
