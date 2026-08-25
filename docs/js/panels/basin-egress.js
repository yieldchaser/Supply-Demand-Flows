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
 *   - CAPACITY SEMANTICS (corrected 2026-08-25): Gulf South's OAC posting is
 *     a RESIDUAL — posted capacity minus scheduled flow (Lonewa
 *     Pearson(TSQ,OAC) = −1.0000; TSQ+OAC constant). Therefore:
 *       * TSQ ÷ operating capacity is INVALID and is never computed;
 *       * share-in-use = TSQ ÷ (TSQ + OAC) is bounded and comparable across
 *         corridors (labeled "share of posted capacity in use");
 *       * the OAC LEVEL itself carries the real constraint signal and is
 *         drawn as dashed per-corridor capacity lines.
 *   - LNG terminal utilization vs published NAMEPLATE elsewhere on this
 *     dashboard uses a true denominator and is unaffected.
 *
 * Reuses panel-base chrome, kpi-card, the LNG utilization color ladder,
 * gas-year x-axis, and design tokens. No new visual style.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { dth_to_mmcf } from '../util/lng-metrics.js';
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
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {number} loc - meter location id
 * @param {'D'|'R'|'?'} declaredFlow - meter's classified flow direction
 * @returns {Object<string, number>} dateStr -> Dth/d
 */
export function buildDailyByMeter(rows, loc, declaredFlow) {
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
 * Build daily final-cycle series per meter for a given quantity kind
 * ('sq' or 'oac'): dateStr -> Dth value.
 *
 * What:
 *   Matches "{source}_{kind}_{loc}_d_{cycle}" series ids and keeps the
 *   freshest cycle per gas day. Used for the SQ flow series and — new —
 *   the parallel OAC level series feeding share-in-use + capacity lines.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {number} loc - meter location id
 * @param {'sq'|'oac'} kind - quantity kind to extract
 * @returns {Object<string, number>} dateStr -> Dth/d
 */
export function buildDailyByKind(rows, loc, kind) {
  const prefix = `${BASIN_SOURCE}_${kind}_${loc}_`.toLowerCase();
  const best = {};
  rows.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const parts = sid.slice(prefix.length).split('_');
    if (parts.length !== 2 || parts[0] !== 'd') return;
    const rank = CYCLE_RANK[parts[1]];
    if (rank === undefined) return;
    const cur = best[r.period];
    if (!cur || rank > cur.rank) best[r.period] = { rank, value: Number(r.value) };
  });
  /** @type {Object<string, number>} */
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
 * What:
 *   Builds the SQ daily series per meter (flow-aware), plus a parallel OAC
 *   daily series for meters that also post operationally-available capacity.
 *   Emits per-day corridor sums split into:
 *     byCorridorSqShared / byCorridorOac — sums over meters carrying BOTH
 *     signals (the only valid population for share-of-capacity-in-use), and
 *     oacLevelByCorridor — raw posted-capacity level (top corridors).
 *
 * @param {Array<Object>} rows - bundle source rows
 * @returns {{daily: Array<{dateStr: string, date: Date, byCorridor: Object<string, number>, highTotal: number, allTotal: number, byCorridorSqShared: Object<string, number>, byCorridorOac: Object<string, number>, oacLevelByCorridor: Object<string, number>}>, latest: Object|null}}
 */
export function buildEgressSeries(rows) {
  const byMeter = new Map(); // loc -> {daily: Map<dateStr, dth>, meter}
  EGRESS_METERS.forEach((m) => {
    const daily = buildDailyByMeter(rows, m.loc, m.flow);
    if (Object.keys(daily).length > 0) byMeter.set(m.loc, { daily, meter: m });
    // Parallel OAC series (final cycle id3) for meters that post one.
    const oacDaily = buildDailyByKind(rows, m.loc, 'oac');
    if (Object.keys(oacDaily).length > 0) {
      const entry = byMeter.get(m.loc);
      if (entry) entry.oacDaily = oacDaily;
      else byMeter.set(m.loc, { daily: {}, meter: m, oacDaily });
    }
  });

  const dates = new Set();
  byMeter.forEach(({ daily }) => Object.keys(daily).forEach((d) => dates.add(d)));
  const sortedDates = [...dates].sort();

  const daily = sortedDates.map((dateStr) => {
    const byCorridor = {};
    const byCorridorSqShared = {};
    const byCorridorOac = {};
    const oacLevelByCorridor = {};
    let highTotal = 0;
    let allTotal = 0;
    byMeter.forEach(({ daily: m, oacDaily, meter }) => {
      const v = m[dateStr];
      if (v !== undefined) {
        byCorridor[meter.corridor] = (byCorridor[meter.corridor] || 0) + v;
        allTotal += v;
        if (meter.inHeadline) highTotal += v;
      }
      // Share-in-use population: meters with BOTH sq and oac on this day.
      if (oacDaily) {
        const oacV = oacDaily[dateStr];
        if (oacV !== undefined && v !== undefined) {
          byCorridorSqShared[meter.corridor] =
            (byCorridorSqShared[meter.corridor] || 0) + v;
          byCorridorOac[meter.corridor] = (byCorridorOac[meter.corridor] || 0) + oacV;
        }
        // Capacity LEVEL for the corridor = max posted OAC among its meters
        // (a corridor's posted capacity is its binding — largest — offer).
        if (oacV !== undefined) {
          const cur = oacLevelByCorridor[meter.corridor];
          oacLevelByCorridor[meter.corridor] =
            cur === undefined ? oacV : Math.max(cur, oacV);
        }
      }
    });
    return {
      dateStr,
      date: new Date(`${dateStr}T00:00:00Z`),
      byCorridor,
      highTotal,
      allTotal,
      byCorridorSqShared,
      byCorridorOac,
      oacLevelByCorridor,
    };
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

  // ── Share-of-posted-capacity strip + footnote ──
  const congEl = document.createElement('div');
  congEl.className = 'basin-egress-congestion';
  bodyEl.appendChild(congEl);
  renderCongestionStrip(congEl, daily, CORRIDORS);

  const footEl = document.createElement('p');
  footEl.className = 'basin-egress-footnote';
  const highCount = EGRESS_METERS.filter((m) => m.inHeadline).length;
  const medCount = EGRESS_METERS.length - highCount;
  footEl.textContent =
    `Headline totals use ${highCount} high-confidence meters only. ` +
    `${medCount} medium-confidence meters appear in the table flagged "med" and are excluded from the headline. ` +
    `Classification: config/meters/classification.json (deterministic rule engine, no guessed classes). ` +
    `Capacity note: Gulf South's Operationally Available Capacity is a residual (posted capacity − scheduled flow), ` +
    `so it can never be used as a utilization denominator — the strip above reports share of posted capacity in use, ` +
    `and the chart's capacity line tracks posted-capacity level (maintenance/constraint signal).`;
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

  // ── Posted-capacity (OAC) level lines — top 5 corridors by latest OAC. ──
  // The genuinely NEW signal: posted-capacity swings are maintenance /
  // constraint events invisible in the flow stack. Rendered as thin dashed
  // lines in each corridor's color, on the same gas-year axis.
  const oacRanked = [...currentYearRows]
    .reverse()
    .map((r) => r.oacLevelByCorridor || {})
    .find((o) => Object.keys(o).length > 0);
  if (oacRanked) {
    const topOacCorridors = Object.keys(oacRanked)
      .sort((a, b) => (oacRanked[b] || 0) - (oacRanked[a] || 0))
      .slice(0, 5);
    topOacCorridors.forEach((corridorKey, oi) => {
      const corridor = CORRIDORS.find((c) => c.key === corridorKey);
      if (!corridor) return;
      const pts = currentYearRows
        .filter((r) => r.oacLevelByCorridor && r.oacLevelByCorridor[corridorKey] !== undefined)
        .map((r) => ({ dayIndex: r.dayIndex, v: r.oacLevelByCorridor[corridorKey] }));
      if (pts.length < 2) return;
      const lineGen = d3
        .line()
        .x((d) => x(d.dayIndex))
        .y((d) => y(dth_to_mmcf(d.v)))
        .curve(d3.curveMonotoneX);
      g.append('path')
        .datum(pts)
        .attr('d', lineGen)
        .attr('fill', 'none')
        .attr('stroke', CORRIDOR_COLORS[activeCorridors.findIndex((c) => c.key === corridorKey) % CORRIDOR_COLORS.length].line)
        .attr('stroke-opacity', 0.85 - oi * 0.12)
        .attr('stroke-width', 1)
        .attr('stroke-dasharray', '4 4');
      // Small label at line end.
      const lastPt = pts[pts.length - 1];
      g.append('text')
        .attr('x', x(lastPt.dayIndex) + 6)
        .attr('y', y(dth_to_mmcf(lastPt.v)) + 3)
        .attr('font-size', '7.5px')
        .attr('font-family', 'var(--font-mono)')
        .style('fill', 'rgba(255,255,255,0.5)')
        .text(`cap ${corridor.label.split(' ')[0]}`);
    });

    // Capacity-line legend note.
    g.append('text')
      .attr('x', width)
      .attr('y', -8)
      .attr('text-anchor', 'end')
      .attr('font-size', '8px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'rgba(255,255,255,0.45)')
      .text('dashed = posted capacity level (top 5 corridors)');
  }

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
 * Share-of-posted-capacity strip.
 *
 * Replaces the retired TSQ ÷ operating-capacity design — Gulf South's OAC is
 * a residual (posted capacity − scheduled flow), making that ratio invalid
 * (see shareInUsePct for the full invalidation note).
 *
 * @param {HTMLElement} container
 */
/**
 * Share-in-use per corridor: TSQ ÷ (TSQ + OAC), bounded [0, 1].
 *
 * WHY NOT TSQ ÷ OPERATING CAPACITY (invalidated 2026-08-25):
 *   Gulf South's "Operationally Available Capacity" is a RESIDUAL —
 *   posted capacity minus scheduled flow — so TSQ/OAC divides flow by its
 *   own complement and produces absurd multiples (Lonewa median 3.8×,
 *   max 11.4×; Pearson(TSQ, OAC) = −1.0000). TSQ+OAC ≡ posted capacity,
 *   so TSQ/(TSQ+OAC) is the honest, always-bounded share of posted
 *   capacity in use.
 *
 * HONESTY NOTE: Spearman(share, volume) = 0.999 on this data — the metric
 * ranks days identically to volume. It adds comparable units across
 * corridors, not new information. It is labeled "share of posted capacity
 * in use", never "utilization". The genuinely NEW signal is the OAC LEVEL
 * itself (posted-capacity swings are maintenance/constraint events), which
 * renders as the corridor capacity line in drawCorridorChart.
 *
 * @param {number} tsqDth - latest final-cycle TSQ for the corridor (Dth/d)
 * @param {number} oacDth - matching operationally-available capacity (Dth/d)
 * @returns {number|null} percent in [0,100], or null when inputs are absent
 */
export function shareInUsePct(tsqDth, oacDth) {
  if (tsqDth == null || oacDth == null) return null;
  const denom = tsqDth + oacDth;
  if (!(denom > 0)) return null;
  return (tsqDth / denom) * 100;
}

/**
 * Congestion strip — share of posted capacity in use, per corridor.
 *
 * Replaces the retired TSQ÷operating-capacity design (see shareInUsePct).
 *
 * @param {HTMLElement} container
 * @param {Array<{dateStr: string, byCorridorOac: Object<string, number>}>} daily
 * @param {Array<{key: string, label: string}>} corridors
 */
function renderCongestionStrip(container, daily, corridors) {
  const levels = [
    { range: '<60%', label: 'light', cls: 'utilization-gray' },
    { range: '60–80%', label: 'moderate', cls: 'utilization-green' },
    { range: '80–92%', label: 'heavy', cls: 'utilization-amber' },
    { range: '>92%', label: 'saturated', cls: 'utilization-red' },
  ];
  // Walk back to the most recent gas day where corridors posted BOTH signals
  // (OAC typically lags SQ by one day — never mix days across the ratio).
  let refDay = null;
  for (let i = daily.length - 1; i >= 0; i--) {
    const o = daily[i].byCorridorOac || {};
    const s = daily[i].byCorridorSqShared || {};
    if (Object.keys(o).length > 0 && Object.keys(s).length > 0) {
      refDay = daily[i];
      break;
    }
  }
  const lastOac = refDay ? refDay.byCorridorOac : {};

  const rowsHtml = corridors
    .map((c) => {
      // Corridor-level share uses only meters that carry BOTH sq and oac.
      const tsqSum = refDay ? refDay.byCorridorSqShared[c.key] : undefined;
      const oacSum = lastOac[c.key];
      if (tsqSum == null || oacSum == null) return '';
      const pct = shareInUsePct(tsqSum, oacSum);
      if (pct == null) return '';
      const level =
        pct < 60 ? levels[0] : pct < 80 ? levels[1] : pct <= 92 ? levels[2] : levels[3];
      return (
        `<div class="congestion-row">` +
        `<span class="congestion-row__name">${c.label}</span>` +
        `<span class="congestion-chip ${level.cls}"><b>${pct.toFixed(1)}%</b> ${level.label}</span>` +
        `</div>`
      );
    })
    .filter(Boolean)
    .join('');

  const asOf = refDay
    ? new Date(`${refDay.dateStr}T00:00:00Z`).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
      })
    : null;
  const body =
    rowsHtml ||
    '<p class="congestion-note">No corridor currently posts both scheduled flow ' +
    'and operationally-available capacity on the same day.</p>';

  container.innerHTML = `
    <div class="congestion-header">
      <h3>Corridors — Share of Posted Capacity in Use</h3>
      <span class="congestion-status congestion-status--pending">TSQ ÷ (TSQ + OAC)${
        asOf ? ` · as of ${asOf}` : ''
      }</span>
    </div>
    <div class="congestion-ladder">
      ${levels.map((l) => `<span class="congestion-chip ${l.cls}"><b>${l.range}</b> ${l.label}</span>`).join('')}
    </div>
    ${body}
    <p class="congestion-note">
      Gulf South's Operationally Available Capacity is a RESIDUAL (posted capacity −
      scheduled flow), so TSQ ÷ operating capacity is mathematically meaningless here —
      it read up to 11× on Lonewa before being retired 2026-08-25. This metric is the
      share of POSTED capacity in use; it ranks days like volume does and adds comparable
      units across corridors, not new signal. The real capacity signal is the OAC level
      line in the chart above — swings there are maintenance/constraint events.
    </p>`;
}
