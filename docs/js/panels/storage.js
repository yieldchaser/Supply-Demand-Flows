/**
 * Storage Panel — weekly EIA working gas, read through `util/storage-analysis`.
 *
 * Transformed into an institutional-grade physical fundamental observatory
 * reproducing the official EIA Weekly Natural Gas Storage Supplement layout:
 *
 *  - Top Controls: View Switcher (National vs Regional), Baseline Horizon
 *    (5Y Tactical, 10Y Decadal, All-Time Secular), and Range Filter (30D, 90D, 1Y, 5Y, MAX).
 *  - National View: Native SVG Dual Plots showing (a) Working Gas in Storage
 *    inventory trajectory against multi-horizon envelope and average, and
 *    (b) Weekly Net Change velocity against historical ranges with 0-line.
 *    Synthesizes live institutional desk commentary bullets.
 *  - Regional View: Native SVG 6-Region Floating Comparison Column Bars
 *    (Pacific, Mountain, SC Salt, SC Non-Salt, Midwest, East) showing selected
 *    horizon min/max, 52-week trailing range, current diamond marker, and WoW header.
 *  - Companion Modules: Days of Cover mini-chart, Season to date standing,
 *    and Regional decomposition table.
 *  - Interactions: cursor: pointer, 1px dashed hairline guide lines, high-precision tooltips.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';
import {
  computeStorageCover,
  computeCoverEnvelope,
  classifyCover,
} from '../util/cover.js';
import { registerChartSnapshot } from '../util/chart-snapshot.js';
import { redrawOnResize } from '../util/responsive-chart.js';
import {
  analyseStorage,
  seasonWeekOf,
  seasonalEnvelope,
  windowChange,
  computeWeeklyNetChanges,
  computeRegionSeasonFill,
  seasonProgress,
  priorSameWeekValues,
} from '../util/storage-analysis.js';
import { matchYearAgo } from '../util/season.js';

const STORAGE_CHART_ID = 'storage-envelope';

const REGIONAL_BAR_KEYS = ['pacific', 'mountain', 'sc_salt', 'sc_nonsalt', 'midwest', 'east'];

const REGION_COLORS = {
  pacific: '#8E561F',
  mountain: '#7A2630',
  sc_salt: '#004B6C',
  sc_nonsalt: '#0284c7',
  midwest: '#C88C00',
  east: '#456B2D',
};

const REGION_LABELS = {
  pacific: 'Pacific',
  mountain: 'Mountain',
  sc_salt: 'South Central: salt',
  sc_nonsalt: 'South Central: nonsalt',
  midwest: 'Midwest',
  east: 'East',
};

export function renderStoragePanel(panelEl, bundle) {
  const source = bundle.sources?.eia_storage;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">EIA Storage data unavailable.</div>';
    return;
  }

  const analysis = analyseStorage(source.data);
  if (!analysis.usable) {
    panelEl.innerHTML = '<div class="panel-error">EIA Storage bundle has no Lower 48 series.</div>';
    return;
  }

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Working Gas in Storage',
    subtitle: 'EIA · Lower 48 working gas & regional decomposition, week ending Friday',
    sourceKey: 'eia_storage',
    latestPeriod: source.latest_period,
    chartExport: STORAGE_CHART_ID,
    draw: () => drawRef.current?.(),
  });

  const { l48, latest, projection, periods } = analysis;

  // Reactive state for View, Horizon, and Range
  const state = {
    view: 'national', // 'national' | 'regional'
    horizon: '5y',    // '5y' | '10y' | 'all'
    range: '1y',      // '30d' | '90d' | '1y' | '5y' | 'max'
  };

  function getLookbackYears() {
    if (state.horizon === '10y') return 10;
    if (state.horizon === 'all') return 32;
    return 5;
  }

  // Create UI Controls Bar and Main Mount inside chartEl
  chartEl.innerHTML = `
    <div class="storage-controls-bar">
      <div class="storage-view-group">
        <button class="storage-btn-pill active" data-view="national">Lower 48 National</button>
        <button class="storage-btn-pill" data-view="regional">Regional Drill-down</button>
        <button class="storage-btn-pill" data-view="salt_texas">Texas 527 Bcf & Salt Caverns</button>
      </div>
      <div class="storage-horizon-group">
        <span style="font-size:10px;text-transform:uppercase;color:var(--text-muted);margin-right:4px;">Baseline:</span>
        <button class="storage-btn-pill active" data-horizon="5y" title="5-Year Tactical Rolling Horizon">5Y Tactical</button>
        <button class="storage-btn-pill" data-horizon="10y" title="10-Year Decadal Rolling Horizon">10Y Decadal</button>
        <button class="storage-btn-pill" data-horizon="all" title="All-Time Secular Baseline: Observed History (2018–present, n=8)">Observed History (2018–present, n=8)</button>
      </div>
      <div class="storage-range-group">
        <span style="font-size:10px;text-transform:uppercase;color:var(--text-muted);margin-right:4px;">Range:</span>
        <button class="storage-btn-pill" data-range="30d">30D</button>
        <button class="storage-btn-pill" data-range="90d">90D</button>
        <button class="storage-btn-pill active" data-range="1y">1Y</button>
        <button class="storage-btn-pill" data-range="5y">5Y</button>
        <button class="storage-btn-pill" data-range="max">MAX</button>
      </div>
    </div>
    <div class="storage-chart-workspace"></div>
    <div class="storage-narrative-workspace"></div>
  `;

  const workspaceEl = chartEl.querySelector('.storage-chart-workspace');
  const narrativeEl = chartEl.querySelector('.storage-narrative-workspace');

  // Wire Controls Event Listeners
  const controlsBar = chartEl.querySelector('.storage-controls-bar');
  if (controlsBar) {
    controlsBar.addEventListener('click', (e) => {
      const btn = e.target.closest('.storage-btn-pill');
      if (!btn) return;

      if (btn.dataset.view) {
        controlsBar.querySelectorAll('[data-view]').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.view = btn.dataset.view;
        drawRef.current?.();
      } else if (btn.dataset.horizon) {
        controlsBar.querySelectorAll('[data-horizon]').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.horizon = btn.dataset.horizon;
        drawRef.current?.();
      } else if (btn.dataset.range) {
        controlsBar.querySelectorAll('[data-range]').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.range = btn.dataset.range;
        drawRef.current?.();
      }
    });
  }

  // Register Snapshot Metadata for export
  const weekKey = (r) => seasonWeekOf(l48, r.period);
  const envelope5Y = envelopeFromRows(l48, latest.period, 5);
  const currentRows = l48.filter((r) => r.period.getFullYear() === latest.period.getFullYear());

  registerChartSnapshot(
    STORAGE_CHART_ID,
    'storage-envelope',
    currentRows.map((r) => {
      const wk = weekKey(r);
      const env = envelope5Y.get(wk);
      return {
        date: r.period.toISOString().slice(0, 10),
        week_of_season: wk,
        bcf: r.value,
        normal_n: env ? env.count : 0,
        normal_mean_bcf: env ? Math.round(env.mean) : null,
        normal_median_bcf: env ? Math.round(env.median) : null,
        p25_bcf: env && Number.isFinite(env.p25) ? Math.round(env.p25) : null,
        p75_bcf: env && Number.isFinite(env.p75) ? Math.round(env.p75) : null,
        min_bcf: env ? Math.round(env.min) : null,
        max_bcf: env ? Math.round(env.max) : null,
        vs_normal_bcf: env ? Math.round(r.value - env.mean) : null,
      };
    }),
    {
      source: 'eia_storage',
      basis: 'Lower 48 weekly working gas (storage_lower48); multi-horizon baselines (5Y, 10Y, All-Time).',
      latest_period: source.latest_period ?? null,
      invariants: {
        periods_checked: analysis.invariants.checked,
        salt_nonsalt_breaks: analysis.invariants.saltPlusNonsalt.length,
        region_sum_breaks: analysis.invariants.regionsSumL48.length,
      },
    },
  );

  // Main Draw Coordinator
  const drawMain = () => {
    if (!workspaceEl) return;
    const lookbackYears = getLookbackYears();

    if (state.view === 'national') {
      const envelope = envelopeFromRows(l48, latest.period, lookbackYears);
      const weeklyNetEnvelope = computeNetChangeEnvelope(l48, latest.period, lookbackYears);
      drawNationalDualPlot(workspaceEl, l48, envelope, weeklyNetEnvelope, state.range, weekKey, projection);
      renderNationalCommentary(narrativeEl, analysis, lookbackYears);
    } else if (state.view === 'regional') {
      const regionalFill = computeRegionSeasonFill(periods, lookbackYears);
      drawRegionalColumnsChart(workspaceEl, regionalFill, lookbackYears);
      renderRegionalCommentary(narrativeEl, regionalFill, lookbackYears);
    } else if (state.view === 'salt_texas') {
      const regionalFill = computeRegionSeasonFill(periods, lookbackYears);
      drawTexasSaltView(workspaceEl, analysis, regionalFill, bundle, lookbackYears);
      renderTexasSaltCommentary(narrativeEl, analysis, regionalFill, lookbackYears);
    }
  };

  drawRef.current = drawMain;
  drawMain();
  redrawOnResize(workspaceEl, drawMain);

  // ── Days-of-Cover Companion Mini-Chart ───────────────────────────────
  const supplySource = bundle.sources?.eia_supply;
  const consumptionRows = supplySource?.data
    ? getSeries(supplySource.data, 'supply_vc0').map((r) => ({
        period: r.period instanceof Date ? r.period : new Date(r.period),
        value: Number(r.value),
      }))
    : [];
  const coverRows = computeStorageCover(l48, consumptionRows);
  const coverEnvelope = computeCoverEnvelope(coverRows, 5);

  insertCoverMiniChart(panelEl, coverRows, coverEnvelope);
  appendSeasonToDate(panelEl, analysis);
  appendRegionTable(panelEl, analysis);
  renderStorageKpis(sidebarEl, analysis);
  appendProvenance(panelEl, analysis);
}

/**
 * Seasonal envelope, keyed by week-of-season.
 */
function envelopeFromRows(l48, anchor, lookbackYears = 5) {
  return new Map(seasonalEnvelope(l48, { anchor, lookbackYears }));
}

/**
 * Velocity envelope for weekly net changes, keyed by week-of-season.
 */
function computeNetChangeEnvelope(l48, anchor, lookbackYears = 5) {
  const cutoffYear = anchor.getFullYear() - lookbackYears;
  const currentYear = anchor.getFullYear();
  const pastRows = l48.filter((r) => r.period.getFullYear() >= cutoffYear && r.period.getFullYear() < currentYear);

  const byYear = new Map();
  for (const r of pastRows) {
    const y = r.period.getFullYear();
    if (!byYear.has(y)) byYear.set(y, []);
    byYear.get(y).push(r);
  }

  const byWeek = new Map();
  for (const [, rows] of byYear) {
    rows.sort((a, b) => a.period - b.period);
    for (let i = 1; i < rows.length; i++) {
      const wk = seasonWeekOf(rows, rows[i].period);
      const delta = rows[i].value - rows[i - 1].value;
      if (!byWeek.has(wk)) byWeek.set(wk, []);
      byWeek.get(wk).push(delta);
    }
  }

  const env = new Map();
  for (const [wk, deltas] of byWeek) {
    if (!deltas.length) continue;
    const min = Math.min(...deltas);
    const max = Math.max(...deltas);
    const sum = deltas.reduce((s, v) => s + v, 0);
    const mean = sum / deltas.length;
    env.set(wk, { min, max, mean, count: deltas.length });
  }
  return env;
}

/* ================================================================== */
/*  1. National Dual Plot (Trajectory + Weekly Net Change Velocity)    */
/* ================================================================== */

function drawNationalDualPlot(container, l48, envelope, netChangeEnv, range, weekKey, projection) {
  container.innerHTML = `
    <div class="storage-dual-grid">
      <div class="storage-plot-card" id="storage-plot-trajectory">
        <div class="storage-plot-header">
          <div>
            <div class="storage-plot-title">Working Gas in Storage (Lower 48)</div>
            <div class="storage-plot-subkpi">billion cubic feet (Bcf) · Friday closes</div>
          </div>
          <div style="text-align:right;">
            <div class="storage-plot-kpi" id="storage-kpi-inventory">—</div>
            <div class="storage-plot-subkpi" id="storage-kpi-asof">—</div>
          </div>
        </div>
        <div class="storage-plot-svg-wrap" id="wrap-plot-inventory"></div>
      </div>

      <div class="storage-plot-card" id="storage-plot-velocity">
        <div class="storage-plot-header">
          <div>
            <div class="storage-plot-title">Weekly Net Change Velocity</div>
            <div class="storage-plot-subkpi">billion cubic feet (Bcf) · net injection / withdrawal</div>
          </div>
          <div style="text-align:right;">
            <div class="storage-plot-kpi" style="color:var(--amber, #f59e0b);" id="storage-kpi-velocity">—</div>
            <div class="storage-plot-subkpi">change from previous week</div>
          </div>
        </div>
        <div class="storage-plot-svg-wrap" id="wrap-plot-velocity"></div>
      </div>
    </div>
  `;

  const wrapInv = container.querySelector('#wrap-plot-inventory');
  const wrapVel = container.querySelector('#wrap-plot-velocity');
  if (!wrapInv || !wrapVel || l48.length === 0) return;

  const latest = l48[l48.length - 1];
  const prev = l48.length >= 2 ? l48[l48.length - 2] : null;
  const wowDelta = prev ? latest.value - prev.value : null;

  const kpiInv = container.querySelector('#storage-kpi-inventory');
  const kpiAsof = container.querySelector('#storage-kpi-asof');
  const kpiVel = container.querySelector('#storage-kpi-velocity');

  if (kpiInv) kpiInv.textContent = `${latest.value.toLocaleString()} Bcf`;
  if (kpiAsof) kpiAsof.textContent = `as of ${latest.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;
  if (kpiVel && wowDelta != null) kpiVel.textContent = `${wowDelta >= 0 ? '+' : ''}${wowDelta.toLocaleString()} Bcf`;

  // Filter current series based on range
  const currentYear = latest.period.getFullYear();
  let displayRows = l48.filter((r) => r.period.getFullYear() === currentYear);

  if (range === '30d') {
    const cutoff = new Date(latest.period.getTime() - 35 * 86400000);
    displayRows = l48.filter((r) => r.period >= cutoff);
  } else if (range === '90d') {
    const cutoff = new Date(latest.period.getTime() - 95 * 86400000);
    displayRows = l48.filter((r) => r.period >= cutoff);
  } else if (range === '5y') {
    const cutoffYear = currentYear - 5;
    displayRows = l48.filter((r) => r.period.getFullYear() >= cutoffYear);
  } else if (range === 'max') {
    displayRows = [...l48];
  }

  // Draw Left Plot: Inventory Trajectory
  drawInventoryPlot(wrapInv, displayRows, envelope, weekKey, projection, range);

  // Draw Right Plot: Weekly Net Change Velocity
  const weeklyChanges = computeWeeklyNetChanges(l48);
  const currentYearChanges = weeklyChanges.filter((c) => c.period.getFullYear() === currentYear);
  let displayChanges = currentYearChanges;
  if (range === '30d') {
    const cutoff = new Date(latest.period.getTime() - 35 * 86400000);
    displayChanges = weeklyChanges.filter((c) => c.period >= cutoff);
  } else if (range === '90d') {
    const cutoff = new Date(latest.period.getTime() - 95 * 86400000);
    displayChanges = weeklyChanges.filter((c) => c.period >= cutoff);
  } else if (range === '5y') {
    const cutoffYear = currentYear - 5;
    displayChanges = weeklyChanges.filter((c) => c.period.getFullYear() >= cutoffYear);
  } else if (range === 'max') {
    displayChanges = [...weeklyChanges];
  }

  drawVelocityPlot(wrapVel, displayChanges, netChangeEnv, weekKey, range);
}

function drawInventoryPlot(container, displayRows, envelope, weekKey, projection, range = '1y') {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 16, right: 16, bottom: 32, left: 54 };
  const width = Math.max(rect.width || 420, 280) - margin.left - margin.right;
  const height = 280 - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 280`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const isShortRange = range === '30d' || range === '90d';
  const rowWeeks = displayRows.map((r) => weekKey(r)).filter((w) => Number.isFinite(w));
  const minWeek = isShortRange && rowWeeks.length ? Math.min(...rowWeeks) : 1;
  const maxWeek = isShortRange && rowWeeks.length ? Math.max(...rowWeeks) : Math.max(52, ...Array.from(envelope.keys()));
  const x = d3.scaleLinear().domain([minWeek, maxWeek]).range([0, width]);

  const allValues = [
    ...displayRows.map((r) => r.value),
    ...Array.from(envelope.values()).flatMap((e) => [e.min, e.max]),
  ];
  if (allValues.length === 0) return;
  const yExtent = d3.extent(allValues);
  const yPadding = (yExtent[1] - yExtent[0]) * 0.06;
  const y = d3.scaleLinear()
    .domain([Math.max(0, yExtent[0] - yPadding), yExtent[1] + yPadding])
    .range([height, 0]);

  // Y gridlines
  const yTicks = y.ticks(5);
  g.selectAll('.gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.05)');

  // Envelope Band (min-max)
  const envData = Array.from(envelope, ([wk, e]) => ({ wk, ...e })).sort((a, b) => a.wk - b.wk);
  if (envData.length > 1) {
    const areaOuter = d3.area()
      .x((d) => x(d.wk)).y0((d) => y(d.min)).y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', areaOuter)
      .attr('fill', 'rgba(125, 211, 252, 0.08)');

    // Historical mean dotted center line
    const meanLine = d3.line()
      .x((d) => x(d.wk)).y((d) => y(d.mean))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', meanLine)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(125, 211, 252, 0.45)')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '3,3');
  }

  // Render seasonal lines: group by calendar year to eliminate zigzags across week 52 -> 1
  const byYear = d3.group(displayRows, (r) => r.period.getFullYear());
  const sortedYears = Array.from(byYear.keys()).sort((a, b) => a - b);
  const latestYear = sortedYears[sortedYears.length - 1];

  const currentLine = d3.line()
    .x((d) => x(weekKey(d))).y((d) => y(d.value))
    .curve(d3.curveMonotoneX);

  for (const yr of sortedYears) {
    const yrRows = byYear.get(yr).sort((a, b) => a.period - b.period);
    const isCurrent = (yr === latestYear);
    g.append('path').datum(yrRows).attr('d', currentLine)
      .attr('fill', 'none')
      .attr('stroke', isCurrent ? 'var(--blue-flame, #0284c7)' : 'rgba(125, 211, 252, 0.3)')
      .attr('stroke-width', isCurrent ? 2.5 : 1.25)
      .attr('stroke-linecap', 'round')
      .style('filter', isCurrent ? 'drop-shadow(0 0 6px rgba(14, 165, 233, 0.5))' : 'none');
  }

  // Latest Point Dot
  if (displayRows.length > 0) {
    const latest = displayRows[displayRows.length - 1];
    g.append('circle')
      .attr('cx', x(weekKey(latest))).attr('cy', y(latest.value))
      .attr('r', 4.5)
      .attr('fill', 'var(--blue-flame, #38bdf8)')
      .attr('stroke', '#ffffff').attr('stroke-width', 1.5)
      .style('filter', 'drop-shadow(0 0 6px rgba(56, 189, 248, 0.7))');
  }

  // X Axis Ticks
  if (isShortRange) {
    const sortedRows = [...displayRows].sort((a, b) => a.period - b.period);
    g.selectAll('.x-tick').data(sortedRows).enter()
      .append('text')
      .attr('x', (d) => x(weekKey(d))).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .attr('fill', 'var(--chart-label, #94a3b8)')
      .text((d) => d.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
  } else {
    const monthTicks = [
      { wk: 1, label: 'Jan' }, { wk: 9, label: 'Mar' },
      { wk: 18, label: 'May' }, { wk: 27, label: 'Jul' },
      { wk: 36, label: 'Sep' }, { wk: 44, label: 'Nov' },
    ];
    g.selectAll('.x-tick').data(monthTicks).enter()
      .append('text')
      .attr('x', (d) => x(d.wk)).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10.5).attr('font-family', 'var(--font-sans)')
      .attr('fill', 'var(--chart-label, #94a3b8)')
      .text((d) => d.label);
  }

  // Y Axis Ticks
  g.selectAll('.y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10.5).attr('font-family', 'var(--font-mono)')
    .attr('fill', 'var(--chart-label, #94a3b8)')
    .text((d) => (d >= 1000 ? `${(d / 1000).toFixed(1)}k` : `${d}`));

  setupTrajectoryHover(svg, g, x, y, width, height, displayRows, envelope, margin, weekKey);
}

function drawVelocityPlot(container, displayChanges, netChangeEnv, weekKey, range = '1y') {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 16, right: 16, bottom: 32, left: 54 };
  const width = Math.max(rect.width || 420, 280) - margin.left - margin.right;
  const height = 280 - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 280`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const isShortRange = range === '30d' || range === '90d';
  const changeWeeks = displayChanges.map((c) => weekKey(c)).filter((w) => Number.isFinite(w));
  const minWeek = isShortRange && changeWeeks.length ? Math.min(...changeWeeks) : 1;
  const maxWeek = isShortRange && changeWeeks.length ? Math.max(...changeWeeks) : Math.max(52, ...Array.from(netChangeEnv.keys()));
  const x = d3.scaleLinear().domain([minWeek, maxWeek]).range([0, width]);

  const allDeltas = [
    ...displayChanges.map((c) => c.value),
    ...Array.from(netChangeEnv.values()).flatMap((e) => [e.min, e.max]),
  ];
  const deltaExtent = allDeltas.length ? d3.extent(allDeltas) : [-150, 150];
  const bound = Math.max(Math.abs(deltaExtent[0]), Math.abs(deltaExtent[1]), 80);
  const y = d3.scaleLinear().domain([-bound * 1.15, bound * 1.15]).range([height, 0]);

  // Zero-equilibrium Line (prominent)
  g.append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', y(0)).attr('y2', y(0))
    .attr('stroke', 'rgba(255, 255, 255, 0.35)')
    .attr('stroke-width', 1.25);

  // Y gridlines
  const yTicks = y.ticks(5);
  g.selectAll('.vel-gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // Shaded Net Change Range Band
  const envData = Array.from(netChangeEnv, ([wk, e]) => ({ wk, ...e })).sort((a, b) => a.wk - b.wk);
  if (envData.length > 1) {
    const areaOuter = d3.area()
      .x((d) => x(d.wk)).y0((d) => y(d.min)).y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', areaOuter)
      .attr('fill', 'rgba(245, 158, 11, 0.08)');

    // Net change average dotted line
    const meanLine = d3.line()
      .x((d) => x(d.wk)).y((d) => y(d.mean))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', meanLine)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(245, 158, 11, 0.45)')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '3,3');
  }

  // Render velocity lines: group by calendar year to eliminate zigzags across week 52 -> 1
  const byYearVel = d3.group(displayChanges, (c) => c.period.getFullYear());
  const sortedVelYears = Array.from(byYearVel.keys()).sort((a, b) => a - b);
  const latestVelYear = sortedVelYears[sortedVelYears.length - 1];

  const velLine = d3.line()
    .x((d) => x(weekKey(d))).y((d) => y(d.value))
    .curve(d3.curveMonotoneX);

  for (const yr of sortedVelYears) {
    const yrChanges = byYearVel.get(yr).sort((a, b) => a.period - b.period);
    const isCurrent = (yr === latestVelYear);
    g.append('path').datum(yrChanges).attr('d', velLine)
      .attr('fill', 'none')
      .attr('stroke', isCurrent ? 'var(--amber, #f59e0b)' : 'rgba(245, 158, 11, 0.3)')
      .attr('stroke-width', isCurrent ? 2.25 : 1.25)
      .attr('stroke-linecap', 'round')
      .style('filter', isCurrent ? 'drop-shadow(0 0 6px rgba(245, 158, 11, 0.4))' : 'none');
  }

  // Latest Point Dot
  if (displayChanges.length > 0) {
    const latest = displayChanges[displayChanges.length - 1];
    g.append('circle')
      .attr('cx', x(weekKey(latest))).attr('cy', y(latest.value))
      .attr('r', 4.5)
      .attr('fill', 'var(--amber, #f59e0b)')
      .attr('stroke', '#ffffff').attr('stroke-width', 1.5)
      .style('filter', 'drop-shadow(0 0 6px rgba(245, 158, 11, 0.7))');
  }

  // X Axis Ticks
  if (isShortRange) {
    const sortedChanges = [...displayChanges].sort((a, b) => a.period - b.period);
    g.selectAll('.x-tick-vel').data(sortedChanges).enter()
      .append('text')
      .attr('x', (d) => x(weekKey(d))).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .attr('fill', 'var(--chart-label, #94a3b8)')
      .text((d) => d.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
  } else {
    const monthTicks = [
      { wk: 1, label: 'Jan' }, { wk: 9, label: 'Mar' },
      { wk: 18, label: 'May' }, { wk: 27, label: 'Jul' },
      { wk: 36, label: 'Sep' }, { wk: 44, label: 'Nov' },
    ];
    g.selectAll('.x-tick-vel').data(monthTicks).enter()
      .append('text')
      .attr('x', (d) => x(d.wk)).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10.5).attr('font-family', 'var(--font-sans)')
      .attr('fill', 'var(--chart-label, #94a3b8)')
      .text((d) => d.label);
  }

  // Y Axis Ticks
  g.selectAll('.y-tick-vel').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10.5).attr('font-family', 'var(--font-mono)')
    .attr('fill', 'var(--chart-label, #94a3b8)')
    .text((d) => (d > 0 ? `+${d}` : `${d}`));

  setupVelocityHover(svg, g, x, y, width, height, displayChanges, netChangeEnv, margin, weekKey);
}

/* ================================================================== */
/*  2. Hover Guides (1px dashed hairline, pointer cursor)              */
/* ================================================================== */

function setupTrajectoryHover(svg, g, x, y, width, height, rows, envelope, margin, weekKey) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'storage-tooltip').style('opacity', 0);

  const guideLine = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.35)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 4.5)
    .style('fill', 'var(--blue-flame, #38bdf8)')
    .attr('stroke', '#ffffff').attr('stroke-width', 1.5)
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('class', 'storage-hitbox')
    .attr('width', width).attr('height', height)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk = Math.round(x.invert(mx));
      const row = [...rows].reverse().find((r) => weekKey(r) === wk);
      const env = envelope.get(wk);

      if (!env && !row) {
        guideLine.style('opacity', 0);
        hoverDot.style('opacity', 0);
        tooltipDiv.style('opacity', 0);
        return;
      }

      guideLine.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const yVal = row ? row.value : env?.mean ?? y.domain()[0];
      const tooltipY = margin.top * scaleX + y(yVal) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 220)}px`)
        .style('top', `${Math.max(tooltipY, 10)}px`);

      if (row) {
        hoverDot.attr('cx', x(wk)).attr('cy', y(row.value)).style('opacity', 1);
        const vsAvg = env ? row.value - env.mean : null;
        const vsAvgPct = env && env.mean ? (vsAvg / env.mean) * 100 : null;

        tooltipDiv.html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} · Wk ${wk}</div>
          <div class="tt-row"><span class="tt-label">Inventory</span><span class="tt-val">${row.value.toLocaleString()} Bcf</span></div>
          ${env ? `<div class="tt-row"><span class="tt-label">Baseline Norm (n=${env.count})</span><span class="tt-val">${Math.round(env.mean).toLocaleString()} Bcf</span></div>` : ''}
          ${vsAvg != null ? `<div class="tt-row"><span class="tt-label">vs Baseline</span><span class="tt-val ${vsAvg >= 0 ? 'pos' : 'neg'}">${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} Bcf (${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%)</span></div>` : ''}
          ${env ? `<div class="tt-row"><span class="tt-label">Range</span><span class="tt-val">${Math.round(env.min).toLocaleString()}–${Math.round(env.max).toLocaleString()} Bcf</span></div>` : ''}
        `);
      } else {
        hoverDot.style('opacity', 0);
        tooltipDiv.html(`
          <div class="tt-date">Week ${wk} · Baseline Norm</div>
          <div class="tt-row"><span class="tt-label">Baseline Mean</span><span class="tt-val">${Math.round(env.mean).toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Range</span><span class="tt-val">${Math.round(env.min).toLocaleString()}–${Math.round(env.max).toLocaleString()} Bcf</span></div>
        `);
      }
    })
    .on('mouseleave', () => {
      guideLine.style('opacity', 0);
      hoverDot.style('opacity', 0);
      tooltipDiv.style('opacity', 0);
    });
}

function setupVelocityHover(svg, g, x, y, width, height, changes, netChangeEnv, margin, weekKey) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'storage-tooltip').style('opacity', 0);

  const guideLine = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.35)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 4.5)
    .style('fill', 'var(--amber, #f59e0b)')
    .attr('stroke', '#ffffff').attr('stroke-width', 1.5)
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('class', 'storage-hitbox')
    .attr('width', width).attr('height', height)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk = Math.round(x.invert(mx));
      const change = [...changes].reverse().find((c) => weekKey(c) === wk);
      const env = netChangeEnv.get(wk);

      if (!change && !env) {
        guideLine.style('opacity', 0);
        hoverDot.style('opacity', 0);
        tooltipDiv.style('opacity', 0);
        return;
      }

      guideLine.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const yVal = change ? change.value : env?.mean ?? 0;
      const tooltipY = margin.top * scaleX + y(yVal) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 220)}px`)
        .style('top', `${Math.max(tooltipY, 10)}px`);

      if (change) {
        hoverDot.attr('cx', x(wk)).attr('cy', y(change.value)).style('opacity', 1);
        const vsMean = env ? change.value - env.mean : null;

        tooltipDiv.html(`
          <div class="tt-date">${change.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} · Wk ${wk}</div>
          <div class="tt-row"><span class="tt-label">Weekly Net Change</span><span class="tt-val ${change.value >= 0 ? 'pos' : 'neg'}">${change.value >= 0 ? '+' : ''}${change.value.toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Injection / Draw Rate</span><span class="tt-val">${(change.value / 7).toFixed(2)} Bcf/d</span></div>
          ${env ? `<div class="tt-row"><span class="tt-label">Norm Net Change (n=${env.count})</span><span class="tt-val">${env.mean >= 0 ? '+' : ''}${Math.round(env.mean)} Bcf</span></div>` : ''}
          ${vsMean != null ? `<div class="tt-row"><span class="tt-label">vs Norm Net Change</span><span class="tt-val ${vsMean >= 0 ? 'pos' : 'neg'}">${vsMean >= 0 ? '+' : ''}${Math.round(vsMean)} Bcf</span></div>` : ''}
          ${env ? `<div class="tt-row"><span class="tt-label">Historical Range</span><span class="tt-val">${Math.round(env.min)} to ${Math.round(env.max)} Bcf</span></div>` : ''}
        `);
      } else {
        hoverDot.style('opacity', 0);
        tooltipDiv.html(`
          <div class="tt-date">Week ${wk} · Historical Net Change</div>
          <div class="tt-row"><span class="tt-label">Norm Average</span><span class="tt-val">${Math.round(env.mean)} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Range</span><span class="tt-val">${Math.round(env.min)} to ${Math.round(env.max)} Bcf</span></div>
        `);
      }
    })
    .on('mouseleave', () => {
      guideLine.style('opacity', 0);
      hoverDot.style('opacity', 0);
      tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  3. 6-Region Floating Comparison Column Bar Chart                  */
/* ================================================================== */

function drawRegionalColumnsChart(container, regionalFillData, lookbackYears) {
  container.innerHTML = `
    <div class="storage-regional-container">
      <div class="storage-plot-header">
        <div>
          <div class="storage-plot-title">Working Natural Gas in Storage by Region</div>
          <div class="storage-plot-subkpi">billion cubic feet (Bcf) · 52-week trailing range vs ${lookbackYears}-year baseline</div>
        </div>
      </div>
      <div class="regional-chart-wrap" style="position:relative;"></div>
      <div class="regional-legend">
        <div class="regional-legend-item"><span class="legend-swatch" style="background:#555;"></span> <span>Gray column: Previous ${lookbackYears} years min–max range</span></div>
        <div class="regional-legend-item"><span class="legend-swatch" style="background:var(--blue-flame, #0284c7);"></span> <span>Color column: Previous 52 weeks min–max range</span></div>
        <div class="regional-legend-item"><span class="legend-diamond"></span> <span>White diamond: Latest weekly inventory</span></div>
        <div class="regional-legend-item"><span style="font-family:var(--font-mono);font-weight:700;color:var(--text-main);">+Δ</span> <span>Header: Change from previous week (Bcf)</span></div>
      </div>
    </div>
  `;

  const chartWrap = container.querySelector('.regional-chart-wrap');
  if (!chartWrap) return;

  const rect = chartWrap.getBoundingClientRect();
  const margin = { top: 36, right: 32, bottom: 48, left: 60 };
  const width = Math.max(rect.width || 800, 480) - margin.left - margin.right;
  const height = 360 - margin.top - margin.bottom;

  const svg = d3.select(chartWrap)
    .append('svg')
    .attr('class', 'regional-columns-svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 360`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Filter and order the 6 regional items
  const items = REGIONAL_BAR_KEYS.map((k) => regionalFillData.find((r) => r.key === k)).filter(Boolean);
  if (items.length === 0) return;

  // X scale: band across the 6 regions
  const x = d3.scaleBand()
    .domain(items.map((d) => d.key))
    .range([0, width])
    .padding(0.42);

  // Y scale: 0 to 1,400 Bcf
  const maxCap = Math.max(1400, ...items.map((d) => d.horizonMaxBcf || 0));
  const y = d3.scaleLinear().domain([0, maxCap]).range([height, 0]);

  // Y gridlines
  const yTicks = [0, 200, 400, 600, 800, 1000, 1200, 1400];
  g.selectAll('.reg-gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.05)');

  // Y labels
  g.selectAll('.reg-y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -10).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 11).attr('font-family', 'var(--font-mono)')
    .attr('fill', 'var(--chart-label, #94a3b8)')
    .text((d) => `${d}`);

  // Floating Columns for each region
  const colGroups = g.selectAll('.reg-col').data(items).enter()
    .append('g').attr('class', 'reg-col')
    .attr('transform', (d) => `translate(${x(d.key)},0)`);

  const barWidth = x.bandwidth();
  const innerInset = 2; // 2px inset on each side => 4px narrower
  const innerWidth = Math.max(2, barWidth - innerInset * 2);

  // 1. Outer Gray Column (Horizon Min to Max) with distinct stroke
  colGroups.append('rect')
    .attr('class', 'reg-col-baseline')
    .attr('x', 0)
    .attr('y', (d) => y(d.horizonMaxBcf || d.observedMaxBcf))
    .attr('width', barWidth)
    .attr('height', (d) => Math.max(4, y(d.horizonMinBcf || 0) - y(d.horizonMaxBcf || d.observedMaxBcf)))
    .attr('fill', 'rgba(255, 255, 255, 0.12)')
    .attr('stroke', 'rgba(255, 255, 255, 0.35)')
    .attr('stroke-width', 1)
    .attr('rx', 2);

  // 2. Inner Colored Column (Trailing 52-week Min to Max, 4px narrower)
  colGroups.append('rect')
    .attr('class', 'reg-col-52w')
    .attr('x', innerInset)
    .attr('y', (d) => y(d.trailing52MaxBcf || d.levelBcf))
    .attr('width', innerWidth)
    .attr('height', (d) => Math.max(4, y(d.trailing52MinBcf || 0) - y(d.trailing52MaxBcf || d.levelBcf)))
    .attr('fill', (d) => REGION_COLORS[d.key] || 'var(--blue-flame)')
    .attr('rx', 2);

  // 3. White Diamond Marker (Latest Value)
  colGroups.each(function (d) {
    const cx = barWidth / 2;
    const cy = y(d.levelBcf);
    const size = 6.5;
    d3.select(this).append('path')
      .attr('d', `M ${cx} ${cy - size} L ${cx + size} ${cy} L ${cx} ${cy + size} L ${cx - size} ${cy} Z`)
      .attr('fill', '#ffffff')
      .attr('stroke', '#0a0e14')
      .attr('stroke-width', 2)
      .style('filter', 'drop-shadow(0 0 3px rgba(0,0,0,0.8))');
  });

  // 4. Header: Signed WoW Net Change
  colGroups.append('text')
    .attr('x', barWidth / 2)
    .attr('y', (d) => {
      const topY = y(Math.max(d.horizonMaxBcf || 0, d.trailing52MaxBcf || 0, d.levelBcf));
      return Math.max(topY - 10, 10);
    })
    .attr('text-anchor', 'middle')
    .attr('font-size', 12.5)
    .attr('font-family', 'var(--font-mono)')
    .attr('font-weight', 700)
    .attr('fill', (d) => REGION_COLORS[d.key] || '#ffffff')
    .text((d) => (d.wowDeltaBcf != null ? (d.wowDeltaBcf > 0 ? `+${d.wowDeltaBcf}` : `${d.wowDeltaBcf}`) : '—'));

  // 5. X Axis Label: Region Name
  colGroups.append('text')
    .attr('x', barWidth / 2)
    .attr('y', height + 18)
    .attr('text-anchor', 'middle')
    .attr('font-size', 11)
    .attr('font-family', 'var(--font-sans)')
    .attr('font-weight', 600)
    .attr('fill', 'var(--text-main, #f1f5f9)')
    .text((d) => REGION_LABELS[d.key] || d.label);

  // Setup Column Hover Tooltip
  setupRegionalHover(svg, g, x, y, width, height, items, barWidth, margin, lookbackYears);
}

function setupRegionalHover(svg, g, x, y, width, height, items, barWidth, margin, lookbackYears) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'storage-tooltip').style('opacity', 0);

  const hitboxes = g.selectAll('.reg-hitbox').data(items).enter()
    .append('rect')
    .attr('class', 'reg-hitbox')
    .attr('x', (d) => x(d.key) - 6)
    .attr('y', 0)
    .attr('width', barWidth + 12)
    .attr('height', height)
    .attr('fill', 'transparent')
    .style('cursor', 'pointer');

  hitboxes
    .on('mousemove', function (event, d) {
      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const [mx] = d3.pointer(event, svg.node());
      const tooltipX = mx + 16;
      const tooltipY = y(d.levelBcf) + margin.top;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 240)}px`)
        .style('top', `${Math.max(tooltipY - 20, 10)}px`)
        .html(`
          <div class="tt-date">${REGION_LABELS[d.key] || d.label}</div>
          <div class="tt-row"><span class="tt-label">Inventory</span><span class="tt-val">${d.levelBcf.toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">WoW Net Change</span><span class="tt-val ${d.wowDeltaBcf >= 0 ? 'pos' : 'neg'}">${d.wowDeltaBcf >= 0 ? '+' : ''}${d.wowDeltaBcf} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">52-Wk Range</span><span class="tt-val">${d.trailing52MinBcf}–${d.trailing52MaxBcf} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">${lookbackYears}-Yr Range</span><span class="tt-val">${d.horizonMinBcf}–${d.horizonMaxBcf} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Fill of Peak</span><span class="tt-val">${d.pctOfObservedMax != null ? `${d.pctOfObservedMax.toFixed(0)}%` : '—'}</span></div>
          ${d.normalMeanBcf != null ? `<div class="tt-row"><span class="tt-label">vs Norm (n=${d.normalN})</span><span class="tt-val ${d.vsNormalBcf >= 0 ? 'pos' : 'neg'}">${d.vsNormalBcf >= 0 ? '+' : ''}${Math.round(d.vsNormalBcf)} Bcf</span></div>` : ''}
        `);
    })
    .on('mouseleave', () => {
      tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  4. Institutional Desk Commentary Cards                            */
/* ================================================================== */

function renderNationalCommentary(container, analysis, lookbackYears) {
  if (!container) return;
  const { latest, l48, environment: env, seasonToDate: std } = analysis;
  const wow = windowChange(l48, 1);
  const yoy = matchYearAgo(l48, latest.period, 1, 6);
  const deltaBcf = wow ? wow.deltaBcf : 0;
  const verb = deltaBcf >= 0 ? 'injections into' : 'withdrawals from';
  const verbNoun = deltaBcf >= 0 ? 'net injections' : 'net withdrawals';
  const when = latest.period.toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
  const year = latest.period.getFullYear();

  const normMean = env.normal?.mean != null ? Math.round(env.normal.mean) : null;
  const vsMean = env.vsNormalBcf != null ? Math.round(env.vsNormalBcf) : null;
  const vsMeanPct = env.vsNormalPct != null ? Math.abs(env.vsNormalPct).toFixed(0) : '0';
  const vsMeanDir = vsMean != null && vsMean >= 0 ? 'more than' : 'lower than';

  const yoyVal = yoy ? yoy.value : null;
  const vsYoy = yoyVal != null ? latest.value - yoyVal : null;
  const vsYoyPct = yoyVal ? ((Math.abs(vsYoy) / yoyVal) * 100).toFixed(0) : '0';
  const vsYoyDir = vsYoy != null && vsYoy >= 0 ? 'higher than' : 'lower than';

  const yoyIdx = l48.findIndex((r) => r.period.getTime() === yoy?.period?.getTime());
  const prevYoy = yoyIdx > 0 ? l48[yoyIdx - 1] : null;
  const yoyNetChange = (yoy && prevYoy) ? yoy.value - prevYoy.value : null;

  const priorWeeks = priorSameWeekValues(l48, latest.period, { lookbackYears });
  const priorDeltas = priorWeeks
    .map((pw) => {
      const idx = l48.findIndex((r) => r.period.getTime() === pw.period.getTime());
      return idx > 0 ? pw.value - l48[idx - 1].value : null;
    })
    .filter((v) => v != null);
  const avgPriorNetChange = priorDeltas.length > 0
    ? Math.round(priorDeltas.reduce((s, v) => s + v, 0) / priorDeltas.length)
    : null;

  const bullets = [
    `Net ${verb} storage totaled ${Math.abs(deltaBcf).toLocaleString()} Bcf for the week ending ${when}, ${avgPriorNetChange != null ? `compared with the ${lookbackYears}-year average ${verbNoun} of ${Math.abs(avgPriorNetChange).toLocaleString()} Bcf and last year's ${yoyNetChange != null ? `${Math.abs(yoyNetChange).toLocaleString()} Bcf` : 'n/a'} during the same week.` : 'according to EIA weekly reporting.'}`,
    `Working natural gas stocks totaled ${latest.value.toLocaleString()} Bcf as of Friday, ${when}, ${year}, according to EIA estimates. Stocks were ${vsMean != null ? `${Math.abs(vsMean).toLocaleString()} Bcf (${vsMeanPct}%) ${vsMeanDir} the ${lookbackYears}-year average` : ''}${vsYoy != null ? ` and ${Math.abs(vsYoy).toLocaleString()} Bcf (${vsYoyPct}%) ${vsYoyDir} last year at this time.` : '.'}`,
  ];

  if (std && std.netChangeBcf != null) {
    const stdVerb = std.netChangeBcf >= 0 ? 'injections into' : 'withdrawals from';
    const seasonInfo = seasonProgress(latest.period);
    const seasonName = seasonInfo.season === 'injection' ? 'refill season (April through October)' : 'withdrawal season (November through March)';
    bullets.push(
      `The average rate of ${stdVerb} storage is ${analysis.pace ? `${Math.abs(analysis.pace.bePerDay).toFixed(1)} Bcf/d` : 'measured'} so far in the ${seasonName}. Cumulative net ${std.netChangeBcf >= 0 ? 'injection' : 'withdrawal'} stands at ${Math.abs(std.netChangeBcf).toLocaleString()} Bcf to date against a ${lookbackYears}-year prior range of ${std.priorMinBcf != null ? std.priorMinBcf.toLocaleString() : '—'} to ${std.priorMaxBcf != null ? std.priorMaxBcf.toLocaleString() : '—'} Bcf.`,
    );
  }

  container.innerHTML = `
    <div class="storage-narrative-card">
      <div class="storage-narrative-title">Official Weekly Storage Disclosures · EIA Form 912</div>
      <ul class="storage-narrative-list">
        ${bullets.map((b) => `<li>${b}</li>`).join('')}
      </ul>
    </div>
  `;
}

function renderRegionalCommentary(container, regionalFillData, lookbackYears) {
  if (!container) return;
  const bullets = [];
  const aboveNorm = regionalFillData.filter((r) => r.vsNormalBcf != null && r.vsNormalBcf > 0);
  const belowNorm = regionalFillData.filter((r) => r.vsNormalBcf != null && r.vsNormalBcf < 0);

  if (aboveNorm.length > 0 && belowNorm.length > 0) {
    bullets.push(
      `Natural gas inventories remain above their ${lookbackYears}-year averages in ${aboveNorm.map((r) => REGION_LABELS[r.key] || r.label).join(', ')}, while ${belowNorm.map((r) => REGION_LABELS[r.key] || r.label).join(', ')} remain below historical norms.`,
    );
  } else if (aboveNorm.length > 0) {
    bullets.push(`Inventories across all reporting regions sit above their ${lookbackYears}-year average baselines.`);
  }

  const scSalt = regionalFillData.find((r) => r.key === 'sc_salt');
  const scNonSalt = regionalFillData.find((r) => r.key === 'sc_nonsalt');
  if (scSalt && scNonSalt) {
    bullets.push(
      `South Central Salt facilities posted a WoW net change of ${scSalt.wowDeltaBcf >= 0 ? '+' : ''}${scSalt.wowDeltaBcf} Bcf (stocks: ${scSalt.levelBcf.toLocaleString()} Bcf), while Non-Salt facilities posted ${scNonSalt.wowDeltaBcf >= 0 ? '+' : ''}${scNonSalt.wowDeltaBcf} Bcf (stocks: ${scNonSalt.levelBcf.toLocaleString()} Bcf).`,
    );
  }

  const otherRegions = regionalFillData.filter((r) => !r.key.startsWith('sc_'));
  const otherSummary = otherRegions.map((r) => `${REGION_LABELS[r.key] || r.label} ${r.wowDeltaBcf >= 0 ? '+' : ''}${r.wowDeltaBcf} Bcf`).join(', ');
  if (otherSummary) {
    bullets.push(`Regional net weekly movements: ${otherSummary}.`);
  }

  container.innerHTML = `
    <div class="storage-narrative-card">
      <div class="storage-narrative-title">Regional Storage Disclosures · EIA Form 912</div>
      <ul class="storage-narrative-list">
        ${bullets.map((b) => `<li>${b}</li>`).join('')}
      </ul>
    </div>
  `;
}

/* ================================================================== */
/*  4b. Texas 527 Bcf Benchmark & 17 Gulf Coast Salt Caverns View     */
/* ================================================================== */

const GULF_COAST_SALT_FACILITIES = [
  { name: 'Petal Gas Storage', state: 'MS', basin: 'Hattiesburg Dome', pipe: 'Gulf South / Tennessee / SESH', re: /petal/i, cycle: 'Rapid 10–12x' },
  { name: 'Napoleonville Storage', state: 'LA', basin: 'Napoleonville Dome', pipe: 'Bridgeline / Gulf South', re: /napoleonville/i, cycle: 'Rapid 8–10x' },
  { name: 'Bobcat Storage', state: 'LA', basin: 'Port Barre Dome', pipe: 'Gulf South', re: /bobcat/i, cycle: 'Rapid 8–10x' },
  { name: 'Arcadia Gas Storage', state: 'LA', basin: 'Arcadia Dome', pipe: 'Gulf South', re: /arcadia/i, cycle: 'Rapid 6–8x' },
  { name: 'Leaf River Storage', state: 'MS', basin: 'Leaf River Dome', pipe: 'Gulf South / Transco', re: /leaf river/i, cycle: 'Rapid 8–10x' },
  { name: 'Bay Gas Storage @ Axis', state: 'AL', basin: 'McIntosh Dome', pipe: 'Gulf South / Florida Gas', re: /bay gas.*axis/i, cycle: 'Rapid 8–10x' },
  { name: 'Bay Gas Storage @ Whistler', state: 'AL', basin: 'McIntosh Dome', pipe: 'Gulf South / Whistler', re: /bay gas.*whistler/i, cycle: 'Rapid 8–10x' },
  { name: 'Jefferson Island Storage', state: 'LA', basin: 'Jefferson Island Dome', pipe: 'Gulf South / NGPL', re: /jefferson island/i, cycle: 'Rapid 6–8x' },
  { name: 'Tres Palacios Storage', state: 'TX', basin: 'Matagorda Salt Dome', pipe: 'Gulf South / TETCO / NGPL', re: /tres palacios/i, cycle: 'Rapid 10–12x' },
  { name: 'Enstor Katy Storage', state: 'TX', basin: 'Katy Gas Hub', pipe: 'Gulf South / Oasis', re: /enstor katy/i, cycle: 'Rapid 8–10x' },
  { name: 'Stratton Ridge (Freeport)', state: 'TX', basin: 'Stratton Ridge Dome', pipe: 'Gulf South / Freeport LNG', re: /stratton ridge/i, cycle: 'Rapid 12x LNG Buffer' },
  { name: 'Bistineau Storage', state: 'LA', basin: 'Bistineau Dome', pipe: 'Gulf South / Enable', re: /bistineau/i, cycle: 'High-cycling 6x' },
  { name: 'Jackson Storage', state: 'MS', basin: 'Jackson Dome', pipe: 'Gulf South', re: /jackson storage/i, cycle: 'Rapid 6–8x' },
  { name: 'Moss Bluff Storage', state: 'TX', basin: 'Moss Bluff Dome', pipe: 'TETCO / Natural Gas', re: /moss bluff/i, cycle: 'Rapid 8–10x' },
  { name: 'Golden Triangle Storage', state: 'TX', basin: 'Jefferson County Dome', pipe: 'Golden Triangle / Port Arthur', re: /golden triangle/i, cycle: 'Rapid 10x' },
  { name: 'Pine Prairie Energy Hub', state: 'LA', basin: 'Pine Prairie Dome', pipe: 'Tennessee / Columbia Gulf', re: /pine prairie/i, cycle: 'Rapid 10–12x' },
  { name: 'Egan Gas Storage', state: 'LA', basin: 'Egan Salt Dome', pipe: 'Columbia Gulf / ANR', re: /egan/i, cycle: 'Rapid 8–10x' },
];

function drawTexasSaltView(container, analysis, regionalFill, bundle, lookbackYears) {
  container.innerHTML = '';
  const scSalt = regionalFill?.find((r) => r.key === 'sc_salt') || {
    levelBcf: 0,
    wowDeltaBcf: 0,
    observedMaxBcf: 400,
    horizonMinBcf: 200,
    horizonMaxBcf: 400,
    trailing52MinBcf: 200,
    trailing52MaxBcf: 400,
  };

  const texasBenchmarkBcf = 527.0;
  const fillPct = ((scSalt.levelBcf / texasBenchmarkBcf) * 100).toFixed(1);
  const horizonLabel = lookbackYears >= 25 ? 'Observed History (2018–present, n=8)' : `${lookbackYears}-Year`;

  // Aggregate salt cavern nominations from pipeline EBB (gulf_south)
  // Strictly filter for scheduled quantity (_sq_) and exclude operational available capacity (_oac_)
  const gs = (bundle?.sources?.gulf_south?.data || []).filter(
    (r) => (r.series_id || '').includes('_sq_') && !(r.series_id || '').includes('_oac_')
  );

  const CYCLE_ORDER = { id3: 5, id2: 4, id1: 3, evening: 2, timely: 1 };

  const cavernRows = GULF_COAST_SALT_FACILITIES.map((f) => {
    const matched = gs.filter((r) => f.re.test(r.series_name || ''));
    if (!matched.length) {
      return {
        ...f,
        delMMcf: 0,
        recMMcf: 0,
        injMMcf: 0,
        wdrMMcf: 0,
        netMMcf: 0,
        status: 'Active Standby / Regional Grid',
        badgeClass: 'salt-badge--idle',
      };
    }
    const latestPeriod = matched.map((r) => r.period).sort().pop();
    const periodRows = matched.filter((r) => r.period === latestPeriod);

    // Group by meter and apply cycle precedence: id3 > id2 > id1 > evening > timely
    const byMeter = new Map();
    for (const r of periodRows) {
      const cycleMatch = (r.series_id || '').match(/_(id3|id2|id1|evening|timely)$/i);
      const cycle = cycleMatch ? cycleMatch[1].toLowerCase() : '';
      const meterKey = (r.series_id || '').replace(/_(id3|id2|id1|evening|timely)$/i, '');
      const priority = CYCLE_ORDER[cycle] || 0;
      if (!byMeter.has(meterKey) || byMeter.get(meterKey).priority < priority) {
        byMeter.set(meterKey, { row: r, priority, cycle });
      }
    }

    let inj = 0, wdr = 0;
    for (const { row: r } of byMeter.values()) {
      const val = Number(r.value || 0);
      const name = r.series_name || '';
      const id = r.series_id || '';
      // Correct physical flow polarity:
      // Pipeline Delivery [D] or [SI] (Storage Injection) = Injection into storage
      // Pipeline Receipt [R] or [SW] (Storage Withdrawal) = Withdrawal from storage
      if (/\[si\]|\[d\]/i.test(name) || /_(si|d)_/i.test(id)) {
        inj += val;
      } else if (/\[sw\]|\[r\]/i.test(name) || /_(sw|r)_/i.test(id)) {
        wdr += val;
      }
    }
    const net = (inj - wdr) / 1000;
    let status = 'Standby / Balanced';
    let badgeClass = 'salt-badge--idle';
    if (net > 0.5) {
      status = `Injection (${net.toFixed(1)} MMcf/d)`;
      badgeClass = 'salt-badge--inj';
    } else if (net < -0.5) {
      status = `Withdrawal (${Math.abs(net).toFixed(1)} MMcf/d)`;
      badgeClass = 'salt-badge--wdr';
    }
    return {
      ...f,
      injMMcf: inj / 1000,
      wdrMMcf: wdr / 1000,
      delMMcf: inj / 1000,
      recMMcf: wdr / 1000,
      netMMcf: net,
      status,
      badgeClass,
      latestPeriod,
    };
  });

  const saltHtml = `
    <div class="storage-salt-container">
      <div class="storage-salt-kpis">
        <div class="storage-salt-card">
          <div class="storage-salt-card__label">Texas Nameplate Working Benchmark</div>
          <div class="storage-salt-card__val">527.0 <span style="font-size:14px;color:var(--text-muted);">Bcf</span></div>
          <div class="storage-salt-card__sub">Texas Railroad Commission (RRC) nameplate working gas benchmark</div>
        </div>
        <div class="storage-salt-card">
          <div class="storage-salt-card__label">South Central Salt Working Stock</div>
          <div class="storage-salt-card__val" style="color:var(--blue-flame);">${scSalt.levelBcf.toLocaleString()} <span style="font-size:14px;color:var(--text-muted);">Bcf</span></div>
          <div class="storage-salt-card__sub">EIA 7-state salt dome formation · ${fillPct}% of Texas nameplate · WoW ${scSalt.wowDeltaBcf >= 0 ? '+' : ''}${scSalt.wowDeltaBcf} Bcf</div>
        </div>
        <div class="storage-salt-card">
          <div class="storage-salt-card__label">${horizonLabel} Salt Operating Band</div>
          <div class="storage-salt-card__val">${scSalt.horizonMinBcf} – ${scSalt.horizonMaxBcf} <span style="font-size:14px;color:var(--text-muted);">Bcf</span></div>
          <div class="storage-salt-card__sub">Trailing 52W: ${scSalt.trailing52MinBcf} – ${scSalt.trailing52MaxBcf} Bcf · 6–12x annual turnover</div>
        </div>
      </div>

      <div class="storage-salt-gauge-card">
        <div class="storage-salt-gauge-title">Texas Storage Inventory vs 527 Bcf Working Gas Benchmark</div>
        <div class="storage-salt-gauge-sub">South Central salt cavern inventory (EIA 7-state formation) benchmarked against aggregate Texas nameplate capacity (EIA Weekly Form 912 & Texas RRC)</div>
        <div class="storage-salt-gauge-svg-wrap"></div>
      </div>

      <div class="storage-salt-table-card">
        <div class="storage-salt-gauge-title" style="margin-bottom:6px;">17 Gulf Coast Rapid-Cycling Salt Cavern Facilities</div>
        <div class="storage-salt-gauge-sub">Daily pipeline deliveries / injections and receipts / withdrawals tracked across interstate EBB nominations</div>
        <table class="storage-salt-table">
          <thead>
            <tr>
              <th scope="col">Facility Name</th>
              <th scope="col">Basin / Salt Dome</th>
              <th scope="col">State</th>
              <th scope="col">Interconnecting Pipeline</th>
              <th scope="col">Cycle Profile</th>
              <th scope="col">Physical Flow Regime</th>
            </tr>
          </thead>
          <tbody>
            ${cavernRows.map((r) => `
              <tr>
                <td style="font-weight:600;">${r.name}</td>
                <td>${r.basin}</td>
                <td><span style="font-family:var(--font-mono);font-size:11px;color:var(--text-muted);">${r.state}</span></td>
                <td style="font-size:11px;color:var(--text-muted);">${r.pipe}</td>
                <td><span style="font-family:var(--font-mono);font-size:11px;">${r.cycle}</span></td>
                <td><span class="salt-badge ${r.badgeClass}">${r.status}</span></td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;

  container.innerHTML = saltHtml;

  // Render SVG Horizontal Fill Gauge
  const svgWrap = container.querySelector('.storage-salt-gauge-svg-wrap');
  if (svgWrap) {
    const totalW = 860;
    const totalH = 100;
    const margin = { left: 40, right: 40, top: 20, bottom: 35 };
    const w = totalW - margin.left - margin.right;
    const h = totalH - margin.top - margin.bottom;

    const svg = d3.select(svgWrap)
      .append('svg')
      .attr('viewBox', `0 0 ${totalW} ${totalH}`)
      .attr('preserveAspectRatio', 'xMidYMid meet')
      .style('width', '100%')
      .style('display', 'block');

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    const x = d3.scaleLinear().domain([0, 527]).range([0, w]);

    // Background track
    g.append('rect')
      .attr('x', 0)
      .attr('y', 10)
      .attr('width', w)
      .attr('height', 24)
      .attr('rx', 4)
      .attr('fill', 'rgba(255, 255, 255, 0.05)')
      .attr('stroke', 'rgba(255, 255, 255, 0.12)')
      .attr('stroke-width', 1);

    // Historical Min-Max Band
    const bandX1 = x(Math.max(0, scSalt.horizonMinBcf));
    const bandX2 = x(Math.min(527, scSalt.horizonMaxBcf));
    g.append('rect')
      .attr('x', bandX1)
      .attr('y', 7)
      .attr('width', Math.max(2, bandX2 - bandX1))
      .attr('height', 30)
      .attr('rx', 3)
      .attr('fill', 'rgba(255, 255, 255, 0.1)')
      .attr('stroke', 'rgba(255, 255, 255, 0.25)')
      .attr('stroke-dasharray', '2 2');

    // Current Fill Bar
    const fillW = Math.max(0, Math.min(w, x(scSalt.levelBcf)));
    g.append('rect')
      .attr('x', 0)
      .attr('y', 10)
      .attr('width', fillW)
      .attr('height', 24)
      .attr('rx', 4)
      .attr('fill', 'url(#salt-fill-grad)');

    // Gradient Definition
    const defs = svg.append('defs');
    const grad = defs.append('linearGradient').attr('id', 'salt-fill-grad').attr('x1', '0%').attr('x2', '100%');
    grad.append('stop').attr('offset', '0%').attr('stop-color', '#004B6C');
    grad.append('stop').attr('offset', '100%').attr('stop-color', '#38bdf8');

    // White Diamond Marker at current inventory
    const diamondSize = 7;
    const curX = x(scSalt.levelBcf);
    g.append('path')
      .attr('d', `M ${curX} ${22 - diamondSize} L ${curX + diamondSize} 22 L ${curX} ${22 + diamondSize} L ${curX - diamondSize} 22 Z`)
      .attr('fill', '#ffffff')
      .attr('stroke', '#0f172a')
      .attr('stroke-width', 1.5)
      .attr('class', 'storage-chart-hitbox')
      .style('cursor', 'pointer');

    // Ticks & Labels
    const ticks = [0, 100, 200, 300, 400, 500, 527];
    for (const t of ticks) {
      const tx = x(t);
      g.append('line')
        .attr('x1', tx).attr('x2', tx)
        .attr('y1', 38).attr('y2', 43)
        .attr('stroke', 'rgba(255, 255, 255, 0.2)');

      g.append('text')
        .attr('x', tx)
        .attr('y', 54)
        .attr('text-anchor', t === 527 ? 'end' : t === 0 ? 'start' : 'middle')
        .attr('font-family', 'var(--font-mono)')
        .attr('font-size', '10px')
        .attr('fill', t === 527 ? 'var(--blue-flame)' : 'var(--text-muted)')
        .text(t === 527 ? '527 Bcf Benchmark' : `${t}`);
    }

    // Callout text above current level
    g.append('text')
      .attr('x', curX)
      .attr('y', 0)
      .attr('text-anchor', curX > w - 80 ? 'end' : curX < 80 ? 'start' : 'middle')
      .attr('font-family', 'var(--font-mono)')
      .attr('font-size', '11px')
      .attr('font-weight', '600')
      .attr('fill', '#ffffff')
      .text(`${scSalt.levelBcf} Bcf (${fillPct}%)`);
  }
}

function renderTexasSaltCommentary(container, analysis, regionalFill, lookbackYears) {
  if (!container) return;
  const scSalt = regionalFill?.find((r) => r.key === 'sc_salt') || { levelBcf: 0, wowDeltaBcf: 0, horizonMinBcf: 200, horizonMaxBcf: 400 };
  const scNonSalt = regionalFill?.find((r) => r.key === 'sc_nonsalt') || { levelBcf: 0, wowDeltaBcf: 0 };
  const texasBenchmarkBcf = 527.0;
  const fillPct = ((scSalt.levelBcf / texasBenchmarkBcf) * 100).toFixed(1);
  const horizonLabel = lookbackYears >= 25 ? 'Observed History (2018–present, n=8)' : `${lookbackYears}-Year`;

  container.innerHTML = `
    <div class="storage-plot-card" style="margin-top: 16px;">
      <div class="storage-plot-header">
        <div>
          <span class="storage-plot-title">Texas 527 Bcf & Salt Deliverability Desk Assessment</span>
          <div class="storage-plot-subkpi">High-deliverability physical cycling across EIA 7-state salt dome formation vs Texas Railroad Commission benchmark</div>
        </div>
        <div class="storage-badge-pill" style="border-color: rgba(56, 189, 248, 0.4); color: var(--blue-flame);">
          ${scSalt.levelBcf} Bcf Working Stock
        </div>
      </div>
      <div class="storage-narrative-prose">
        <p>
          South Central salt dome storage inventory (covering the EIA 7-state salt dome formation across TX, LA, MS, AL, AR, KS, OK) stands at <strong style="color:var(--text-main);">${scSalt.levelBcf} Bcf</strong>,
          benchmarked against the 527.0 Bcf Texas Railroad Commission (RRC) nameplate working gas capacity (<strong style="color:var(--blue-flame);">${fillPct}%</strong> equivalent).
          Weekly net change registered <strong style="color:var(--text-main);">${scSalt.wowDeltaBcf >= 0 ? '+' : ''}${scSalt.wowDeltaBcf} Bcf</strong>,
          operating within the ${horizonLabel} historical seasonal band of ${scSalt.horizonMinBcf} to ${scSalt.horizonMaxBcf} Bcf.
        </p>
        <p>
          Unlike depleted sandstone reservoirs in the South Central Non-Salt region (<strong style="color:var(--text-main);">${scNonSalt.levelBcf} Bcf</strong>,
          WoW ${scNonSalt.wowDeltaBcf >= 0 ? '+' : ''}${scNonSalt.wowDeltaBcf} Bcf) which turn over once or twice per year, Gulf Coast salt caverns operate with rapid-cycling capability (6 to 12 inventory turns per annual cycle).
          These facilities provide immediate physical balancing for intraday swings across Gulf Coast LNG liquefaction trains (Freeport, Sabine Pass, Cameron, Corpus Christi)
          and sudden ERCOT/MISO electric power generation demand surges.
        </p>
      </div>
    </div>
  `;
}

/* ================================================================== */
/*  5. Days-of-Cover Companion Mini-Chart                             */
/* ================================================================== */

function insertCoverMiniChart(panelEl, coverRows, coverEnvelope) {
  const panelBody = panelEl.querySelector('.panel-body');
  if (!panelBody || coverRows.length === 0) return;

  const latestCover = coverRows[coverRows.length - 1];
  const classification = classifyCover(latestCover, coverEnvelope);
  const env = classification?.envelope;

  const classKind =
    ['critical', 'low'].includes(classification?.label) ? 'bearish'
    : ['elevated', 'comfortable'].includes(classification?.label) ? 'bullish'
    : 'neutral';

  const rangeText = env
    ? `5-yr range: ${Math.round(env.min)}–${Math.round(env.max)} days`
    : 'Storage ÷ daily consumption';

  const wrapper = document.createElement('div');
  wrapper.className = 'panel-subchart';
  wrapper.setAttribute('data-chapter-section', 'overview-cover');
  wrapper.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Days of Cover</span>
        <span class="panel-subchart__hero num">${Math.round(latestCover.daysOfCover)} days</span>
        <span class="panel-subchart__badge panel-subchart__badge--${classKind}">${(classification?.label || 'normal').toUpperCase()}</span>
      </div>
      <span class="panel-subchart__subtitle">Weekly trajectory · ${rangeText}</span>
    </div>
    <div class="panel-subchart__body"></div>
  `;

  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrapper, sidebar || null);

  const coverBody = wrapper.querySelector('.panel-subchart__body');
  const drawCover = () => drawCoverMiniChart(coverBody, coverRows, coverEnvelope);
  drawCover();
  redrawOnResize(coverBody, drawCover);
}

function drawCoverMiniChart(container, coverRows, envelope) {
  if (!container) return;
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const W = Math.max(rect.width || 600, 300);
  const H = 140;
  const margin = { top: 8, right: 16, bottom: 32, left: 48 };
  const width = W - margin.left - margin.right;
  const height = H - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${W} ${H}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  if (!coverRows || coverRows.length === 0) {
    container.innerHTML = '<div style="color:rgba(255,255,255,0.3);font-size:12px;padding:16px">No cover history available.</div>';
    return;
  }
  const currentYear = Math.max(...coverRows.map((r) => r.period.getFullYear()));
  const currentRows = coverRows.filter((r) => r.period.getFullYear() === currentYear);
  if (currentRows.length === 0) {
    container.innerHTML = '<div style="color:rgba(255,255,255,0.3);font-size:12px;padding:16px">Insufficient data for current year.</div>';
    return;
  }

  const weekKey = (r) => seasonWeekOf(currentRows, r.period);
  const maxWeek = Math.max(52, ...Array.from(envelope.keys()));
  const x = d3.scaleLinear().domain([1, maxWeek]).range([0, width]);

  const envValues = Array.from(envelope.values()).flatMap((e) => [e.min, e.max]);
  const curValues = currentRows.map((r) => r.daysOfCover);
  const allValues = [...envValues, ...curValues].filter((v) => v != null && isFinite(v));
  if (allValues.length === 0) return;
  const yExtent = d3.extent(allValues);
  const yPad = Math.max((yExtent[1] - yExtent[0]) * 0.08, 2);
  const y = d3.scaleLinear()
    .domain([Math.max(0, yExtent[0] - yPad), yExtent[1] + yPad])
    .range([height, 0]);

  const yTicks = y.ticks(4);
  g.selectAll('.cov-gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  g.selectAll('.cov-y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
    .style('font-feature-settings', "'tnum'")
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => `${Math.round(d)}d`);

  const monthTicks = [
    { wk: 1, label: 'Jan' }, { wk: 9, label: 'Mar' },
    { wk: 18, label: 'May' }, { wk: 27, label: 'Jul' },
    { wk: 36, label: 'Sep' }, { wk: 44, label: 'Nov' },
  ];
  g.selectAll('.cov-x-tick').data(monthTicks).enter()
    .append('text')
    .attr('x', (d) => x(d.wk)).attr('y', height + 18)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => d.label);

  const envData = Array.from(envelope, ([wk, e]) => ({ wk, ...e })).sort((a, b) => a.wk - b.wk);
  if (envData.length > 1) {
    const areaGen = d3.area()
      .x((d) => x(d.wk)).y0((d) => y(d.min)).y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', areaGen)
      .attr('fill', 'rgba(125, 211, 252, 0.08)');

    const medianLine = d3.line()
      .x((d) => x(d.wk)).y((d) => y(d.median))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', medianLine)
      .attr('fill', 'none')
      .style('stroke', 'rgba(125, 211, 252, 0.35)')
      .attr('stroke-width', 1)
      .attr('stroke-dasharray', '2,3');
  }

  const lineGen = d3.line()
    .x((d) => x(weekKey(d)))
    .y((d) => y(d.daysOfCover))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(currentRows).attr('d', lineGen)
    .attr('fill', 'none')
    .style('stroke', 'var(--blue-flame)')
    .attr('stroke-width', 2)
    .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.4))');

  const latest = currentRows[currentRows.length - 1];
  g.append('circle')
    .attr('cx', x(weekKey(latest)))
    .attr('cy', y(latest.daysOfCover))
    .attr('r', 3.5)
    .style('fill', 'var(--blue-flame)')
    .style('filter', 'drop-shadow(0 0 5px rgba(125, 211, 252, 0.5))');

  setupCoverHover(svg, g, x, y, width, height, currentRows, margin, weekKey, envelope);
}

function setupCoverHover(svg, g, x, y, width, height, currentRows, margin, weekKey, envelope) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip').style('opacity', 0);

  const guideLine = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 4)
    .style('fill', 'var(--blue-flame)')
    .attr('stroke', 'rgba(10,14,20,0.8)').attr('stroke-width', 2)
    .style('pointer-events', 'none')
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk = Math.round(x.invert(mx));
      const row = currentRows.find((r) => weekKey(r) === wk);
      if (!row) {
        guideLine.style('opacity', 0);
        hoverDot.style('opacity', 0);
        tooltipDiv.style('opacity', 0);
        return;
      }
      guideLine.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);
      hoverDot.attr('cx', x(wk)).attr('cy', y(row.daysOfCover)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const tooltipY = margin.top * scaleX + y(row.daysOfCover) * scaleX - 20;

      const env = envelope?.get(wk) ?? null;
      const vsNormal = env ? Math.round(row.daysOfCover - env.mean) : null;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 190)}px`)
        .style('top', `${Math.max(tooltipY, 10)}px`)
        .html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Days of Cover</span><span class="num">${Math.round(row.daysOfCover)} days</span></div>
          ${env ? `<div class="tt-row"><span class="tt-label">5-Year Seasonal Norm</span><span class="num">${Math.round(env.mean)} days</span></div>
          <div class="tt-row"><span class="tt-label">vs 5-Year Norm</span><span class="num">${vsNormal > 0 ? '+' : ''}${vsNormal} days</span></div>
          <div class="tt-row"><span class="tt-label">Sample</span><span class="num">n=${env.count}</span></div>` : '<div class="tt-row"><span class="tt-label">5-Year Seasonal Norm</span><span class="num">n/a</span></div>'}
        `);
    })
    .on('mouseleave', () => {
      guideLine.style('opacity', 0);
      hoverDot.style('opacity', 0);
      tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  6. KPI Sidebar                                                    */
/* ================================================================== */

function renderStorageKpis(sidebarEl, analysis) {
  const { environment: env, projection, pace, season, l48 } = analysis;
  if (!env || l48.length < 2) return;

  const wow = windowChange(l48, 1);
  const latestWk = env.normal?.weekOfSeason;

  const pos = (v) => (v >= 0 ? 'bullish' : 'bearish');
  const signed = (v) => `${v >= 0 ? '+' : ''}${Math.round(v).toLocaleString()}`;

  const cards = [
    kpiCardHtml({
      label: 'Latest',
      value: `${env.valueBcf.toLocaleString()} Bcf`,
      helpText: `Week ending ${env.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`,
    }),
    kpiCardHtml({
      label: 'WoW change',
      value: wow ? `${signed(wow.deltaBcf)} Bcf` : 'n/a',
      delta: wow ? { value: `${wow.rate >= 0 ? '+' : ''}${wow.rate.toFixed(2)} Bcf/d`, kind: pos(wow.deltaBcf) } : null,
      helpText: wow ? `over ${wow.days} days` : '',
    }),
    kpiCardHtml({
      label: `vs mean n=${env.normal?.n ?? 0}`,
      value: env.vsNormalBcf != null ? `${signed(env.vsNormalBcf)} Bcf` : 'n/a',
      delta:
        env.vsNormalPct != null
          ? { value: `${env.vsNormalPct >= 0 ? '+' : ''}${env.vsNormalPct.toFixed(1)}%`, kind: pos(env.vsNormalBcf) }
          : null,
      helpText: `Week ${latestWk} · vs median ${env.vsMedianBcf != null ? signed(env.vsMedianBcf) : 'n/a'} Bcf`,
    }),
    kpiCardHtml({
      label: 'Of nominal max',
      value: `${env.pctOfMax.toFixed(1)}%`,
      helpText: `4,000 Bcf nominal · observed peak ${env.observedMaxBcf?.toLocaleString() ?? 'n/a'} Bcf`,
    }),
    kpiCardHtml({
      label: `Rank ${env.rankPct != null ? `p${Math.round(env.rankPct)}` : '—'}`,
      value: `of n=${env.rankN}`,
      helpText: env.rankN ? 'Same calendar week, prior years' : 'Insufficient prior years',
    }),
    kpiCardHtml({
      label: `Season wk ${season.weeksElapsed}/${season.weeksTotal}`,
      value: season.season === 'injection' ? 'Injection' : 'Withdrawal',
      helpText: `${season.progressPct.toFixed(0)}% elapsed`,
    }),
    kpiCardHtml({
      label: `Pace ${pace?.n ?? 0}w`,
      value: pace ? `${pace.bePerDay >= 0 ? '+' : ''}${pace.bePerDay.toFixed(2)} Bcf/d` : 'n/a',
      delta: pace?.seBePerDay != null ? { value: `± ${pace.seBePerDay.toFixed(2)}`, kind: 'neutral' } : null,
      helpText: pace
        ? `R² ${pace.r2.toFixed(2)} · last week ${pace.thisWeekBePerDay.toFixed(2)} Bcf/d`
        : '',
    }),
  ];

  if (projection) {
    cards.push(
      kpiCardHtml({
        label: `+${projection.horizonWeeks}w model`,
        value: `${Math.round(projection.modelBcf).toLocaleString()} Bcf`,
        delta:
          projection.residualQ50 != null
            ? {
                value: `median residual ${signed(projection.residualQ50)} (n=${projection.empiricalN})`,
                kind: 'neutral',
              }
            : null,
        helpText:
          projection.residualQ10 != null && projection.residualQ90 != null
            ? `Realised 8w band ${Math.round(projection.residualQ10)} to ${Math.round(projection.residualQ90)} Bcf`
            : 'Insufficient prior years for a band',
      }),
    );
  }

  sidebarEl.innerHTML = cards.join('');
}

/* ================================================================== */
/*  7. Regional Decomposition Table                                   */
/* ================================================================== */

function appendRegionTable(panelEl, analysis) {
  const panelBody = panelEl.querySelector('.panel-body');
  if (!panelBody || analysis.regions.length === 0) return;

  const change = analysis.regionChange;
  const when = analysis.latest.period.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });

  const wrap = document.createElement('div');
  wrap.className = 'panel-subchart';
  wrap.setAttribute('data-chapter-section', 'overview-regions');
  wrap.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Regional decomposition</span>
        <span class="panel-subchart__subtitle">Week ending ${when}</span>
      </div>
      <span class="panel-subchart__subtitle">${change.nBuild} built · ${change.nDraw} drew</span>
    </div>
    <div class="panel-subchart__body panel-subchart__body--auto">
      <table class="panel-table">
        <caption>Level, week-over-week change and same-week normal by EIA region. (n) is the number of prior years that actually contributed.</caption>
        <thead>
          <tr>
            <th>Region</th>
            <th>Bcf</th>
            <th>Share</th>
            <th title="Level as a share of this region's own observed maximum">Fill</th>
            <th>WoW</th>
            <th>Bcf/d</th>
            <th>4w</th>
            <th>Mean (n)</th>
            <th>vs mean</th>
          </tr>
        </thead>
        <tbody>
          ${analysis.regions.map(tableRow).join('')}
          ${l48Row(analysis)}
        </tbody>
      </table>
      <span class="panel-table__note">
        Regional changes sum to ${Math.round(change.sumBcf)} Bcf${
          change.residualVsL48Bcf != null
            ? `; ${Math.round(change.residualVsL48Bcf)} Bcf of that is EIA rounding each region independently against the L48 print`
            : ''
        }.
      </span>
    </div>
  `;

  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrap, sidebar || null);
}

function signedCell(v, digits = 0) {
  if (v == null || !Number.isFinite(v)) return '—';
  const s = Math.abs(v).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  return `${v > 0 ? '+' : v < 0 ? '\u2212' : ''}${s}`;
}

function signClass(v) {
  if (v == null || !Number.isFinite(v)) return 'muted';
  return v > 0 ? 'pos' : v < 0 ? 'neg' : 'muted';
}

function tableRow(r) {
  const child = r.key === 'sc_salt' || r.key === 'sc_nonsalt';
  return `
    <tr class="${child ? 'is-child' : ''}">
      <td class="lbl">${r.label}</td>
      <td>${r.levelBcf.toLocaleString()}</td>
      <td class="muted">${r.shareOfL48Pct != null ? `${r.shareOfL48Pct.toFixed(1)}%` : '—'}</td>
      <td ${fillTooltip(r)}>${r.fillPctOfObservedMax != null ? `${r.fillPctOfObservedMax.toFixed(0)}%` : '—'}</td>
      <td class="${signClass(r.wowBcf)}">${signedCell(r.wowBcf)}</td>
      <td class="${signClass(r.wowRate)}">${signedCell(r.wowRate, 2)}</td>
      <td class="${signClass(r.fourWeekBcf)}">${signedCell(r.fourWeekBcf)}</td>
      <td class="muted">${r.normalMeanBcf != null ? `${Math.round(r.normalMeanBcf).toLocaleString()} (${r.normalN})` : '—'}</td>
      <td class="${signClass(r.vsNormalBcf)}">${signedCell(r.vsNormalBcf)}</td>
    </tr>
  `;
}

function fillTooltip(r) {
  if (r.fillPctOfObservedMax == null) return 'class="muted"';
  const max = Math.round(r.fillPctOfObservedMax) + '%';
  const room = r.roomToObservedMaxBcf == null ? '' : ` · ${Math.round(r.roomToObservedMaxBcf).toLocaleString()} Bcf below that peak`;
  return `title="${r.label}: ${max} of its own observed peak${room}"`;
}

function l48Row(analysis) {
  const { l48, environment: env } = analysis;
  const wow = windowChange(l48, 1);
  const four = windowChange(l48, 4);
  return `
    <tr>
      <td class="lbl"><strong>Lower 48</strong></td>
      <td><strong>${env.valueBcf.toLocaleString()}</strong></td>
      <td class="muted">100%</td>
      <td class="muted" title="Fill vs the nominal 4,000 Bcf convention, not the observed peak">${env.pctOfMax.toFixed(0)}%</td>
      <td class="${signClass(wow?.deltaBcf)}"><strong>${signedCell(wow ? wow.deltaBcf : null)}</strong></td>
      <td>${wow ? wow.rate.toFixed(2) : '—'}</td>
      <td class="${signClass(four?.deltaBcf)}">${signedCell(four ? four.deltaBcf : null)}</td>
      <td class="muted">${env.normal?.mean != null ? `${Math.round(env.normal.mean).toLocaleString()} (${env.normal.n})` : '—'}</td>
      <td class="${signClass(env.vsNormalBcf)}">${signedCell(env.vsNormalBcf)}</td>
    </tr>
  `;
}

/* ================================================================== */
/*  8. Season to Date                                                 */
/* ================================================================== */

export function seasonToDateStanding(std) {
  if (!std || std.netChangeBcf == null || std.n < 2) return 'mid-range';
  const signedLike = (v) => (v >= 0) === (std.netChangeBcf >= 0);
  const mag = Math.abs(std.netChangeBcf);
  if (std.prior.every((p) => signedLike(p.netChangeBcf) && Math.abs(p.netChangeBcf) <= mag)) return 'largest';
  if (std.prior.every((p) => signedLike(p.netChangeBcf) && Math.abs(p.netChangeBcf) >= mag)) return 'smallest';
  return 'mid-range';
}

function appendSeasonToDate(panelEl, analysis) {
  const panelBody = panelEl.querySelector('.panel-body');
  const std = analysis.seasonToDate;
  if (!panelBody || !std || std.netChangeBcf == null) return;

  const fmtDay = (d) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  const signed = (v, digits = 0) => `${v > 0 ? '+' : v < 0 ? '\u2212' : ''}${Math.abs(v).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
  const verb = std.netChangeBcf >= 0 ? 'injected' : 'withdrawn';

  const priorText = std.n === 0
    ? 'No prior year has a date-matched window for this period, so there is nothing to compare against.'
    : `Same window in prior years: ${std.prior
        .map((p) => `${p.year} ${signed(p.netChangeBcf)} Bcf`)
        .join(' · ')}. Range ${signed(std.priorMinBcf)} to ${signed(std.priorMaxBcf)} Bcf.`;

  const direction = seasonToDateStanding(std);
  const years = std.n === 1 ? '1 YEAR' : `${std.n} YEARS`;
  const badge =
    std.n === 0
      ? 'NO PRIOR YEARS'
      : direction === 'largest'
        ? `ABOVE ALL ${years}`
        : direction === 'smallest'
          ? `BELOW ALL ${years}`
          : std.n === 1
            ? 'ONLY 1 YEAR'
            : `MIDDLE OF ${years}`;

  const rest = analysis.seasonRest;
  const restText = rest
    ? `If the rest of this season repeats last year's path, the ${rest.seasonEnd.getFullYear() === std.end.getFullYear() ? 'season ends' : 'new year opens'} at about ${Math.round(rest.impliedSeasonEndBcf).toLocaleString()} Bcf (${rest.weeksAhead} weeks ahead, arithmetic repeat — not a forecast).`
    : '';

  const wrap = document.createElement('div');
  wrap.className = 'panel-subchart';
  wrap.setAttribute('data-chapter-section', 'overview-regions');
  wrap.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Season to date</span>
        <span class="panel-subchart__hero num">${signed(std.netChangeBcf)} Bcf</span>
        <span class="panel-subchart__badge panel-subchart__badge--neutral" title="This season's net change against the same calendar window in prior years">${badge}</span>
      </div>
      <span class="panel-subchart__subtitle">${fmtDay(std.start)} → ${fmtDay(std.end)} · net ${verb}</span>
    </div>
    <div class="panel-subchart__body panel-subchart__body--prose">
      <span class="panel-table__note">${priorText}${restText ? `<br>${restText}` : ''}</span>
    </div>
  `;
  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrap, sidebar || null);
}

/* ================================================================== */
/*  9. Provenance Audit Footer                                        */
/* ================================================================== */

function appendProvenance(panelEl, analysis) {
  const body = panelEl.querySelector('.panel-body');
  if (!body) return;
  const inv = analysis.invariants;
  const breaks = inv.saltPlusNonsalt.length + inv.regionsSumL48.length;
  const el = document.createElement('span');
  el.className = 'data-provenance';
  el.innerHTML = [
    `${analysis.sample.weeks} weeks loaded (${analysis.sample.firstKey} &rarr; ${analysis.sample.lastKey})`,
    `${inv.checked} periods identity-checked, tolerance &plusmn;${inv.toleranceBcf} Bcf`,
    breaks === 0
      ? 'salt + non-salt and regional sums close'
      : `<span class="data-provenance__flag">${breaks} identity break(s)</span>`,
    `${analysis.dropped} unparseable row(s) dropped`,
    analysis.duplicateKeys ? `<span class="data-provenance__flag">${analysis.duplicateKeys} duplicate row(s)</span>` : null,
    'Multi-horizon baselines: 5Y Tactical, 10Y Decadal, Observed History (2018–present, n=8)',
    'EIA Weekly Supplement native SVG dual plot (Lower 48) and 6-region floating comparison columns',
    'Fill is a share of each region\'s own observed peak; only Lower 48 uses the nominal 4,000 Bcf convention',
    'Zero synthetic forecast models: season-end path repeats prior realised arithmetic',
  ]
    .filter(Boolean)
    .join(' &middot; ');
  body.appendChild(el);
}
