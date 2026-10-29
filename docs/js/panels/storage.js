/**
 * Storage Panel — weekly EIA working gas, read through `util/storage-analysis`.
 *
 * Every number printed here comes from the analysis module, not from a local
 * rollup. Two data defects the previous version shipped are gone by design:
 *
 *  - the Lower 48 level was `max(value per period)`, which silently returns the
 *    largest *region* whenever the aggregate row is missing. It is now the
 *    `storage_lower48` series, cross-checked against the sum of the five regions.
 *  - the envelope was keyed by ISO week while the data is EIA Friday weeks, so
 *    a 53-week year shifted every later label by one. The envelope is now keyed
 *    by week-of-season (see `seasonalEnvelope`) and prints its sample size.
 *
 * Pure formatting helpers live here; all arithmetic lives in the module.
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
} from '../util/storage-analysis.js';

const STORAGE_CHART_ID = 'storage-envelope';

export function renderStoragePanel(panelEl, bundle) {
  const source = bundle.sources.eia_storage;
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
    title: 'Weekly Storage',
    subtitle: 'EIA · Lower 48 working gas, week ending Friday',
    sourceKey: 'eia_storage',
    latestPeriod: source.latest_period,
    chartExport: STORAGE_CHART_ID,
    draw: () => drawRef.current?.(),
  });

  const { l48, latest, projection } = analysis;
  const currentRows = l48.filter((r) => r.period.getFullYear() === latest.period.getFullYear());

  // Week-of-season key for every row; the envelope is indexed by the same key.
  const weekKey = (r) => seasonWeekOf(l48, r.period);
  const envelope = envelopeFromRows(l48, latest.period);

  registerChartSnapshot(
    STORAGE_CHART_ID,
    'storage-envelope',
    currentRows.map((r) => {
      const wk = weekKey(r);
      const env = envelope.get(wk);
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
      basis:
        'Lower 48 weekly working gas (storage_lower48); band = same week-of-season in the trailing 5 years, sample size per week reported.',
      latest_period: source.latest_period ?? null,
      invariants: {
        periods_checked: analysis.invariants.checked,
        salt_nonsalt_breaks: analysis.invariants.saltPlusNonsalt.length,
        region_sum_breaks: analysis.invariants.regionsSumL48.length,
      },
    },
  );

  const drawMain = () => drawStorageChart(chartEl, currentRows, envelope, weekKey, projection);
  drawRef.current = drawMain;
  drawMain();

  // ── Days-of-Cover ──────────────────────────────────────────────────
  // Consumption comes from the supply source (supply_vc0, monthly MMcf).
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
  // ───────────────────────────────────────────────────────────────────

  renderStorageKpis(sidebarEl, analysis);
  appendSeasonToDate(panelEl, analysis);
  appendRegionTable(panelEl, analysis);
  appendProvenance(panelEl, analysis);
}

/**
 * The seasonal envelope, keyed by week-of-season, built once for the chart.
 * `seasonalEnvelope` is re-exported by the analysis module so the panel and the
 * engine cannot drift onto different anchors.
 */
function envelopeFromRows(l48, anchor, lookbackYears = 5) {
  return new Map(seasonalEnvelope(l48, { anchor, lookbackYears }));
}

/* ================================================================== */
/*  Main Storage Chart                                                 */
/* ================================================================== */

function drawStorageChart(container, currentYearRows, envelope, weekKey, projection) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 24, right: 16, bottom: 36, left: 56 };
  const width = Math.max(rect.width, 300) - margin.left - margin.right;
  const height = 360 - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 360`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // X scale spans whole storage seasons (52 or 53 weeks), so a 53-week year
  // does not push its tail past the axis.
  const maxWeek = Math.max(52, ...Array.from(envelope.keys()));
  const x = d3.scaleLinear().domain([1, maxWeek]).range([0, width]);

  // Y scale: extent of envelope + current year
  const allValues = [
    ...currentYearRows.map((r) => r.value),
    ...Array.from(envelope.values()).flatMap((e) => [e.min, e.max]),
  ];
  if (allValues.length === 0) return;
  const yExtent = d3.extent(allValues);
  const yPadding = (yExtent[1] - yExtent[0]) * 0.05;
  const y = d3.scaleLinear()
    .domain([yExtent[0] - yPadding, yExtent[1] + yPadding])
    .range([height, 0]);

  // Y gridlines
  const yTicks = y.ticks(6);
  g.selectAll('.gridline')
    .data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // 5-year envelope data sorted by week
  const envelopeData = Array.from(envelope, ([wk, e]) => ({ wk, ...e }))
    .sort((a, b) => a.wk - b.wk);

  // Outer band: min-max
  const areaOuter = d3.area()
    .x((d) => x(d.wk)).y0((d) => y(d.min)).y1((d) => y(d.max))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(envelopeData).attr('d', areaOuter)
    .attr('fill', 'rgba(125, 211, 252, 0.06)');

  // Inner band: p25-p75
  const areaInner = d3.area()
    .x((d) => x(d.wk)).y0((d) => y(d.p25)).y1((d) => y(d.p75))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(envelopeData).attr('d', areaInner)
    .attr('fill', 'rgba(125, 211, 252, 0.12)');

  // 5-year mean dashed center line
  const meanLine = d3.line()
    .x((d) => x(d.wk)).y((d) => y(d.mean))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(envelopeData).attr('d', meanLine)
    .attr('fill', 'none')
    .attr('stroke', 'rgba(125, 211, 252, 0.4)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,4');

  // Current-year line — style() resolves CSS custom properties; attr() does not
  const currentLine = d3.line()
    .x((d) => x(weekKey(d))).y((d) => y(d.value))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(currentYearRows).attr('d', currentLine)
    .attr('fill', 'none')
    .style('stroke', 'var(--blue-flame)')
    .attr('stroke-width', 2.5)
    .attr('stroke-linecap', 'round').attr('stroke-linejoin', 'round')
    .style('filter', 'drop-shadow(0 0 8px rgba(125, 211, 252, 0.4))');

  // Latest point dot
  if (currentYearRows.length > 0) {
    const latest = currentYearRows[currentYearRows.length - 1];
    g.append('circle')
      .attr('cx', x(weekKey(latest))).attr('cy', y(latest.value))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)')
      .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.6))');
  }

  // Projection ghost — the fitted line only, never a filled "forecast".
  if (projection && Number.isFinite(projection.modelBcf)) {
    const anchorRow = currentYearRows[currentYearRows.length - 1];
    const anchorWeek = weekKey(anchorRow);
    const projWeeks = projection.horizonWeeks;
    g.append('line')
      .attr('x1', x(anchorWeek)).attr('y1', y(anchorRow.value))
      .attr('x2', x(Math.min(anchorWeek + projWeeks, maxWeek))).attr('y2', y(projection.modelBcf))
      .attr('stroke', 'rgba(255,255,255,0.35)')
      .attr('stroke-width', 1.25)
      .attr('stroke-dasharray', '3,3');
    g.append('text')
      .attr('x', x(Math.min(anchorWeek + projWeeks, maxWeek)) + 4)
      .attr('y', y(projection.modelBcf) + 3)
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'rgba(255,255,255,0.45)')
      .text(`${projWeeks}w model`);
  }

  // X-axis: 6 month labels (every other month)
  const monthTicks = [
    { wk: 1,  label: 'Jan' }, { wk: 9,  label: 'Mar' },
    { wk: 18, label: 'May' }, { wk: 27, label: 'Jul' },
    { wk: 36, label: 'Sep' }, { wk: 44, label: 'Nov' },
  ];
  g.selectAll('.x-tick').data(monthTicks).enter()
    .append('text')
    .attr('x', (d) => x(d.wk)).attr('y', height + 22)
    .attr('text-anchor', 'middle')
    .attr('font-size', 11).attr('font-family', 'var(--font-sans)')
    .style('fill', 'var(--chart-label)')
    .text((d) => d.label);

  // Y-axis labels
  g.selectAll('.y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 11).attr('font-family', 'var(--font-mono)')
    .attr('font-feature-settings', "'tnum'")
    .style('fill', 'var(--chart-label)')
    .text((d) => d >= 1000 ? `${(d / 1000).toFixed(1)}k` : `${d}`);

  setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin, weekKey);
}

/* ================================================================== */
/*  Storage hover                                                      */
/* ================================================================== */

function setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin, weekKey) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip').style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2').style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 5)
    .style('fill', 'var(--blue-flame)')
    .attr('stroke', 'rgba(10,14,20,0.8)').attr('stroke-width', 2)
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'none')
    .style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk  = Math.round(x.invert(mx));
      const row = currentYearRows.find((r) => weekKey(r) === wk);
      const env = envelope.get(wk);
      
      if (!env) {
        crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
        return;
      }

      crosshair.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect  = svg.node().getBoundingClientRect();
      const scaleX   = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const yVal     = row ? row.value : env.mean;
      const tooltipY = margin.top  * scaleX + y(yVal) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 200)}px`)
        .style('top',  `${Math.max(tooltipY, 10)}px`);

      if (row) {
        hoverDot.attr('cx', x(wk)).attr('cy', y(row.value)).style('opacity', 1);

        const vsAvg    = row.value - env.mean;
        const vsAvgPct = (vsAvg / env.mean) * 100;

        tooltipDiv.html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Storage</span><span class="num">${row.value.toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Mean n=${env.count}</span><span class="num">${Math.round(env.mean).toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Median</span><span class="num">${Math.round(env.median).toLocaleString()} Bcf</span></div>
          <div class="tt-row tt-row--accent"><span class="tt-label">vs mean</span><span class="num">${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} (${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%)</span></div>
          <div class="tt-row"><span class="tt-label">Range</span><span class="num">${Math.round(env.min).toLocaleString()}–${Math.round(env.max).toLocaleString()} Bcf</span></div>
          ${env.years?.length ? `<div class="tt-row"><span class="tt-label">Years</span><span class="num">${env.years.join(', ')}</span></div>` : ''}
        `);
      } else {
        hoverDot.style('opacity', 0);
        
        tooltipDiv.html(`
          <div class="tt-date">Week ${wk} \u00b7 Seasonal avg</div>
          <div class="tt-row"><span class="tt-label">5y avg</span><span class="num">${Math.round(env.mean).toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Band</span><span class="num">${Math.round(env.min).toLocaleString()}\u2013${Math.round(env.max).toLocaleString()} Bcf</span></div>
        `);
      }
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  Days-of-Cover companion mini-chart                                */
/* ================================================================== */

function insertCoverMiniChart(panelEl, coverRows, coverEnvelope) {
  const panelBody = panelEl.querySelector('.panel-body');
  if (!panelBody || coverRows.length === 0) return;

  const latestCover    = coverRows[coverRows.length - 1];
  const classification = classifyCover(latestCover, coverEnvelope);
  const env            = classification?.envelope;

  const classKind =
    ['critical', 'low'].includes(classification?.label)          ? 'bearish' :
    ['elevated', 'comfortable'].includes(classification?.label)  ? 'bullish' :
    'neutral';

  const rangeText = env
    ? `5-yr range: ${Math.round(env.min)}\u2013${Math.round(env.max)} days`
    : 'Storage \u00f7 daily consumption';

  const wrapper = document.createElement('div');
  wrapper.className = 'panel-subchart';
  wrapper.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Days of Cover</span>
        <span class="panel-subchart__hero num">${Math.round(latestCover.daysOfCover)} days</span>
        <span class="panel-subchart__badge panel-subchart__badge--${classKind}">${(classification?.label || 'normal').toUpperCase()}</span>
      </div>
      <span class="panel-subchart__subtitle">Weekly trajectory \u00b7 ${rangeText}</span>
    </div>
    <div class="panel-subchart__body"></div>
  `;

  // Insert before .panel-sidebar (the KPI strip)
  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrapper, sidebar);

  const coverBody = wrapper.querySelector('.panel-subchart__body');
  const drawCover = () => drawCoverMiniChart(coverBody, coverRows, coverEnvelope);
  drawCover();
  redrawOnResize(coverBody, drawCover);
}

function drawCoverMiniChart(container, coverRows, envelope) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const W = Math.max(rect.width || 600, 300);
  const H = 140;
  const margin = { top: 8, right: 16, bottom: 32, left: 48 };
  const width  = W - margin.left - margin.right;
  const height = H - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${W} ${H}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Reference year comes from the series, never the wall clock: a replayed or
  // archived bundle must render its own latest year, not whatever year the
  // viewer happens to open the page in.
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

  // X: week-of-season, not ISO week. The envelope is keyed by week-of-season
  // (`seasonalEnvelope` measures from each year's own first observation), so an
  // ISO-keyed hero line would sit half a week off its own band in years whose
  // first Friday differs — the same defect the main chart was fixed for.
  const weekKey = (r) => seasonWeekOf(currentRows, r.period);
  const maxWeek = Math.max(52, ...Array.from(envelope.keys()));
  const x = d3.scaleLinear().domain([1, maxWeek]).range([0, width]);

  // Y: envelope + current year combined extent
  const envValues = Array.from(envelope.values()).flatMap((e) => [e.min, e.max]);
  const curValues = currentRows.map((r) => r.daysOfCover);
  const allValues = [...envValues, ...curValues].filter((v) => v != null && isFinite(v));
  if (allValues.length === 0) return;
  const yExtent = d3.extent(allValues);
  const yPad = Math.max((yExtent[1] - yExtent[0]) * 0.08, 2);
  const y = d3.scaleLinear()
    .domain([Math.max(0, yExtent[0] - yPad), yExtent[1] + yPad])
    .range([height, 0]);

  // Y gridlines
  const yTicks = y.ticks(4);
  g.selectAll('.cov-gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // Y labels
  g.selectAll('.cov-y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
    .style('font-feature-settings', "'tnum'")
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => `${Math.round(d)}d`);

  // X labels — same 6-month cadence as main chart
  const monthTicks = [
    { wk: 1,  label: 'Jan' }, { wk: 9,  label: 'Mar' },
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

  // Envelope band + median
  const envData = Array.from(envelope, ([wk, e]) => ({ wk, ...e }))
    .sort((a, b) => a.wk - b.wk);

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

  // Current-year cover line — hero
  const lineGen = d3.line()
    .x((d) => x(weekKey(d)))
    .y((d) => y(d.daysOfCover))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(currentRows).attr('d', lineGen)
    .attr('fill', 'none')
    .style('stroke', 'var(--blue-flame)')
    .attr('stroke-width', 2)
    .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.4))');

  // Latest dot
  const latest = currentRows[currentRows.length - 1];
  g.append('circle')
    .attr('cx', x(weekKey(latest)))
    .attr('cy', y(latest.daysOfCover))
    .attr('r', 3.5)
    .style('fill', 'var(--blue-flame)')
    .style('filter', 'drop-shadow(0 0 5px rgba(125, 211, 252, 0.5))');

  setupCoverHover(svg, g, x, y, width, height, currentRows, margin, weekKey, envelope);
}

/* ================================================================== */
/*  Cover hover                                                        */
/* ================================================================== */

function setupCoverHover(svg, g, x, y, width, height, currentRows, margin, weekKey, envelope) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip').style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2').style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 4)
    .style('fill', 'var(--blue-flame)')
    .attr('stroke', 'rgba(10,14,20,0.8)').attr('stroke-width', 2)
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'none')
    .style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk  = Math.round(x.invert(mx));
      const row = currentRows.find((r) => weekKey(r) === wk);
      if (!row) {
        crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
        return;
      }
      crosshair.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);
      hoverDot.attr('cx', x(wk)).attr('cy', y(row.daysOfCover)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect  = svg.node().getBoundingClientRect();
      const scaleX   = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const tooltipY = margin.top  * scaleX + y(row.daysOfCover) * scaleX - 20;

      // The cover figure is only meaningful next to what it is being compared
      // with; the old tooltip printed a bare number with no normal at all.
      const env = envelope?.get(wk) ?? null;
      const vsNormal = env ? Math.round(row.daysOfCover - env.mean) : null;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 190)}px`)
        .style('top',  `${Math.max(tooltipY, 10)}px`)
        .html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Cover</span><span class="num">${Math.round(row.daysOfCover)} days</span></div>
          ${env ? `<div class="tt-row"><span class="tt-label">5y normal</span><span class="num">${Math.round(env.mean)} days</span></div>
          <div class="tt-row"><span class="tt-label">vs normal</span><span class="num">${vsNormal > 0 ? '+' : ''}${vsNormal} days</span></div>
          <div class="tt-row"><span class="tt-label">Sample</span><span class="num">n=${env.count}</span></div>` : `<div class="tt-row"><span class="tt-label">5y normal</span><span class="num">n/a</span></div>`}
        `);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  KPI sidebar                                                        */
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
/*  Regional decomposition table                                       */
/* ================================================================== */

/**
 * The table that makes the panel checkable: every region's level, change and
 * same-week normal, with the L48 row as the sum it must match. Order mirrors
 * EIA's own regional table so a reader can diff by eye.
 */
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
  wrap.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Regional decomposition</span>
        <span class="panel-subchart__subtitle">Week ending ${when}</span>
      </div>
      <span class="panel-subchart__subtitle">${change.nBuild} built · ${change.nDraw} drew</span>
    </div>
    <div class="panel-subchart__body">
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
  panelBody.insertBefore(wrap, sidebar);
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
  const room = r.roomToObservedMaxBcf == null ? '' : ` \u00b7 ${Math.round(r.roomToObservedMaxBcf).toLocaleString()} Bcf below that peak`;
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
/*  Season to date                                                     */
/* ================================================================== */

/**
 * How far the season's net injection or withdrawal has run, next to what the
 * same calendar window did in prior years.
 *
 * This is the question a level plus a weekly change cannot answer: "are we 6%
 * above normal" says nothing about whether the season is running hot or whether
 * it started there. The prior-year window is date-matched and its n is printed;
 * the season-end figure is explicitly "if this repeats", not a forecast.
 */
/**
 * Where this season's net change sits against the date-matched prior years.
 * With one prior year the ordering is not evidence of anything, so it reads
 * `mid-range` rather than crowning it the largest on a sample of one.
 */
export function seasonToDateStanding(std) {
  if (!std || std.netChangeBcf == null || std.n < 2) return 'mid-range';
  const signedLike = (v) => (v >= 0) === (std.netChangeBcf >= 0);
  const mag = Math.abs(std.netChangeBcf);
  if (std.prior.every((p) => signedLike(p.netChangeBcf) && Math.abs(p.netChangeBcf) <= mag)) return 'largest';
  // Opposite-direction prior years make "smallest" misleading: minus 40 sits
  // below plus 30 in level but not in size of the swing.
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
    : `Same window, prior years (n=${std.n}): ${std.prior
        .map((p) => `${p.year} ${signed(p.netChangeBcf)}`)
        .join(' \u00b7 ')}. Range ${signed(std.priorMinBcf)} to ${signed(std.priorMaxBcf)} Bcf.`;

  const direction = seasonToDateStanding(std);

  const rest = analysis.seasonRest;
  const restText = rest
    ? `If the rest of this season repeats last year's path, the ${rest.seasonEnd.getFullYear() === std.end.getFullYear() ? 'season ends' : 'new year opens'} at about ${Math.round(rest.impliedSeasonEndBcf).toLocaleString()} Bcf (${rest.weeksAhead} weeks ahead, arithmetic repeat \u2014 not a forecast).`
    : '';

  const wrap = document.createElement('div');
  wrap.className = 'panel-subchart';
  wrap.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Season to date</span>
        <span class="panel-subchart__hero num">${signed(std.netChangeBcf)} Bcf</span>
        <span class="panel-subchart__badge panel-subchart__badge--neutral">${direction.toUpperCase()}</span>
      </div>
      <span class="panel-subchart__subtitle">${fmtDay(std.start)} \u2192 ${fmtDay(std.end)} \u00b7 net ${verb}</span>
    </div>
    <div class="panel-subchart__body">
      <span class="panel-table__note">${priorText}${restText ? `<br>${restText}` : ''}</span>
    </div>
  `;
  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrap, sidebar);
}

/* ================================================================== */
/*  Provenance                                                         */
/* ================================================================== */

/**
 * What a reader needs in order to trust the numbers above: how many weeks went
 * in, whether the additive identities close, and which defects were dropped
 * rather than averaged away.
 */
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
    'Norms are calendar-date matched, never week-index matched',
    'Fill is a share of each region\'s own observed peak; only the Lower 48 row uses the nominal 4,000 Bcf convention',
    'Nothing on this page forecasts demand, prices or the weather: the season-end line repeats a prior path arithmetically',
  ]
    .filter(Boolean)
    .join(' &middot; ');
  body.appendChild(el);
}
