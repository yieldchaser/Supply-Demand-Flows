/**
 * Storage Panel — Weekly EIA Storage with 5-year envelope + Days-of-Cover.
 *
 * Data schema: all rows have series_id='storage', region='NA'.
 * 8 rows per period — the max value row is the Lower 48 total.
 * We extract the L48 total per period, then overlay current year
 * against a 5-year band (min/max, p25/p75, mean).
 *
 * Phase 2.2 addition: Days-of-Cover companion mini-chart and 5th KPI card.
 *   Cover = storage_Bcf ÷ daily_consumption_Bcf_per_day
 *   Joined from eia_supply (supply_vc0, monthly MMcf) via piecewise interpolation.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { computeWeeklyEnvelope, isoWeek } from '../util/series.js';
import { getSeries } from '../util/series.js';
import {
  computeStorageCover,
  computeCoverEnvelope,
  classifyCover,
  isoWeekFromDate,
} from '../util/cover.js';

export function renderStoragePanel(panelEl, bundle) {
  const source = bundle.sources.eia_storage;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">EIA Storage data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Weekly Storage',
    subtitle: 'EIA · Lower 48 working gas, 5-year band',
    sourceKey: 'eia_storage',
    latestPeriod: source.latest_period,
  });

  // Extract Lower 48 total: max value per period (the aggregate row)
  const byPeriod = d3.rollup(
    source.data,
    (g) => d3.max(g, (r) => Number(r.value)),
    (r) => r.period
  );
  const seriesRows = Array.from(byPeriod, ([period, value]) => ({
    period: new Date(period),
    value,
  })).sort((a, b) => a.period - b.period);

  // Current-year subset for the prominent line
  const currentYear = new Date().getFullYear();
  const currentYearRows = seriesRows.filter((r) => r.period.getFullYear() === currentYear);

  // 5-year envelope
  const envelope = computeWeeklyEnvelope(seriesRows, 5);

  // ── Days-of-Cover ──────────────────────────────────────────────────
  // Consumption comes from the supply source (supply_vc0, monthly MMcf)
  const supplySource = bundle.sources?.eia_supply;
  const rawConsumptionRows = supplySource?.data
    ? getSeries(supplySource.data, 'supply_vc0')
    : [];
  // getSeries already parses period to Date; values are in MMcf
  const consumptionRows = rawConsumptionRows.map((r) => ({
    period: r.period instanceof Date ? r.period : new Date(r.period),
    value:  Number(r.value), // MMcf — computeStorageCover handles the /1000 conversion
  }));

  const coverRows     = computeStorageCover(seriesRows, consumptionRows);
  const coverEnvelope = computeCoverEnvelope(coverRows, 5);
  // ───────────────────────────────────────────────────────────────────

  drawStorageChart(chartEl, currentYearRows, envelope);

  // Insert companion mini-chart between chart and KPI strip
  insertCoverMiniChart(panelEl, coverRows, coverEnvelope);

  renderStorageKpis(sidebarEl, seriesRows, envelope, coverRows, coverEnvelope);
}

/* ================================================================== */
/*  Main Storage Chart                                                 */
/* ================================================================== */

function drawStorageChart(container, currentYearRows, envelope) {
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

  // X scale: 1..52 weeks
  const x = d3.scaleLinear().domain([1, 52]).range([0, width]);

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
    .x((d) => x(isoWeek(d.period))).y((d) => y(d.value))
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
      .attr('cx', x(isoWeek(latest.period))).attr('cy', y(latest.value))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)')
      .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.6))');
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

  setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin);
}

/* ================================================================== */
/*  Storage hover                                                      */
/* ================================================================== */

function setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin) {
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
    .attr('width', width).attr('height', height).attr('fill', 'transparent');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk  = Math.round(x.invert(mx));
      const row = currentYearRows.find((r) => isoWeek(r.period) === wk);
      const env = envelope.get(wk);
      if (!row || !env) {
        crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
        return;
      }
      crosshair.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);
      hoverDot.attr('cx', x(wk)).attr('cy', y(row.value)).style('opacity', 1);

      const vsAvg    = row.value - env.mean;
      const vsAvgPct = (vsAvg / env.mean) * 100;

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect  = svg.node().getBoundingClientRect();
      const scaleX   = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const tooltipY = margin.top  * scaleX + y(row.value) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 200)}px`)
        .style('top',  `${Math.max(tooltipY, 10)}px`)
        .html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Storage</span><span class="num">${row.value.toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">5y avg</span><span class="num">${Math.round(env.mean).toLocaleString()} Bcf</span></div>
          <div class="tt-row tt-row--accent"><span class="tt-label">vs avg</span><span class="num">${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} (${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%)</span></div>
        `);
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
  if (!panelBody) return;
  if (coverRows.length === 0) return;

  const wrapper = document.createElement('div');
  wrapper.className = 'panel-subchart';
  wrapper.innerHTML = `
    <div class="panel-subchart__header">
      <span class="panel-subchart__title">Days of Cover</span>
      <span class="panel-subchart__subtitle">Weekly trajectory · 5-year seasonal range</span>
    </div>
    <div class="panel-subchart__body"></div>
  `;

  // Insert before .panel-sidebar (the KPI strip)
  const sidebar = panelBody.querySelector('.panel-sidebar');
  panelBody.insertBefore(wrapper, sidebar);

  drawCoverMiniChart(wrapper.querySelector('.panel-subchart__body'), coverRows, coverEnvelope);
}

function drawCoverMiniChart(container, coverRows, envelope) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const W = Math.max(rect.width || 600, 300);
  const H = 200;
  const margin = { top: 12, right: 16, bottom: 24, left: 48 };
  const width  = W - margin.left - margin.right;
  const height = H - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${W} ${H}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Current-year rows only for the hero line
  const currentYear = new Date().getFullYear();
  const currentRows = coverRows.filter((r) => r.period.getFullYear() === currentYear);
  if (currentRows.length === 0) {
    container.innerHTML = '<div style="color:rgba(255,255,255,0.3);font-size:12px;padding:16px">Insufficient data for current year.</div>';
    return;
  }

  // X: ISO week 1-52
  const x = d3.scaleLinear().domain([1, 52]).range([0, width]);

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
    .attr('x', (d) => x(d.wk)).attr('y', height + 16)
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
    .x((d) => x(isoWeekFromDate(d.period)))
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
    .attr('cx', x(isoWeekFromDate(latest.period)))
    .attr('cy', y(latest.daysOfCover))
    .attr('r', 3.5)
    .style('fill', 'var(--blue-flame)')
    .style('filter', 'drop-shadow(0 0 5px rgba(125, 211, 252, 0.5))');
}

/* ================================================================== */
/*  KPI sidebar                                                        */
/* ================================================================== */

function renderStorageKpis(sidebarEl, seriesRows, envelope, coverRows = [], coverEnvelope = new Map()) {
  if (seriesRows.length < 2) return;

  // Defensive Date coercion — JSON parse may leave period as string
  seriesRows = seriesRows.map((r) => ({
    ...r,
    period: r.period instanceof Date ? r.period : new Date(r.period),
  }));

  const latest    = seriesRows[seriesRows.length - 1];
  const prior     = seriesRows[seriesRows.length - 2];
  const latestWk  = isoWeek(latest.period);
  console.debug('[storage] latestWk:', latestWk, 'envelope size:', envelope.size, 'keys sample:', Array.from(envelope.keys()).slice(0, 8));
  const env = envelope.get(latestWk);

  const wow       = latest.value - prior.value;
  const vsAvg     = env ? latest.value - env.mean : null;
  const vsAvgPct  = env && env.mean !== 0 ? (vsAvg / env.mean) * 100 : null;

  // YoY: same week last year
  const yearAgoTarget = new Date(latest.period);
  yearAgoTarget.setFullYear(yearAgoTarget.getFullYear() - 1);
  const yearAgo = seriesRows
    .filter((r) => Math.abs(r.period - yearAgoTarget) < 10 * 86400000)
    .sort((a, b) => Math.abs(a.period - yearAgoTarget) - Math.abs(b.period - yearAgoTarget))[0];
  const yoy    = yearAgo ? latest.value - yearAgo.value : null;
  const yoyPct = yearAgo && yearAgo.value !== 0 ? (yoy / yearAgo.value) * 100 : null;

  // Days-of-Cover KPI
  const latestCover  = coverRows.length > 0 ? coverRows[coverRows.length - 1] : null;
  const classification = latestCover ? classifyCover(latestCover, coverEnvelope) : null;
  const coverEnvEntry  = classification?.envelope;

  const coverKpi = latestCover && latestCover.daysOfCover != null
    ? kpiCardHtml({
        label: 'Days of Cover',
        value: `${Math.round(latestCover.daysOfCover)} days`,
        delta: classification ? {
          value: classification.label.toUpperCase(),
          kind:
            ['critical', 'low'].includes(classification.label) ? 'bearish' :
            ['elevated', 'comfortable'].includes(classification.label) ? 'bullish' :
            'neutral',
        } : null,
        helpText: coverEnvEntry
          ? `5-yr range: ${Math.round(coverEnvEntry.min)}–${Math.round(coverEnvEntry.max)} days`
          : 'Storage ÷ daily consumption',
      })
    : '';

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Latest',
      value: `${latest.value.toLocaleString()} Bcf`,
      helpText: `Week ending ${latest.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`,
    }),
    kpiCardHtml({
      label: 'WoW change',
      value: `${wow >= 0 ? '+' : ''}${wow.toLocaleString()} Bcf`,
      delta: {
        value: wow > 0 ? 'Storage build' : wow < 0 ? 'Storage draw' : 'Flat',
        kind:  wow > 0 ? 'bullish' : wow < 0 ? 'bearish' : 'neutral',
      },
    }),
    kpiCardHtml({
      label: 'vs 5y avg',
      value: vsAvg != null ? `${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} Bcf` : 'n/a',
      delta: vsAvgPct != null ? {
        value: `${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%`,
        kind:  vsAvgPct >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: env ? `5-year average for week ${latestWk}` : '',
    }),
    kpiCardHtml({
      label: 'YoY',
      value: yoy != null ? `${yoy >= 0 ? '+' : ''}${yoy.toLocaleString()} Bcf` : 'n/a',
      delta: yoyPct != null ? {
        value: `${yoyPct >= 0 ? '+' : ''}${yoyPct.toFixed(1)}%`,
        kind:  yoyPct >= 0 ? 'bullish' : 'bearish',
      } : null,
    }),
    coverKpi,
  ].join('');
}
