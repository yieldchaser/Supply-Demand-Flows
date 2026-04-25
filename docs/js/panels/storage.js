/**
 * Storage Panel — Weekly EIA Storage with 5-year envelope.
 *
 * Data schema: all rows have series_id='storage', region='NA'.
 * 8 rows per period — the max value row is the Lower 48 total.
 * We extract the L48 total per period, then overlay current year
 * against a 5-year band (min/max, p25/p75, mean).
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { computeWeeklyEnvelope, isoWeek } from '../util/series.js';

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

  drawStorageChart(chartEl, currentYearRows, envelope);
  renderStorageKpis(sidebarEl, seriesRows, envelope);
}

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
    .data(yTicks)
    .enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // 5-year envelope data sorted by week
  const envelopeData = Array.from(envelope, ([wk, e]) => ({ wk, ...e }))
    .sort((a, b) => a.wk - b.wk);

  // Outer band: min-max
  const areaOuter = d3.area()
    .x((d) => x(d.wk))
    .y0((d) => y(d.min))
    .y1((d) => y(d.max))
    .curve(d3.curveMonotoneX);
  g.append('path')
    .datum(envelopeData)
    .attr('d', areaOuter)
    .attr('fill', 'rgba(125, 211, 252, 0.06)');

  // Inner band: p25-p75
  const areaInner = d3.area()
    .x((d) => x(d.wk))
    .y0((d) => y(d.p25))
    .y1((d) => y(d.p75))
    .curve(d3.curveMonotoneX);
  g.append('path')
    .datum(envelopeData)
    .attr('d', areaInner)
    .attr('fill', 'rgba(125, 211, 252, 0.12)');

  // 5-year mean dashed center line
  const meanLine = d3.line()
    .x((d) => x(d.wk))
    .y((d) => y(d.mean))
    .curve(d3.curveMonotoneX);
  g.append('path')
    .datum(envelopeData)
    .attr('d', meanLine)
    .attr('fill', 'none')
    .attr('stroke', 'rgba(125, 211, 252, 0.4)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,4');

  // Current-year line
  const currentLine = d3.line()
    .x((d) => x(isoWeek(d.period)))
    .y((d) => y(d.value))
    .curve(d3.curveMonotoneX);
  g.append('path')
    .datum(currentYearRows)
    .attr('d', currentLine)
    .attr('fill', 'none')
    .style('stroke', 'var(--blue-flame)')   // style() resolves CSS custom properties; attr() does not
    .attr('stroke-width', 2.5)
    .attr('stroke-linecap', 'round')
    .attr('stroke-linejoin', 'round')
    .style('filter', 'drop-shadow(0 0 8px rgba(125, 211, 252, 0.4))');

  // Latest point dot
  if (currentYearRows.length > 0) {
    const latest = currentYearRows[currentYearRows.length - 1];
    g.append('circle')
      .attr('cx', x(isoWeek(latest.period)))
      .attr('cy', y(latest.value))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)')   // style() resolves CSS custom properties
      .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.6))');
  }

  // X-axis: 6 month labels (every other month) — prevents crowding
  const monthTicks = [
    { wk: 1,  label: 'Jan' },
    { wk: 9,  label: 'Mar' },
    { wk: 18, label: 'May' },
    { wk: 27, label: 'Jul' },
    { wk: 36, label: 'Sep' },
    { wk: 44, label: 'Nov' },
  ];
  g.selectAll('.x-tick')
    .data(monthTicks)
    .enter()
    .append('text')
    .attr('x', (d) => x(d.wk))
    .attr('y', height + 22)
    .attr('text-anchor', 'middle')
    .attr('font-size', 11)
    .attr('font-family', 'var(--font-sans)')
    .style('fill', 'var(--chart-label)')  // style() resolves CSS custom properties
    .text((d) => d.label);

  // Y-axis labels
  g.selectAll('.y-tick')
    .data(yTicks)
    .enter()
    .append('text')
    .attr('x', -8)
    .attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 11)
    .attr('font-family', 'var(--font-mono)')
    .attr('font-feature-settings', "'tnum'")
    .style('fill', 'var(--chart-label)')  // style() resolves CSS custom properties
    .text((d) => d >= 1000 ? `${(d / 1000).toFixed(1)}k` : `${d}`);

  // Hover crosshair + tooltip
  setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin);
}

function setupStorageHover(svg, g, x, y, width, height, currentYearRows, envelope, margin) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip')
    .style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('opacity', 0);

  const hoverDot = g.append('circle')
    .attr('r', 5)
    .style('fill', 'var(--blue-flame)')   // style() resolves CSS custom properties
    .attr('stroke', 'rgba(10,14,20,0.8)')
    .attr('stroke-width', 2)
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'transparent');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk = Math.round(x.invert(mx));
      const row = currentYearRows.find((r) => isoWeek(r.period) === wk);
      const env = envelope.get(wk);
      if (!row || !env) {
        crosshair.style('opacity', 0);
        hoverDot.style('opacity', 0);
        tooltipDiv.style('opacity', 0);
        return;
      }
      crosshair.attr('x1', x(wk)).attr('x2', x(wk)).style('opacity', 1);
      hoverDot.attr('cx', x(wk)).attr('cy', y(row.value)).style('opacity', 1);

      const vsAvg = row.value - env.mean;
      const vsAvgPct = (vsAvg / env.mean) * 100;

      // Position tooltip relative to container
      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(wk) * scaleX + 16;
      const tooltipY = margin.top * scaleX + y(row.value) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 200)}px`)
        .style('top', `${Math.max(tooltipY, 10)}px`)
        .html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Storage</span><span class="num">${row.value.toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">5y avg</span><span class="num">${Math.round(env.mean).toLocaleString()} Bcf</span></div>
          <div class="tt-row tt-row--accent"><span class="tt-label">vs avg</span><span class="num">${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} (${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%)</span></div>
        `);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      hoverDot.style('opacity', 0);
      tooltipDiv.style('opacity', 0);
    });
}

function renderStorageKpis(sidebarEl, seriesRows, envelope) {
  if (seriesRows.length < 2) return;

  // Defensive Date coercion — JSON parse may leave period as string
  seriesRows = seriesRows.map((r) => ({
    ...r,
    period: r.period instanceof Date ? r.period : new Date(r.period),
  }));

  const latest = seriesRows[seriesRows.length - 1];
  const prior = seriesRows[seriesRows.length - 2];
  const latestWk = isoWeek(latest.period);
  console.debug('[storage] latestWk:', latestWk, 'envelope size:', envelope.size, 'keys sample:', Array.from(envelope.keys()).slice(0, 8));
  const env = envelope.get(latestWk);

  const wow = latest.value - prior.value;
  const vsAvg = env ? latest.value - env.mean : null;
  const vsAvgPct = env && env.mean !== 0 ? (vsAvg / env.mean) * 100 : null;

  // YoY: same week last year
  const yearAgoTarget = new Date(latest.period);
  yearAgoTarget.setFullYear(yearAgoTarget.getFullYear() - 1);
  const yearAgo = seriesRows
    .filter((r) => Math.abs(r.period - yearAgoTarget) < 10 * 86400000)
    .sort((a, b) => Math.abs(a.period - yearAgoTarget) - Math.abs(b.period - yearAgoTarget))[0];
  const yoy = yearAgo ? latest.value - yearAgo.value : null;
  const yoyPct = yearAgo && yearAgo.value !== 0 ? (yoy / yearAgo.value) * 100 : null;

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
        kind: wow > 0 ? 'bullish' : wow < 0 ? 'bearish' : 'neutral',
      },
    }),
    kpiCardHtml({
      label: 'vs 5y avg',
      value: vsAvg != null ? `${vsAvg >= 0 ? '+' : ''}${Math.round(vsAvg).toLocaleString()} Bcf` : 'n/a',
      delta: vsAvgPct != null ? {
        value: `${vsAvgPct >= 0 ? '+' : ''}${vsAvgPct.toFixed(1)}%`,
        kind: vsAvgPct >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: env ? `5-year average for week ${latestWk}` : '',
    }),
    kpiCardHtml({
      label: 'YoY',
      value: yoy != null ? `${yoy >= 0 ? '+' : ''}${yoy.toLocaleString()} Bcf` : 'n/a',
      delta: yoyPct != null ? {
        value: `${yoyPct >= 0 ? '+' : ''}${yoyPct.toFixed(1)}%`,
        kind: yoyPct >= 0 ? 'bullish' : 'bearish',
      } : null,
    }),
  ].join('');
}
