/**
 * LNG Total Panel — US Monthly LNG Export Volumes.
 *
 * Layout:
 *   Chart (full-width): monthly bar chart of total LNG exports (last 36 months)
 *                       with 12-month moving-average overlay.
 *   KPIs: latest month, MoM delta, YoY delta, 12-month rolling total.
 *
 * Series: lng_export_total  (Bcf, monthly, from eia_lng source)
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';

const MONTHS_DISPLAY = 36;

export function renderLngTotalPanel(panelEl, bundle) {
  const source = bundle.sources?.eia_lng;
  if (!source?.data?.length) {
    panelEl.innerHTML = '<div class="panel-error">EIA LNG export data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'US LNG Exports',
    subtitle: 'EIA · total monthly volume · Bcf',
    sourceKey: 'eia_lng',
    latestPeriod: source.latest_period,
  });

  const allRows = getSeries(source.data, 'lng_export_total');
  if (allRows.length < 2) {
    chartEl.innerHTML = '<div class="panel-error" style="font-size:12px">Insufficient LNG total data.</div>';
    return;
  }

  const rows = allRows.slice(-MONTHS_DISPLAY);

  drawLngBarChart(chartEl, rows);
  renderLngTotalKpis(sidebarEl, allRows);
}

/* ================================================================== */
/*  Bar Chart with MA overlay                                          */
/* ================================================================== */

function drawLngBarChart(container, rows) {
  container.innerHTML = '';

  const margin = { top: 24, right: 16, bottom: 48, left: 56 };
  const totalH = 320;
  const width  = Math.max((container.getBoundingClientRect().width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleBand()
    .domain(rows.map((_, i) => i))
    .range([0, width])
    .padding(0.25);

  const yMax = d3.max(rows, d => d.value) * 1.12;
  const y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);

  // Gridlines
  [0.25, 0.5, 0.75, 1.0].map(f => yMax * f).forEach(v => {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(v)).attr('y2', y(v))
      .attr('stroke', 'rgba(255,255,255,0.04)');
  });

  // Bars
  g.selectAll('.lng-bar')
    .data(rows)
    .enter().append('rect')
    .attr('class', 'lng-bar')
    .attr('x', (_, i) => x(i))
    .attr('y', d => y(d.value))
    .attr('width', x.bandwidth())
    .attr('height', d => height - y(d.value))
    .style('fill', 'var(--blue-flame)')
    .attr('fill-opacity', 0.75)
    .attr('rx', 2)
    .on('mouseover', function() { d3.select(this).attr('fill-opacity', 1); })
    .on('mouseout',  function() { d3.select(this).attr('fill-opacity', 0.75); });

  // 12-month moving average overlay
  const MA_N = 12;
  const maRows = rows.map((r, i) => {
    if (i < MA_N - 1) return null;
    const slice = rows.slice(i - MA_N + 1, i + 1);
    return { i, value: d3.mean(slice, d => d.value) };
  }).filter(Boolean);

  if (maRows.length > 1) {
    const maLine = d3.line()
      .x(d => x(d.i) + x.bandwidth() / 2)
      .y(d => y(d.value))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(maRows).attr('d', maLine)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(125,211,252,0.45)')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4,3');
  }

  // X-axis: label every 6th month
  rows.forEach((r, i) => {
    if (i % 6 !== 0) return;
    const label = r.period.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
    g.append('text')
      .attr('x', x(i) + x.bandwidth() / 2)
      .attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(label);
  });

  // Y-axis labels
  [0, Math.round(yMax * 0.5), Math.round(yMax)].forEach(v => {
    g.append('text')
      .attr('x', -8).attr('y', y(v) + 4)
      .attr('text-anchor', 'end')
      .attr('font-size', 11).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)')
      .text(`${v} Bcf`);
  });

  // Latest bar label
  const last = rows[rows.length - 1];
  const lastI = rows.length - 1;
  g.append('text')
    .attr('x', x(lastI) + x.bandwidth() / 2)
    .attr('y', y(last.value) - 6)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10).attr('font-weight', 600)
    .attr('font-family', 'var(--font-mono)')
    .style('fill', 'var(--blue-flame)')
    .text(`${last.value.toFixed(1)}`);
}

/* ================================================================== */
/*  KPI Strip                                                          */
/* ================================================================== */

function renderLngTotalKpis(sidebarEl, rows) {
  if (rows.length < 2) return;

  const latest = rows[rows.length - 1];
  const prior  = rows[rows.length - 2];
  const momDelta = latest.value - prior.value;
  const momPct   = prior.value > 0 ? (momDelta / prior.value) * 100 : null;

  // YoY: find same month last year (within ±20 days)
  const yearAgo = new Date(latest.period.getTime());
  yearAgo.setFullYear(yearAgo.getFullYear() - 1);
  const yoyMatch = rows
    .filter(r => Math.abs(r.period.getTime() - yearAgo.getTime()) < 40 * 86400000)
    .sort((a, b) => Math.abs(a.period - yearAgo) - Math.abs(b.period - yearAgo))[0];
  const yoyDelta = yoyMatch ? latest.value - yoyMatch.value : null;

  // Rolling 12-month totals
  const last12 = rows.slice(-12);
  const prior12 = rows.slice(-24, -12);
  const rolling12 = last12.reduce((s, r) => s + r.value, 0);
  const prior12Sum = prior12.reduce((s, r) => s + r.value, 0);
  const rolling12Delta = prior12.length >= 12 ? rolling12 - prior12Sum : null;

  const periodLabel = latest.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Latest month',
      value: `${latest.value.toFixed(1)} Bcf`,
      delta: momPct != null ? {
        value: `${momPct >= 0 ? '+' : ''}${momPct.toFixed(1)}% MoM`,
        kind: momDelta >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: periodLabel,
    }),
    kpiCardHtml({
      label: 'MoM change',
      value: `${momDelta >= 0 ? '+' : ''}${momDelta.toFixed(1)} Bcf`,
      delta: { value: momDelta >= 0 ? 'More exports' : 'Fewer exports', kind: momDelta >= 0 ? 'bullish' : 'bearish' },
      helpText: `vs ${prior.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}`,
    }),
    kpiCardHtml({
      label: 'YoY change',
      value: yoyDelta != null ? `${yoyDelta >= 0 ? '+' : ''}${yoyDelta.toFixed(1)} Bcf` : 'n/a',
      delta: yoyDelta != null ? {
        value: `${yoyDelta >= 0 ? '+' : ''}${((yoyDelta / yoyMatch.value) * 100).toFixed(1)}% YoY`,
        kind: yoyDelta >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: yoyMatch ? `vs ${yoyMatch.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}` : '',
    }),
    kpiCardHtml({
      label: '12-month total',
      value: `${rolling12.toFixed(0)} Bcf`,
      delta: rolling12Delta != null ? {
        value: `${rolling12Delta >= 0 ? '+' : ''}${rolling12Delta.toFixed(0)} Bcf vs prior 12mo`,
        kind: rolling12Delta >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: 'Rolling last 12 months',
    }),
  ].join('');
}
