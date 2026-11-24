/**
 * LNG Total Panel — US Monthly LNG Export Volumes.
 *
 * Layout:
 *   Chart (full-width): monthly exports as a per-day rate (last 36 months)
 *                       with the trailing 12-month rate overlaid.
 *   KPIs: latest month, MoM and YoY change in Bcf/d, 12-month total in Bcf.
 *
 * Series: lng_export_total  (Bcf per month, eia_lng_exports). It is EIA's own
 * national total; the per-destination rows sum to it within ~0.5 Bcf.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';

const MONTHS_DISPLAY = 36;

export function renderLngTotalPanel(panelEl, bundle) {
  const source = bundle.sources?.eia_lng_exports;
  if (!source?.data?.length) {
    panelEl.innerHTML = '<div class="panel-error">EIA LNG export data unavailable.</div>';
    return;
  }

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'US LNG Exports',
    subtitle: 'EIA · monthly exports to all destinations · Bcf/d (monthly volume ÷ days in month)',
    sourceKey: 'eia_lng_exports',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  const allRows = buildLngMonthly(getSeries(source.data, 'lng_export_total'));
  if (allRows.length < 2) {
    chartEl.innerHTML = '<div class="panel-error" style="font-size:12px">Insufficient LNG total data.</div>';
    return;
  }

  const rows = allRows.slice(-MONTHS_DISPLAY);

  drawRef.current = () => drawLngBarChart(chartEl, rows);
  drawRef.current();
  renderLngTotalKpis(sidebarEl, allRows);
}

/**
 * Monthly totals with their per-day rate, and the trailing 12-month mean of
 * that rate. Months differ by up to three days, so month-on-month changes in
 * Bcf carry a ±10% calendar effect (every February "drops"); the rate does not.
 *
 * @param {Array<{period: Date, value: number}>} rows  lng_export_total, Bcf
 * @returns {Array<{period: Date, bcf: number, days: number, bcfd: number, ma12: number|null}>}
 */
export function buildLngMonthly(rows) {
  const out = rows
    .filter((r) => r.period instanceof Date && Number.isFinite(Number(r.value)))
    .map((r) => {
      const days = new Date(r.period.getFullYear(), r.period.getMonth() + 1, 0).getDate();
      const bcf = Number(r.value);
      return { period: r.period, bcf, days, bcfd: bcf / days };
    });
  out.forEach((r, i) => {
    // 12 consecutive calendar months ending here, or nothing: a gap in the
    // series must not stretch the window.
    const w = out.slice(Math.max(0, i - 11), i + 1);
    const span = i >= 11 ? monthsBetween(w[0].period, r.period) : -1;
    r.ma12 = span === 11 ? w.reduce((s, x) => s + x.bcf, 0) / w.reduce((s, x) => s + x.days, 0) : null;
  });
  return out;
}

function monthsBetween(a, b) {
  return (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth());
}

function monthLabel(d) {
  return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

/* ================================================================== */
/*  Bar Chart with MA overlay                                          */
/* ================================================================== */

function drawLngBarChart(container, rows) {
  container.innerHTML = '';

  const margin = { top: 24, right: 16, bottom: 36, left: 44 };
  const totalH = 320;
  const outerW = Math.max((container.getBoundingClientRect().width || 600), 300);
  const width = outerW - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${outerW} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block')
    .style('width', '100%');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleBand()
    .domain(rows.map((_, i) => i))
    .range([0, width])
    .padding(0.25);

  const y = d3.scaleLinear()
    .domain([0, d3.max(rows, (d) => Math.max(d.bcfd, d.ma12 ?? 0)) * 1.08])
    .nice()
    .range([height, 0]);
  const ticks = y.ticks(5);

  g.append('g').selectAll('line').data(ticks).join('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (v) => y(v)).attr('y2', (v) => y(v))
    .attr('stroke', 'var(--chart-grid)');
  g.append('g').selectAll('text').data(ticks).join('text')
    .attr('x', -8).attr('y', (v) => y(v) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
    .style('fill', 'var(--chart-label)')
    .text((v) => v);
  g.append('text').attr('x', -8).attr('y', -10).attr('text-anchor', 'end')
    .attr('font-size', 10).style('fill', 'var(--chart-label)').text('Bcf/d');

  const bars = g.selectAll('.lng-bar')
    .data(rows)
    .join('rect')
    .attr('class', 'lng-bar')
    .attr('x', (_, i) => x(i))
    .attr('y', (d) => y(d.bcfd))
    .attr('width', x.bandwidth())
    .attr('height', (d) => height - y(d.bcfd))
    .style('fill', 'var(--blue-flame)')
    .attr('fill-opacity', 0.75)
    .attr('rx', 2);

  // Trailing 12-month mean, computed over the full history so it spans every
  // bar shown rather than starting at the twelfth.
  const ma = rows.map((r, i) => ({ i, v: r.ma12 })).filter((d) => d.v != null);
  if (ma.length > 1) {
    g.append('path').datum(ma)
      .attr('d', d3.line().x((d) => x(d.i) + x.bandwidth() / 2).y((d) => y(d.v)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .attr('stroke', 'var(--text-secondary)')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4,3');
  }

  // X-axis: January of each year, plus the first bar when it is not one.
  rows.forEach((r, i) => {
    if (r.period.getMonth() !== 0 && i !== 0) return;
    if (i === 0 && r.period.getMonth() > 8) return; // too close to the next January label
    g.append('text')
      .attr('x', x(i) + x.bandwidth() / 2)
      .attr('y', height + 18)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)')
      .text(r.period.getMonth() === 0 ? r.period.getFullYear() : r.period.toLocaleDateString('en-US', { month: 'short', year: '2-digit' }));
  });

  // Legend
  const lg = svg.append('g').attr('transform', `translate(${margin.left + 40}, 10)`);
  lg.append('rect').attr('width', 10).attr('height', 10).style('fill', 'var(--blue-flame)').attr('fill-opacity', 0.75);
  lg.append('text').attr('class', 'chart-legend-text').attr('x', 15).attr('y', 9).text('Monthly average');
  lg.append('line').attr('x1', 130).attr('x2', 144).attr('y1', 5).attr('y2', 5)
    .attr('stroke', 'var(--text-secondary)').attr('stroke-dasharray', '4,3').attr('stroke-width', 1.5);
  lg.append('text').attr('class', 'chart-legend-text').attr('x', 150).attr('y', 9).text('Trailing 12 months');

  // Latest bar label
  const lastI = rows.length - 1;
  g.append('text')
    .attr('x', x(lastI) + x.bandwidth() / 2)
    .attr('y', y(rows[lastI].bcfd) - 6)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-weight', 600)
    .attr('font-family', 'var(--font-mono)')
    .style('fill', 'var(--blue-flame)')
    .text(rows[lastI].bcfd.toFixed(1));

  // Hover
  const host = d3.select(container).style('position', 'relative');
  const tip = host.append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const yearAgo = (r) => rows.find((o) => o.period.getFullYear() === r.period.getFullYear() - 1
    && o.period.getMonth() === r.period.getMonth());
  const sgn = (v) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(2)}`;
  g.append('rect').attr('width', width).attr('height', height).attr('fill', 'transparent')
    .on('mousemove', (event) => {
      const [mx] = d3.pointer(event);
      const i = Math.max(0, Math.min(rows.length - 1, Math.floor(mx / x.step())));
      const r = rows[i];
      const prev = rows[i - 1];
      const ya = yearAgo(r);
      bars.attr('fill-opacity', (_, j) => (j === i ? 1 : 0.75));
      const row = (l, v) => `<div class="tt-row"><span class="tt-label">${l}</span><span class="num">${v}</span></div>`;
      const box = container.getBoundingClientRect();
      const px = ((margin.left + x(i)) / outerW) * box.width;
      tip.style('opacity', 1)
        .style('left', `${Math.max(0, Math.min(px + 14, box.width - 230))}px`).style('top', '30px')
        .html(`<div class="tt-date">${monthLabel(r.period)} · ${r.days} days</div>
          ${row('Exports', `${r.bcfd.toFixed(2)} Bcf/d`)}
          ${row('Monthly volume', `${r.bcf.toFixed(1)} Bcf`)}
          ${prev ? row(`vs ${monthLabel(prev.period)}`, `${sgn(r.bcfd - prev.bcfd)} Bcf/d`) : ''}
          ${ya ? row(`vs ${monthLabel(ya.period)}`, `${sgn(r.bcfd - ya.bcfd)} Bcf/d`) : ''}
          ${r.ma12 == null ? '' : row('Trailing 12 months', `${r.ma12.toFixed(2)} Bcf/d`)}`);
    })
    .on('mouseleave', () => { tip.style('opacity', 0); bars.attr('fill-opacity', 0.75); });
}

/* ================================================================== */
/*  KPI Strip                                                          */
/* ================================================================== */

function renderLngTotalKpis(sidebarEl, rows) {
  if (rows.length < 2) return;

  const latest = rows[rows.length - 1];
  const prior = rows[rows.length - 2];
  const momDelta = latest.bcfd - prior.bcfd;
  const yoyMatch = rows.find((r) => r.period.getFullYear() === latest.period.getFullYear() - 1
    && r.period.getMonth() === latest.period.getMonth());
  const yoyDelta = yoyMatch ? latest.bcfd - yoyMatch.bcfd : null;

  const last12 = rows.slice(-12);
  const prior12 = rows.slice(-24, -12);
  const rolling12 = last12.reduce((s, r) => s + r.bcf, 0);
  const prior12Sum = prior12.reduce((s, r) => s + r.bcf, 0);
  const rolling12Delta = prior12.length >= 12 ? rolling12 - prior12Sum : null;
  const sgn = (v, d = 1) => `${v >= 0 ? '+' : '−'}${Math.abs(v).toFixed(d)}`;

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Latest month',
      value: `${latest.bcfd.toFixed(1)} Bcf/d`,
      delta: null,
      helpText: `${monthLabel(latest.period)} · ${latest.bcf.toFixed(1)} Bcf over ${latest.days} days`,
    }),
    kpiCardHtml({
      label: 'Month on month',
      value: `${sgn(momDelta, 2)} Bcf/d`,
      delta: { value: `${sgn((momDelta / prior.bcfd) * 100)}%`, kind: momDelta >= 0 ? 'bullish' : 'bearish' },
      helpText: `vs ${monthLabel(prior.period)} (${prior.bcfd.toFixed(1)} Bcf/d) · per-day, so month length does not count`,
    }),
    kpiCardHtml({
      label: 'Year on year',
      value: yoyDelta != null ? `${sgn(yoyDelta, 2)} Bcf/d` : 'n/a',
      delta: yoyDelta != null ? {
        value: `${sgn((yoyDelta / yoyMatch.bcfd) * 100)}%`,
        kind: yoyDelta >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: yoyMatch ? `vs ${monthLabel(yoyMatch.period)} (${yoyMatch.bcfd.toFixed(1)} Bcf/d)` : 'No same month a year earlier',
    }),
    kpiCardHtml({
      label: '12-month total',
      value: `${rolling12.toFixed(0)} Bcf`,
      delta: rolling12Delta != null ? {
        value: `${sgn(rolling12Delta, 0)} Bcf vs prior 12 mo`,
        kind: rolling12Delta >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: `${monthLabel(last12[0].period)} – ${monthLabel(latest.period)} · ${latest.ma12 == null ? '' : `${latest.ma12.toFixed(1)} Bcf/d average`}`,
    }),
  ].join('');
}
