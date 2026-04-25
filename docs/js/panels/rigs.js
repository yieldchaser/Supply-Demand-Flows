/**
 * Rigs Panel — US Rig Count Monitor.
 *
 * Dual-line chart: gas rigs (Blue Flame hero) + oil rigs (muted gray).
 * 4-week moving average overlay on gas line. KPI sidebar: US Total,
 * Gas Rigs (hero with 4w trend), Oil Rigs, Horizontal share.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';

export function renderRigsPanel(panelEl, bundle) {
  const source = bundle.sources.baker_hughes_weekly;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">Baker Hughes data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'US Rig Count Monitor',
    subtitle: 'Baker Hughes · gas vs oil drilling activity',
    sourceKey: 'baker_hughes_weekly',
    latestPeriod: source.latest_period,
  });

  const us_total = getSeries(source.data, 'bh_rollup_us_total');
  const us_gas   = getSeries(source.data, 'bh_rollup_us_gas');
  const us_oil   = getSeries(source.data, 'bh_rollup_us_oil');
  const us_horiz = getSeries(source.data, 'bh_rollup_us_horizontal');

  drawRigsChart(chartEl, us_gas, us_oil);
  renderRigsKpis(sidebarEl, us_total, us_gas, us_oil, us_horiz);
}

/* ------------------------------------------------------------------ */
/*  Chart                                                              */
/* ------------------------------------------------------------------ */

function drawRigsChart(container, gasRows, oilRows) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 32, right: 16, bottom: 36, left: 48 };
  const width = Math.max(rect.width, 300) - margin.left - margin.right;
  const height = 360 - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 360`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Last 2 years
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - 2);
  gasRows = gasRows.filter((r) => r.period >= cutoff);
  oilRows = oilRows.filter((r) => r.period >= cutoff);

  if (gasRows.length === 0 && oilRows.length === 0) {
    container.innerHTML = '<div class="panel-error">No rig data in the last 2 years.</div>';
    return;
  }

  const allRows = [...gasRows, ...oilRows];
  const x = d3.scaleTime()
    .domain(d3.extent(allRows, (r) => r.period))
    .range([0, width]);
  const yMax = d3.max(allRows, (r) => r.value) * 1.1;
  const y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);

  // Y gridlines
  const yTicks = y.ticks(5);
  g.selectAll('.gridline').data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // Y labels
  g.selectAll('.y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 11)
    .attr('font-family', 'var(--font-mono)')
    .style('font-feature-settings', "'tnum'")
    .style('fill', 'rgba(255,255,255,0.55)')
    .text((d) => d.toLocaleString());

  const lineGen = (rows) => d3.line()
    .x((d) => x(d.period))
    .y((d) => y(d.value))
    .curve(d3.curveMonotoneX)(rows);

  // Oil line — muted
  if (oilRows.length > 1) {
    g.append('path')
      .attr('d', lineGen(oilRows))
      .attr('fill', 'none')
      .style('stroke', 'rgba(156, 163, 175, 0.5)')
      .attr('stroke-width', 1.5);
  }

  // 4-week MA of gas — thin dashed lighter cyan
  if (gasRows.length >= 4) {
    const gasMA = computeMovingAverage(gasRows, 4);
    g.append('path')
      .attr('d', lineGen(gasMA))
      .attr('fill', 'none')
      .style('stroke', 'rgba(125, 211, 252, 0.4)')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '3,4');
  }

  // Gas line — Blue Flame hero
  if (gasRows.length > 1) {
    g.append('path')
      .attr('d', lineGen(gasRows))
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2.5)
      .style('filter', 'drop-shadow(0 0 8px rgba(125, 211, 252, 0.4))');
  }

  // Latest dots
  if (gasRows.length > 0) {
    const latestGas = gasRows[gasRows.length - 1];
    g.append('circle')
      .attr('cx', x(latestGas.period)).attr('cy', y(latestGas.value))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)')
      .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.6))');
  }
  if (oilRows.length > 0) {
    const latestOil = oilRows[oilRows.length - 1];
    g.append('circle')
      .attr('cx', x(latestOil.period)).attr('cy', y(latestOil.value))
      .attr('r', 3)
      .style('fill', 'rgba(156, 163, 175, 0.7)');
  }

  // X axis
  const xAxis = d3.axisBottom(x)
    .ticks(d3.timeMonth.every(3))
    .tickFormat(d3.timeFormat("%b '%y"));
  g.append('g').attr('transform', `translate(0,${height})`).call(xAxis)
    .call((sel) => {
      sel.selectAll('line, path').attr('stroke', 'rgba(255,255,255,0.08)');
      sel.selectAll('text')
        .style('fill', 'rgba(255,255,255,0.55)')
        .attr('font-size', 11)
        .attr('font-family', 'var(--font-sans)');
    });

  // Legend
  const legend = svg.append('g').attr('transform', `translate(${margin.left + 8}, 14)`);
  const legendItems = [
    { label: 'Gas rigs', color: 'var(--blue-flame)', isHero: true },
    { label: 'Oil rigs', color: 'rgba(156, 163, 175, 0.7)' },
    { label: '4w avg (gas)', color: 'rgba(125, 211, 252, 0.4)', dashed: true },
  ];
  let xOff = 0;
  legendItems.forEach((it) => {
    const grp = legend.append('g').attr('transform', `translate(${xOff}, 0)`);
    if (it.dashed) {
      grp.append('line')
        .attr('x1', 0).attr('x2', 14).attr('y1', 5).attr('y2', 5)
        .style('stroke', it.color).attr('stroke-width', 1.5).attr('stroke-dasharray', '3,3');
    } else {
      grp.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2)
        .style('fill', it.color);
    }
    grp.append('text')
      .attr('x', 18).attr('y', 9)
      .attr('font-size', 11)
      .attr('font-family', 'var(--font-sans)')
      .style('fill', it.isHero ? 'rgba(125, 211, 252, 0.95)' : 'rgba(255,255,255,0.65)')
      .style('font-weight', it.isHero ? '500' : '400')
      .text(it.label);
    xOff += it.label.length * 7 + 36;
  });

  // Hover crosshair + tooltip
  setupRigsHover(svg, g, x, y, width, height, gasRows, oilRows, margin);
}

/* ------------------------------------------------------------------ */
/*  Moving average                                                     */
/* ------------------------------------------------------------------ */

function computeMovingAverage(rows, window) {
  if (rows.length < window) return [];
  const out = [];
  for (let i = window - 1; i < rows.length; i++) {
    const slice = rows.slice(i - window + 1, i + 1);
    const avg = slice.reduce((s, r) => s + r.value, 0) / window;
    out.push({ period: rows[i].period, value: avg });
  }
  return out;
}

/* ------------------------------------------------------------------ */
/*  Hover interaction                                                  */
/* ------------------------------------------------------------------ */

function closestRow(rows, target) {
  if (rows.length === 0) return null;
  return rows.reduce((best, r) =>
    Math.abs(r.period - target) < Math.abs(best.period - target) ? r : best
  );
}

function setupRigsHover(svg, g, x, y, width, height, gasRows, oilRows, margin) {
  const tooltip = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip')
    .style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width).attr('height', height)
    .attr('fill', 'transparent');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const dateAtX = x.invert(mx);
      const gas = closestRow(gasRows, dateAtX);
      const oil = closestRow(oilRows, dateAtX);
      if (!gas) {
        crosshair.style('opacity', 0);
        tooltip.style('opacity', 0);
        return;
      }
      crosshair.attr('x1', x(gas.period)).attr('x2', x(gas.period)).style('opacity', 1);

      // WoW delta
      const idx = gasRows.indexOf(gas);
      const priorGas = idx > 0 ? gasRows[idx - 1] : null;
      const wow = priorGas ? gas.value - priorGas.value : null;

      // Position tooltip
      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(gas.period) * scaleX + 16;

      tooltip
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 200)}px`)
        .style('top', '40px')
        .html(`
          <div class="tt-date">${gas.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Gas rigs</span><span class="num">${gas.value}</span></div>
          ${oil ? `<div class="tt-row"><span class="tt-label">Oil rigs</span><span class="num">${oil.value}</span></div>` : ''}
          ${wow !== null ? `<div class="tt-row tt-row--accent"><span class="tt-label">Gas WoW</span><span class="num">${wow >= 0 ? '+' : ''}${wow}</span></div>` : ''}
        `);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      tooltip.style('opacity', 0);
    });
}

/* ------------------------------------------------------------------ */
/*  KPI sidebar                                                        */
/* ------------------------------------------------------------------ */

function renderRigsKpis(sidebarEl, total, gas, oil, horiz) {
  const last = (arr) => arr.length > 0 ? arr[arr.length - 1] : null;
  const prior = (arr) => arr.length > 1 ? arr[arr.length - 2] : null;
  const fourWeeksAgo = (arr) => arr.length > 4 ? arr[arr.length - 5] : null;

  const totalLatest = last(total);
  const totalPrior = prior(total);
  const gasLatest = last(gas);
  const gasPrior = prior(gas);
  const gas4wAgo = fourWeeksAgo(gas);
  const oilLatest = last(oil);
  const oilPrior = prior(oil);
  const horizLatest = last(horiz);

  const totalWoW = totalPrior && totalLatest ? totalLatest.value - totalPrior.value : null;
  const gasWoW = gasPrior && gasLatest ? gasLatest.value - gasPrior.value : null;
  const gas4w = gas4wAgo && gasLatest ? gasLatest.value - gas4wAgo.value : null;
  const oilWoW = oilPrior && oilLatest ? oilLatest.value - oilPrior.value : null;
  const horizPct = totalLatest && horizLatest && totalLatest.value > 0
    ? (horizLatest.value / totalLatest.value) * 100 : null;

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'US Total',
      value: totalLatest ? `${totalLatest.value} rigs` : 'n/a',
      delta: totalWoW != null ? {
        value: `${totalWoW >= 0 ? '+' : ''}${totalWoW} WoW`,
        kind: totalWoW > 0 ? 'bullish' : totalWoW < 0 ? 'bearish' : 'neutral',
      } : null,
    }),
    kpiCardHtml({
      label: 'Gas Rigs',
      value: gasLatest ? `${gasLatest.value} rigs` : 'n/a',
      delta: gasWoW != null ? {
        value: `${gasWoW >= 0 ? '+' : ''}${gasWoW} WoW${gas4w != null ? ` · ${gas4w >= 0 ? '+' : ''}${gas4w} over 4w` : ''}`,
        kind: gasWoW > 0 || (gas4w != null && gas4w > 0) ? 'bullish' : (gasWoW < 0 || (gas4w != null && gas4w < 0) ? 'bearish' : 'neutral'),
      } : null,
      helpText: 'Leading supply-side indicator',
    }),
    kpiCardHtml({
      label: 'Oil Rigs',
      value: oilLatest ? `${oilLatest.value} rigs` : 'n/a',
      delta: oilWoW != null ? {
        value: `${oilWoW >= 0 ? '+' : ''}${oilWoW} WoW`,
        kind: 'neutral',
      } : null,
    }),
    kpiCardHtml({
      label: 'Horizontal share',
      value: horizPct != null ? `${horizPct.toFixed(1)}%` : 'n/a',
      helpText: 'Higher = more shale-style drilling',
    }),
  ].join('');
}
