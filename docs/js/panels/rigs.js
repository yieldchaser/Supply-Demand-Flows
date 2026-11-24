/**
 * Rigs Panel — US Rig Count Monitor.
 *
 * Two stacked panels on one time axis: gas rigs (Blue Flame hero, with a
 * 4-week average) over oil rigs, each on its own labelled scale. KPI sidebar: US Total,
 * Gas Rigs (hero with 4w trend), Oil Rigs, Horizontal share.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries, referenceDate } from '../util/series.js';

export function renderRigsPanel(panelEl, bundle) {
  const source = bundle.sources.baker_hughes_weekly;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">Baker Hughes data unavailable.</div>';
    return;
  }

  // `chartEl` only exists after the chrome renders, so the redraw hook reads it
  // through the ref rather than closing over the not-yet-declared binding.
  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'US Rig Count Monitor',
    subtitle: 'Baker Hughes · gas vs oil drilling activity',
    sourceKey: 'baker_hughes_weekly',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  const us_total = getSeries(source.data, 'bh_rollup_us_total');
  const us_gas   = getSeries(source.data, 'bh_rollup_us_gas');
  const us_oil   = getSeries(source.data, 'bh_rollup_us_oil');
  const us_horiz = getSeries(source.data, 'bh_rollup_us_horizontal');

  drawRef.current = () => drawRigsChart(chartEl, us_gas, us_oil);
  drawRef.current();
  renderRigsKpis(sidebarEl, us_total, us_gas, us_oil, us_horiz);
}

/* ------------------------------------------------------------------ */
/*  Chart                                                              */
/* ------------------------------------------------------------------ */

function drawRigsChart(container, gasRows, oilRows) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 30, right: 16, bottom: 28, left: 48 };
  const totalH = 380;
  const width = Math.max(rect.width, 300) - margin.left - margin.right;
  const gap = 26;
  // Plot height excluding the gap between the two panels; the panels plus the
  // gap end at `bottom`, where the shared time axis sits.
  const inner = totalH - margin.top - margin.bottom - gap;
  const bottom = inner + gap;
  // Two panels on one time axis. On a shared 0–500 axis the gas count (~130)
  // was a flat line along the floor; each panel now scales to its own range,
  // labelled, so a 5% move reads as one.
  const gasH = Math.round(inner * 0.6);
  const oilH = inner - gasH;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block')
    .style('width', '100%');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Last 2 years, anchored on the newest rig observation rather than the wall
  // clock: a bundle read after a long outage would otherwise lose its history
  // one week at a time on every redraw.
  const anchor = referenceDate(gasRows, oilRows);
  const cutoff = anchor ? new Date(anchor) : null;
  if (cutoff) cutoff.setFullYear(cutoff.getFullYear() - 2);
  const allGas = gasRows;
  const allOil = oilRows;
  gasRows = cutoff ? gasRows.filter((r) => r.period >= cutoff) : [];
  oilRows = cutoff ? oilRows.filter((r) => r.period >= cutoff) : [];

  if (gasRows.length === 0 && oilRows.length === 0) {
    container.innerHTML = '<div class="panel-error">No rig observations in the last 2 years of data.</div>';
    return;
  }

  const x = d3.scaleTime()
    .domain(d3.extent([...gasRows, ...oilRows], (r) => r.period))
    .range([0, width]);

  const panel = (rows, top, h, color, label, hero) => {
    const pg = g.append('g').attr('transform', `translate(0,${top})`);
    const [lo, hi] = d3.extent(rows, (r) => r.value);
    const pad = Math.max((hi - lo) * 0.12, 4);
    const y = d3.scaleLinear().domain([lo - pad, hi + pad]).nice(4).range([h, 0]);
    const ticks = y.ticks(4);
    pg.selectAll('line.grid').data(ticks).join('line')
      .attr('x1', 0).attr('x2', width).attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
      .attr('stroke', 'var(--chart-grid)');
    pg.selectAll('text.tick').data(ticks).join('text')
      .attr('x', -8).attr('y', (d) => y(d) + 4).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)').text((d) => d.toLocaleString());
    pg.append('text').attr('x', 4).attr('y', -6)
      .attr('font-size', 11).style('font-weight', hero ? 500 : 400)
      .style('fill', hero ? 'var(--blue-flame)' : 'var(--text-secondary)').text(label);
    const line = d3.line().x((d) => x(d.period)).y((d) => y(d.value)).curve(d3.curveMonotoneX);
    if (hero && rows.length >= 4) {
      pg.append('path').attr('d', line(computeMovingAverage(rows, 4)))
        .attr('fill', 'none').style('stroke', 'rgba(125, 211, 252, 0.45)')
        .attr('stroke-width', 1.5).attr('stroke-dasharray', '3,4');
    }
    pg.append('path').attr('d', line(rows)).attr('fill', 'none')
      .style('stroke', color).attr('stroke-width', hero ? 2.5 : 1.8);
    const last = rows[rows.length - 1];
    pg.append('circle').attr('cx', x(last.period)).attr('cy', y(last.value)).attr('r', hero ? 4 : 3).style('fill', color);
    // Label on the side the line is not coming from, so it never sits on it.
    const prev = rows[rows.length - 2];
    const below = prev && prev.value > last.value;
    pg.append('text').attr('x', x(last.period) - 8).attr('y', y(last.value) + (below ? 14 : -8)).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-weight', 600).attr('font-family', 'var(--font-mono)')
      .style('fill', color).text(last.value);
    return { y, top, pg };
  };

  const gasP = gasRows.length > 1 ? panel(gasRows, 0, gasH, 'var(--blue-flame)', 'Gas rigs · 4-week average dashed', true) : null;
  const oilP = oilRows.length > 1 ? panel(oilRows, gasH + gap, oilH, 'rgba(156, 163, 175, 0.85)', 'Oil rigs', false) : null;

  g.append('g').attr('transform', `translate(0,${bottom})`)
    // Quarterly labels collide below ~500px; half-yearly there.
    .call(d3.axisBottom(x).ticks(d3.timeMonth.every(width < 500 ? 6 : 3)).tickFormat(d3.timeFormat("%b '%y")))
    .call((sel) => {
      sel.selectAll('line, path').attr('stroke', 'var(--chart-axis)');
      sel.selectAll('text').style('fill', 'var(--chart-label)').attr('font-size', 10).attr('font-family', 'var(--font-mono)');
    });

  setupRigsHover(svg, g, x, width, bottom, allGas, allOil, gasRows, margin, { gasP, oilP });
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

function setupRigsHover(svg, g, x, width, height, allGas, allOil, gasRows, margin, panels) {
  const tooltip = d3.select(svg.node().parentNode).style('position', 'relative').append('div')
    .attr('class', 'chart-tooltip')
    .style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('opacity', 0);

  // Same week a year earlier (within 4 days), from the full history.
  const yearAgo = (rows, row) => {
    const t = new Date(row.period);
    t.setFullYear(t.getFullYear() - 1);
    const r = closestRow(rows, t);
    return r && Math.abs(r.period - t) <= 4 * 86400000 ? r : null;
  };
  const sgn = (v) => (v == null ? '—' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v)}`);

  g.append('rect')
    .attr('width', width).attr('height', height)
    .attr('fill', 'transparent')
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const dateAtX = x.invert(mx);
      const gas = closestRow(gasRows, dateAtX);
      if (!gas) {
        crosshair.style('opacity', 0);
        tooltip.style('opacity', 0);
        return;
      }
      const oil = closestRow(allOil, gas.period);
      crosshair.attr('x1', x(gas.period)).attr('x2', x(gas.period)).style('opacity', 1);

      const gi = allGas.indexOf(gas);
      const gWow = gi > 0 ? gas.value - allGas[gi - 1].value : null;
      const oi = oil ? allOil.indexOf(oil) : -1;
      const oWow = oi > 0 ? oil.value - allOil[oi - 1].value : null;
      const gYa = yearAgo(allGas, gas);
      const oYa = oil ? yearAgo(allOil, oil) : null;

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + x(gas.period) * scaleX + 16;
      const row = (l, v, cls = '') => `<div class="tt-row ${cls}"><span class="tt-label">${l}</span><span class="num">${v}</span></div>`;

      tooltip
        .style('opacity', 1)
        .style('left', `${Math.max(0, Math.min(tooltipX, containerRect.width - 210))}px`)
        .style('top', '40px')
        .html(`
          <div class="tt-date">Week ending ${gas.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          ${row('Gas rigs', gas.value, 'tt-row--accent')}
          ${row('vs prior week', sgn(gWow))}
          ${gYa ? row('vs a year earlier', `${sgn(gas.value - gYa.value)} (${gYa.value})`) : ''}
          ${oil ? row('Oil rigs', oil.value) : ''}
          ${oil ? row('vs prior week', sgn(oWow)) : ''}
          ${oYa ? row('vs a year earlier', `${sgn(oil.value - oYa.value)} (${oYa.value})`) : ''}
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
