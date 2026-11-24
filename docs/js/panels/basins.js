/**
 * Basin Momentum Panel — small-multiples sparkline grid.
 *
 * 6-card grid showing 12-week trajectory for top basins, color-coded
 * by 4-week momentum (accelerating cyan / decelerating amber / flat gray).
 * Top row: Marcellus, Haynesville, Permian (gas-relevant trio).
 * Bottom row: Eagle Ford, DJ-Niobrara, Bakken.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { getSeries } from '../util/series.js';

const FEATURED_BASINS = [
  { id: 'marcellus',   label: 'Marcellus',   gasFlavor: 'pure_gas' },
  { id: 'haynesville', label: 'Haynesville', gasFlavor: 'pure_gas' },
  { id: 'permian',     label: 'Permian',     gasFlavor: 'associated' },
  { id: 'eagle_ford',  label: 'Eagle Ford',  gasFlavor: 'mixed' },
  { id: 'dj_niobrara', label: 'DJ-Niobrara', gasFlavor: 'oil_basin' },
  { id: 'williston',   label: 'Bakken (Williston)', gasFlavor: 'oil_basin' },
];

export function renderBasinsPanel(panelEl, bundle) {
  const source = bundle.sources.baker_hughes_weekly;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">Baker Hughes data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Basin Momentum',
    subtitle: 'Baker Hughes · top 6 basins, 12-week trajectory',
    sourceKey: 'baker_hughes_weekly',
    latestPeriod: source.latest_period,
  });

  // Hide sidebar — basins panel uses chart-grid full width
  sidebarEl.style.display = 'none';
  chartEl.style.gridColumn = '1 / -1';

  // Build basin grid
  chartEl.innerHTML = '<div class="basin-grid"></div>';
  const grid = chartEl.querySelector('.basin-grid');

  for (const basin of FEATURED_BASINS) {
    const series = getSeries(source.data, `bh_rollup_basin_${basin.id}`);
    if (series.length === 0) {
      grid.insertAdjacentHTML('beforeend', `
        <div class="basin-card basin-card--empty">
          <div class="basin-card__name">${basin.label}</div>
          <div class="basin-card__empty-msg">no data</div>
        </div>
      `);
      continue;
    }
    grid.insertAdjacentHTML('beforeend', renderBasinCardHtml(basin, series));
    const cardEl = grid.lastElementChild;
    drawBasinSparkline(cardEl.querySelector('.basin-card__spark'), series);
  }
}

/* ------------------------------------------------------------------ */
/*  Basin card HTML                                                    */
/* ------------------------------------------------------------------ */

/**
 * Momentum over four weeks, judged against the basin's size: a move must be
 * at least 2 rigs AND 3% of the level. A flat ±1 rule called +3 on Permian's
 * 275 (1%) "accelerating" with the same weight as +3 on a 23-rig basin.
 */
export function basinMomentum(series) {
  const last12 = series.slice(-12);
  const latest = last12[last12.length - 1];
  const fourWeeksAgo = last12.length >= 5 ? last12[last12.length - 5] : last12[0];
  const delta4w = latest.value - fourWeeksAgo.value;
  const threshold = Math.max(2, Math.abs(fourWeeksAgo.value) * 0.03);
  const dir = delta4w >= threshold ? 'up' : delta4w <= -threshold ? 'down' : 'flat';
  const vals = last12.map((d) => d.value);
  return {
    last12, latest, delta4w, dir,
    pct4w: fourWeeksAgo.value ? (delta4w / fourWeeksAgo.value) * 100 : null,
    lo: Math.min(...vals), hi: Math.max(...vals),
  };
}

function renderBasinCardHtml(basin, series) {
  const { latest, delta4w, dir, pct4w, lo, hi, last12 } = basinMomentum(series);

  const momentumClass = { up: 'basin-card--accel', down: 'basin-card--decel', flat: 'basin-card--flat' }[dir];
  const arrow = { up: '↑', down: '↓', flat: '→' }[dir];

  const flavorBadge = {
    pure_gas:   '<span class="basin-card__flavor basin-card__flavor--gas">PURE GAS</span>',
    associated: '<span class="basin-card__flavor basin-card__flavor--mixed">ASSOC. GAS</span>',
    mixed:      '<span class="basin-card__flavor basin-card__flavor--mixed">MIXED</span>',
    oil_basin:  '<span class="basin-card__flavor basin-card__flavor--oil">OIL</span>',
  }[basin.gasFlavor] || '';

  return `
    <div class="basin-card ${momentumClass}" title="${basin.label} · Latest: ${latest.value} rigs (${delta4w >= 0 ? '+' : ''}${delta4w} vs 4-week prior)">
      <div class="basin-card__head">
        <span class="basin-card__name">${basin.label}</span>
        ${flavorBadge}
      </div>
      <div class="basin-card__metric">
        <span class="basin-card__value num">${latest.value}</span>
        <span class="basin-card__delta">
          <span class="basin-card__arrow">${arrow}</span>
          <span class="num">${delta4w >= 0 ? '+' : ''}${delta4w} / 4w${pct4w == null ? '' : ` (${pct4w >= 0 ? '+' : ''}${pct4w.toFixed(0)}%)`}</span>
        </span>
      </div>
      <svg class="basin-card__spark" viewBox="0 0 200 60" preserveAspectRatio="none"></svg>
      <div class="basin-card__range num">${last12.length} wk range ${lo}–${hi}</div>
    </div>
  `;
}

/* ------------------------------------------------------------------ */
/*  Sparkline renderer                                                 */
/* ------------------------------------------------------------------ */

function drawBasinSparkline(svgEl, series) {
  const last12 = series.slice(-12);
  if (last12.length < 2) return;

  const w = 200, h = 60;
  const x = d3.scaleLinear().domain([0, last12.length - 1]).range([4, w - 4]);
  const [lo, hi] = d3.extent(last12, (d) => d.value);
  // Vertical span is at least 20% of the level (and 4 rigs), centred on the
  // data: a min–max stretch made a 1% wiggle fill the card like a collapse.
  const span = Math.max(hi - lo, Math.abs(last12[last12.length - 1].value) * 0.2, 4);
  const mid = (lo + hi) / 2;
  const y = d3.scaleLinear().domain([mid - span * 0.6, mid + span * 0.6]).range([h - 4, 4]);

  const { dir } = basinMomentum(series);
  const lineColor = dir === 'up' ? 'var(--blue-flame)' : dir === 'down' ? '#f59e0b' : 'rgba(156, 163, 175, 0.7)';
  const fillColor = dir === 'up' ? 'rgba(125, 211, 252, 0.15)' : dir === 'down' ? 'rgba(245, 158, 11, 0.12)' : 'rgba(156, 163, 175, 0.1)';

  const lineGen = d3.line()
    .x((_, i) => x(i))
    .y((d) => y(d.value))
    .curve(d3.curveMonotoneX);
  const areaGen = d3.area()
    .x((_, i) => x(i))
    .y0(h - 4)
    .y1((d) => y(d.value))
    .curve(d3.curveMonotoneX);

  const svg = d3.select(svgEl);

  // Gradient fill under the sparkline
  svg.append('path').datum(last12)
    .attr('d', areaGen)
    .style('fill', fillColor);

  // Sparkline
  svg.append('path').datum(last12)
    .attr('d', lineGen)
    .attr('fill', 'none')
    .style('stroke', lineColor)
    .attr('stroke-width', 1.5);

  // Latest-point dot
  svg.append('circle')
    .attr('cx', x(last12.length - 1))
    .attr('cy', y(last12[last12.length - 1].value))
    .attr('r', 2.5)
    .style('fill', lineColor);
}
