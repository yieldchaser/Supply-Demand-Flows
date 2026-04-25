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
  { id: 'bakken',      label: 'Bakken',      gasFlavor: 'oil_basin' },
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

function renderBasinCardHtml(basin, series) {
  const last12 = series.slice(-12);
  const latest = last12[last12.length - 1];
  const fourWeeksAgo = last12.length >= 5 ? last12[last12.length - 5] : last12[0];
  const delta4w = latest.value - fourWeeksAgo.value;

  let momentumClass, arrow;
  if (delta4w > 1)       { momentumClass = 'basin-card--accel'; arrow = '↑'; }
  else if (delta4w < -1) { momentumClass = 'basin-card--decel'; arrow = '↓'; }
  else                   { momentumClass = 'basin-card--flat';  arrow = '→'; }

  const flavorBadge = {
    pure_gas:   '<span class="basin-card__flavor basin-card__flavor--gas">PURE GAS</span>',
    associated: '<span class="basin-card__flavor basin-card__flavor--mixed">ASSOC. GAS</span>',
    mixed:      '<span class="basin-card__flavor basin-card__flavor--mixed">MIXED</span>',
    oil_basin:  '<span class="basin-card__flavor basin-card__flavor--oil">OIL</span>',
  }[basin.gasFlavor] || '';

  return `
    <div class="basin-card ${momentumClass}">
      <div class="basin-card__head">
        <span class="basin-card__name">${basin.label}</span>
        ${flavorBadge}
      </div>
      <div class="basin-card__metric">
        <span class="basin-card__value num">${latest.value}</span>
        <span class="basin-card__delta">
          <span class="basin-card__arrow">${arrow}</span>
          <span class="num">${delta4w >= 0 ? '+' : ''}${delta4w} / 4w</span>
        </span>
      </div>
      <svg class="basin-card__spark" viewBox="0 0 200 60" preserveAspectRatio="none"></svg>
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
  const yExtent = d3.extent(last12, (d) => d.value);
  const yPad = Math.max((yExtent[1] - yExtent[0]) * 0.2, 1);
  const y = d3.scaleLinear().domain([yExtent[0] - yPad, yExtent[1] + yPad]).range([h - 4, 4]);

  const fourWeeksAgoVal = last12.length >= 5 ? last12[last12.length - 5].value : last12[0].value;
  const delta4w = last12[last12.length - 1].value - fourWeeksAgoVal;

  const lineColor = delta4w > 1 ? 'var(--blue-flame)' : delta4w < -1 ? '#f59e0b' : 'rgba(156, 163, 175, 0.7)';
  const fillColor = delta4w > 1 ? 'rgba(125, 211, 252, 0.15)' : delta4w < -1 ? 'rgba(245, 158, 11, 0.12)' : 'rgba(156, 163, 175, 0.1)';

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
