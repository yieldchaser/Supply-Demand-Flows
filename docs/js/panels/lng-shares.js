/**
 * LNG Shares Panel — Regional Destination Share Evolution.
 *
 * Layout:
 *   Chart: Stacked area of Europe / Asia / LatAm / Other share (%) over 36 months.
 *   KPIs: biggest destination, Europe share, Asia share, biggest MoM mover.
 *
 * Series used:
 *   lng_export_total           — denominator for share calculation
 *   lng_export_region_europe   — Europe volume
 *   lng_export_region_asia     — Asia volume
 *   lng_export_region_latam    — LatAm volume
 *   lng_export_region_other    — Other volume (or remainder)
 *
 * This panel contextualises the Transatlantic Divergence card: when the
 * divergence reads "Bullish for US LNG exports," Europe's share should
 * be elevated in this chart.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';

const MONTHS_DISPLAY = 36;

// Region definitions: id, label, color
const REGIONS = [
  { id: 'europe', label: 'Europe',       color: 'rgba(125,211,252,0.85)' },
  { id: 'asia',   label: 'Asia',         color: 'rgba(56, 189,248,0.75)' },
  { id: 'latam',  label: 'Latin America',color: 'rgba(34,211,164,0.70)' },
  { id: 'other',  label: 'Other',        color: 'rgba(156,163,175,0.50)' },
];

export function renderLngSharesPanel(panelEl, bundle) {
  const source = bundle.sources?.eia_lng_exports;
  if (!source?.data?.length) {
    panelEl.innerHTML = '<div class="panel-error">EIA LNG regional data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'LNG Destination Shares',
    subtitle: 'Regional allocation of US LNG exports · % of total',
    sourceKey: 'eia_lng_exports',
    latestPeriod: source.latest_period,
  });

  // Pull all regional series
  const totalRows = getSeries(source.data, 'lng_export_total');
  const regionSeries = REGIONS.map(r => ({
    ...r,
    rows: getSeries(source.data, `lng_export_region_${r.id}`),
  }));

  if (totalRows.length < 3) {
    chartEl.innerHTML = '<div class="panel-error" style="font-size:12px">Insufficient LNG regional data.</div>';
    return;
  }

  // Build share table: for each period in total, compute % per region
  const totalByPeriod = new Map(totalRows.map(r => [r.period.getTime(), r.value]));

  const shareSeries = regionSeries.map(reg => {
    const rows = reg.rows.map(r => {
      const total = totalByPeriod.get(r.period.getTime());
      const share = (total && total > 0) ? (r.value / total) * 100 : 0;
      return { period: r.period, value: r.value, share };
    });
    return { ...reg, rows };
  });

  // Align all regions to the common period list (inner join on totalRows)
  const periods = totalRows.slice(-MONTHS_DISPLAY).map(r => r.period);

  const aligned = periods.map(period => {
    const t = period.getTime();
    const entry: Record<string, number | Date> = { period };
    let stackBase = 0;
    for (const reg of shareSeries) {
      const match = reg.rows.find(r => r.period.getTime() === t);
      const share = match ? match.share : 0;
      entry[reg.id] = share;
      stackBase += share;
    }
    // Clamp: if stack > 100 due to rounding, normalise
    if (stackBase > 101) {
      const scale = 100 / stackBase;
      for (const reg of REGIONS) entry[reg.id] = (entry[reg.id] as number) * scale;
    }
    return entry;
  });

  drawSharesChart(chartEl, aligned, periods);
  renderSharesKpis(sidebarEl, aligned, shareSeries, periods);
}

/* ================================================================== */
/*  Stacked Area Chart                                                  */
/* ================================================================== */

function drawSharesChart(container, aligned, periods) {
  container.innerHTML = '';

  const margin = { top: 24, right: 120, bottom: 48, left: 48 };
  const totalH = 320;
  const width  = Math.max((container.getBoundingClientRect().width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleTime()
    .domain(d3.extent(periods))
    .range([0, width]);

  const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

  // Grid at 25/50/75
  [25, 50, 75, 100].forEach(v => {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(v)).attr('y2', y(v))
      .attr('stroke', v === 50 ? 'rgba(255,255,255,0.07)' : 'rgba(255,255,255,0.04)');
  });

  // D3 stack on region shares
  const stack = d3.stack()
    .keys(REGIONS.map(r => r.id))
    .order(d3.stackOrderNone)
    .offset(d3.stackOffsetNone);

  const stacked = stack(aligned);
  const areaGen = d3.area()
    .x((_, i) => x(periods[i]))
    .y0(d => y(d[0]))
    .y1(d => y(d[1]))
    .curve(d3.curveCatmullRom.alpha(0.5));

  stacked.forEach((layer, li) => {
    const color = REGIONS[li].color;
    g.append('path').datum(layer)
      .attr('d', areaGen)
      .style('fill', color)
      .attr('stroke', 'none');
  });

  // X-axis ticks every 6 months
  const xTicks = periods.filter((_, i) => i % 6 === 0);
  xTicks.forEach(d => {
    g.append('text')
      .attr('x', x(d)).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' }));
  });

  // Y-axis labels
  [0, 25, 50, 75, 100].forEach(v => {
    g.append('text')
      .attr('x', -8).attr('y', y(v) + 4)
      .attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)')
      .text(`${v}%`);
  });

  // Legend (right of chart inside SVG viewBox)
  const legendX = width + 12;
  REGIONS.forEach((r, i) => {
    const ly = i * 22 + 20;
    g.append('rect')
      .attr('x', legendX).attr('y', ly - 10)
      .attr('width', 12).attr('height', 12)
      .attr('rx', 2)
      .style('fill', r.color);
    g.append('text')
      .attr('x', legendX + 16).attr('y', ly)
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--text-secondary)')
      .text(r.label);
  });

  // Hover crosshair + tooltip
  setupSharesHover(svg, g, x, y, width, height, periods, aligned, margin);
}

function setupSharesHover(svg, g, x, y, width, height, periods, aligned, margin) {
  const tooltip = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip').style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)')
    .attr('stroke-dasharray', '2,2')
    .style('opacity', 0);

  g.append('rect')
    .attr('width', width).attr('height', height)
    .attr('fill', 'none').style('pointer-events', 'all')
    .on('mousemove', function(event) {
      const [mx] = d3.pointer(event);
      const date = x.invert(mx);
      const idx  = d3.bisectCenter(periods.map(p => p.getTime()), date.getTime());
      const row  = aligned[Math.max(0, Math.min(idx, aligned.length - 1))];
      if (!row) return;

      crosshair.attr('x1', x(row.period)).attr('x2', x(row.period)).style('opacity', 1);

      const svgRect = svg.node().getBoundingClientRect();
      const scaleX  = svgRect.width / parseInt(svg.attr('viewBox').split(' ')[2]);
      const tooltipX = margin.left * scaleX + x(row.period) * scaleX + 12;

      tooltip
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, svgRect.width - 180)}px`)
        .style('top', '12px')
        .html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}</div>
          ${REGIONS.map(r => `
            <div class="tt-row">
              <span class="tt-label">${r.label}</span>
              <span class="num">${(row[r.id] || 0).toFixed(1)}%</span>
            </div>`).join('')}
        `);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      tooltip.style('opacity', 0);
    });
}

/* ================================================================== */
/*  KPI Strip                                                          */
/* ================================================================== */

function renderSharesKpis(sidebarEl, aligned, shareSeries, periods) {
  if (aligned.length < 2) return;

  const latest = aligned[aligned.length - 1];
  const prior  = aligned[aligned.length - 2];

  // Biggest destination by current share
  const currentShares = REGIONS.map(r => ({ label: r.label, share: latest[r.id] || 0 }));
  currentShares.sort((a, b) => b.share - a.share);
  const biggest = currentShares[0];

  // Europe share + MoM
  const euNow  = latest['europe'] || 0;
  const euPrev = prior['europe']  || 0;
  const euDelta = euNow - euPrev;

  // Asia share + MoM
  const asiaNow  = latest['asia'] || 0;
  const asiaPrev = prior['asia']  || 0;
  const asiaDelta = asiaNow - asiaPrev;

  // Biggest MoM mover
  const movers = REGIONS.map(r => ({
    label: r.label,
    delta: (latest[r.id] || 0) - (prior[r.id] || 0),
  })).sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
  const bigMover = movers[0];

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Biggest destination',
      value: `${biggest.share.toFixed(1)}%`,
      delta: { value: biggest.label, kind: 'bullish' },
      helpText: 'Share of total US LNG this month',
    }),
    kpiCardHtml({
      label: 'Europe share',
      value: `${euNow.toFixed(1)}%`,
      delta: {
        value: `${euDelta >= 0 ? '+' : ''}${euDelta.toFixed(1)} pp MoM`,
        kind: euDelta >= 0 ? 'bullish' : 'bearish',
      },
      helpText: 'Elevated when EU storage is tight',
    }),
    kpiCardHtml({
      label: 'Asia share',
      value: `${asiaNow.toFixed(1)}%`,
      delta: {
        value: `${asiaDelta >= 0 ? '+' : ''}${asiaDelta.toFixed(1)} pp MoM`,
        kind: asiaDelta >= 0 ? 'bullish' : 'bearish',
      },
      helpText: 'JPN, KOR, CHN, IND + others',
    }),
    kpiCardHtml({
      label: 'Biggest mover',
      value: `${bigMover.delta >= 0 ? '+' : ''}${bigMover.delta.toFixed(1)} pp`,
      delta: { value: bigMover.label, kind: bigMover.delta >= 0 ? 'bullish' : 'bearish' },
      helpText: 'Largest MoM share change',
    }),
  ].join('');
}
