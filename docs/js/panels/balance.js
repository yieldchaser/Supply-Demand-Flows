/**
 * Balance Panel — Supply & Demand Balance.
 *
 * Data schema: series_ids are supply_fpd (production), supply_vc0 (consumption),
 * supply_sai (injections), supply_saw (withdrawals), supply_ovi, supply_vg4.
 * Values are in MMcf — convert to Bcf (/1000) for display.
 * Periods are YYYY-MM strings.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';

export function renderBalancePanel(panelEl, bundle) {
  const source = bundle.sources.eia_supply;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">EIA Supply data unavailable.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Supply & Demand Balance',
    subtitle: 'EIA · Monthly US production, consumption, storage flows',
    sourceKey: 'eia_supply',
    latestPeriod: source.latest_period,
  });

  // Parse series — values are MMcf, convert to Bcf
  const parseSeries = (seriesId) =>
    getSeries(source.data, seriesId).map((r) => ({
      ...r,
      period: new Date(r.period.getFullYear(), r.period.getMonth(), 1),
      value: r.value / 1000,
    }));

  const groups = {
    FPD: parseSeries('supply_fpd'),
    VC0: parseSeries('supply_vc0'),
    SAI: parseSeries('supply_sai'),
    SAW: parseSeries('supply_saw'),
  };

  drawBalanceChart(chartEl, groups);
  renderBalanceKpis(sidebarEl, groups);
}

function drawBalanceChart(container, groups) {
  container.innerHTML = '';
  const rect = container.getBoundingClientRect();
  const margin = { top: 32, right: 16, bottom: 36, left: 60 };
  const width = Math.max(rect.width, 300) - margin.left - margin.right;
  const height = 360 - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} 360`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Last ~3 years
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - 3);
  const productionRows = groups.FPD.filter((r) => r.period >= cutoff);
  const consumptionRows = groups.VC0.filter((r) => r.period >= cutoff);

  if (productionRows.length === 0 || consumptionRows.length === 0) {
    container.innerHTML = '<div class="panel-error">Production or consumption series missing.</div>';
    return;
  }

  const allRows = [...productionRows, ...consumptionRows];
  const x = d3.scaleTime()
    .domain(d3.extent(allRows, (r) => r.period))
    .range([0, width]);
  const y = d3.scaleLinear()
    .domain([0, d3.max(allRows, (r) => r.value) * 1.08])
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

  // Implied flow shading between lines
  const flowData = productionRows.map((p) => {
    const c = consumptionRows.find((cr) => cr.period.getTime() === p.period.getTime());
    if (!c) return null;
    return { period: p.period, prod: p.value, cons: c.value, flow: p.value - c.value };
  }).filter(Boolean);

  // Green shading where prod > cons (build), red where cons > prod (draw)
  for (const d of flowData) {
    if (flowData.indexOf(d) === 0) continue;
    const prev = flowData[flowData.indexOf(d) - 1];
    const color = d.flow >= 0 ? 'rgba(34, 211, 164, 0.12)' : 'rgba(239, 78, 92, 0.12)';
    g.append('path')
      .attr('d', `
        M${x(prev.period)},${y(prev.prod)}
        L${x(d.period)},${y(d.prod)}
        L${x(d.period)},${y(d.cons)}
        L${x(prev.period)},${y(prev.cons)}Z
      `)
      .attr('fill', color);
  }

  // Production line (Blue Flame)
  const lineGen = d3.line()
    .x((d) => x(d.period))
    .y((d) => y(d.value))
    .curve(d3.curveMonotoneX);

  g.append('path')
    .datum(productionRows)
    .attr('d', lineGen)
    .attr('fill', 'none')
    .attr('stroke', 'var(--blue-flame)')
    .attr('stroke-width', 2.5)
    .style('filter', 'drop-shadow(0 0 8px rgba(125, 211, 252, 0.3))');

  // Consumption line (amber)
  g.append('path')
    .datum(consumptionRows)
    .attr('d', lineGen)
    .attr('fill', 'none')
    .attr('stroke', '#f59e0b')
    .attr('stroke-width', 2.5)
    .style('filter', 'drop-shadow(0 0 6px rgba(245, 158, 11, 0.25))');

  // Y-axis labels
  g.selectAll('.y-tick-text')
    .data(yTicks)
    .enter()
    .append('text')
    .attr('x', -8)
    .attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 11)
    .attr('font-family', 'var(--font-mono)')
    .attr('font-feature-settings', "'tnum'")
    .attr('fill', 'var(--chart-label)')
    .text((d) => d >= 1000 ? `${(d / 1000).toFixed(0)}k` : `${Math.round(d)}`);

  // X-axis
  const xAxis = d3.axisBottom(x)
    .ticks(d3.timeYear.every(1))
    .tickFormat(d3.timeFormat('%Y'));
  g.append('g')
    .attr('transform', `translate(0,${height})`)
    .call(xAxis)
    .call((sel) => {
      sel.selectAll('line, path').attr('stroke', 'rgba(255,255,255,0.08)');
      sel.selectAll('text')
        .attr('fill', 'var(--chart-label)')
        .attr('font-size', 11)
        .attr('font-family', 'var(--font-sans)');
    });

  // Legend
  const legend = svg.append('g').attr('transform', `translate(${margin.left + 8}, 14)`);
  const legendItems = [
    { label: 'Production', color: 'var(--blue-flame)' },
    { label: 'Consumption', color: '#f59e0b' },
    { label: 'Build', color: 'rgba(34, 211, 164, 0.5)' },
    { label: 'Draw', color: 'rgba(239, 78, 92, 0.5)' },
  ];
  let xOff = 0;
  legendItems.forEach((it) => {
    const grp = legend.append('g').attr('transform', `translate(${xOff}, 0)`);
    grp.append('rect').attr('width', 10).attr('height', 10).attr('fill', it.color).attr('rx', 2);
    const textEl = grp.append('text')
      .attr('x', 14).attr('y', 9)
      .attr('font-size', 11)
      .attr('font-family', 'var(--font-sans)')
      .attr('fill', 'rgba(255,255,255,0.7)')
      .text(it.label);
    // Measure text width for spacing
    xOff += it.label.length * 6.5 + 28;
  });

  // Hover crosshair
  setupBalanceHover(svg, g, x, y, width, height, productionRows, consumptionRows, flowData, margin);
}

function setupBalanceHover(svg, g, x, y, width, height, prodRows, consRows, flowData, margin) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip')
    .style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'transparent');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const dateAtMouse = x.invert(mx);
      // Find nearest month
      const nearestProd = prodRows.reduce((best, r) =>
        Math.abs(r.period - dateAtMouse) < Math.abs(best.period - dateAtMouse) ? r : best
      );
      const nearestCons = consRows.find((r) => r.period.getTime() === nearestProd.period.getTime());
      if (!nearestCons) {
        crosshair.style('opacity', 0);
        tooltipDiv.style('opacity', 0);
        return;
      }

      const xPos = x(nearestProd.period);
      crosshair.attr('x1', xPos).attr('x2', xPos).style('opacity', 1);

      const flow = nearestProd.value - nearestCons.value;
      const flowLabel = flow >= 0 ? 'Net injection' : 'Net withdrawal';
      const flowColor = flow >= 0 ? 'var(--color-bullish-supply)' : 'var(--color-bearish-supply)';

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / (width + margin.left + margin.right);
      const tooltipX = margin.left * scaleX + xPos * scaleX + 16;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 220)}px`)
        .style('top', '40px')
        .html(`
          <div class="tt-date">${nearestProd.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">Production</span><span class="num">${Math.round(nearestProd.value).toLocaleString()} Bcf</span></div>
          <div class="tt-row"><span class="tt-label">Consumption</span><span class="num">${Math.round(nearestCons.value).toLocaleString()} Bcf</span></div>
          <div class="tt-row tt-row--accent"><span class="tt-label">${flowLabel}</span><span class="num" style="color:${flowColor}">${flow >= 0 ? '+' : ''}${Math.round(flow).toLocaleString()} Bcf</span></div>
        `);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      tooltipDiv.style('opacity', 0);
    });
}

function renderBalanceKpis(sidebarEl, groups) {
  const fpd = groups.FPD;
  const vc0 = groups.VC0;
  if (fpd.length === 0 || vc0.length === 0) return;

  const latestFpd = fpd[fpd.length - 1];
  const latestVc0 = vc0[vc0.length - 1];

  // YoY: 12 months back
  const yoyFpd = fpd.length >= 13 ? fpd[fpd.length - 13] : null;
  const yoyVc0 = vc0.length >= 13 ? vc0[vc0.length - 13] : null;

  const fpdYoyPct = yoyFpd ? ((latestFpd.value - yoyFpd.value) / yoyFpd.value) * 100 : null;
  const vc0YoyPct = yoyVc0 ? ((latestVc0.value - yoyVc0.value) / yoyVc0.value) * 100 : null;

  const flow = latestFpd.value - latestVc0.value;

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Production',
      value: `${Math.round(latestFpd.value).toLocaleString()} Bcf`,
      delta: fpdYoyPct != null ? {
        value: `${fpdYoyPct >= 0 ? '+' : ''}${fpdYoyPct.toFixed(1)}% YoY`,
        kind: fpdYoyPct >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: `${latestFpd.period.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}`,
    }),
    kpiCardHtml({
      label: 'Consumption',
      value: `${Math.round(latestVc0.value).toLocaleString()} Bcf`,
      delta: vc0YoyPct != null ? {
        value: `${vc0YoyPct >= 0 ? '+' : ''}${vc0YoyPct.toFixed(1)}% YoY`,
        kind: vc0YoyPct >= 0 ? 'bearish' : 'bullish',  // demand up = bearish supply
      } : null,
    }),
    kpiCardHtml({
      label: 'Implied flow',
      value: `${flow >= 0 ? '+' : ''}${Math.round(flow).toLocaleString()} Bcf`,
      delta: {
        value: flow >= 0 ? 'Net injection' : 'Net withdrawal',
        kind: flow >= 0 ? 'bullish' : 'bearish',
      },
      helpText: 'Production minus consumption',
    }),
  ].join('');
}
