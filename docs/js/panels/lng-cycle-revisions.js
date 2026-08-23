/**
 * Intraday Cycle Revisions Component.
 * Renders Today's gas day scheduled quantities and revisions across cycles.
 */

import * as d3 from 'd3';

/**
 * Render the cycle revisions chart.
 *
 * @param {HTMLElement} container - container element
 * @param {Array<{cycle: string, value: number}>} cycleData - list of cycle items sorted chronologically
 * @param {{platformNote?: string}} [opts] - optional rendering options
 */
export function renderCycleRevisions(container, cycleData, opts = {}) {
  container.innerHTML = '';

  const header = document.createElement('h3');
  header.className = 'revisions-title';
  header.innerText = 'Intraday Cycle Revisions';
  container.appendChild(header);

  const sub = document.createElement('p');
  sub.className = 'revisions-subtitle';
  sub.innerText = "Today's gas day flow adjustments across publication windows";
  container.appendChild(sub);

  // Platform limitation note (optional): some EBB platforms only publish a
  // subset of cycles as CSV — render what exists instead of implying 5 slots.
  if (opts.platformNote) {
    const note = document.createElement('p');
    note.className = 'revisions-platform-note';
    note.innerText = opts.platformNote;
    container.appendChild(note);
  }

  const chartDiv = document.createElement('div');
  chartDiv.className = 'revisions-chart-container';
  container.appendChild(chartDiv);

  if (!cycleData || cycleData.length === 0) {
    chartDiv.innerHTML = '<div class="panel-error">No cycle revision data for today.</div>';
    return;
  }

  // Margins & Dimensions
  const margin = { top: 12, right: 90, bottom: 8, left: 75 };
  const rowHeight = 32;
  const height = cycleData.length * rowHeight + margin.top + margin.bottom;
  const width = Math.max((chartDiv.getBoundingClientRect().width || 280), 200);

  const svg = d3.select(chartDiv)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;

  // Scales
  const y = d3.scaleBand()
    .domain(cycleData.map(d => d.cycle))
    .range([0, chartHeight])
    .padding(0.35);

  const maxVal = d3.max(cycleData, d => d.value) || 0;
  const xMax = Math.max(maxVal * 1.15, 100); // at least 100 MMcf/d range

  const x = d3.scaleLinear()
    .domain([0, xMax])
    .range([0, chartWidth]);

  // Render bars
  const latestIndex = cycleData.length - 1;

  g.selectAll('.rev-bar')
    .data(cycleData)
    .enter().append('rect')
    .attr('class', (d, i) => i === latestIndex ? 'rev-bar rev-bar--latest' : 'rev-bar')
    .attr('x', 0)
    .attr('y', d => y(d.cycle))
    .attr('width', d => Math.max(x(d.value), 2))
    .attr('height', y.bandwidth())
    .attr('rx', 2)
    .style('fill', (d, i) => i === latestIndex ? 'var(--blue-flame)' : 'none')
    .style('stroke', 'var(--blue-flame)')
    .style('stroke-width', '1.5px')
    .style('fill-opacity', 0.85);

  // Cycle labels
  g.selectAll('.rev-label')
    .data(cycleData)
    .enter().append('text')
    .attr('class', 'rev-label')
    .attr('x', -8)
    .attr('y', d => y(d.cycle) + y.bandwidth() / 2 + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', '10px')
    .attr('font-family', 'var(--font-mono)')
    .style('font-feature-settings', '"tnum"')
    .style('fill', 'var(--chart-label)')
    .text(d => d.cycle);

  // Values + Deltas
  cycleData.forEach((d, i) => {
    const valText = `${d.value.toFixed(0)} MMcf`;
    let deltaText = '';
    let deltaClass = 'rev-delta--neutral';

    if (i > 0 && cycleData.length > 1) {
      const diff = d.value - cycleData[i - 1].value;
      if (diff === 0) {
        deltaText = '—';
        deltaClass = 'rev-delta--neutral';
      } else {
        const arrow = diff > 0 ? '▲' : '▼';
        deltaText = `${arrow} ${Math.abs(diff).toFixed(0)}`;
        deltaClass = diff > 0 ? 'rev-delta--up' : 'rev-delta--down';
      }
    }

    const textNode = g.append('text')
      .attr('x', Math.max(x(d.value), 2) + 6)
      .attr('y', y(d.cycle) + y.bandwidth() / 2 + 4)
      .attr('font-size', '10px')
      .attr('font-family', 'var(--font-mono)')
      .style('font-feature-settings', '"tnum"')
      .style('letter-spacing', 'normal')
      .style('fill', i === latestIndex ? '#fff' : 'rgba(255, 255, 255, 0.7)');

    // Value segment
    textNode.append('tspan')
      .style('font-weight', i === latestIndex ? 'bold' : 'normal')
      .text(valText);

    // Delta segment
    if (deltaText) {
      textNode.append('tspan')
        .attr('dx', '8px') // Spacing between value and delta
        .attr('class', `rev-delta ${deltaClass}`)
        .attr('font-size', '9px')
        .style('font-weight', 'normal')
        .text(deltaText);
    }
  });

  // Small bottom scale line
  g.append('line')
    .attr('x1', 0)
    .attr('x2', chartWidth)
    .attr('y1', chartHeight)
    .attr('y2', chartHeight)
    .attr('stroke', 'rgba(255, 255, 255, 0.1)');
}
