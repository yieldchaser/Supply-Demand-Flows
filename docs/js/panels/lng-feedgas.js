/**
 * LNG Feedgas Panel — Scheduled Quantities and Utilization at LNG Export Terminals.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { LNG_NAMEPLATE_MMCFD, LNG_METERS, dth_to_mmcf, get_utilization_level, calculate_wow_delta, should_show_envelope } from '../util/lng-metrics.js';
import { renderCycleRevisions } from './lng-cycle-revisions.js';

/**
 * Render the LNG feedgas panel.
 *
 * @param {HTMLElement} panelEl - the element to render the panel inside
 * @param {Object} bundle - the dashboard data bundle
 * @param {string} [terminalId='freeport_lng'] - the active terminal ID
 */
export function renderLngFeedgasPanel(panelEl, bundle, terminalId = 'freeport_lng') {
  panelEl.innerHTML = '';

  // 1. Render Terminal Chips
  const headerSection = document.createElement('div');
  headerSection.className = 'lng-observatory-header';
  
  const chipsContainer = document.createElement('div');
  chipsContainer.className = 'terminal-chips';
  
  const terminals = [
    { id: 'freeport_lng', label: 'Freeport', active: true, status: 'active' },
    { id: 'golden_pass', label: 'Golden Pass · W2', active: false, status: 'w2' },
    { id: 'cameron_lng', label: 'Cameron · W2', active: false, status: 'w2' },
    { id: 'sabine_pass', label: 'Sabine · W2', active: false, status: 'w2' },
    { id: 'plaquemines', label: 'Plaquemines · later', active: false, status: 'later' },
  ];

  terminals.forEach((term) => {
    const button = document.createElement('button');
    button.className = `chip ${term.id === terminalId ? 'chip--active' : ''} ${term.status !== 'active' ? 'chip--disabled' : ''}`;
    button.innerText = `${term.id === terminalId || term.status === 'active' ? '●' : '○'} ${term.label}`;
    
    if (term.status !== 'active') {
      button.title = term.status === 'w2' ? 'Coming in W2' : 'Coming later';
    } else {
      button.addEventListener('click', () => {
        renderLngFeedgasPanel(panelEl, bundle, term.id);
      });
    }
    chipsContainer.appendChild(button);
  });
  
  headerSection.appendChild(chipsContainer);
  panelEl.appendChild(headerSection);

  // 2. Fetch Terminal Config
  const meterConfig = LNG_METERS[terminalId];
  if (!meterConfig) {
    const errorEl = document.createElement('div');
    errorEl.className = 'panel-error';
    errorEl.innerText = `Configuration for terminal ID '${terminalId}' is missing in LNG_METERS.`;
    panelEl.appendChild(errorEl);
    return;
  }

  const { pipeline, loc_id, loc_name } = meterConfig;
  const source = bundle.sources?.[pipeline];
  
  if (!source || !source.data || source.data.length === 0) {
    const errorEl = document.createElement('div');
    errorEl.className = 'panel-error';
    errorEl.innerText = `Awaiting first scrape for ${loc_name} on ${pipeline}.`;
    panelEl.appendChild(errorEl);
    return;
  }

  // 3. Render Panel Chrome
  const bodyEl = document.createElement('div');
  bodyEl.className = 'lng-panel-body-wrapper';
  panelEl.appendChild(bodyEl);

  const nameplate = LNG_NAMEPLATE_MMCFD[terminalId] || 1000;
  
  const { chartEl, sidebarEl } = renderPanelChrome(bodyEl, {
    title: `${terminalId === 'freeport_lng' ? 'Freeport LNG' : loc_name} · Feed Gas Deliveries`,
    subtitle: `${loc_name} interconnect · ${pipeline.toUpperCase().replace('_', ' ')} Pipeline`,
    sourceKey: pipeline,
    latestPeriod: source.latest_period,
  });

  // 4. Parse & Transform Curated Rows
  // Filter for TSQ series matching this location
  const prefix = `${pipeline}_sq_${loc_id}_`;
  const tsqRows = source.data.filter((r) => r.series_id.startsWith(prefix));

  if (tsqRows.length === 0) {
    chartEl.innerHTML = `<div class="panel-error">No feedgas flow data found for location ${loc_id}.</div>`;
    return;
  }

  // Group TSQ rows by period (gas day)
  const rowsByDate = {}; // date_str -> { timely, evening, id1, id2, id3, latestVal, latestCycle }
  const cyclePriority = { timely: 1, evening: 2, id1: 3, id2: 4, id3: 5 };

  tsqRows.forEach((row) => {
    const dateStr = row.period;
    const cycle = row.series_id.substring(prefix.length).toLowerCase();
    const mmcfVal = dth_to_mmcf(Number(row.value));

    if (!rowsByDate[dateStr]) {
      rowsByDate[dateStr] = {};
    }
    rowsByDate[dateStr][cycle] = mmcfVal;
  });

  // For each date, compute the latest available cycle value
  const dailySeries = []; // Array of { date: Date, dateStr: string, value: number, cycle: string, gasYear: number, dayIndex: number }
  
  Object.keys(rowsByDate).forEach((dateStr) => {
    const dateObj = new Date(dateStr);
    const dayCycles = rowsByDate[dateStr];
    
    let bestCycle = null;
    let bestPriority = -1;
    
    Object.keys(dayCycles).forEach((cy) => {
      const prio = cyclePriority[cy] || 0;
      if (prio > bestPriority) {
        bestPriority = prio;
        bestCycle = cy;
      }
    });

    if (bestCycle) {
      const val = dayCycles[bestCycle];
      const { gasYear, dayIndex } = getGasYearInfo(dateObj);
      
      dailySeries.push({
        date: dateObj,
        dateStr,
        value: val,
        cycle: bestCycle,
        gasYear,
        dayIndex,
      });
    }
  });

  // Sort daily series chronologically
  dailySeries.sort((a, b) => a.date - b.date);

  if (dailySeries.length === 0) {
    chartEl.innerHTML = '<div class="panel-error">No valid daily scheduled quantities could be parsed.</div>';
    return;
  }

  const latestData = dailySeries[dailySeries.length - 1];
  const totalDays = dailySeries.length;

  // Determine the most recent cycle in the entire dataset
  const latestCycleCode = latestData.cycle;

  // Latest-cycle dotted line: filter dataset for the last 14 gas days and take ONLY that cycle
  const dottedSeries = [];
  const last14Days = dailySeries.slice(-14);
  last14Days.forEach((d) => {
    const dateStr = d.dateStr;
    const dayCycles = rowsByDate[dateStr];
    if (dayCycles && dayCycles[latestCycleCode] !== undefined) {
      dottedSeries.push({
        dayIndex: d.dayIndex,
        value: dayCycles[latestCycleCode],
      });
    }
  });

  // 5. Draw Hero Chart
  drawHeroChart(chartEl, dailySeries, dottedSeries, nameplate, totalDays);

  // 6. Render KPI Strip
  renderKpiStrip(sidebarEl, dailySeries, rowsByDate, nameplate, latestData, latestCycleCode);

  // 7. Render Intraday Cycle Revisions (bottom of sidebar)
  const revisionsContainer = document.createElement('div');
  revisionsContainer.className = 'cycle-revisions-panel';
  sidebarEl.appendChild(revisionsContainer);

  const todayCyclesMap = rowsByDate[latestData.dateStr] || {};
  const todayCyclesData = Object.keys(todayCyclesMap)
    .map((cy) => ({
      cycle: cy.toUpperCase(),
      value: todayCyclesMap[cy],
      priority: cyclePriority[cy] || 0,
    }))
    .sort((a, b) => a.priority - b.priority);

  renderCycleRevisions(revisionsContainer, todayCyclesData);
  
  // Render Footnote
  const footerContainer = document.createElement('div');
  footerContainer.className = 'lng-panel-footnote';
  footerContainer.innerHTML = `
    <p><strong>Source:</strong> Boardwalk Pipelines public OAC (Gulf South Pipeline, tspId=1)</p>
    <p><strong>Method:</strong> TSQ at ${loc_name} (loc ${loc_id}, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d</p>
    <p><strong>Last updated:</strong> ${source.latest_period || 'N/A'} · <em>Note: Freeport also fed by TETCO (KM) — full coverage in a later wave.</em></p>
  `;
  panelEl.appendChild(footerContainer);
}

/**
 * Determine the gas year and day index (0-365) within that gas year (Feb 1st to Jan 31st).
 *
 * @param {Date} date
 * @returns {{gasYear: number, dayIndex: number}} gas year metadata
 */
function getGasYearInfo(date) {
  const y = date.getFullYear();
  const m = date.getMonth(); // 0 = Jan, 1 = Feb, ...
  let gasYear;
  let start;
  if (m >= 1) { // Feb or later
    gasYear = y;
    start = new Date(y, 1, 1);
  } else { // Jan
    gasYear = y - 1;
    start = new Date(y - 1, 1, 1);
  }
  const dayIndex = Math.floor((date.getTime() - start.getTime()) / 86400000);
  return { gasYear, dayIndex };
}

/**
 * Draw the Hero Chart with D3.
 */
function drawHeroChart(container, dailySeries, dottedSeries, nameplate, totalDays) {
  container.innerHTML = '';
  
  const rect = container.getBoundingClientRect();
  const margin = { top: 24, right: 32, bottom: 40, left: 56 };
  const totalH = 340;
  const width = Math.max((rect.width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // X scale: Gas Year Day Index (Feb 1st to Jan 31st: 0 to 365)
  const x = d3.scaleLinear().domain([0, 365]).range([0, width]);

  // Group by Gas Year
  const seriesByGasYear = d3.group(dailySeries, (d) => d.gasYear);
  const currentGasYear = d3.max(dailySeries, (d) => d.gasYear);
  
  // Y scale: determined by capacity, values, and nameplate
  const maxVal = d3.max(dailySeries, (d) => d.value) || 0;
  const yMax = Math.max(maxVal, nameplate) * 1.15;
  const y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);

  // Y Gridlines (every 500 MMcf/d)
  const yTicks = [];
  for (let val = 500; val < yMax; val += 500) {
    yTicks.push(val);
  }
  if (yTicks.length === 0 || yTicks[0] !== 0) {
    yTicks.unshift(0);
  }

  g.selectAll('.gridline')
    .data(yTicks).enter()
    .append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // Nameplate Reference Line
  g.append('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', y(nameplate)).attr('y2', y(nameplate))
    .attr('stroke', '#4b5563')
    .attr('stroke-width', 1.5)
    .attr('stroke-dasharray', '4,4');

  g.append('text')
    .attr('x', width - 8)
    .attr('y', y(nameplate) - 6)
    .attr('text-anchor', 'end')
    .attr('font-size', '10px')
    .attr('font-family', 'var(--font-mono)')
    .style('fill', 'rgba(255,255,255,0.45)')
    .text(`Nameplate ${nameplate.toLocaleString()} MMcf/d`);

  // Draw lines based on history length (Graceful Degradation gate)
  if (!should_show_envelope(totalDays)) {
    // Graceful degradation: Draw only current gas year + nameplate ref
    const currentYearRows = seriesByGasYear.get(currentGasYear) || [];
    
    if (currentYearRows.length > 0) {
      const lineGen = d3.line()
        .x((d) => x(d.dayIndex))
        .y((d) => y(d.value))
        .curve(d3.curveLinear); // linear since points are sparse

      g.append('path').datum(currentYearRows).attr('d', lineGen)
        .attr('fill', 'none')
        .style('stroke', 'var(--blue-flame)')
        .attr('stroke-width', 2.5)
        .attr('stroke-linecap', 'round')
        .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.4))');

      // Points
      g.selectAll('.dot')
        .data(currentYearRows).enter()
        .append('circle')
        .attr('cx', (d) => x(d.dayIndex))
        .attr('cy', (d) => y(d.value))
        .attr('r', 4)
        .style('fill', 'var(--blue-flame)')
        .style('stroke', '#0c0f16')
        .style('stroke-width', '1.5px');
    }

    // Add info note to chart subtitle space
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', 12)
      .attr('font-size', '10px')
      .style('fill', 'rgba(255,255,255,0.4)')
      .style('font-style', 'italic')
      .text('Historical envelope builds as data accumulates.');

  } else {
    // Full visual capability (>=30 days of data)
    const priorGasYear = currentGasYear - 1;
    const prior2GasYear = currentGasYear - 2;

    // Prior Year Line (muted gray)
    const priorYearRows = seriesByGasYear.get(priorGasYear) || [];
    if (priorYearRows.length > 0) {
      const priorLine = d3.line()
        .x((d) => x(d.dayIndex))
        .y((d) => y(d.value))
        .curve(d3.curveMonotoneX);

      g.append('path').datum(priorYearRows).attr('d', priorLine)
        .attr('fill', 'none')
        .style('stroke', '#6b7280')
        .attr('stroke-width', 1.5)
        .attr('stroke-opacity', 0.5);
    }

    // Envelope calculations if 2 prior years exist
    const has2PriorYears = seriesByGasYear.has(priorGasYear) && seriesByGasYear.has(prior2GasYear);
    if (has2PriorYears) {
      const priorRows1 = seriesByGasYear.get(priorGasYear);
      const priorRows2 = seriesByGasYear.get(prior2GasYear);

      // Compute envelope per dayIndex
      const envelopeData = [];
      for (let i = 0; i <= 365; i++) {
        const val1 = priorRows1.find(r => r.dayIndex === i)?.value;
        const val2 = priorRows2.find(r => r.dayIndex === i)?.value;
        const valid = [];
        if (val1 !== undefined) valid.push(val1);
        if (val2 !== undefined) valid.push(val2);

        if (valid.length > 0) {
          envelopeData.push({
            dayIndex: i,
            min: d3.min(valid),
            max: d3.max(valid),
          });
        }
      }

      if (envelopeData.length > 1) {
        // Red 2y max envelope line
        const maxLine = d3.line()
          .x(d => x(d.dayIndex))
          .y(d => y(d.max))
          .curve(d3.curveMonotoneX);

        g.append('path').datum(envelopeData).attr('d', maxLine)
          .attr('fill', 'none')
          .style('stroke', '#ef4444')
          .attr('stroke-width', 1)
          .attr('stroke-opacity', 0.6)
          .attr('stroke-dasharray', '4,2');

        // Cyan 2y min envelope line
        const minLine = d3.line()
          .x(d => x(d.dayIndex))
          .y(d => y(d.min))
          .curve(d3.curveMonotoneX);

        g.append('path').datum(envelopeData).attr('d', minLine)
          .attr('fill', 'none')
          .style('stroke', '#22d3ee')
          .attr('stroke-width', 1)
          .attr('stroke-opacity', 0.6)
          .attr('stroke-dasharray', '4,2');
      }
    }

    // Current Year Line (solid blue flame)
    const currentYearRows = seriesByGasYear.get(currentGasYear) || [];
    if (currentYearRows.length > 0) {
      const curLine = d3.line()
        .x((d) => x(d.dayIndex))
        .y((d) => y(d.value))
        .curve(d3.curveMonotoneX);

      g.append('path').datum(currentYearRows).attr('d', curLine)
        .attr('fill', 'none')
        .style('stroke', 'var(--blue-flame)')
        .attr('stroke-width', 2.5)
        .attr('stroke-linecap', 'round')
        .style('filter', 'drop-shadow(0 0 6px rgba(125, 211, 252, 0.4))');
    }
  }

  // Dotted Latest-Cycle Line (if we have dotted rows)
  if (dottedSeries.length > 0) {
    const dottedLine = d3.line()
      .x((d) => x(d.dayIndex))
      .y((d) => y(d.value));

    g.append('path').datum(dottedSeries).attr('d', dottedLine)
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', '2,3');
  }

  // X Axis Ticks (Responsive labels)
  const isMobile = width < 500;
  const start = new Date(currentGasYear, 1, 1);
  const months = ['Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan'];
  
  months.forEach((m, offset) => {
    if (isMobile && offset % 2 !== 0) return; // skip every second label on mobile
    const d = new Date(currentGasYear + (1 + offset >= 12 ? 1 : 0), (1 + offset) % 12, 1);
    const dayIdx = Math.floor((d.getTime() - start.getTime()) / 86400000);

    g.append('text')
      .attr('x', x(dayIdx))
      .attr('y', height + 22)
      .attr('text-anchor', 'middle')
      .attr('font-size', '10px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(m);
  });

  // Y Axis Ticks
  g.selectAll('.y-tick').data(yTicks).enter()
    .append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', '10px')
    .attr('font-family', 'var(--font-mono)')
    .style('fill', 'var(--chart-label)')
    .text((d) => `${d}`);
}

/**
 * Render the KPI Cards Strip.
 */
function renderKpiStrip(sidebarEl, dailySeries, rowsByDate, nameplate, latestData, latestCycleCode) {
  // 1. LATEST Card
  const latestValue = latestData.value;
  const latestPeriodLabel = latestData.date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  const latestKpi = kpiCardHtml({
    label: 'Latest Scheduled',
    value: `${latestValue.toFixed(0)} MMcf/d`,
    helpText: `${latestCycleCode.toUpperCase()} cycle · ${latestPeriodLabel}`,
  });

  // 2. UTILIZATION Card with Inline Progress Bar
  const utilPct = (latestValue / nameplate) * 100;
  const utilLevel = get_utilization_level(utilPct);
  
  // Capacity creep check (exceeds >5%)
  const showTooltip = utilPct > 105;
  const tooltipHtml = showTooltip 
    ? `<span class="capacity-creep-tooltip" title="running above stated nameplate — capacity creep or measurement basis">ⓘ</span>`
    : '';

  const utilizationKpi = `
    <div class="kpi-card">
      <div class="kpi-card__label">Utilization ${tooltipHtml}</div>
      <div class="kpi-card__value num">${utilPct.toFixed(1)}%</div>
      <div class="kpi-card__delta kpi-card__delta--${utilLevel.colorClass}">${utilLevel.label.toUpperCase()}</div>
      <div class="kpi-card__help">
        <div class="utilization-bar-bg">
          <div class="utilization-bar-fill ${utilLevel.colorClass}" style="width: ${Math.min(utilPct, 100)}%"></div>
        </div>
        <div class="utilization-context">
          ${utilPct < 40 ? 'Low utilization in shoulder season may indicate train maintenance.' : `Stated nameplate capacity: ${nameplate.toLocaleString()} MMcf/d.`}
        </div>
      </div>
    </div>
  `;

  // 3. WoW Delta Card
  const { valueText: wowValueText, deltaProps: wowDeltaProps, helpText: wowHelpText } = calculate_wow_delta(
    latestValue,
    latestCycleCode,
    latestData.date,
    rowsByDate
  );

  const wowKpi = kpiCardHtml({
    label: 'WoW Change',
    value: wowValueText,
    delta: wowDeltaProps,
    helpText: wowHelpText,
  });

  // 4. vs PY (Prior Year) Card
  let pyValueText = '—';
  let pyDeltaProps = null;
  let pyHelpText = 'No prior-year data';

  const pyTargetDate = new Date(latestData.date);
  pyTargetDate.setFullYear(pyTargetDate.getFullYear() - 1);
  const pyTargetDateStr = pyTargetDate.toISOString().split('T')[0];
  const pyTargetCycles = rowsByDate[pyTargetDateStr];

  if (pyTargetCycles && pyTargetCycles[latestCycleCode] !== undefined) {
    const valPy = pyTargetCycles[latestCycleCode];
    const pyDelta = latestValue - valPy;
    const pyPct = valPy > 0 ? (pyDelta / valPy) * 100 : 0;
    
    pyValueText = `${pyDelta >= 0 ? '+' : ''}${pyDelta.toFixed(0)} MMcf/d`;
    pyDeltaProps = {
      value: `${pyPct >= 0 ? '+' : ''}${pyPct.toFixed(1)}%`,
      kind: pyDelta > 0 ? 'bullish' : pyDelta < 0 ? 'bearish' : 'neutral',
    };
    pyHelpText = `vs same day ${pyTargetDate.getFullYear()}`;
  }

  const pyKpi = kpiCardHtml({
    label: 'vs Prior Year',
    value: pyValueText,
    delta: pyDeltaProps,
    helpText: pyHelpText,
  });

  // Render cards
  sidebarEl.innerHTML = [latestKpi, utilizationKpi, wowKpi, pyKpi].join('');
}
