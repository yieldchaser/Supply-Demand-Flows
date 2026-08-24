/**
 * LNG Feedgas Panel — Scheduled Quantities and Utilization at LNG Export Terminals.
 *
 * Driven entirely by the terminal registry (docs/js/util/lng-terminals.js):
 * nine chips render from LNG_FLEET_ORDER; metadata is never hardcoded here.
 * Direct-SQ terminals plot published TSQ; oac-proxy terminals (Cheniere)
 * plot INFERRED capacity consumption (design − OAC) and are labelled as a
 * proxy everywhere the number appears.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import {
  dth_to_mmcf,
  get_utilization_level,
  calculate_wow_delta,
  should_show_envelope,
} from '../util/lng-metrics.js';
import {
  LNG_TERMINALS,
  LNG_FLEET_ORDER,
  DEFAULT_TERMINAL_ID,
  terminalSeriesPrefixes,
  terminalCycleInfo,
} from '../util/lng-terminals.js';
import {
  buildSqCycleMaps,
  buildProxyImpliedByDate,
  buildDailyFromCycles,
} from './lng-fleet-overview.js';
import { renderCycleRevisions } from './lng-cycle-revisions.js';

const CYCLE_PRIORITY = { timely: 1, evening: 2, id1: 3, id2: 4, id3: 5 };

/** Stacked-area fill colors per feed index (base -> top). */
const FEED_COLORS = [
  { area: 'rgba(14, 165, 233, 0.30)', line: '#38bdf8' },  // sky (Gulf South)
  { area: 'rgba(52, 211, 153, 0.28)', line: '#34d399' },  // emerald (TETCO)
  { area: 'rgba(251, 191, 36, 0.25)', line: '#fbbf24' },  // amber (future feeds)
];

/**
 * Build a date -> {cycle -> MMcf} map for one feed of a multi-feed terminal.
 * `seriesStem` is the registry's series stem ("{prefix}_sq_{loc}_{flow}").
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {string} seriesStem — e.g. "gulf_south_sq_24329_d"
 * @returns {Object<string, Object<string, number>>}
 */
function buildFeedCycleMaps(rows, seriesStem) {
  const prefix = `${seriesStem.toLowerCase()}_`;
  const byDate = {};
  rows.forEach((r) => {
    const sid = r.series_id.toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const cycle = sid.slice(prefix.length).toLowerCase();
    if (!byDate[r.period]) byDate[r.period] = {};
    byDate[r.period][cycle] = dth_to_mmcf(Number(r.value));
  });
  return byDate;
}

/**
 * Build a combined daily series + per-feed daily maps for multi-feed terminals.
 * Combined = sum of the highest-priority cycle available per feed that day.
 *
 * @param {Object} bundle
 * @param {LngTerminal} t
 * @returns {{
 *   dailySeries: Array<{dateStr: string, date: Date, value: number}>,
 *   rowsByDate: Object<string, Object<string, number>>,  // date -> {feedLabel -> MMcf}
 *   feedLabels: string[],
 *   latestSplit: Object<string, number>|null,
 * }}
 */
export function buildMultiFeedData(bundle, t) {
  const feedLabels = [];
  const feedMaps = [];
  for (const feed of t.feeds || []) {
    const src = bundle.sources?.[feed.source];
    if (!src || !src.data) continue;
    const map = buildFeedCycleMaps(src.data, feed.series);
    if (Object.keys(map).length === 0) continue;
    feedLabels.push(feed.label);
    feedMaps.push({ label: feed.label, map });
  }

  /** @type {Object<string, Object<string, number>>} */
  const rowsByDate = {};
  const allDates = new Set();
  feedMaps.forEach((fm) => Object.keys(fm.map).forEach((d) => allDates.add(d)));

  allDates.forEach((dateStr) => {
    rowsByDate[dateStr] = {};
    feedMaps.forEach((fm) => {
      const cycles = fm.map[dateStr];
      if (!cycles) return;
      let best = null;
      let bestPrio = -1;
      Object.keys(cycles).forEach((cy) => {
        const prio = CYCLE_PRIORITY[cy] || 0;
        if (prio > bestPrio) {
          bestPrio = prio;
          best = cy;
        }
      });
      if (best !== null) {
        // Zeros are data — sum them in.
        rowsByDate[dateStr][fm.label] = cycles[best];
      }
    });
  });

  const dailySeries = [];
  Object.keys(rowsByDate).forEach((dateStr) => {
    const feeds = rowsByDate[dateStr];
    if (Object.keys(feeds).length === 0) return;
    let total = 0;
    Object.values(feeds).forEach((v) => {
      total += v;
    });
    dailySeries.push({ dateStr, date: new Date(dateStr), value: total });
  });
  dailySeries.sort((a, b) => a.date.getTime() - b.date.getTime());

  // Latest per-feed split (for the breakdown line).
  let latestSplit = null;
  if (dailySeries.length > 0) {
    latestSplit = {};
    const lastFeeds = rowsByDate[dailySeries[dailySeries.length - 1].dateStr];
    Object.keys(lastFeeds).forEach((label) => {
      latestSplit[label] = lastFeeds[label];
    });
  }

  return { dailySeries, rowsByDate, feedLabels, latestSplit };
}


/**
 * Render the LNG feedgas hero panel.
 *
 * @param {HTMLElement} panelEl - the element to render the panel inside
 * @param {Object} bundle - the dashboard data bundle
 * @param {string} [terminalId] - active terminal ID (defaults to registry default)
 * @param {{onSelect?: (id: string) => void}} [opts] - callbacks (fleet wiring)
 */
export function renderLngFeedgasPanel(panelEl, bundle, terminalId = DEFAULT_TERMINAL_ID, opts = {}) {
  panelEl.innerHTML = '';
  const t = LNG_TERMINALS[terminalId] || LNG_TERMINALS[DEFAULT_TERMINAL_ID];
  const effectiveId = t.id;

  // 1. Terminal Chips (all nine, straight from the registry order)
  const headerSection = document.createElement('div');
  headerSection.className = 'lng-observatory-header';

  const chipsContainer = document.createElement('div');
  chipsContainer.className = 'terminal-chips';

  LNG_FLEET_ORDER.forEach((id) => {
    const chipTerm = LNG_TERMINALS[id];
    if (!chipTerm) return;
    const offline = chipTerm.operational === false;
    const button = document.createElement('button');
    button.className =
      'chip' +
      (id === effectiveId ? ' chip--active' : '') +
      (offline ? ' chip--disabled' : '');
    button.innerText = `${id === effectiveId ? '●' : offline ? '○' : '○'} ${chipTerm.display}`;
    if (offline) {
      button.title = chipTerm.statusText || 'Not operational';
      button.disabled = true;
    } else {
      button.addEventListener('click', () => {
        if (typeof opts.onSelect === 'function') opts.onSelect(id);
        else renderLngFeedgasPanel(panelEl, bundle, id, opts);
      });
    }
    chipsContainer.appendChild(button);
  });

  headerSection.appendChild(chipsContainer);
  panelEl.appendChild(headerSection);

  // 2. Resolve bundle source for this terminal
  const source = bundle.sources?.[t.source];

  if (!source || !source.data || source.data.length === 0) {
    const bodyEl = document.createElement('div');
    bodyEl.className = 'lng-panel-body-wrapper';
    panelEl.appendChild(bodyEl);
    const chrome = renderPanelChrome(bodyEl, {
      title: `${t.display} · Feed Gas Deliveries`,
      subtitle: `${t.locName ?? ''} · ${t.platformLabel ?? ''}`,
      sourceKey: t.source,
      latestPeriod: null,
    });
    chrome.chartEl.innerHTML =
      '<div class="panel-error">Awaiting first scrape for this terminal.</div>';
    return;
  }

  // 3. Panel chrome
  const bodyEl = document.createElement('div');
  bodyEl.className = 'lng-panel-body-wrapper';
  panelEl.appendChild(bodyEl);

  const nameplate = t.nameplate;

  // Multi-feed terminals (e.g. Freeport: Gulf South + TETCO) extract per-feed
  // cycle maps and combine them; everything downstream renders stacked.
  const isMultiFeed = Array.isArray(t.feeds) && t.feeds.length > 0;
  let multi = null;
  let source = bundle.sources?.[t.source];
  if (isMultiFeed) {
    multi = buildMultiFeedData(bundle, t);
    if (multi.feedLabels.length > 0) {
      source = bundle.sources?.[t.feeds[0].source];
    }
  }

  if (
    (!isMultiFeed || !multi || multi.feedLabels.length === 0) &&
    (!source || !source.data || source.data.length === 0)
  ) {
    const bodyEl2 = document.createElement('div');
    bodyEl2.className = 'lng-panel-body-wrapper';
    panelEl.appendChild(bodyEl2);
    const chrome = renderPanelChrome(bodyEl2, {
      title: `${t.display} · Feed Gas Deliveries`,
      subtitle: `${t.locName ?? ''} · ${t.platformLabel ?? ''}`,
      sourceKey: t.source,
      latestPeriod: null,
    });
    chrome.chartEl.innerHTML =
      '<div class="panel-error">Awaiting first scrape for this terminal.</div>';
    return;
  }

  const { chartEl, sidebarEl } = renderPanelChrome(bodyEl, {
    title: `${t.display} · Feed Gas Deliveries`,
    subtitle: `${t.locName} · ${t.platformLabel}`,
    sourceKey: t.source,
    latestPeriod: source ? source.latest_period : null,
  });

  const isProxy = t.signal === 'oac-proxy';

  // 4. Parse & transform curated rows (registry-driven, proxy-aware)
  let rowsByDate;
  if (isMultiFeed) {
    rowsByDate = multi.rowsByDate;
  } else if (isProxy) {
    rowsByDate = buildProxyImpliedByDate(source.data, t);
  } else {
    rowsByDate = buildSqCycleMaps(source.data, t);
  }

  if (!isMultiFeed && Object.keys(rowsByDate).length === 0) {
    chartEl.innerHTML = `<div class="panel-error">No flow data found for location ${t.loc}.</div>`;
    return;
  }

  let dailySeries;
  if (isMultiFeed) {
    dailySeries = multi.dailySeries.map((d) => ({
      dateStr: d.dateStr,
      date: d.date,
      value: d.value,
      cycle: 'combined',
    }));
  } else {
    dailySeries = buildDailyFromCycles(rowsByDate);
  }

  if (dailySeries.length === 0) {
    chartEl.innerHTML = '<div class="panel-error">No valid daily values could be parsed.</div>';
    return;
  }

  const latestData = dailySeries[dailySeries.length - 1];
  const totalDays = dailySeries.length;
  const latestCycleCode = latestData.cycle;

  // Latest-cycle dotted line: last 14 gas days, only that cycle.
  const dottedSeries = [];
  dailySeries.slice(-14).forEach((d) => {
    if (isMultiFeed) {
      dottedSeries.push({ dayIndex: getGasYearInfo(d.date).dayIndex, value: d.value });
      return;
    }
    const dayCycles = rowsByDate[d.dateStr];
    if (dayCycles && dayCycles[latestCycleCode] !== undefined) {
      dottedSeries.push({ dayIndex: getGasYearInfo(d.date).dayIndex, value: dayCycles[latestCycleCode] });
    }
  });

  // 5. Hero chart (stacked areas for multi-feed)
  drawHeroChart(chartEl, dailySeries, dottedSeries, nameplate, totalDays, isMultiFeed ? multi : null);

  // 6. KPI strip + per-feed breakdown line
  renderKpiStrip(sidebarEl, dailySeries, rowsByDate, nameplate, latestData, latestCycleCode, isProxy);

  if (isMultiFeed && multi.latestSplit) {
    const parts = multi.feedLabels
      .filter((label) => multi.latestSplit[label] !== undefined)
      .map((label) => `${label} ${multi.latestSplit[label].toFixed(0)}`);
    if (parts.length > 0) {
      const breakdown = document.createElement('div');
      breakdown.className = 'feed-breakdown';
      breakdown.innerHTML =
        `<span class="feed-breakdown__line">${parts.join(' · ')} ` +
        `<span class="feed-breakdown__eq">=</span> ` +
        `<strong>${latestData.value.toFixed(0)}</strong> MMcf/d</span>` +
        `<span class="feed-breakdown__note">per-pipe split at the latest common gas day</span>`;
      sidebarEl.appendChild(breakdown);
    }
  }

  // 7. Intraday cycle revisions — single-source only (multi-feed platforms
  // publish on different cycles, so per-cycle revisions are not comparable).
  if (!isMultiFeed) {
    const revisionsContainer = document.createElement('div');
    revisionsContainer.className = 'cycle-revisions-panel';
    sidebarEl.appendChild(revisionsContainer);

    const todayCyclesMap = rowsByDate[latestData.dateStr] || {};
    const todayCyclesData = Object.keys(todayCyclesMap)
      .map((cy) => ({
        cycle: cy.toUpperCase(),
        value: todayCyclesMap[cy],
        priority: CYCLE_PRIORITY[cy] || 0,
      }))
      .sort((a, b) => a.priority - b.priority);

    const cycleInfo = terminalCycleInfo(t);
    renderCycleRevisions(revisionsContainer, todayCyclesData, { platformNote: cycleInfo.note });
  }

  // 8. Footnote — from the registry, per terminal
  const footerContainer = document.createElement('div');
  footerContainer.className = 'lng-panel-footnote';
  const proxyLine = isProxy
    ? `<p><strong>⚠ Proxy signal:</strong> Cheniere does not publish Scheduled Quantities. Value = Design Capacity − Operationally Available (inferred consumption), not measured feedgas.</p>`
    : '';
  const latestPeriodText = source && source.latest_period ? source.latest_period : latestData.dateStr;
  footerContainer.innerHTML = `
    <p><strong>Source:</strong> ${t.methodLine}</p>
    ${proxyLine}
    <p><strong>Last updated:</strong> ${latestPeriodText} · ${totalDays.toLocaleString()} gas days of history</p>
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
 *
 * When `multi` is provided (multi-feed terminal), renders one STACKED area
 * per feed (base = first feed) instead of a single line — the per-pipe
 * split is the signal. A dashed median-reference marker is drawn when the
 * current latest value deviates strongly from the trailing 90-day median
 * (guards against implying an outlier day is normal).
 */
function drawHeroChart(container, dailySeries, dottedSeries, nameplate, totalDays, multi = null) {
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

  // Assign gas year / day indices FIRST, then group by them.
  dailySeries.forEach((d) => {
    const info = getGasYearInfo(d.date);
    d.gasYear = info.gasYear;
    d.dayIndex = info.dayIndex;
  });

  // Group by Gas Year
  const seriesByGasYear = d3.group(dailySeries, (d) => d.gasYear);
  const currentGasYear = d3.max(dailySeries, (d) => d.gasYear);

  // Y scale: determined by capacity, values, and nameplate
  const maxVal = d3.max(dailySeries, (d) => d.value) || 0;
  const yMax = !should_show_envelope(totalDays)
    ? Math.max(nameplate * 1.1, maxVal * 1.25)
    : Math.max(maxVal, nameplate) * 1.15;
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
    .attr('y1', (dd) => y(dd)).attr('y2', (dd) => y(dd))
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
    .style('font-feature-settings', '"tnum"')
    .style('letter-spacing', '0.02em')
    .style('fill', 'rgba(255,255,255,0.45)')
    .text(`Nameplate ${nameplate.toLocaleString()} MMcf/d`);

  // Median reference marker — only when today deviates >25% from the
  // trailing-90-day median, so an unusual day is never read as normal.
  const recentValues = dailySeries.slice(-90).map((d) => d.value);
  if (recentValues.length >= 30) {
    const medianVal = d3.median(recentValues);
    const latest = dailySeries[dailySeries.length - 1].value;
    if (medianVal > 0 && Math.abs(latest - medianVal) / medianVal > 0.25) {
      g.append('line')
        .attr('x1', 0).attr('x2', width)
        .attr('y1', y(medianVal)).attr('y2', y(medianVal))
        .attr('stroke', 'rgba(148, 163, 184, 0.7)')
        .attr('stroke-width', 1)
        .attr('stroke-dasharray', '2,3');
      g.append('text')
        .attr('x', width - 8)
        .attr('y', y(medianVal) + 11)
        .attr('text-anchor', 'end')
        .attr('font-size', '9px')
        .attr('font-family', 'var(--font-mono)')
        .style('fill', 'rgba(148, 163, 184, 0.8)')
        .text(`90d median ${medianVal.toFixed(0)} · today ${((latest / medianVal - 1) * 100).toFixed(0)}%`);
    }
  }

  // Draw lines based on history length (Graceful Degradation gate)
  if (multi) {
    // ── MULTI-FEED: stacked areas, current gas year only. ──
    // One band per feed: feed i is drawn between the cumulative sum of
    // feeds [0..i-1] and [0..i]. The per-pipe split IS the signal.
    const currentYearRows = seriesByGasYear.get(currentGasYear) || [];
    if (currentYearRows.length > 0) {
      const labels = multi.feedLabels;
      const cum = new Map();
      labels.forEach((label, fi) => {
        const lower = new Map(cum);
        const upper = new Map();
        currentYearRows.forEach((row) => {
          const prev = cum.get(row.dayIndex) || 0;
          const feedVal =
            (multi.rowsByDate[row.dateStr] && multi.rowsByDate[row.dateStr][label]) || 0;
          upper.set(row.dayIndex, prev + feedVal);
        });
        const color = FEED_COLORS[fi % FEED_COLORS.length];
        const areaGen = d3
          .area()
          .x((d) => x(d))
          .y0((d) => y(lower.get(d) || 0))
          .y1((d) => y(upper.get(d) || 0))
          .curve(d3.curveMonotoneX);
        g.append('path')
          .datum([...upper.keys()].sort((a2, b2) => a2 - b2))
          .attr('d', areaGen)
          .attr('fill', color.area)
          .attr('stroke', color.line)
          .attr('stroke-width', 1.5)
          .attr('stroke-opacity', 0.9);
        upper.forEach((v, k) => cum.set(k, v));
        stackTop.set(fi, upper);
      });

      // Total line on top of the stack + latest callout.
      const totalLine = d3
        .line()
        .x((d) => x(d.dayIndex))
        .y((d) => y(d.value))
        .curve(d3.curveMonotoneX);
      g.append('path')
        .datum(currentYearRows)
        .attr('d', totalLine)
        .attr('fill', 'none')
        .style('stroke', 'var(--blue-flame)')
        .attr('stroke-width', 2)
        .attr('stroke-linecap', 'round');

      const latestPoint = currentYearRows[currentYearRows.length - 1];
      if (latestPoint) {
        g.append('circle')
          .attr('cx', x(latestPoint.dayIndex))
          .attr('cy', y(latestPoint.value))
          .attr('r', 6)
          .style('fill', 'var(--blue-flame)')
          .style('stroke', '#0c0f16')
          .style('stroke-width', '2px');

        const callout = g
          .append('g')
          .attr(
            'transform',
            `translate(${x(latestPoint.dayIndex) + 12}, ${y(latestPoint.value) - 10})`
          );
        callout
          .append('rect')
          .attr('x', -4)
          .attr('y', -11)
          .attr('width', 74)
          .attr('height', 15)
          .attr('rx', 3)
          .style('fill', 'rgba(15, 23, 42, 0.85)')
          .style('stroke', 'rgba(125, 211, 252, 0.3)')
          .style('stroke-width', '0.5px');
        callout
          .append('text')
          .attr('x', 2)
          .attr('y', 0)
          .attr('font-size', '8.5px')
          .attr('font-family', 'var(--font-mono)')
          .style('font-feature-settings', '"tnum"')
          .style('fill', '#fff')
          .style('font-weight', '600')
          .text(`${latestPoint.value.toFixed(0)} combined`);

        // Feed legend under the callout colors.
        const legend = svg
          .append('g')
          .attr('transform', `translate(${margin.left}, ${totalH - 6})`);
        labels.forEach((label, fi) => {
          const item = legend
            .append('g')
            .attr('transform', `translate(${fi * 110}, 0)`);
          item
            .append('rect')
            .attr('width', 8)
            .attr('height', 8)
            .attr('ry', 1)
            .style('fill', FEED_COLORS[fi % FEED_COLORS.length].area)
            .style('stroke', FEED_COLORS[fi % FEED_COLORS.length].line);
          item
            .append('text')
            .attr('x', 12)
            .attr('y', 7)
            .attr('font-size', '9px')
            .attr('font-family', 'var(--font-sans)')
            .style('fill', 'rgba(255,255,255,0.65)')
            .text(label);
        });
      }
    }
  } else if (!should_show_envelope(totalDays)) {
    // Graceful degradation: Draw only current gas year + nameplate ref
    const currentYearRows = seriesByGasYear.get(currentGasYear) || [];

    if (currentYearRows.length > 0) {
      // Area Fill
      const areaGen = d3.area()
        .x((d) => x(d.dayIndex))
        .y0(height)
        .y1((d) => y(d.value))
        .curve(d3.curveLinear);

      g.append('path')
        .datum(currentYearRows)
        .attr('d', areaGen)
        .attr('fill', 'rgba(14, 165, 233, 0.06)')
        .attr('stroke', 'none');

      // Line
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

      // Regular Points
      g.selectAll('.dot')
        .data(currentYearRows).enter()
        .append('circle')
        .attr('cx', (d) => x(d.dayIndex))
        .attr('cy', (d) => y(d.value))
        .attr('r', 4)
        .style('fill', 'var(--blue-flame)')
        .style('stroke', '#0c0f16')
        .style('stroke-width', '1.5px');

      // Latest point special glowing callout
      const latestPoint = currentYearRows[currentYearRows.length - 1];
      if (latestPoint) {
        // Pulse ring animation
        g.append('circle')
          .attr('cx', x(latestPoint.dayIndex))
          .attr('cy', y(latestPoint.value))
          .attr('r', 8)
          .style('fill', 'var(--blue-flame)')
          .style('opacity', 0.4)
          .style('filter', 'blur(2px)')
          .append('animate')
          .attr('attributeName', 'r')
          .attr('values', '6;12;6')
          .attr('dur', '2.5s')
          .attr('repeatCount', 'indefinite');

        // Core large dot
        g.append('circle')
          .attr('cx', x(latestPoint.dayIndex))
          .attr('cy', y(latestPoint.value))
          .attr('r', 6)
          .style('fill', 'var(--blue-flame)')
          .style('stroke', '#0c0f16')
          .style('stroke-width', '2px');

        // Callout text label
        const callout = g.append('g')
          .attr('transform', `translate(${x(latestPoint.dayIndex) + 12}, ${y(latestPoint.value) - 10})`);

        callout.append('rect')
          .attr('x', -4)
          .attr('y', -11)
          .attr('width', 74)
          .attr('height', 15)
          .attr('rx', 3)
          .style('fill', 'rgba(15, 23, 42, 0.85)')
          .style('stroke', 'rgba(125, 211, 252, 0.3)')
          .style('stroke-width', '0.5px');

        callout.append('text')
          .attr('x', 2)
          .attr('y', 0)
          .attr('font-size', '8.5px')
          .attr('font-family', 'var(--font-mono)')
          .style('font-feature-settings', '"tnum"')
          .style('letter-spacing', '0.02em')
          .style('fill', '#fff')
          .style('font-weight', '600')
          .text(`${latestPoint.value.toFixed(0)} (${latestPoint.cycle.toUpperCase()})`);
      }
    }

    // Add info note to chart subtitle space
    // (drawn here in the thin-data branch; the >=30-day branch draws it too,
    // because the 2-year envelope stays unavailable until two prior gas
    // years of history exist)
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
      const byDay1 = new Map(priorRows1.map((r) => [r.dayIndex, r.value]));
      const byDay2 = new Map(priorRows2.map((r) => [r.dayIndex, r.value]));
      const envelopeData = [];
      for (let i = 0; i <= 365; i++) {
        const valid = [];
        const v1 = byDay1.get(i);
        const v2 = byDay2.get(i);
        if (v1 !== undefined) valid.push(v1);
        if (v2 !== undefined) valid.push(v2);
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
          .x((d) => x(d.dayIndex))
          .y((d) => y(d.max))
          .curve(d3.curveMonotoneX);

        g.append('path').datum(envelopeData).attr('d', maxLine)
          .attr('fill', 'none')
          .style('stroke', '#ef4444')
          .attr('stroke-width', 1)
          .attr('stroke-opacity', 0.6)
          .attr('stroke-dasharray', '4,2');

        // Cyan 2y min envelope line
        const minLine = d3.line()
          .x((d) => x(d.dayIndex))
          .y((d) => y(d.min))
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
      // Area fill grounds the line across the full gas-year range
      const curArea = d3.area()
        .x((d) => x(d.dayIndex))
        .y0(height)
        .y1((d) => y(d.value))
        .curve(d3.curveMonotoneX);

      g.append('path')
        .datum(currentYearRows)
        .attr('d', curArea)
        .attr('fill', 'rgba(14, 165, 233, 0.06)')
        .attr('stroke', 'none');

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

    // Add info note to chart subtitle space — kept from the thin-data state
    // because the 2-year envelope only becomes available once two prior gas
    // years of history have accumulated.
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', 12)
      .attr('font-size', '10px')
      .style('fill', 'rgba(255,255,255,0.4)')
      .style('font-style', 'italic')
      .text('Historical envelope builds as data accumulates.');
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
      .style('letter-spacing', 'normal')
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
    .style('font-feature-settings', '"tnum"')
    .style('letter-spacing', 'normal')
    .style('fill', 'var(--chart-label)')
    .text((d) => `${d}`);
}

/**
 * Render the KPI Cards Strip.
 */
function renderKpiStrip(sidebarEl, dailySeries, rowsByDate, nameplate, latestData, latestCycleCode, isProxy) {
  const latestLabel = isProxy ? 'Latest Implied Flow' : 'Latest Scheduled';

  // 1. LATEST Card
  const latestValue = latestData.value;
  const latestPeriodLabel = latestData.date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  const latestKpi = kpiCardHtml({
    label: latestLabel,
    value: `${latestValue.toFixed(0)} MMcf/d`,
    helpText: `${latestCycleCode.toUpperCase()} cycle · ${latestPeriodLabel}${isProxy ? ' · proxy ⓘ' : ''}`,
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
