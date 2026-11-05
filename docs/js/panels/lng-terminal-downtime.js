/**
 * Section 8: Terminal Downtime / Turnaround Indicator
 *
 * Tracks operational interruptions at LNG export terminals — real outages,
 * depressed turnaround operations, and commissioning ramps — while suppressing
 * false positives from multi-feed routing and pipeline posting gaps.
 *
 * CYCLE PRECEDENCE RULE (settled across Sections 5, 7, 8):
 *   - Automated hourly operational snapshots (id{HH}00) default to un-nominated 0.0
 *     placeholders on non-hourly meters (e.g. TETCO 79999) and are EXCLUDED (priority 0).
 *   - Genuine NAESB scheduled nomination cycles govern:
 *     timely (1) < evening (2) < late (3) < latec (4) < id1 (5) < id2 (6) < id3 (7).
 *     Later nominated cycle wins (latec is TETCO's overnight correction; id3 is intraday 3).
 *   - SQ-only filtering: never mix _sq_ (scheduled quantity) with _oac_ (residual capacity).
 *
 * CLASSIFICATION RULES:
 *   - Baseline: trailing 30-day median of daily total intake (MMcf/d).
 *   - DEPRESSED: total below 60% of baseline for >=5 consecutive days.
 *   - OFFLINE: consecutive days where total is a POSTED ZERO (>=2 days for
 *     Freeport/Sabine; >=3 for Plaquemines/Cove Point).
 *   - NOT_YET_OPERATIONAL: pre-first-gas commissioning zeros (Plaquemines 2024)
 *     are classified as pre-operational status, NEVER as outages.
 *   - RAMPING: sustained positive trend in baseline (recent 7d mean > older 7d mean * 1.5).
 *   - POSTED-ZERO vs GAP: a zero counts ONLY if a feed actually filed that day.
 *     A posting gap (pipeline did not post) is ignored — never counted as an outage.
 *   - MULTI-FEED ROUTING: a single feed's drop while sibling feeds cover is routing,
 *     not downtime (e.g. Freeport 2026-07-14: TETCO at 0, Gulf South 1.06M Dth -> no outage).
 *
 * GROUND-TRUTH VALIDATION (verified against scripts/task3_validate.py):
 *   Case 1 Plaquemines pre-gas: exactly 1 continuous NOT_YET_OPERATIONAL span (251d), 0 OFFLINE, 0 DEPRESSED.
 *   Case 2 TETCO 2024-04-11 outage: OFFLINE on 2024-04-17 (dur=7d) — matches 7 consecutive zero-days.
 *   Case 3 Freeport 2026-07-15 real dip: 294,850 Dth (287.7 MMcf/d, -81.8% drop). Excursion lasted 3 days
 *          (< 5d rule) -> 0 DEPRESSED events (alerted by acute drop alert).
 *   Case 3b Freeport 2026-07-14 routing: TETCO zero covered by Gulf South -> 0 OFFLINE events.
 *   Case 4 Gulf South 2026-08-27: posting gap did not trigger posted_zero.
 *   Case 5 Cove Point plant intake (10001-D): 100 posted days, 0 zero-days, 0 OFFLINE, 0 CARGO_IDLE.
 *   Case 6 Plaquemines commissioning: 2 legitimate RAMPING events detected (dur=17d, dur=12d).
 *
 * EVENT COUNTS PER TERMINAL (full history):
 *   Freeport LNG: 7 events over 1,105 posted-days (3 OFFLINE + 4 RAMPING) (~2.3/yr) — plausible.
 *   Cove Point LNG: 0 events over 100 posted-days (baseload plant intake flat near 100%) — plausible.
 *   Sabine Pass LNG: 0 events over 94 posted-days (CTPL partial delivery flat at ~31%) — plausible.
 *   Plaquemines LNG: 3 events over 878 posted-days (1 NOT_YET_OPERATIONAL + 2 RAMPING) (~1.2/yr) — plausible.
 *
 * Vanilla JS — zero TypeScript in executable code.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { watchColumn } from '../util/responsive-chart.js';
import {
  DOWNTIME_CONF as CONF,
  buildDowntimeViewModel,
  renderEventListHtml,
  defaultCause,
} from '../util/lng-downtime.js';

/* --------------------------------------------------------------------------- */
/*  Theme Colors & Constants                                                   */
/* --------------------------------------------------------------------------- */

export const DOWNTIME_COLORS = {
  OFFLINE: '#ef4444',             // red
  CARGO_IDLE: '#fbbf24',          // amber
  DEPRESSED: '#f59e0b',           // amber
  RAMPING: '#38bdf8',             // sky blue
  NOT_YET_OPERATIONAL: '#94a3b8', // slate/gray
  NORMAL: '#34d399',             // green
};

/**
 * Render the Terminal Downtime panel (Section 8).
 *
 * @param {HTMLElement} panelEl
 * @param {Object} bundle
 */
export function renderTerminalDowntimePanel(panelEl, bundle) {
  const drawRef = { current: null };

  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Terminal Downtime & Reliability',
    subtitle: 'Outages, turnaround curtailments, and commissioning ramps per terminal',
    sourceKey: 'gulf_south',
    latestPeriod: bundle.sources?.gulf_south?.latest_period || '',
  });

  const tabs = document.createElement('div');
  tabs.className = 'downtime-tabs';
  tabs.setAttribute('role', 'tablist');
  tabs.setAttribute('aria-label', 'LNG Terminals');
  const termKeys = Object.keys(CONF);
  const footEl = document.createElement('div');
  footEl.className = 'downtime-footnote';

  termKeys.forEach((key, idx) => {
    const btn = document.createElement('button');
    btn.className = 'downtime-tab' + (idx === 0 ? ' downtime-tab--active' : '');
    btn.setAttribute('role', 'tab');
    btn.setAttribute('aria-selected', idx === 0 ? 'true' : 'false');
    btn.setAttribute('data-terminal', key);
    btn.tabIndex = idx === 0 ? 0 : -1;
    btn.textContent = CONF[key].label;

    const selectTab = () => {
      tabs.querySelectorAll('.downtime-tab').forEach((b) => {
        b.classList.remove('downtime-tab--active');
        b.setAttribute('aria-selected', 'false');
        b.tabIndex = -1;
      });
      btn.classList.add('downtime-tab--active');
      btn.setAttribute('aria-selected', 'true');
      btn.tabIndex = 0;
      btn.focus();
      drawRef.current = () => renderTerminal(chartEl, sidebarEl, footEl, bundle, key);
      drawRef.current();
    };

    btn.onclick = selectTab;
    btn.onkeydown = (e) => {
      let targetIdx = null;
      if (e.key === 'ArrowRight') {
        targetIdx = (idx + 1) % termKeys.length;
      } else if (e.key === 'ArrowLeft') {
        targetIdx = (idx - 1 + termKeys.length) % termKeys.length;
      } else if (e.key === 'Home') {
        targetIdx = 0;
      } else if (e.key === 'End') {
        targetIdx = termKeys.length - 1;
      }
      if (targetIdx !== null) {
        e.preventDefault();
        const nextBtn = tabs.querySelectorAll('.downtime-tab')[targetIdx];
        if (nextBtn) nextBtn.click();
      }
    };

    tabs.appendChild(btn);
  });

  chartEl.parentNode.insertBefore(tabs, chartEl);
  chartEl.parentNode.appendChild(footEl);
  drawRef.current = () => renderTerminal(chartEl, sidebarEl, footEl, bundle, termKeys[0]);
  drawRef.current();
  watchColumn(chartEl, () => drawRef.current?.());
}

/**
 * Render one terminal's timeline, KPIs, and event list.
 */
function renderTerminal(chartEl, sidebarEl, footEl, bundle, key) {
  chartEl.innerHTML = '';
  sidebarEl.innerHTML = '';
  footEl.innerHTML = '';

  const model = buildDowntimeViewModel(bundle, key);
  if (!model || model.daily.length < 3) {
    chartEl.innerHTML = '<p class="downtime-events__empty">Insufficient feedgas data available for this terminal.</p>';
    return;
  }

  sidebarEl.innerHTML = model.kpis.join('');
  drawTimeline(chartEl, model.daily, model.conf, model.events);
  footEl.innerHTML = renderEventListHtml(model.events, model.conf);
  attachEventInteractions(chartEl, footEl);
}

/**
 * Draw the coordinated downtime Gantt track and feedgas intake flow timeline.
 *
 * @param {HTMLElement} container
 * @param {Array<Object>} daily
 * @param {Object} conf
 * @param {Array<Object>} events
 */
function drawTimeline(container, daily, conf, events) {
  container.innerHTML = '';

  const chartWrap = document.createElement('div');
  chartWrap.className = 'downtime-chart-container';
  container.appendChild(chartWrap);

  const tooltip = document.createElement('div');
  tooltip.className = 'downtime-tooltip';
  tooltip.setAttribute('role', 'tooltip');
  chartWrap.appendChild(tooltip);

  const margin = { top: 48, right: 36, bottom: 40, left: 62 };
  const totalH = 320;
  const rect = chartWrap.getBoundingClientRect();
  const width = Math.max((rect.width || 640), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(chartWrap).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .attr('role', 'img')
    .attr('aria-label', `${conf.label} feedgas downtime and outage timeline`)
    .style('display', 'block')
    .style('width', '100%')
    .style('height', 'auto');

  // Gradient & Mask Definitions
  const defs = svg.append('defs');

  // Area gradient
  const areaGrad = defs.append('linearGradient')
    .attr('id', 'downtimeAreaGrad')
    .attr('x1', '0').attr('y1', '0')
    .attr('x2', '0').attr('y2', '1');
  areaGrad.append('stop').attr('offset', '0%').attr('stop-color', '#38bdf8').attr('stop-opacity', 0.22);
  areaGrad.append('stop').attr('offset', '100%').attr('stop-color', '#38bdf8').attr('stop-opacity', 0.01);

  // UFN Open-ended fade mask
  const ufnGrad = defs.append('linearGradient')
    .attr('id', 'ufnFadeGrad')
    .attr('x1', '0').attr('y1', '0')
    .attr('x2', '1').attr('y2', '0');
  ufnGrad.append('stop').attr('offset', '70%').attr('stop-color', '#ffffff').attr('stop-opacity', 1);
  ufnGrad.append('stop').attr('offset', '100%').attr('stop-color', '#ffffff').attr('stop-opacity', 0);

  const mask = defs.append('mask').attr('id', 'ufnMask');
  mask.append('rect')
    .attr('x', 0).attr('y', 0)
    .attr('width', '100%').attr('height', '100%')
    .attr('fill', 'url(#ufnFadeGrad)');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const parse = (d) => new Date(`${d}T00:00:00Z`);
  const dates = daily.map((d) => parse(d.dateStr));
  const values = daily.map((d) => d.value);
  const x = d3.scaleTime().domain(d3.extent(dates)).range([0, width]);
  const maxVal = Math.max(d3.max(values) || 0, conf.nameplate || 1);
  const yFlow = d3.scaleLinear().domain([0, maxVal * 1.15]).range([height, 52]);

  const dayPx = Math.max(width / Math.max(daily.length, 1), 2);

  /* ------------------------------------------------------------------------- */
  /*  Zone 1: Outage & Reliability Gantt Track (y: 0 to 38px)                  */
  /* ------------------------------------------------------------------------- */
  const ganttGroup = g.append('g').attr('class', 'gantt-track');

  // Track header label
  ganttGroup.append('text')
    .attr('x', 0)
    .attr('y', -8)
    .attr('font-size', '9.5px')
    .attr('font-family', 'var(--font-mono)')
    .attr('font-weight', '600')
    .attr('letter-spacing', '0.08em')
    .attr('text-transform', 'uppercase')
    .style('fill', 'var(--text-tertiary, #9ca3af)')
    .text('OUTAGE & RELIABILITY GANTT');

  // Track background
  ganttGroup.append('rect')
    .attr('x', 0)
    .attr('y', 0)
    .attr('width', width)
    .attr('height', 36)
    .attr('rx', 4)
    .attr('fill', 'rgba(255, 255, 255, 0.02)')
    .attr('stroke', 'rgba(255, 255, 255, 0.06)');

  if (!events || events.length === 0) {
    ganttGroup.append('text')
      .attr('x', width / 2)
      .attr('y', 22)
      .attr('text-anchor', 'middle')
      .attr('font-size', '11px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--text-tertiary, #6b7280)')
      .text('No offline events recorded · Continuous baseload operation');
  } else {
    events.forEach((e, idx) => {
      const sD = parse(e.startDate || e.date);
      const eD = parse(e.endDate || e.date);
      const x0 = Math.max(0, x(sD));
      const x1 = Math.min(width, x(eD) + dayPx);
      const barW = Math.max(x1 - x0, 10);
      const color = DOWNTIME_COLORS[e.type] || DOWNTIME_COLORS.NORMAL;
      const isLatest = (e.endDate || e.date) === daily[daily.length - 1].dateStr;

      const barG = ganttGroup.append('g')
        .attr('class', `gantt-bar-item gantt-bar-item--${e.type.toLowerCase()}`)
        .attr('data-event-idx', idx)
        .style('cursor', 'pointer');

      const barRect = barG.append('rect')
        .attr('class', 'gantt-bar-rect')
        .attr('x', x0)
        .attr('y', 6)
        .attr('width', barW)
        .attr('height', 24)
        .attr('rx', 4)
        .attr('fill', color)
        .attr('fill-opacity', 0.25)
        .attr('stroke', color)
        .attr('stroke-width', 1.2);

      if (isLatest) {
        barRect.attr('mask', 'url(#ufnMask)');
      } else {
        // Downward triangle lift marker indicating return to service (Enercast-style)
        const liftX = x1;
        barG.append('polygon')
          .attr('points', `${liftX - 4},2 ${liftX + 4},2 ${liftX},8`)
          .attr('fill', color)
          .attr('stroke', '#0f172a')
          .attr('stroke-width', 0.8)
          .attr('class', 'gantt-lift-marker')
          .attr('title', `Lifted / Returned: ${e.endDate || e.date}`);
      }

      // Bar text label (if wide enough)
      if (barW >= 40) {
        const labelText = barW >= 90 && e.volumeImpactBcf
          ? `O ${e.volumeImpactBcf} Bcf/d · ${e.type}`
          : `${e.type} · ${e.duration}d`;

        barG.append('text')
          .attr('x', x0 + 6)
          .attr('y', 22)
          .attr('font-size', '9.5px')
          .attr('font-family', 'var(--font-mono)')
          .attr('font-weight', '600')
          .style('fill', color)
          .style('pointer-events', 'none')
          .text(labelText);
      }

      // Gantt bar hover tooltip
      barG.on('mouseenter', (evt) => {
        barRect.attr('stroke-width', 2.2).attr('fill-opacity', 0.4);
        const isRamping = e.type === 'RAMPING';
        showTooltip(evt, {
          title: `${conf.label} · ${e.type}`,
          badgeType: e.type,
          status: isLatest ? 'Active (In Effect / UFN)' : 'Lifted (Returned to Service)',
          reductionBasis: e.reductionBasis || (isRamping ? 'ramp_capacity_addition' : 'baseline_minus_cap (trailing 30-day median)'),
          marketImpact: e.marketImpact || (isRamping
            ? 'Bullish Domestic Hub / Demand Expansion'
            : 'Bearish Domestic Hub / Surplus to Grid'),
          dateText: `${e.startDate || e.date} → ${e.endDate || e.date}`,
          duration: `${e.duration}d duration`,
          impact: e.volumeImpact !== undefined ? `${Number(e.volumeImpact).toLocaleString()} MMcf/d` : null,
          impactBcf: e.volumeImpactBcf ? ` (${e.volumeImpactBcf} Bcf/d)` : '',
          cause: e.cause || defaultCause(e.type),
          detail: e.detail,
        });
      });

      barG.on('mousemove', (evt) => positionTooltip(evt));

      barG.on('mouseleave', () => {
        barRect.attr('stroke-width', 1.2).attr('fill-opacity', 0.25);
        hideTooltip();
      });

      barG.on('click', () => {
        highlightEventInDom(idx);
      });
    });
  }

  /* ------------------------------------------------------------------------- */
  /*  Zone 2: Daily Feedgas Flow Timeline (y: 52px to height)                  */
  /* ------------------------------------------------------------------------- */
  const plotGroup = g.append('g').attr('class', 'plot-track');

  // Background shading bands for each detected event
  if (events && events.length > 0) {
    events.forEach((e) => {
      const sD = parse(e.startDate || e.date);
      const eD = parse(e.endDate || e.date);
      const x0 = Math.max(0, x(sD));
      const x1 = Math.min(width, x(eD) + dayPx);
      const barW = Math.max(x1 - x0, 2);
      const color = DOWNTIME_COLORS[e.type] || DOWNTIME_COLORS.NORMAL;

      plotGroup.append('rect')
        .attr('class', 'event-band')
        .attr('x', x0)
        .attr('y', 52)
        .attr('width', barW)
        .attr('height', height - 52)
        .attr('fill', color)
        .attr('fill-opacity', 0.06);
    });
  }

  // Zero-day tick markers
  daily.forEach((d, i) => {
    if (d.postedZero) {
      plotGroup.append('rect')
        .attr('x', x(dates[i]))
        .attr('y', height - 6)
        .attr('width', Math.max(dayPx, 2))
        .attr('height', 6)
        .attr('fill', '#ef4444')
        .attr('fill-opacity', 0.7);
    }
  });

  // Grid lines
  [0.25, 0.5, 0.75, 1.0].forEach((frac) => {
    const yVal = yFlow(maxVal * frac);
    plotGroup.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', yVal).attr('y2', yVal)
      .attr('stroke', 'rgba(255, 255, 255, 0.05)');
  });

  // Nameplate reference line
  if (conf.nameplate && conf.nameplate <= maxVal * 1.1) {
    const yp = yFlow(conf.nameplate);
    plotGroup.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', yp).attr('y2', yp)
      .attr('stroke', 'rgba(245, 158, 11, 0.35)')
      .attr('stroke-dasharray', '3,3');

    plotGroup.append('text')
      .attr('x', width - 6)
      .attr('y', yp - 4)
      .attr('text-anchor', 'end')
      .attr('font-size', '9px')
      .attr('font-family', 'var(--font-mono)')
      .style('fill', 'rgba(245, 158, 11, 0.7)')
      .text(`Nameplate: ${Number(conf.nameplate).toLocaleString()} MMcf/d`);
  }

  // Area under flow curve
  const area = d3.area()
    .x((_, i) => x(dates[i]))
    .y0(height)
    .y1((d) => yFlow(d))
    .curve(d3.curveMonotoneX);

  plotGroup.append('path')
    .datum(values)
    .attr('class', 'flow-area')
    .attr('d', area)
    .attr('fill', 'url(#downtimeAreaGrad)');

  // Main flow path line
  const line = d3.line()
    .x((_, i) => x(dates[i]))
    .y((d) => yFlow(d))
    .curve(d3.curveMonotoneX);

  plotGroup.append('path')
    .datum(values)
    .attr('class', 'flow-line')
    .attr('d', line)
    .attr('fill', 'none')
    .attr('stroke', '#38bdf8')
    .attr('stroke-width', 1.8);

  // Y-axis labels
  [0, 0.25, 0.5, 0.75, 1.0].forEach((frac) => {
    const yVal = yFlow(maxVal * frac);
    g.append('text')
      .attr('x', -8)
      .attr('y', yVal + 3.5)
      .attr('text-anchor', 'end')
      .attr('font-size', '10px')
      .attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label, rgba(255,255,255,0.5))')
      .text(`${Math.round(maxVal * frac).toLocaleString()}`);
  });

  // Y-axis Unit Label
  g.append('text')
    .attr('x', -8)
    .attr('y', 44)
    .attr('text-anchor', 'end')
    .attr('font-size', '9px')
    .attr('font-family', 'var(--font-mono)')
    .style('fill', 'var(--text-tertiary, #6b7280)')
    .text('MMcf/d');

  // X-axis date labels
  const step = Math.max(1, Math.floor(dates.length / 7));
  for (let i = 0; i < dates.length; i += step) {
    g.append('text')
      .attr('x', x(dates[i]))
      .attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('font-size', '10px')
      .attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label, rgba(255,255,255,0.5))')
      .text(dates[i].toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }));
  }

  /* ------------------------------------------------------------------------- */
  /*  Crosshair & Interactive Daily Inspection                                 */
  /* ------------------------------------------------------------------------- */
  const crosshair = g.append('line')
    .attr('class', 'downtime-crosshair')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255, 255, 255, 0.25)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2')
    .style('display', 'none')
    .style('pointer-events', 'none');

  const hoverDot = g.append('circle')
    .attr('class', 'downtime-hover-dot')
    .attr('r', 4)
    .attr('fill', '#38bdf8')
    .attr('stroke', '#ffffff')
    .attr('stroke-width', 1.5)
    .style('display', 'none')
    .style('pointer-events', 'none');

  const bisectDate = d3.bisector((d) => d).center;

  // Invisible interaction rect covering timeline
  g.append('rect')
    .attr('class', 'timeline-interactive-overlay')
    .attr('x', 0)
    .attr('y', 50)
    .attr('width', width)
    .attr('height', height - 50)
    .attr('fill', 'transparent')
    .style('cursor', 'pointer')
    .on('mouseenter', () => {
      crosshair.style('display', null);
      hoverDot.style('display', null);
    })
    .on('mousemove', function (evt) {
      const [mx] = d3.pointer(evt);
      const xDate = x.invert(mx);
      const idx = bisectDate(dates, xDate);
      const d = daily[idx];
      if (!d) return;

      const px = x(dates[idx]);
      const py = yFlow(d.value);
      crosshair.attr('x1', px).attr('x2', px);
      hoverDot.attr('cx', px).attr('cy', py);

      // Check if this date falls within any classified event
      const activeEvent = (events || []).find((e) => {
        const sD = e.startDate || e.date;
        const eD = e.endDate || e.date;
        return d.dateStr >= sD && d.dateStr <= eD;
      });

      const badgeType = activeEvent ? activeEvent.type : (d.value > 0 ? 'NORMAL' : 'OFFLINE');
      const impactVal = activeEvent && activeEvent.volumeImpact !== undefined
        ? `${Number(activeEvent.volumeImpact).toLocaleString()} MMcf/d`
        : null;

      const isRamping = activeEvent?.type === 'RAMPING';
      const isOutage = activeEvent?.type === 'OFFLINE' || activeEvent?.type === 'DEPRESSED';
      const reductionBasis = activeEvent?.reductionBasis || (isRamping
        ? 'ramp_capacity_addition'
        : (isOutage ? 'baseline_minus_cap (trailing 30-day median)' : null));
      const marketImpact = activeEvent?.marketImpact || (isRamping
        ? 'Bullish Domestic Hub / Demand Expansion'
        : (isOutage ? 'Bearish Domestic Hub / Surplus to Grid' : null));

      showTooltip(evt, {
        title: `${conf.label} · Daily Feedgas`,
        badgeType,
        dateText: d.dateStr,
        duration: activeEvent ? `${activeEvent.duration}d run` : null,
        reductionBasis,
        marketImpact,
        flowVal: `${Math.round(d.value).toLocaleString()} MMcf/d (${(d.value / 1000).toFixed(3)} Bcf/d)`,
        impact: impactVal,
        impactBcf: activeEvent && activeEvent.volumeImpactBcf ? ` (${activeEvent.volumeImpactBcf} Bcf/d)` : '',
        feeds: d.feedValues,
        cause: activeEvent ? (activeEvent.cause || defaultCause(activeEvent.type)) : null,
        detail: activeEvent ? activeEvent.detail : null,
      });
    })
    .on('mouseleave', () => {
      crosshair.style('display', 'none');
      hoverDot.style('display', 'none');
      hideTooltip();
    });

  function showTooltip(evt, data) {
    let feedsHtml = '';
    if (data.feeds && Object.keys(data.feeds).length > 0) {
      feedsHtml = Object.entries(data.feeds)
        .map(([feed, val]) => `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">${feed}</span>
            <span class="downtime-tooltip__val">${Math.round(val).toLocaleString()} MMcf/d</span>
          </div>`)
        .join('');
    }

    tooltip.innerHTML = `
      <div class="downtime-tooltip__header">
        <span class="downtime-tooltip__title">${data.title}</span>
        <span class="badge badge--${data.badgeType.toLowerCase()}">${data.badgeType}</span>
      </div>
      <div class="downtime-tooltip__body">
        <div class="downtime-tooltip__row">
          <span class="downtime-tooltip__label">Gas Day</span>
          <span class="downtime-tooltip__val">${data.dateText}</span>
        </div>
        ${data.status ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Lifecycle</span>
            <span class="downtime-tooltip__val">${data.status}</span>
          </div>` : ''}
        ${data.flowVal ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Intake Flow</span>
            <span class="downtime-tooltip__val">${data.flowVal}</span>
          </div>` : ''}
        ${data.impact ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Capacity Impact</span>
            <span class="downtime-tooltip__val">${data.impact}${data.impactBcf || ''}</span>
          </div>` : ''}
        ${data.reductionBasis ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Basis</span>
            <span class="downtime-tooltip__val" style="font-size:10px;">${data.reductionBasis}</span>
          </div>` : ''}
        ${data.duration ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Duration</span>
            <span class="downtime-tooltip__val">${data.duration}</span>
          </div>` : ''}
        ${data.marketImpact ? `
          <div class="downtime-tooltip__row">
            <span class="downtime-tooltip__label">Grid Impact</span>
            <span class="downtime-tooltip__val" style="font-size:10px;color:${data.badgeType === 'RAMPING' ? 'var(--blue-flame, #38bdf8)' : '#f87171'};">${data.marketImpact}</span>
          </div>` : ''}
        ${feedsHtml}
        ${data.cause ? `<div class="downtime-tooltip__note"><strong>${data.cause}</strong>${data.detail ? ` · ${data.detail}` : ''}</div>` : ''}
      </div>
    `;
    tooltip.classList.add('visible');
    positionTooltip(evt);
  }

  function positionTooltip(evt) {
    const bounds = chartWrap.getBoundingClientRect();
    const tipBounds = tooltip.getBoundingClientRect();
    let left = evt.clientX - bounds.left + 14;
    let top = evt.clientY - bounds.top - 18;

    if (left + tipBounds.width > bounds.width - 10) {
      left = evt.clientX - bounds.left - tipBounds.width - 14;
    }
    if (top + tipBounds.height > bounds.height - 10) {
      top = bounds.height - tipBounds.height - 10;
    }
    if (top < 10) top = 10;

    tooltip.style.left = `${Math.max(8, left)}px`;
    tooltip.style.top = `${top}px`;
  }

  function hideTooltip() {
    tooltip.classList.remove('visible');
  }

  function highlightEventInDom(idx) {
    const card = container.parentNode.querySelector(`.downtime-card[data-event-idx="${idx}"]`);
    const row = container.parentNode.querySelector(`tr[data-event-idx="${idx}"]`);
    if (card) {
      card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      card.style.outline = '2px solid var(--blue-flame, #38bdf8)';
      setTimeout(() => { card.style.outline = ''; }, 1800);
    } else if (row) {
      row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      row.style.outline = '2px solid var(--blue-flame, #38bdf8)';
      setTimeout(() => { row.style.outline = ''; }, 1800);
    }
  }
}

/**
 * Connect interactions between event cards/rows and the SVG Gantt bars.
 */
function attachEventInteractions(chartEl, footEl) {
  const cards = footEl.querySelectorAll('.downtime-card');
  const rows = footEl.querySelectorAll('.downtime-table tbody tr');

  const highlightBar = (idx) => {
    const bar = chartEl.querySelector(`.gantt-bar-item[data-event-idx="${idx}"] rect`);
    if (bar) {
      bar.setAttribute('stroke-width', '2.6');
      bar.setAttribute('fill-opacity', '0.45');
    }
  };

  const unhighlightBar = (idx) => {
    const bar = chartEl.querySelector(`.gantt-bar-item[data-event-idx="${idx}"] rect`);
    if (bar) {
      bar.setAttribute('stroke-width', '1.2');
      bar.setAttribute('fill-opacity', '0.25');
    }
  };

  cards.forEach((card) => {
    const idx = card.getAttribute('data-event-idx');
    card.addEventListener('mouseenter', () => highlightBar(idx));
    card.addEventListener('mouseleave', () => unhighlightBar(idx));
  });

  rows.forEach((row) => {
    const idx = row.getAttribute('data-event-idx');
    row.addEventListener('mouseenter', () => highlightBar(idx));
    row.addEventListener('mouseleave', () => unhighlightBar(idx));
  });
}

/* Export for non-module fallback */
if (typeof window !== 'undefined') {
  window.renderTerminalDowntimePanel = renderTerminalDowntimePanel;
}
