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
import { kpiCardHtml } from '../components/kpi-card.js';
import { dth_to_mmcf } from '../util/lng-metrics.js';
import {
  DOWNTIME_CONF as CONF,
  detectDowntime,
  buildDailyTotal,
  buildDowntimeViewModel,
  renderEventListHtml,
} from '../util/lng-downtime.js';

/* --------------------------------------------------------------------------- */
/*  Rendering                                                                */
/* --------------------------------------------------------------------------- */

const COLORS = {
  OFFLINE: '#f87171',             // red
  CARGO_IDLE: '#fbbf24',          // amber
  DEPRESSED: '#fb971d',           // orange
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
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Section 8 · Terminal Downtime / Turnaround Indicator',
    subtitle: 'Real outages, cargo idle, and commissioning ramps — false-positive guarded',
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
    btn.tabIndex = 0;
    btn.textContent = CONF[key].label;

    const selectTab = () => {
      tabs.querySelectorAll('.downtime-tab').forEach((b) => {
        b.classList.remove('downtime-tab--active');
        b.setAttribute('aria-selected', 'false');
      });
      btn.classList.add('downtime-tab--active');
      btn.setAttribute('aria-selected', 'true');
      btn.focus();
      renderTerminal(chartEl, sidebarEl, footEl, bundle, key);
    };

    btn.onclick = selectTab;
    btn.onkeydown = (e) => {
      if (e.key === 'ArrowRight') {
        const next = tabs.querySelectorAll('.downtime-tab')[(idx + 1) % termKeys.length];
        if (next) next.click();
      } else if (e.key === 'ArrowLeft') {
        const prev = tabs.querySelectorAll('.downtime-tab')[(idx - 1 + termKeys.length) % termKeys.length];
        if (prev) prev.click();
      }
    };

    tabs.appendChild(btn);
  });

  chartEl.parentNode.insertBefore(tabs, chartEl);
  chartEl.parentNode.appendChild(footEl);
  renderTerminal(chartEl, sidebarEl, footEl, bundle, termKeys[0]);
}

function renderTerminal(chartEl, sidebarEl, footEl, bundle, key) {
  chartEl.innerHTML = '';
  sidebarEl.innerHTML = '';
  footEl.innerHTML = '';

  const model = buildDowntimeViewModel(bundle, key);
  if (!model || model.daily.length < 3) {
    chartEl.innerHTML = '<p>Insufficient data for this terminal.</p>';
    return;
  }

  sidebarEl.innerHTML = model.kpis.join('');
  drawTimeline(chartEl, model.daily, model.conf, model.events);
  footEl.innerHTML = renderEventListHtml(model.events, model.conf);
}

/**
 * Draw a timeline chart: daily total MMcf/d with event bands and zero-day shading.
 */
function drawTimeline(container, daily, conf, events) {
  const margin = { top: 20, right: 30, bottom: 40, left: 56 };
  const totalH = 260;
  const rect = container.getBoundingClientRect();
  const width = Math.max((rect.width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .attr('role', 'img')
    .attr('aria-label', `${conf.label} feedgas downtime and outage timeline`)
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const parse = (d) => new Date(`${d}T00:00:00Z`);
  const dates = daily.map((d) => parse(d.dateStr));
  const values = daily.map((d) => d.value);
  const x = d3.scaleTime().domain(d3.extent(dates)).range([0, width]);
  const maxVal = Math.max(d3.max(values), 1);
  const y = d3.scaleLinear().domain([0, maxVal * 1.1]).range([height, 0]);

  // grid lines
  [0.25, 0.5, 0.75, 1].forEach((frac) => {
    g.append('line').attr('x1', 0).attr('x2', width)
      .attr('y1', y(maxVal * frac)).attr('y2', y(maxVal * frac))
      .attr('stroke', 'rgba(255,255,255,0.05)');
  });

  // zero-day shading
  daily.forEach((d, i) => {
    if (d.postedZero) {
      g.append('rect').attr('x', x(dates[i])).attr('width', 2)
        .attr('y', 0).attr('height', height)
        .attr('fill', conf.cargoZero ? 'rgba(251,191,36,0.12)' : 'rgba(248,181,181,0.10)');
    }
  });

  // total line
  const line = d3.line().x((_, i) => x(dates[i])).y((d) => y(d)).curve(d3.curveMonotoneX);
  g.append('path').datum(values).attr('d', line)
    .style('fill', 'none').style('stroke', 'rgba(255,255,255,0.55)')
    .style('stroke-width', 1.4);

  // event bands
  events.forEach((e) => {
    const ed = parse(e.date);
    const bandWidth = Math.max(e.duration * (width / Math.max(daily.length, 1)), 3);
    g.append('rect').attr('x', x(ed)).attr('width', bandWidth)
      .attr('y', 0).attr('height', height)
      .attr('fill', COLORS[e.type] || COLORS.NORMAL).attr('fill-opacity', 0.08);
    g.append('text').attr('x', x(ed)).attr('y', 14).attr('font-size', 9)
      .attr('font-family', 'var(--font-mono)').style('fill', COLORS[e.type] || '#34d399')
      .text(`${e.type} (${e.duration}d)`);
  });

  // y-axis labels
  [0, 0.25, 0.5, 0.75, 1].forEach((frac) => {
    g.append('text').attr('x', -8).attr('y', y(maxVal * frac) + 4).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)').text(`${(maxVal * frac).toFixed(0)}`);
  });

  // x-axis ticks
  const step = Math.max(1, Math.floor(dates.length / 6));
  for (let i = 0; i < dates.length; i += step) {
    g.append('text').attr('x', x(dates[i])).attr('y', height + 20).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(dates[i].toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }));
  }
}

/* Export for non-module fallback */
if (typeof window !== 'undefined') {
  window.renderTerminalDowntimePanel = renderTerminalDowntimePanel;
}
