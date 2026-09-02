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
  const termKeys = Object.keys(CONF);
  const footEl = document.createElement('div');
  footEl.className = 'downtime-footnote';

  termKeys.forEach((key, idx) => {
    const btn = document.createElement('button');
    btn.className = 'downtime-tab' + (idx === 0 ? ' downtime-tab--active' : '');
    btn.textContent = CONF[key].label;
    btn.onclick = () => {
      tabs.querySelectorAll('.downtime-tab').forEach((b) => b.classList.remove('downtime-tab--active'));
      btn.classList.add('downtime-tab--active');
      renderTerminal(chartEl, sidebarEl, footEl, bundle, key);
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

  const conf = CONF[key];
  const daily = buildDailyTotal(bundle, conf);

  if (daily.length < 3) {
    chartEl.innerHTML = '<p>Insufficient data for this terminal.</p>';
    return;
  }

  const events = detectDowntime(daily, conf);
  const status = currentStatus(events, daily[daily.length - 1].dateStr);

  const kpis = [
    kpiCardHtml({ label: 'Current status', value: status.type,
                  delta: { value: status.duration + 'd', kind: status.kind === 'OFFLINE' ? 'bearish' : 'neutral' },
                  helpText: 'Latest classified state' }),
    kpiCardHtml({ label: 'Total events', value: String(events.length),
                  delta: { value: 'full history', kind: 'neutral' },
                  helpText: 'All detected downtime/turnaround events' }),
    kpiCardHtml({ label: 'Data span', value: `${daily.length}d`,
                  delta: { value: daily[0].dateStr + ' to ' + daily[daily.length - 1].dateStr, kind: 'neutral' },
                  helpText: 'Posted gas days with at least one feed filing' }),
  ];
  sidebarEl.innerHTML = kpis.join('');

  drawTimeline(chartEl, daily, conf, events);
  renderEventList(footEl, events, conf);
}

function currentStatus(events, latestDateStr) {
  if (events.length === 0) return { type: 'NORMAL', duration: 0, kind: 'neutral' };
  const last = events[events.length - 1];
  if (latestDateStr && (new Date(latestDateStr) - new Date(last.date)) / 86400000 > 3) {
    return { type: 'NORMAL', duration: 0, kind: 'neutral' };
  }
  return { type: last.type, duration: last.duration,
           kind: last.type === 'OFFLINE' ? 'bearish' : 'neutral' };
}

/**
 * Draw a timeline chart: daily total MMcf/d with event bands and zero-day shading.
 */
function drawTimeline(container, daily, conf, events) {
  const margin = { top: 28, right: 120, bottom: 48, left: 48 };
  const totalH = 320;
  const width = Math.max((container.getBoundingClientRect().width || 560) - margin.left - margin.right, 260);
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const parse = (d) => new Date(d);
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
      .text(dates[i].toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
  }
}

/**
 * Render the event list table + honesty footnote.
 */
function renderEventList(footEl, events, conf) {
  const wrap = document.createElement('div');
  wrap.className = 'downtime-events';
  const h = document.createElement('h3');
  h.textContent = `${conf.label} · ${events.length} events (full history)`;
  wrap.appendChild(h);

  if (!events.length) {
    const p = document.createElement('p');
    p.className = 'downtime-events__empty';
    p.textContent = conf.label === 'Sabine Pass'
      ? 'No downtime events — terminal runs flat at ~31% nameplate (measured-partial). Correct behavior.'
      : 'No downtime events in the measured window.';
    wrap.appendChild(p);
  } else {
    const table = document.createElement('table');
    table.className = 'downtime-table';
    table.innerHTML = `
      <thead><tr><th>Date</th><th>Type</th><th>Duration</th><th>Detail</th></tr></thead>
      <tbody>
        ${events.map((e) => `
          <tr>
            <td class="num">${e.date}</td>
            <td><span class="badge badge--${e.type.toLowerCase()}">${e.type}</span></td>
            <td class="num">${e.duration}d</td>
            <td>${e.detail}</td>
          </tr>`).join('')}
      </tbody>`;
    wrap.appendChild(table);
  }
  footEl.appendChild(wrap);

  const honest = document.createElement('p');
  honest.className = 'downtime-honesty';
  honest.innerHTML = `⚠ ${conf.honesty}`;
  footEl.appendChild(honest);
}

/* Export for non-module fallback */
if (typeof window !== 'undefined') {
  window.renderTerminalDowntimePanel = renderTerminalDowntimePanel;
}
