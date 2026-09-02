/**
 * LNG Feed Substitution Panel — Section 7.
 *
 * THE INSIGHT
 *   Multi-feed terminals (Freeport: Gulf South + TETCO; Cove Point: Transco +
 *   TCO) are fed by independent pipelines. When one pipe's SHARE of terminal
 *   intake swings while TOTAL holds, that is ROUTING — a shipper moving
 *   molecules between paths — not a change in terminal demand. When BOTH drop,
 *   that IS demand or an outage. No other public source separates these,
 *   because no other public source measures both pipes into one terminal.
 *
 * WHAT IT SHOWS
 *   1. Per terminal, a 100% stacked area of feed SHARE over time (composition,
 *      not volume — volume is Section 5).
 *   2. Total intake volume as a thin line (secondary axis) so the viewer sees
 *      whether a share shift happened at constant total (routing) or falling
 *      total (demand/outage).
 *   3. A substitution-event table: days where one feed's share moved >= N points
 *      while total stayed within +/- M%. Thresholds tuned against the data
 *      (see REPORT threshold-tuning note): N=10 pts, M=8%.
 *   4. Mechanically-generated interpretation per event: routing vs supply event.
 *
 * HONESTY CONSTRAINTS (rendered on the card)
 *   - Share is only meaningful across feeds WE MEASURE. Freeport's KMTP intrastate
 *     lateral is invisible, so a share shift could reflect an unmeasured third
 *     path. Stated on the card.
 *   - Cove Point's feeders sum to pipeline throughput, ~37% of which passes
 *     through to LDC/power and never reaches the plant. The CP share view is
 *     labelled CPL THROUGHPUT composition, NOT feedgas composition.
 *   - Never calls a share shift a supply loss. The whole point is separating
 *     composition from volume.
 *   - Cove Point 40704 (EGTS) is EXCLUDED: it is an OAC/capacity series (not SQ
 *     flow) and, per the twin-meter check, a sequential re-measurement of the
 *     same molecules as 45001 — including it would double-count and mix units.
 *   - Sabine is currently single measured feed (CT200111-D); 3592 is included as
 *     context (idle, 0) so it slots in if it returns.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { cyclePriority } from './lng-fleet-overview.js';

/* ---- threshold tuning (see REPORT) ---- */
const N_SHARE_PTS = 10;   // a feed's share moved >= 10 points
const M_TOTAL_PCT = 8;    // total changed within +/- 8%

/* ---- terminal / feed definitions ---- */
const TERMINALS = [
  {
    id: 'freeport',
    label: 'Freeport',
    feeds: [
      { src: 'gulf_south', stem: 'gulf_south_sq_24329_d', label: 'Gulf South 24329', color: 'rgba(56,189,248,0.85)' },
      { src: 'enbridge',    stem: 'tetco_sq_79999_d',       label: 'TETCO 79999',     color: 'rgba(34,211,164,0.80)' },
    ],
    honesty: 'Share = Gulf South + TETCO only (~80% of terminal feedgas). Freeport’s unposted KMTP intrastate lateral delivers the remaining ~20% (~400–450 MMcf/d), so an interstate share shift may reflect an unmeasured shift to/from KMTP, not pure pipeline routing.',
  },
  {
    id: 'cove_point',
    label: 'Cove Point',
    feeds: [
      { src: 'bhe', stem: 'cpl_sq_45001_d', label: 'Transco 45001', color: 'rgba(125,211,252,0.85)' },
      { src: 'bhe', stem: 'cpl_sq_37001_d', label: 'TCO 37001',   color: 'rgba(251,191,36,0.80)' },
    ],
    honesty: 'CPL THROUGHPUT composition (Transco + TCO receipts), NOT feedgas. ~37% of CPL throughput passes through to LDC/power and never reaches the plant. EGTS 40704 excluded: OAC/capacity series + twin of 45001. Feeders currently co-move (no independent substitution signal in the 56-day overlap).',
  },
  {
    id: 'sabine',
    label: 'Sabine',
    feeds: [
      { src: 'cheniere',       stem: 'creole_trail_sq_CT200111_d', label: 'CTPL CT200111-D', color: 'rgba(56,189,248,0.85)', partial: true },
      { src: 'kinder_morgan',  stem: 'km_ngpl_sq_3592_d',         label: 'NGPL 3592',      color: 'rgba(156,163,175,0.55)', context: true },
    ],
    honesty: 'MEASURED-PARTIAL: only CTPL’s EBB-visible share. Non-CTPL feedgas + Transco Z3 (Williams SPA) are invisible. 3592 shown as context (idle, 0) — promote to measured if it returns to posting TSQ.',
  },
];

/* ------------------------------------------------------------------ */
import {
  dailyFromFeed,
  alignFeeds,
  buildShareSeries,
  detectSubstitutionEvents as detectEvents,
  N_SHARE_PTS,
  M_TOTAL_PCT,
} from '../util/lng-substitution-data.js';

/* ------------------------------------------------------------------ */
/*  Panel entry                                                       */
/* ------------------------------------------------------------------ */

export function renderLngFeedSubstitutionPanel(panelEl, bundle) {
  // Build chrome with a synthetic source key (we read several sources)
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Section 7 · Feed Substitution at Multi-Feed Terminals',
    subtitle: 'Routing vs demand — share shift at constant total = re-routing, not lost volume',
    sourceKey: 'gulf_south',
    latestPeriod: bundle.sources?.gulf_south?.latest_period || '',
  });

  // Tabs
  const tabBar = document.createElement('div');
  tabBar.className = 'subst-tabs';
  TERMINALS.forEach((t, i) => {
    const btn = document.createElement('button');
    btn.className = 'subst-tab' + (i === 0 ? ' subst-tab--active' : '');
    btn.textContent = t.label;
    btn.onclick = () => {
      tabBar.querySelectorAll('.subst-tab').forEach((b) => b.classList.remove('subst-tab--active'));
      btn.classList.add('subst-tab--active');
      renderTerminal(chartEl, sidebarEl, footEl, bundle, t);
    };
    tabBar.appendChild(btn);
  });

  const footEl = document.createElement('div');
  footEl.className = 'subst-footnote';

  chartEl.parentNode.insertBefore(tabBar, chartEl);
  chartEl.parentNode.appendChild(footEl);

  renderTerminal(chartEl, sidebarEl, footEl, bundle, TERMINALS[0]);
}

function renderTerminal(chartEl, sidebarEl, footEl, bundle, terminal) {
  chartEl.innerHTML = '';
  sidebarEl.innerHTML = '';
  footEl.innerHTML = '';

  const aligned = alignFeeds(bundle, terminal.feeds);
  const feedsWithData = terminal.feeds.filter((f) => (aligned.perFeed.get(f.label)?.size || 0) > 0);

  if (aligned.dates.length < 5) {
    // Graceful: single-feed terminals (e.g. Sabine, where the second feed is
    // context/idle) are not an error — they simply cannot show substitution.
    const onlyOne = feedsWithData.length === 1;
    chartEl.innerHTML = `
      <div class="subst-singlefeed">
        <p><strong>${terminal.label}</strong> is currently a ${
          onlyOne ? 'single measured feed' : 'low-overlap terminal'
        }. Substitution requires ≥2 measured, overlapping feeds — no routing signal is possible yet.</p>
        ${onlyOne ? `<p class="subst-singlefeed__note">Measured: ${feedsWithData[0]?.label}. ${
          terminal.feeds.length > 1 ? `The second feed (${terminal.feeds.find((f) => f !== feedsWithData[0])?.label}) is idle/context and slots in here if it returns.` : ''
        }</p>` : ''}
      </div>`;
    const honest = document.createElement('p');
    honest.className = 'subst-honesty';
    honest.textContent = '⚠ ' + terminal.honesty;
    footEl.appendChild(honest);
    return;
  }
  const records = buildShareSeries(aligned);
  const events = detectEvents(records, terminal.feeds);

  drawChart(chartEl, records, terminal);
  renderKpis(sidebarEl, records, terminal, events);
  renderEventTable(footEl, events, terminal);

  const honest = document.createElement('p');
  honest.className = 'subst-honesty';
  honest.textContent = '⚠ ' + terminal.honesty;
  footEl.appendChild(honest);
}

/* ------------------------------------------------------------------ */
/*  Chart: 100% share area + total volume line                        */
/* ------------------------------------------------------------------ */

function drawChart(container, records, terminal) {
  container.innerHTML = '';
  const margin = { top: 24, right: 130, bottom: 48, left: 48 };
  const totalH = 340;
  const width = Math.max((container.getBoundingClientRect().width || 600) - margin.left - margin.right, 300);
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const parse = (d) => new Date(d + 'T00:00:00Z');
  const dates = records.map((r) => parse(r.date));
  const x = d3.scaleTime().domain(d3.extent(dates)).range([0, width]);
  const yShare = d3.scaleLinear().domain([0, 100]).range([height, 0]);

  const maxTotal = d3.max(records, (r) => r.total) || 1;
  const yVol = d3.scaleLinear().domain([0, maxTotal * 1.1]).range([height, 0]);

  // grid
  [25, 50, 75, 100].forEach((v) => {
    g.append('line').attr('x1', 0).attr('x2', width).attr('y1', yShare(v)).attr('y2', yShare(v))
      .attr('stroke', v === 50 ? 'rgba(255,255,255,0.07)' : 'rgba(255,255,255,0.04)');
  });

  // stack feeds
  const stack = d3.stack().keys(terminal.feeds.map((f) => f.label)).offset(d3.stackOffsetNone);
  const stacked = stack(records);
  const area = d3.area()
    .x((_, i) => x(dates[i]))
    .y0((d) => yShare(d[0])).y1((d) => yShare(d[1]))
    .curve(d3.curveCatmullRom.alpha(0.5));
  stacked.forEach((layer, li) => {
    g.append('path').datum(layer).attr('d', area).style('fill', terminal.feeds[li].color).attr('stroke', 'none');
  });

  // total volume line (secondary axis)
  const line = d3.line().x((_, i) => x(dates[i])).y((r) => yVol(r.total)).curve(d3.curveMonotoneX);
  g.append('path').datum(records).attr('d', line)
    .style('fill', 'none').style('stroke', 'rgba(255,255,255,0.55)').style('stroke-width', 1.2)
    .style('stroke-dasharray', '3,2');

  // axes labels
  [0, 25, 50, 75, 100].forEach((v) => {
    g.append('text').attr('x', -8).attr('y', yShare(v) + 4).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)').style('fill', 'var(--chart-label)')
      .text(`${v}%`);
  });
  const xTicks = dates.filter((_, i) => i % Math.max(1, Math.floor(dates.length / 6)) === 0);
  xTicks.forEach((d) => {
    g.append('text').attr('x', x(d)).attr('y', height + 20).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)').style('fill', 'var(--chart-label)')
      .text(d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
  });

  // legend (feeds + total line)
  const legendX = width + 12;
  terminal.feeds.forEach((f, i) => {
    const ly = i * 22 + 20;
    g.append('rect').attr('x', legendX).attr('y', ly - 10).attr('width', 12).attr('height', 12).attr('rx', 2).style('fill', f.color);
    g.append('text').attr('x', legendX + 16).attr('y', ly).attr('font-size', 10).attr('font-family', 'var(--font-sans)').style('fill', 'var(--text-secondary)').text(f.label);
  });
  const ly2 = terminal.feeds.length * 22 + 20;
  g.append('line').attr('x1', legendX).attr('x2', legendX + 12).attr('y1', ly2 - 4).attr('y2', ly2 - 4)
    .style('stroke', 'rgba(255,255,255,0.55)').style('stroke-dasharray', '3,2');
  g.append('text').attr('x', legendX + 16).attr('y', ly2).attr('font-size', 10).attr('font-family', 'var(--font-sans)').style('fill', 'var(--text-secondary)').text('Total intake');

  setupHover(svg, g, x, yShare, yVol, width, height, dates, records, terminal, margin);
}

function setupHover(svg, g, x, yShare, yVol, width, height, dates, records, terminal, margin) {
  const tooltip = d3.select(svg.node().parentNode).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const cross = g.append('line').attr('y1', 0).attr('y2', height).attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-dasharray', '2,2').style('opacity', 0);
  g.append('rect').attr('width', width).attr('height', height).attr('fill', 'none').style('pointer-events', 'all')
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const date = x.invert(mx);
      const idx = d3.bisectCenter(dates.map((d) => d.getTime()), date.getTime());
      const i = Math.max(0, Math.min(idx, records.length - 1));
      const r = records[i];
      cross.attr('x1', x(dates[i])).attr('x2', x(dates[i])).style('opacity', 1);
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX = svgRect.width / parseInt(svg.attr('viewBox').split(' ')[2]);
      tooltip.style('opacity', 1)
        .style('left', `${Math.min(margin.left * scaleX + x(dates[i]) * scaleX + 12, svgRect.width - 190)}px`)
        .style('top', '12px')
        .html(`<div class="tt-date">${dates[i].toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>` +
          terminal.feeds.map((f) => `<div class="tt-row"><span class="tt-label">${f.label}</span><span class="num">${r[f.label].toFixed(1)}%</span></div>`).join('') +
          `<div class="tt-row"><span class="tt-label">Total</span><span class="num">${(r.total / 1025).toFixed(0)} MMcf/d</span></div>`);
    })
    .on('mouseleave', () => { cross.style('opacity', 0); tooltip.style('opacity', 0); });
}

/* ------------------------------------------------------------------ */
/*  KPI strip + event table                                          */
/* ------------------------------------------------------------------ */

function renderKpis(sidebarEl, records, terminal, events) {
  if (records.length < 2) return;
  const latest = records[records.length - 1];
  const dominant = terminal.feeds.reduce((a, b) => (latest[a.label] >= latest[b.label] ? a : b));
  const routingEvents = events.filter((e) => e.kind === 'routing').length;
  const supplyEvents = events.filter((e) => e.kind === 'supply').length;

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Dominant feed (latest)',
      value: `${latest[dominant.label].toFixed(0)}%`,
      delta: { value: dominant.label, kind: 'neutral' },
      helpText: `Share of ${terminal.label} intake across measured feeds`,
    }),
    kpiCardHtml({
      label: 'Routing events',
      value: `${routingEvents}`,
      delta: { value: `N≥${N_SHARE_PTS}pts @ |Δtotal|≤${M_TOTAL_PCT}%`, kind: 'neutral' },
      helpText: 'Share flip at stable total = re-routing, not lost volume',
    }),
    kpiCardHtml({
      label: 'Supply events',
      value: `${supplyEvents}`,
      delta: { value: 'both feeds down', kind: supplyEvents ? 'bearish' : 'neutral' },
      helpText: 'Both feeds falling = possible outage/demand drop',
    }),
    kpiCardHtml({
      label: 'Overlap window',
      value: `${records.length}d`,
      delta: { value: 'measured feeds', kind: 'neutral' },
      helpText: 'Common days across all feeds shown',
    }),
  ].join('');
}

function renderEventTable(footEl, events, terminal) {
  const wrap = document.createElement('div');
  wrap.className = 'subst-events';
  const h = document.createElement('h3');
  h.className = 'subst-events__title';
  h.textContent = `Substitution events (share ≥${N_SHARE_PTS} pts, |Δtotal|≤${M_TOTAL_PCT}%) — ${events.length} found`;
  wrap.appendChild(h);

  if (!events.length) {
    const p = document.createElement('p');
    p.className = 'subst-events__none';
    p.textContent = 'No substitution events in the measured window at these thresholds. (Rarity is itself a finding — routing flips are occasional, not daily.)';
    wrap.appendChild(p);
  } else {
    const table = document.createElement('table');
    table.className = 'subst-table';
    table.innerHTML = `
      <thead><tr><th>Date</th><th>Mover</th><th>Share Δ</th><th>Total Δ</th><th>Read</th></tr></thead>
      <tbody>
        ${events.map((e) => `
          <tr>
            <td class="num">${e.date}</td>
            <td>${e.mover}</td>
            <td class="num ${e.moverChg >= 0 ? 'pos' : 'neg'}">${e.moverChg >= 0 ? '+' : ''}${e.moverChg.toFixed(1)} pts</td>
            <td class="num ${e.totalChg >= 0 ? 'pos' : 'neg'}">${e.totalChg >= 0 ? '+' : ''}${e.totalChg.toFixed(1)}%</td>
            <td>${e.kind === 'routing'
              ? `${e.mover} ${e.moverChg >= 0 ? 'gained' : 'lost'} share at stable total → routing`
              : `both feeds down → supply event`}</td>
          </tr>`).join('')}
      </tbody>`;
    wrap.appendChild(table);
  }
  footEl.appendChild(wrap);
}
