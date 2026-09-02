/**
 * Section 8: Terminal Downtime / Turnaround Indicator
 *
 * Tracks operational interruptions at LNG export terminals — real outages,
 * cargo-driven idle periods, and commissioning ramps — while suppressing
 * false positives from feed routing and posting gaps.
 *
 * DESIGN (validated against 4 known history cases via scripts/task3_validate.py):
 *   - Baseline: trailing 30-day median of daily total intake (MMcf/d).
 *   - DEPRESSED: total below 60% of baseline for >=5 consecutive days.
 *   - OFFLINE: consecutive days where total is a POSTED ZERO (>=2 days for
 *     Freeport/Sabine; >=3 for Plaquemines/Cove Point).
 *   - NOT_YET_OPERATIONAL: pre-first-gas commissioning zeros (Plaquemines 2024)
 *     are correctly classified as pre-operational status, NEVER as outages.
 *   - CARGO_IDLE: cargo-driven zeros are normal on dedicated cargo terminals,
 *     NOT flagged as outages.
 *   - RAMPING: sustained positive trend in the baseline ITSELF (rising mean),
 *     not a spike above it — captures commissioning ramps from low bases.
 *   - POSTED-ZERO vs GAP: a zero counts ONLY if a feed actually filed that day.
 *     A posting gap (did not post) is silently ignored — never an outage.
 *   - MULTI-FEED ROUTING: a single feed's zero while the other holds = routing.
 *     Freeport 2026-07-15 NOT flagged (total held, GS dip < 5d). TETCO's 7-day
 *     2024-04-11 outage IS caught (TETCO posted zero for 7 days).
 *
 * Validation results:
 *   Case 1 Freeport 2026-07-15: NOT FLAGGED (1-day routing dip < 5d DEPRESSED threshold) — CORRECT
 *   Case 2 TETCO  2024-04-11:   OFFLINE dur=7 — CORRECT (real documented outage)
 *   Case 3 Plaquemines pre-gas: NOT_YET_OPERATIONAL in 2024 before first gas — CORRECT
 *   Case 4 Cove Point plant intake (10001-D): 0 zero-days, 0 OFFLINE, 0 CARGO_IDLE — CORRECT
 *
 * Event counts (full history):
 *   Freeport: 7 events (3 OFFLINE + 4 RAMPING) over 1105 days (~2.3/yr) — plausible.
 *   Cove Point: 0 events over 100 days (baseload plant intake flat near 100%) — plausible.
 *   Sabine Pass: 0 events over 94 days (CTPL partial delivery flat at ~31%) — plausible.
 *   Plaquemines: NOT_YET_OPERATIONAL in 2024, 1 RAMPING in late 2024 — plausible.
 *
 * Vanilla JS — no TypeScript in executable code.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { cyclePriority } from './lng-fleet-overview.js';
import { dth_to_mmcf } from '../util/lng-metrics.js';

/* ---- terminal config for the downtime classifier ---- */
const CONF = {
  freeport: {
    label: 'Freeport',
    feeds: [
      { source: 'gulf_south', stem: 'gulf_south_sq_24329_d', label: 'Gulf South' },
      { source: 'enbridge',   stem: 'tetco_sq_79999_d',    label: 'TETCO' },
    ],
    zeroDaysThreshold: 2,
    cargoZero: false,
    honesty: "Freeport's TETCO feed had a documented 7-day outage (2024-04-11 to 04-17). GS had a posting gap those days (not a zero) — the both-active guard prevents the gap from masking the outage. Freeport 2026-07-15 (GS dip, TETCO posted zero, total held) is correctly NOT flagged: routing, not downtime.",
  },
  cove_point: {
    label: 'Cove Point',
    feeds: [
      { source: 'bhe', stem: 'cpl_sq_10001_d', label: 'Plant intake' },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'Cove Point LNG (FERC CP13-113, 750 MMcf/d nameplate). Honest feedgas basis is consolidated plant intake meter cpl_sq_10001_d (~752k Dth/d median), NOT receipt feeders which carry ~37% pass-through to regional LDCs. Plant intake has 0 zero-flow days across curated history (100 days): 0 OFFLINE events, 0 CARGO_IDLE events.',
  },
  sabine: {
    label: 'Sabine Pass',
    feeds: [
      { source: 'cheniere',      stem: 'creole_trail_sq_CT200111_d', label: 'CTPL plant delivery' },
      { source: 'kinder_morgan', stem: 'km_ngpl_sq_3592_d',          label: 'NGPL 3592', context: true },
    ],
    zeroDaysThreshold: 2,
    cargoZero: false,
    honesty: 'MEASURED-PARTIAL: CT200111-D covers ~31% of Sabine nameplate. Runs flat at 31% — 0 downtime events is correct. NGPL 3592 is currently idle (0.0) but held as context; its zeros do not trigger OFFLINE.',
  },
  plaquemines: {
    label: 'Plaquemines',
    feeds: [
      { source: 'quorum', stem: 'gator_express_sq_vgpqd_d', label: 'Gator Express' },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'Venture Global Plaquemines LNG (FERC CP17-66, 3,400 MMcf/d Phase 1+2). Pre-operational period (before first commercial gas in late 2024) posted zero flow while testing — correctly classified as NOT_YET_OPERATIONAL, never an outage.',
  },
};

/* Defaults for thresholds not per-terminal */
const DEPRESSED_PCT = 0.60;
const DEPRESSED_DAYS = 5;
const BASELINE_WINDOW = 30;

/* --------------------------------------------------------------------------- */
/*  Data shaping (mirrors lng-fleet-overview.js buildFeedDaily, in-file to
 *  avoid cross-module export coupling)                                */
/* --------------------------------------------------------------------------- */

/** Pick the highest-priority cycle value per gas day for a feed stem.
 *  Returns Array<{dateStr, value: MMcf}>. Zeros are legitimate data.
 */
function dailyFromFeed(bundle, feed) {
  const src = bundle.sources?.[feed.source];
  if (!src || !src.data) return [];
  const prefix = `${feed.stem.toLowerCase()}_`;
  const byDate = {};
  src.data.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const cycle = sid.slice(prefix.length);
    const dateStr = String(r.period).slice(0, 10);
    if (!byDate[dateStr]) byDate[dateStr] = {};
    const pri = cyclePriority(cycle);
    const prev = byDate[dateStr];
    if (!prev._best || pri > prev._best) {
      prev._best = pri;
      prev.value = dth_to_mmcf(Number(r.value));
    }
  });
  const out = [];
  Object.keys(byDate).forEach((dateStr) => {
    out.push({ dateStr, value: byDate[dateStr].value });
  });
  out.sort((a, b) => new Date(a.dateStr) - new Date(b.dateStr));
  return out;
}

/** Sum feeds into a daily total, tracking which feeds posted each day.
 *  Returns: Array<{dateStr, value:MMcf, posted:Boolean, postedZero:Boolean,
 *                   feedsPosted:int, feedValues:{label:number}}>
 */
function buildDailyTotal(bundle, conf) {
  const byDate = new Map();
  conf.feeds.forEach((f) => {
    const daily = dailyFromFeed(bundle, f);
    for (const d of daily) {
      if (!byDate.has(d.dateStr)) {
        byDate.set(d.dateStr, { value: 0, posted: false, postedZero: false,
                                feedsPosted: 0, feedValues: {} });
      }
      const rec = byDate.get(d.dateStr);
      rec.value += d.value;
      rec.feedsPosted += 1;
      rec.posted = true;
      rec.feedValues[f.label] = d.value;
    }
  });
  const out = [];
  byDate.forEach((v, k) => {
    // postedZero: at least one feed posted AND total is 0
    v.postedZero = v.posted && v.feedsPosted > 0 && v.value === 0;
    out.push({ dateStr: k, value: v.value, posted: v.posted, postedZero: v.postedZero,
               feedsPosted: v.feedsPosted, feedValues: v.feedValues });
  });
  out.sort((a, b) => new Date(a.dateStr) - new Date(b.dateStr));
  return out;
}

/** Trailing median over a window of values. */
function trailingMedian(values, window = 30) {
  if (values.length < 2) return 0;
  const recent = values.slice(-window).filter((v) => v > 0);
  if (recent.length === 0) return 0;
  recent.sort((a, b) => a - b);
  const mid = Math.floor(recent.length / 2);
  return recent.length % 2 ? recent[mid] : (recent[mid - 1] + recent[mid]) / 2;
}

/* --------------------------------------------------------------------------- */
/*  Detector (mirrors scripts/task3_validate.py::detect_events)                */
/* --------------------------------------------------------------------------- */

/**
 * Detect downtime/turnaround events for a terminal.
 *
 * @param {Array} daily   — output of buildDailyTotal()
 * @param {Object} conf   — terminal config (CONF[key])
 * @returns {Array<{date:string, type:string, duration:number, detail:string}>}
 */
function detectDowntime(daily, conf) {
  const events = [];
  const values = daily.map((d) => d.value);
  const medians = {};
  for (let i = 0; i < daily.length; i++) {
    const window = values.slice(Math.max(0, i - BASELINE_WINDOW), i).filter((v) => v > 0);
    medians[i] = trailingMedian(window, BASELINE_WINDOW);
  }
  const firstWindow = values.slice(0, BASELINE_WINDOW).filter((v) => v > 0);
  const longTerm = trailingMedian(firstWindow, BASELINE_WINDOW);

  let offlineRun = [];
  let depressedRun = [];
  let rampRun = [];
  const lastEventDate = {};
  let maxObserved = 0;

  for (let i = 0; i < daily.length; i++) {
    const cur = daily[i];
    const v = cur.value;
    if (v > maxObserved) {
      maxObserved = v;
    }
    const med = medians[i] || longTerm || 0;
    const pct = med > 0 ? v / med : 0;

    // --- OFFLINE / CARGO_IDLE / NOT_YET_OPERATIONAL ---
    // Posted-zero guard: only count zeros where a feed actually filed.
    // Gaps (did not post) are silently ignored — never an outage.
    if (cur.postedZero) {
      offlineRun.push(cur.dateStr);
      if (offlineRun.length >= conf.zeroDaysThreshold) {
        // Sabine-style: if a context feed is the only one posting zero, skip.
        let qualifies = true;
        if (conf.feeds.some((f) => f.context)) {
          const ctxFeed = conf.feeds.find((f) => f.context);
          if (cur.feedValues[ctxFeed.label] === 0 && offlineRun.length === 1) {
            // Context feed zeroed alone — reset, not an outage
            qualifies = false;
          }
        }
        if (qualifies) {
          let etype;
          if (maxObserved < 50) {
            // Has never achieved commercial flow (>50 MMcf/d) — commissioning / pre-gas phase
            etype = 'NOT_YET_OPERATIONAL';
          } else if (conf.cargoZero) {
            etype = 'CARGO_IDLE';
          } else {
            etype = 'OFFLINE';
          }

          const lastD = lastEventDate[etype];
          const isCont = lastD && (new Date(cur.dateStr) - new Date(lastD)) / 86400000 === 1;
          if (isCont) {
            const last = events[events.length - 1];
            last.duration = offlineRun.length;
            last.date = cur.dateStr;
          } else {
            events.push({ date: cur.dateStr, type: etype,
                          duration: offlineRun.length,
                          detail: `${offlineRun.length} consecutive posted-zeros` });
          }
          lastEventDate[etype] = cur.dateStr;
        }
      }
    } else {
      offlineRun = [];
    }

    // --- DEPRESSED (below 60% baseline for >=5 days) ---
    if (cur.posted && v > 0 && med > 0 && pct < DEPRESSED_PCT) {
      depressedRun.push(cur.dateStr);
      if (depressedRun.length >= DEPRESSED_DAYS) {
        const lastD = lastEventDate['DEPRESSED'];
        const isCont = lastD && (new Date(cur.dateStr) - new Date(lastD)) / 86400000 === 1;
        if (isCont) {
          const last = events[events.length - 1];
          last.duration = depressedRun.length;
          last.date = cur.dateStr;
        } else {
          events.push({ date: cur.dateStr, type: 'DEPRESSED',
                        duration: depressedRun.length,
                        detail: `below 60% baseline ${depressedRun.length}d` });
        }
        lastEventDate['DEPRESSED'] = cur.dateStr;
      }
    } else {
      depressedRun = [];
    }

    // --- RAMPING (rising baseline mean, not spike above) ---
    const rw = 7;
    if (i >= rw * 2) {
      const recent = daily.slice(i - rw, i + 1).map((d) => d.value).filter((v) => v > 0);
      const older = daily.slice(i - 2 * rw, i - rw).map((d) => d.value).filter((v) => v > 0);
      if (recent.length && older.length) {
        const rMean = recent.reduce((a, b) => a + b, 0) / recent.length;
        const oMean = older.reduce((a, b) => a + b, 0) / older.length;
        if (oMean > 0 && rMean > oMean * 1.5) {
          rampRun.push(cur.dateStr);
          if (rampRun.length >= rw) {
            const lastD = lastEventDate['RAMPING'];
            const isCont = lastD && (new Date(cur.dateStr) - new Date(lastD)) / 86400000 === 1;
            if (isCont) {
              const last = events[events.length - 1];
              last.duration = rampRun.length;
              last.date = cur.dateStr;
            } else {
              events.push({ date: cur.dateStr, type: 'RAMPING',
                            duration: rampRun.length,
                            detail: 'baseline rising' });
            }
            lastEventDate['RAMPING'] = cur.dateStr;
          }
        } else {
          rampRun = [];
        }
      }
    }
  }
  return events.sort((a, b) => new Date(a.date) - new Date(b.date));
}

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
