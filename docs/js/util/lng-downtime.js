/**
 * LNG Terminal Downtime and Feedgas Event Classifier.
 *
 * Core analytical layer shared between browser panels and Node test runners.
 * Contains pure data-shaping and classification logic without DOM or D3 dependencies.
 *
 * Ground rules:
 * - Vanilla JS only (zero TypeScript in executable code).
 * - RAW Dth/d in Python; converted to MMcf/d via dth / 1.025 / 1000 in JS.
 * - Single continuous pre-operational period (NOT_YET_OPERATIONAL).
 * - Multi-feed routing suppression: single feed zero is routing, not outage.
 * - Posting gap (no rows filed) is never counted as zero.
 */

import { dth_to_mmcf } from './lng-metrics.js';
import { kpiCardHtml } from '../components/kpi-card.js';

/** Cycle publication priority for genuine NAESB scheduled nomination cycles. */
export const CYCLE_PRIORITY = {
  best: 1, // Sub-timely fallback when sole cycle present
  timely: 2,
  evening: 3,
  // Retained for backward compatibility with legacy KM EBB tokens; can be dropped after 2026-09-17 (14 days post-migration)
  evng: 3,
  late: 4,
  latec: 5,
  id1: 6,
  // Retained for backward compatibility with legacy KM EBB tokens; can be dropped after 2026-09-17 (14 days post-migration)
  itrd1: 6,
  id2: 7,
  // Retained for backward compatibility with legacy KM EBB tokens; can be dropped after 2026-09-17 (14 days post-migration)
  itrd2: 7,
  id3: 8,
  // Retained for backward compatibility with legacy KM EBB tokens; can be dropped after 2026-09-17 (14 days post-migration)
  itrd3: 8,
};

/**
 * Cycle priority for genuine NAESB scheduled nomination cycles.
 *
 * Excludes automated hourly snapshot cycles (id{HH}00) entirely by returning 0.
 * Why: TETCO MLC posts hourly automated capacity snapshots that default to 0.0
 * for meters without hourly intraday allocations, even on days with full baseload
 * scheduled flow in timely/evening. Genuine nominated cycles must always win.
 *
 * @param {string} cycle
 * @returns {number}
 */
export function cyclePriority(cycle) {
  const c = String(cycle || '').toLowerCase();
  if (/^id\d{4}$/.test(c)) return 0;
  if (CYCLE_PRIORITY[c] !== undefined) return CYCLE_PRIORITY[c];
  return 0;
}

/** Terminal configuration for downtime classification. */
export const DOWNTIME_CONF = {
  freeport: {
    label: 'Freeport',
    nameplate: 2100, // 2,100 MMcf/d — matches registry expectedCoveragePct basis (see lng-terminals.js)
    feeds: [
      { source: 'gulf_south', stem: 'gulf_south_sq_24329_d', label: 'Gulf South' },
      { source: 'enbridge',   stem: 'tetco_sq_79999_d',    label: 'TETCO' },
    ],
    zeroDaysThreshold: 2,
    cargoZero: false,
    honesty: 'KMTP intrastate feed (~400–450 MMcf/d) is invisible. Combined total covers 52.9% median of 2,100 MMcf/d nameplate (1,111.5 MMcf/d median over 100-day overlap; peak 30d 1,538 MMcf/d = 73.2%).',
  },
  cove_point: {
    label: 'Cove Point',
    nameplate: 750, // FERC CP13-113 (single train 0.75 Bcf/d)
    feeds: [
      { source: 'bhe', stem: 'cpl_sq_10001_d', label: 'Plant Intake' },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'Direct plant intake meter 10001-D measures actual feedgas. Local power/LDC pass-through excluded.',
  },
  sabine_pass: {
    label: 'Sabine Pass',
    nameplate: 4500, // FERC CP11-56 (6 trains x ~0.75 Bcf/d)
    feeds: [
      { source: 'cheniere',      stem: 'creole_trail_sq_ct200111_d', label: 'Creole Trail' },
      { source: 'kinder_morgan', stem: 'km_ngpl_sq_3592_d',          label: 'NGPL (context)', context: true },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'Non-CTPL feeds (~69% of terminal) are not public. Panel measures CTPL delivery only (~31%).',
  },
  plaquemines: {
    label: 'Plaquemines',
    nameplate: 3400, // FERC CP17-66 (Phase 1 + 2, 36 trains x 0.094 Bcf/d)
    feeds: [
      { source: 'quorum', stem: 'gator_express_sq_vgpqd_d', label: 'Gator Express' },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'Phase 1 commissioning commenced late 2024. Gator Express meter VGPQD measures feedgas.',
  },
  cameron: {
    label: 'Cameron',
    nameplate: 2000,
    feeds: [
      { source: 'gasnom', stem: 'cameron_interstate_sq_772300_d', label: 'Cameron Interstate' },
    ],
    zeroDaysThreshold: 3,
    cargoZero: false,
    honesty: 'CIP delivery meter 772300 operates near capacity at ~1.46 Bcf/d (~73% of 2.0 Bcf/d nameplate). Remaining ~27% arrives via unmeasured Columbia Gulf Transmission (TC Energy Cameron Extension).',
  },
  calcasieu: {
    label: 'Calcasieu Pass',
    nameplate: 1300,
    feeds: [
      { source: 'quorum', stem: 'trans_cameron_sq_vgcpd_d', label: 'TransCameron' },
    ],
    zeroDaysThreshold: 3, // inherited default, unvalidated
    cargoZero: false, // inherited default, unvalidated
    honesty: 'TSQ at VGCPD (delivery into Calcasieu Pass LNG) via Quorum TransCameron OpAvail (TspNo=10). Claimed 123.5% median coverage reflects baseload operation above design.',
  },
  golden_pass: {
    label: 'Golden Pass',
    nameplate: 2600,
    feeds: [
      { source: 'gasnom', stem: 'golden_pass_sq_1097217_d', label: 'Golden Pass Pipeline' },
    ],
    zeroDaysThreshold: 3, // inherited default, unvalidated
    cargoZero: false, // inherited default, unvalidated
    honesty: 'Full-terminal consolidated delivery meter (loc 1097217) via GasNom. Current flow (~12.7% of nameplate) represents Train 1 commissioning ramp.',
  },
  corpus_christi: {
    label: 'Corpus Christi',
    nameplate: 2400,
    feeds: [
      { source: 'cheniere', stem: 'corpus_christi_sq_CC200221_d', label: 'CCPL' },
    ],
    zeroDaysThreshold: 3, // inherited default, unvalidated
    cargoZero: false, // inherited default, unvalidated
    honesty: 'CC200221 delivery interconnect measures full-terminal inflow at 99.4% median coverage of 2,400 MMcf/d nameplate via Cheniere LNG Connection. TGP Sinton interconnect is treated as cross-check.',
  },
};

export const DEPRESSED_PCT = 0.60;
export const DEPRESSED_DAYS = 5;
export const BASELINE_WINDOW = 30;

/**
 * Trailing median over a window of values.
 *
 * @param {number[]} values
 * @param {number} window
 * @returns {number}
 */
export function trailingMedian(values, window = 30) {
  if (values.length < 2) return 0;
  const recent = values.slice(-window).filter((v) => v > 0);
  if (recent.length === 0) return 0;
  recent.sort((a, b) => a - b);
  const mid = Math.floor(recent.length / 2);
  return recent.length % 2 ? recent[mid] : (recent[mid - 1] + recent[mid]) / 2;
}

/**
 * Pick highest-priority cycle value per gas day for a feed stem.
 *
 * @param {Object} bundle
 * @param {{source: string, stem: string, label: string}} feed
 * @returns {Array<{dateStr: string, value: number}>}
 */
export function dailyFromFeed(bundle, feed) {
  const src = bundle.sources?.[feed.source];
  if (!src || !src.data) return [];
  const prefix = `${feed.stem.toLowerCase()}_`;
  const byDate = {};

  src.data.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const cycle = sid.slice(prefix.length);
    const pri = cyclePriority(cycle);
    if (pri <= 0) return;
    const dateStr = String(r.period).slice(0, 10);
    if (!byDate[dateStr]) byDate[dateStr] = {};
    const prev = byDate[dateStr];
    if (prev._best === undefined || pri > prev._best) {
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

/**
 * Sum feeds into a daily total, tracking which feeds posted each day.
 *
 * @param {Object} bundle
 * @param {Object} conf
 * @returns {Array<{dateStr: string, value: number, posted: boolean, postedZero: boolean, feedsPosted: number, feedValues: Object}>}
 */
export function buildDailyTotal(bundle, conf) {
  const byDate = new Map();
  // Track min date per feed to determine expected feeds on each date
  const feedMinDates = new Map();
  conf.feeds.forEach((f) => {
    const daily = dailyFromFeed(bundle, f);
    if (daily.length > 0) {
      feedMinDates.set(f.label, daily[0].dateStr);
    }
    for (const d of daily) {
      if (!byDate.has(d.dateStr)) {
        byDate.set(d.dateStr, {
          value: 0,
          posted: false,
          postedZero: false,
          feedsPosted: 0,
          feedValues: {},
        });
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
    // Check feed parity: all feeds active by date k must have reported
    let expectedFeeds = 0;
    feedMinDates.forEach((minD) => {
      if (k >= minD) expectedFeeds++;
    });

    // Incomplete day suppression (e.g. newest day where one feed hasn't filed yet)
    if (v.feedsPosted < expectedFeeds) return;

    v.postedZero = v.posted && v.feedsPosted > 0 && v.value === 0;
    out.push({
      dateStr: k,
      value: v.value,
      posted: v.posted,
      postedZero: v.postedZero,
      feedsPosted: v.feedsPosted,
      feedValues: v.feedValues,
    });
  });
  out.sort((a, b) => new Date(a.dateStr) - new Date(b.dateStr));
  return out;
}

/**
 * Detect downtime/turnaround events for a terminal.
 *
 * @param {Array<{dateStr: string, value: number, posted: boolean, postedZero: boolean, feedsPosted?: number, feedValues?: Object}>} daily
 * @param {Object} conf
 * @returns {Array<{date: string, type: string, duration: number, detail: string}>}
 */
export function detectDowntime(daily, conf) {
  const events = [];
  if (!daily || daily.length === 0) return events;

  // 1. Determine first commercial operation date from data
  // Flow sustained >= flowThreshold for >= 3 consecutive days marks operational state.
  // Adaptive: 50,000 for raw Dth feeds, 50 for MMcf/d feeds.
  const flowThreshold = daily.some((d) => d.value > 10000) ? 50000 : 50;
  let firstOpIdx = daily.length;
  for (let i = 0; i < daily.length; i++) {
    if (daily[i].value >= flowThreshold) {
      if (
        i + 2 < daily.length &&
        daily[i + 1].value >= flowThreshold &&
        daily[i + 2].value >= flowThreshold
      ) {
        firstOpIdx = i;
        break;
      }
      if (i + 2 >= daily.length || daily.slice(i, i + 3).some((d) => d.value >= flowThreshold)) {
        firstOpIdx = i;
        break;
      }
    }
  }

  // Pre-operational span is recorded as ONE continuous NOT_YET_OPERATIONAL event.
  if (firstOpIdx > 0) {
    events.push({
      date: daily[firstOpIdx - 1].dateStr,
      type: 'NOT_YET_OPERATIONAL',
      duration: firstOpIdx,
      detail: `pre-first-gas commissioning (${firstOpIdx} days)`,
    });
  }

  // 2. Operational evaluation (runs only from firstOpIdx onwards)
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

  for (let i = firstOpIdx; i < daily.length; i++) {
    const cur = daily[i];
    const v = cur.value;
    const med = medians[i] || longTerm || 0;
    const pct = med > 0 ? v / med : 0;

    // OFFLINE / CARGO_IDLE
    if (cur.postedZero) {
      offlineRun.push(cur.dateStr);
      if (offlineRun.length >= conf.zeroDaysThreshold) {
        let qualifies = true;
        if (conf.feeds && conf.feeds.some((f) => f.context)) {
          const ctxFeed = conf.feeds.find((f) => f.context);
          if (cur.feedValues && cur.feedValues[ctxFeed.label] === 0 && offlineRun.length === 1) {
            qualifies = false;
          }
        }
        if (qualifies) {
          const etype = conf.cargoZero ? 'CARGO_IDLE' : 'OFFLINE';
          const lastD = lastEventDate[etype];
          const isCont = lastD && (new Date(cur.dateStr) - new Date(lastD)) / 86400000 === 1;
          if (isCont) {
            const last = events[events.length - 1];
            last.duration = offlineRun.length;
            last.date = cur.dateStr;
          } else {
            events.push({
              date: cur.dateStr,
              type: etype,
              duration: offlineRun.length,
              detail: `${offlineRun.length} consecutive posted-zeros`,
            });
          }
          lastEventDate[etype] = cur.dateStr;
        }
      }
    } else {
      offlineRun = [];
    }

    // DEPRESSED (below 60% baseline for >=5 days)
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
          events.push({
            date: cur.dateStr,
            type: 'DEPRESSED',
            duration: depressedRun.length,
            detail: `below ${Math.round(DEPRESSED_PCT * 100)}% baseline (${depressedRun.length}d)`,
          });
        }
        lastEventDate['DEPRESSED'] = cur.dateStr;
      }
    } else {
      depressedRun = [];
    }

    // RAMPING (baseline rising)
    const rw = 7;
    if (i >= rw * 2) {
      const recent = values.slice(i - rw, i + 1).filter((val) => val > 0);
      const older = values.slice(i - 2 * rw, i - rw).filter((val) => val > 0);
      if (recent.length > 0 && older.length > 0) {
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
              events.push({
                date: cur.dateStr,
                type: 'RAMPING',
                duration: rampRun.length,
                detail: 'baseline rising',
              });
            }
            lastEventDate['RAMPING'] = cur.dateStr;
          }
        } else {
          rampRun = [];
        }
      }
    } else {
      rampRun = [];
    }
  }

  return events;
}

/**
 * Determine current operational status from event list and latest gas day.
 */
export function currentStatus(events, latestDateStr) {
  if (!events || events.length === 0) return { type: 'NORMAL', duration: 0, kind: 'neutral' };
  const last = events[events.length - 1];
  if (latestDateStr && (new Date(latestDateStr) - new Date(last.date)) / 86400000 > 3) {
    return { type: 'NORMAL', duration: 0, kind: 'neutral' };
  }
  return {
    type: last.type,
    duration: last.duration,
    kind: last.type === 'OFFLINE' ? 'bearish' : 'neutral',
  };
}

/**
 * Pure analytical view-model builder for Section 8 LNG Terminal Downtime.
 * Free of DOM and D3 dependencies; directly callable from Node tests.
 */
export function buildDowntimeViewModel(bundle, key) {
  const conf = DOWNTIME_CONF[key];
  if (!conf) return null;
  const daily = buildDailyTotal(bundle, conf);
  if (daily.length < 3) {
    return {
      conf,
      daily,
      events: [],
      status: { type: 'INSUFFICIENT_DATA', duration: 0, kind: 'neutral' },
      kpis: [],
    };
  }
  const events = detectDowntime(daily, conf);
  const status = currentStatus(events, daily[daily.length - 1].dateStr);
  const kpis = [
    kpiCardHtml({
      label: 'Current status',
      value: status.type,
      delta: { value: status.duration + 'd', kind: status.kind === 'OFFLINE' ? 'bearish' : 'neutral' },
      helpText: 'Latest classified state',
    }),
    kpiCardHtml({
      label: 'Total events',
      value: String(events.length),
      delta: { value: 'full history', kind: 'neutral' },
      helpText: 'All detected downtime/turnaround events',
    }),
    kpiCardHtml({
      label: 'Data span',
      value: `${daily.length}d`,
      delta: { value: daily[0].dateStr + ' to ' + daily[daily.length - 1].dateStr, kind: 'neutral' },
      helpText: 'Posted gas days with at least one feed filing',
    }),
  ];
  return { conf, daily, events, status, kpis };
}

/**
 * Render the event list table + honesty footnote markup.
 * Pure string generator; free of DOM.
 */
export function renderEventListHtml(events, conf) {
  let tableHtml = '';
  if (!events || !events.length) {
    const emptyMsg = conf.label === 'Sabine Pass'
      ? 'No downtime events — terminal runs flat at ~31% nameplate (measured-partial). Correct behavior.'
      : 'No downtime events in the measured window.';
    tableHtml = `<p class="downtime-events__empty">${emptyMsg}</p>`;
  } else {
    tableHtml = `
      <table class="downtime-table">
        <thead><tr><th>Date</th><th>Type</th><th>Duration</th><th>Detail</th></tr></thead>
        <tbody>
          ${events.map((e) => `
            <tr>
              <td class="num">${e.date}</td>
              <td><span class="badge badge--${e.type.toLowerCase()}">${e.type}</span></td>
              <td class="num">${e.duration}d</td>
              <td>${e.detail}</td>
            </tr>`).join('')}
        </tbody>
      </table>`;
  }

  return `
    <div class="downtime-events">
      <h3>${conf.label} · ${events ? events.length : 0} events (full history)</h3>
      ${tableHtml}
    </div>
    <p class="downtime-honesty">⚠ ${conf.honesty}</p>
  `;
}

