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

/** Cycle publication priority for genuine NAESB scheduled nomination cycles. */
export const CYCLE_PRIORITY = {
  timely: 1,
  evening: 2,
  late: 3,
  latec: 4,
  id1: 5,
  id2: 6,
  id3: 7,
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
    nameplate: 2140, // FERC CP12-509 (3 trains x 0.71 Bcf/d)
    feeds: [
      { source: 'gulf_south', stem: 'gulf_south_sq_24329_d', label: 'Gulf South' },
      { source: 'enbridge',   stem: 'tetco_sq_79999_d',    label: 'TETCO' },
    ],
    zeroDaysThreshold: 2,
    cargoZero: false,
    honesty: 'KMTP intrastate feed (~400 MMcf/d) is invisible. Combined total covers ~81% of feedgas.',
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
  conf.feeds.forEach((f) => {
    const daily = dailyFromFeed(bundle, f);
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
