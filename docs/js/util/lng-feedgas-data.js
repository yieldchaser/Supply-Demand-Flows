/**
 * Pure data-shaping functions for LNG Feedgas Hero Panel (Section 5).
 * Zero DOM, zero D3 dependencies — fully testable in Node.js.
 */

import { dth_to_mmcf } from './lng-metrics.js';
import { cyclePriority } from './lng-downtime.js';

/**
 * Build a date -> {cycle -> MMcf} map for one feed of a multi-feed terminal.
 * `seriesStem` is the registry's series stem ("{prefix}_sq_{loc}_{flow}").
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {string} seriesStem — e.g. "gulf_south_sq_24329_d"
 * @returns {Object<string, Object<string, number>>}
 */
export function buildFeedCycleMaps(rows, seriesStem) {
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
 * @param {Object} t - LNG terminal config
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
  /** Labels that may enter the summed flow series ('proxy' shows but never sums). */
  const summableLabels = new Set();
  for (const feed of t.feeds || []) {
    const kind = feed.kind;
    if (kind === 'comparison' || kind === 'context') continue;
    const src = bundle.sources?.[feed.source];
    if (!src || !src.data) continue;
    const map = buildFeedCycleMaps(src.data, feed.series);
    if (Object.keys(map).length === 0) continue;
    feedLabels.push(feed.label);
    feedMaps.push({ label: feed.label, map });
    if (kind !== 'proxy') summableLabels.add(feed.label);
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
        const prio = cyclePriority(cy);
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

  // Determine each summable feed's start date
  const feedMinDates = new Map();
  feedMaps.forEach((fm) => {
    if (summableLabels.has(fm.label)) {
      const dates = Object.keys(fm.map).sort();
      if (dates.length > 0) feedMinDates.set(fm.label, dates[0]);
    }
  });

  const dailySeries = [];
  Object.keys(rowsByDate).forEach((dateStr) => {
    const feeds = rowsByDate[dateStr];
    if (Object.keys(feeds).length === 0) return;

    // Check feed parity: all feeds active by this date must have reported.
    // An incomplete day (e.g. trailing edge where one pipe hasn't filed yet)
    // is omitted from total flow to prevent false-cliff rendering.
    let expectedCount = 0;
    feedMinDates.forEach((minDate) => {
      if (dateStr >= minDate) expectedCount++;
    });

    let reportedCount = 0;
    let total = 0;
    Object.entries(feeds).forEach(([label, v]) => {
      if (summableLabels.has(label)) {
        reportedCount++;
        total += v;
      }
    });

    if (reportedCount < expectedCount) return;

    dailySeries.push({ dateStr, date: new Date(dateStr), value: total });
  });
  dailySeries.sort((a, b) => a.date.getTime() - b.date.getTime());

  let latestSplit = null;
  for (let i = dailySeries.length - 1; i >= 0 && latestSplit === null; i--) {
    const dayFeeds = rowsByDate[dailySeries[i].dateStr];
    const present = feedLabels.filter((label) => dayFeeds[label] !== undefined);
    if (feedLabels.length > 0 && present.length === feedLabels.length) {
      latestSplit = {};
      feedLabels.forEach((label) => {
        latestSplit[label] = dayFeeds[label];
      });
    }
  }
  if (latestSplit === null && dailySeries.length > 0) {
    latestSplit = { ...rowsByDate[dailySeries[dailySeries.length - 1].dateStr] };
  }

  return { dailySeries, rowsByDate, feedLabels, latestSplit };
}
