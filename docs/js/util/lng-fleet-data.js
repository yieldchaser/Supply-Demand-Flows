/**
 * Pure data-shaping and aggregations for LNG Fleet Overview (Section 7).
 * Zero DOM, zero D3 dependencies — fully testable in Node.js.
 */

import {
  LNG_TERMINALS,
  LNG_FLEET_ORDER,
  FLEET_PROXY_EXCLUSIONS,
  COMPARISON_FEED_EXCLUSIONS,
  terminalSeriesPrefixes,
} from './lng-terminals.js';
import { dth_to_mmcf, get_utilization_level } from './lng-metrics.js';
import { cyclePriority } from './lng-downtime.js';

export { cyclePriority };

/**
 * Build a date -> {cycle -> MMcf} map for a direct-SQ terminal.
 * Zeros are legitimate — never treated as missing.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {Object} t - terminal config
 * @returns {Object<string, Object<string, number>>}
 */
export function buildSqCycleMaps(rows, t) {
  const sqPrefix = `${t.seriesPrefix}_sq_${String(t.loc).toLowerCase()}_${t.flow || 'd'}_`;
  const byDate = {};
  rows.forEach((r) => {
    const sid = r.series_id.toLowerCase();
    if (!sid.startsWith(sqPrefix)) return;
    const cycle = sid.slice(sqPrefix.length).toLowerCase();
    if (!byDate[r.period]) byDate[r.period] = {};
    byDate[r.period][cycle] = dth_to_mmcf(Number(r.value));
  });
  return byDate;
}

/**
 * Build a daily series (one point per gas day, latest cycle wins) for a
 * direct-SQ terminal.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {Object} t
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailySqSeries(rows, t) {
  return buildDailyFromCycles(buildSqCycleMaps(rows, t));
}

/**
 * Build a date -> {cycle -> implied MMcf} map for an oac-proxy terminal:
 *   implied_flow(cycle) = (design ?? opcap) − oac   [Dth -> MMcf]
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {Object} t
 * @returns {Object<string, Object<string, number>>}
 */
export function buildProxyImpliedByDate(rows, t) {
  const { kindPrefixes } = terminalSeriesPrefixes(t);
  const rawByDate = {};
  rows.forEach((r) => {
    const sid = r.series_id.toLowerCase();
    for (const kind of Object.keys(kindPrefixes)) {
      const p = kindPrefixes[kind];
      if (!sid.startsWith(p)) continue;
      const cycle = sid.slice(p.length).toLowerCase();
      if (!rawByDate[r.period]) rawByDate[r.period] = {};
      if (!rawByDate[r.period][cycle]) rawByDate[r.period][cycle] = {};
      rawByDate[r.period][cycle][kind] = Number(r.value);
      break;
    }
  });

  const impliedByDate = {};
  Object.keys(rawByDate).forEach((dateStr) => {
    Object.keys(rawByDate[dateStr]).forEach((cycle) => {
      const parts = rawByDate[dateStr][cycle];
      const capacity =
        parts.design !== undefined ? parts.design : parts.opcap;
      if (capacity === undefined || parts.oac === undefined) return;
      if (!impliedByDate[dateStr]) impliedByDate[dateStr] = {};
      impliedByDate[dateStr][cycle] = dth_to_mmcf(capacity - parts.oac);
    });
  });
  return impliedByDate;
}

/**
 * Build a daily series for an oac-proxy terminal.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {Object} t
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailyProxySeries(rows, t) {
  return buildDailyFromCycles(buildProxyImpliedByDate(rows, t));
}

/**
 * Collapse a date -> {cycle -> MMcf} map into a chronologically sorted daily
 * series keeping the highest-priority cycle per day.
 *
 * @param {Object<string, Object<string, number>>} byDate
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailyFromCycles(byDate) {
  const out = [];
  Object.keys(byDate).forEach((dateStr) => {
    const cycles = byDate[dateStr];
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
      out.push({ dateStr, date: new Date(dateStr), value: cycles[best], cycle: best });
    }
  });
  out.sort((a, b) => a.date.getTime() - b.date.getTime());
  return out;
}

/**
 * Build the combined daily series for one feed entry of a multi-feed terminal.
 *
 * @param {Object} bundle
 * @param {{source: string, series: string}} feed
 * @returns {Array<{dateStr: string, date: Date, value: number}>}
 */
export function buildFeedDaily(bundle, feed) {
  const src = bundle.sources?.[feed.source];
  if (!src || !src.data) return [];
  const prefix = `${feed.series.toLowerCase()}_`;
  const byDate = {};
  src.data.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const cycle = sid.slice(prefix.length);
    if (!byDate[r.period]) byDate[r.period] = {};
    byDate[r.period][cycle] = dth_to_mmcf(Number(r.value));
  });
  const out = [];
  Object.keys(byDate).forEach((dateStr) => {
    const cycles = byDate[dateStr];
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
      out.push({ dateStr, date: new Date(dateStr), value: cycles[best] });
    }
  });
  out.sort((a, b) => a.date.getTime() - b.date.getTime());
  return out;
}

/**
 * Shared summary math for a finished daily series.
 *
 * @param {Array<{dateStr: string, date: Date, value: number}>} daily
 * @param {number} nameplate
 * @param {Object|null} headlineSource
 * @returns {{ok: boolean, latest: number, utilPct: number, spark: number[],
 *   wow: {delta: number, pct: number}|null, days: number}}
 */
export function summarizeDaily(daily, nameplate, headlineSource = null) {
  const latestPoint = headlineSource || daily[daily.length - 1];
  const latest = latestPoint.value;
  const utilPct = (latest / nameplate) * 100;

  const sparkWindow = daily.slice(-8);
  const spark = sparkWindow.map((d) => d.value);

  let wow = null;
  const target = new Date(latestPoint.date);
  target.setDate(target.getDate() - 7);
  const targetStr = target.toISOString().slice(0, 10);
  const weekAgo = daily.find((d) => d.dateStr === targetStr);
  if (weekAgo) {
    wow = {
      delta: latest - weekAgo.value,
      pct: weekAgo.value !== 0 ? ((latest - weekAgo.value) / weekAgo.value) * 100 : 0,
    };
  }
  return { ok: true, latest, utilPct, spark, wow, days: daily.length };
}

/**
 * Compute per-terminal summary metrics used by cards + the aggregate line.
 *
 * @param {Object} bundle
 * @param {Object} t
 * @returns {{ok: boolean, latest?: number, utilPct?: number, spark?: number[],
 *   wow?: {delta: number, pct: number}, days?: number, daily?: Array<{dateStr: string, date: Date, value: number}>}}
 */
export function terminalSummary(bundle, t) {
  if (Array.isArray(t.feeds) && t.feeds.length > 0) {
    const summable = t.feeds.filter((f) => {
      const kind = f.kind;
      if (kind === 'comparison' || kind === 'proxy' || kind === 'context') return false;
      return !COMPARISON_FEED_EXCLUSIONS.some((stem) =>
        f.series.toLowerCase().startsWith(stem)
      );
    });
    const feedDailies = [];
    for (const feed of summable) {
      const d = buildFeedDaily(bundle, feed);
      if (d.length) feedDailies.push(d);
    }
    if (feedDailies.length === 0) return { ok: false };

    const byDate = {};
    feedDailies.forEach((daily) => {
      daily.forEach((d) => {
        if (!byDate[d.dateStr]) byDate[d.dateStr] = { date: d.date, total: 0 };
        byDate[d.dateStr].total += d.value;
      });
    });
    const merged = Object.entries(byDate)
      .map(([dateStr, v]) => ({ dateStr, date: v.date, value: v.total }))
      .sort((a, b) => a.date.getTime() - b.date.getTime());
    if (merged.length === 0) return { ok: false };

    const nFeeds = feedDailies.length;
    const counts = {};
    feedDailies.forEach((daily) => {
      daily.forEach((d) => {
        counts[d.dateStr] = (counts[d.dateStr] || 0) + 1;
      });
    });
    let headlineSource = null;
    for (let i = merged.length - 1; i >= 0; i--) {
      if (counts[merged[i].dateStr] === nFeeds) {
        headlineSource = merged[i];
        break;
      }
    }
    const sum = summarizeDaily(merged, t.nameplate, headlineSource);
    sum.daily = merged;
    return sum;
  }

  const src = bundle.sources?.[t.source];
  if (!src || !src.data || !src.data.length) return { ok: false };
  const daily =
    t.signal === 'oac-proxy'
      ? buildDailyProxySeries(src.data, t)
      : buildDailySqSeries(src.data, t);
  if (daily.length === 0) return { ok: false };
  const sum = summarizeDaily(daily, t.nameplate);
  sum.daily = daily;
  return sum;
}
