/**
 * Pure data-shaping functions for LNG Feed Substitution (Section 6/7).
 * Zero DOM, zero D3 dependencies — fully testable in Node.js.
 */

import { cyclePriority } from './lng-downtime.js';

export const N_SHARE_PTS = 10;   // a feed's share moved >= 10 points
export const M_TOTAL_PCT = 8;    // total changed within +/- 8%

/**
 * Pick the highest-priority cycle value per gas day for a feed stem.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {string} stem
 * @returns {Map<string, {pri: number, value: number, date: string}>}
 */
export function dailyFromFeed(rows, stem) {
  const byDate = new Map();
  for (const r of rows) {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(stem.toLowerCase() + '_')) continue;
    if (!sid.includes('_d_')) continue;
    const cyc = sid.slice(stem.toLowerCase().length + 1);
    const cycTok = cyc.split('_')[0];
    const date = r.period instanceof Date ? r.period.toISOString().slice(0, 10) : String(r.period).slice(0, 10);
    const val = Number(r.value);
    const pri = cyclePriority(cycTok);
    if (pri <= 0) continue;
    if (!byDate.has(date) || pri > byDate.get(date).pri) {
      byDate.set(date, { pri, value: val, date });
    }
  }
  return byDate;
}

/**
 * Align feeds to common dates.
 *
 * @param {Object} bundle
 * @param {Array<{src: string, stem: string, label: string}>} feeds
 * @returns {{ dates: string[], perFeed: Map<string, Map<string, {value: number}>>, total: Map<string, number>, feeds: Array<any> }}
 */
export function alignFeeds(bundle, feeds) {
  const perFeed = new Map();
  let common = null;
  for (const f of feeds) {
    const src = bundle.sources?.[f.src];
    const rows = src?.data ?? [];
    const daily = dailyFromFeed(rows, f.stem);
    perFeed.set(f.label, daily);
    const dates = new Set(daily.keys());
    common = common === null ? dates : new Set([...common].filter((d) => dates.has(d)));
  }
  const dates = common ? [...common].sort() : [];
  const total = new Map();
  for (const d of dates) {
    let t = 0;
    for (const f of feeds) t += perFeed.get(f.label)?.get(d)?.value ?? 0;
    total.set(d, t);
  }
  return { dates, perFeed, total, feeds };
}

/**
 * Build share+total records over common dates.
 *
 * @param {{ dates: string[], perFeed: Map<string, Map<string, {value: number}>>, total: Map<string, number>, feeds: Array<any> }} aligned
 * @returns {Array<Object>}
 */
export function buildShareSeries(aligned) {
  const { dates, perFeed, total, feeds } = aligned;
  return dates.map((d) => {
    const tot = total.get(d) || 0;
    const entry = { date: d, total: tot, totalDth: tot };
    for (const f of feeds) {
      const v = perFeed.get(f.label)?.get(d)?.value ?? 0;
      entry[f.label] = tot > 0 ? (v / tot) * 100 : 0;
      entry[`${f.label}__val`] = v;
    }
    return entry;
  });
}

/**
 * Detect substitution events per the tuned thresholds.
 *
 * @param {Array<Object>} records
 * @param {Array<{label: string}>} feeds
 * @param {number} nSharePts
 * @param {number} mTotalPct
 * @returns {Array<Object>}
 */
export function detectSubstitutionEvents(records, feeds, nSharePts = N_SHARE_PTS, mTotalPct = M_TOTAL_PCT) {
  const events = [];
  for (let i = 1; i < records.length; i++) {
    const cur = records[i];
    const prev = records[i - 1];
    const totChg = prev.total > 0 ? ((cur.total - prev.total) / prev.total) * 100 : 0;
    if (Math.abs(totChg) > mTotalPct) continue;

    const bothActive = feeds.every((f) => {
      const cv = cur[`${f.label}__val`] || 0;
      const pv = prev[`${f.label}__val`] || 0;
      return cv > 0 && pv > 0;
    });
    if (!bothActive) continue;

    let maxMover = null;
    let maxChg = 0;
    for (const f of feeds) {
      const d = cur[f.label] - prev[f.label];
      if (Math.abs(d) > Math.abs(maxChg)) { maxChg = d; maxMover = f; }
    }
    if (maxMover && Math.abs(maxChg) >= nSharePts) {
      const others = feeds.filter((f) => f !== maxMover);
      const othersDown = others.every((f) => cur[f.label] < prev[f.label] - 1);
      const kind = othersDown ? 'supply' : 'routing';
      events.push({
        date: cur.date,
        mover: maxMover.label,
        moverChg: maxChg,
        totalChg: totChg,
        kind,
        shares: feeds.map((f) => ({ label: f.label, from: prev[f.label], to: cur[f.label] })),
      });
    }
  }
  return events;
}
