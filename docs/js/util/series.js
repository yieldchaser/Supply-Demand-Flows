/**
 * Series utilities — query helpers for the long-format curated data schema.
 * Schema: source | series_id | series_name | period | value | unit | region | ingested_at
 */

import { quantileSorted } from './season.js';

/**
 * Filter rows from a source by series_id, returning chronologically sorted records.
 * @param {Array} rows - bundle.sources[sourceKey].data
 * @param {string} seriesId - exact series_id match
 * @returns {Array<{period: Date, value: number, ...}>}
 */
export function getSeries(rows, seriesId) {
  return rows
    .filter((r) => r.series_id === seriesId)
    .map((r) => ({ ...r, period: new Date(r.period), value: Number(r.value) }))
    .sort((a, b) => a.period - b.period);
}

/**
 * Group rows by series_id, return a Map<series_id, sorted-rows>.
 */
export function groupBySeriesId(rows) {
  const map = new Map();
  for (const r of rows) {
    if (!map.has(r.series_id)) map.set(r.series_id, []);
    map.get(r.series_id).push(r);
  }
  for (const arr of map.values()) {
    for (const item of arr) {
      item.period = new Date(item.period);
      item.value = Number(item.value);
    }
    arr.sort((a, b) => a.period - b.period);
  }
  return map;
}

/**
 * Year that a series is "about": the calendar year of its latest observation.
 *
 * The reference year must come from the data, never from the wall clock. A
 * bundle built in December and read in January used to relabel last week's
 * storage as "last year", which emptied the envelope and blanked the chart.
 */
export function referenceYear(rows) {
  const latest = referenceDate(rows);
  return latest === null ? null : latest.getFullYear();
}

/**
 * Latest observation across one or more row arrays, as a Date (or null).
 *
 * Windows such as "last 3 years" or "last 24 months" must be anchored here,
 * not on the wall clock: a bundle built last quarter read next quarter would
 * slide its whole window forward and silently drop real history, and a chart
 * that redraws on resize would change shape without the data changing.
 * Accepts several series so a window covering production and consumption is
 * anchored on the newest row of either.
 */
export function referenceDate(...seriesList) {
  let max = null;
  for (const rows of seriesList) {
    for (const r of rows || []) {
      if (r == null) continue;
      const p = r.period instanceof Date ? r.period : new Date(r.period);
      const t = p.getTime();
      if (!Number.isFinite(t)) continue;
      if (max === null || t > max) max = t;
    }
  }
  return max === null ? null : new Date(max);
}

/**
 * For a weekly series, compute week-of-year statistics for a band overlay.
 * Returns Map<weekNumber, {min, max, mean, p25, p75, p90, count, n}> across
 * prior years.
 *
 * Two rules the previous version broke:
 *  - the reference year comes from the series (see `referenceYear`), not from
 *    `new Date()`;
 *  - quartiles are interpolated (Hyndman-Fan type 7), because indexing into the
 *    sorted sample returns an order statistic, not a quartile — with 4 years of
 *    history `sorted[floor(4*0.25)]` is the *minimum*, not p25.
 */
export function computeWeeklyEnvelope(rows, lookbackYears = 5, opts = {}) {
  const currentYear = opts.referenceYear ?? referenceYear(rows);
  if (currentYear === null) return new Map();
  const cutoffYear = currentYear - lookbackYears;
  const filtered = (rows || []).filter((r) => {
    const p = r.period instanceof Date ? r.period : new Date(r.period);
    const yr = p.getFullYear();
    return Number.isFinite(p.getTime()) && yr < currentYear && yr >= cutoffYear;
  });

  const byWeek = new Map();
  for (const r of filtered) {
    const p = r.period instanceof Date ? r.period : new Date(r.period);
    const wk = isoWeek(p);
    const v = Number(r.value);
    if (!Number.isFinite(v)) continue;
    if (!byWeek.has(wk)) byWeek.set(wk, []);
    byWeek.get(wk).push(v);
  }

  const envelope = new Map();
  for (const [wk, vals] of byWeek) {
    const sorted = [...vals].sort((a, b) => a - b);
    envelope.set(wk, {
      min: sorted[0],
      max: sorted[sorted.length - 1],
      mean: vals.reduce((s, v) => s + v, 0) / vals.length,
      p25: quantileSorted(sorted, 0.25),
      p75: quantileSorted(sorted, 0.75),
      p90: quantileSorted(sorted, 0.90),
      count: vals.length,
      n: vals.length,
    });
  }
  return envelope;
}

/**
 * ISO week number (1-53) for a given date.
 */
export function isoWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
}

/**
 * Compute year-over-year deltas for a series.
 * Returns rows enriched with .yoyDelta and .yoyPct fields.
 */
export function computeYoY(rows) {
  return rows.map((r) => {
    const yearAgo = new Date(r.period.getFullYear() - 1, r.period.getMonth(), r.period.getDate());
    const match = rows
      .filter((p) => Math.abs(p.period - yearAgo) < 7 * 86400000)
      .sort((a, b) => Math.abs(a.period - yearAgo) - Math.abs(b.period - yearAgo))[0];
    return {
      ...r,
      yoyDelta: match ? r.value - match.value : null,
      yoyPct: match && match.value !== 0 ? (r.value - match.value) / match.value : null,
    };
  });
}
