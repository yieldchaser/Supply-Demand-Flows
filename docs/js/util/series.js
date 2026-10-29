/**
 * Series utilities — query helpers for the long-format curated data schema.
 * Schema: source | series_id | series_name | period | value | unit | region | ingested_at
 */

import { parsePeriod, withParsedPeriods } from './period.js';

/**
 * Filter rows from a source by series_id, returning chronologically sorted records.
 *
 * Periods are read with `parsePeriod`, the project's single parsing rule. The
 * previous `new Date(r.period)` read a bare `YYYY-MM-DD` as UTC midnight, which
 * shifts the label a day west of Greenwich and can move a month-boundary row
 * into the previous month. Rows whose period cannot be read are dropped rather
 * than sorted as `Invalid Date`.
 *
 * @param {Array} rows - bundle.sources[sourceKey].data
 * @param {string} seriesId - exact series_id match
 * @returns {Array<{period: Date, value: number, ...}>}
 */
export function getSeries(rows, seriesId) {
  const matching = (rows || []).filter((r) => r.series_id === seriesId);
  const { rows: parsed } = withParsedPeriods(matching);
  return parsed
    .map((r) => ({ ...r, value: Number(r.value) }))
    .sort((a, b) => a.period - b.period);
}

/**
 * Group rows by series_id, return a Map<series_id, sorted-rows>.
 *
 * Same parsing rule as `getSeries`, and the same row objects are copied rather
 * than mutated: a caller that later hands these rows to one of the envelope
 * helpers previously found their `period` field already replaced.
 */
export function groupBySeriesId(rows) {
  const map = new Map();
  for (const r of rows || []) {
    if (!map.has(r.series_id)) map.set(r.series_id, []);
    map.get(r.series_id).push(r);
  }
  for (const [id, arr] of map) {
    const { rows: parsed } = withParsedPeriods(arr);
    map.set(id, parsed.sort((a, b) => a.period - b.period));
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
      const p = r.period instanceof Date ? r.period : parsePeriod(r.period);
      if (!p) continue;
      const t = p.getTime();
      if (!Number.isFinite(t)) continue;
      if (max === null || t > max) max = t;
    }
  }
  return max === null ? null : new Date(max);
}

// `computeWeeklyEnvelope` and `isoWeek` used to live here. They bucketed storage
// weeks by ISO week number, which is not the axis these series live on: the EIA
// week ends on Friday and its ISO number shifts an extra week between years, so
// a reading could be compared against a normal from a different week of the
// season. `seasonalEnvelope`/`seasonWeek` in season.js (week-of-season counted
// from each year's first observation) replaced it, and nothing should reintroduce
// the ISO-keyed band.

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
