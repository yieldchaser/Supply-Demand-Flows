/**
 * EU storage analysis — every number the European surface prints.
 *
 * Pure and synchronous: no DOM, no `d3`, no `Date.now()`. The panel draws what
 * this module returns and the Node tests assert the same values, so a change in
 * methodology has to pass a test rather than quietly change a chart.
 *
 * Units, as reported by GIE AGSI+ in this bundle:
 *   full_pct          % of a country's working-gas capacity, daily
 *   gas_in_storage    TWh, daily
 *   injection         GWh per day, daily
 *   withdrawal        GWh per day, daily
 *
 * Two things this module refuses to do:
 *
 *  1. **Invent capacity.** Working gas is `gas_in_storage / full_pct × 100`,
 *     derived per country per day from the two series that both exist. There is
 *     no hard-coded TWh ceiling anywhere.
 *
 *  2. **Call four years five.** AGSI history in this bundle starts 2021-01-01,
 *     so a "normal" for a 2026 week can draw on at most four or five *prior*
 *     years. Every comparison returns its own `years` array and the UI prints
 *     exactly that, never a constant.
 */

import { dayOnly } from './season.js';

/** The country set that is actually in the bundle; labels are ours, codes are GIE's. */
export const EU_COUNTRIES = [
  { cc: 'de', label: 'Germany' },
  { cc: 'fr', label: 'France' },
  { cc: 'it', label: 'Italy' },
  { cc: 'nl', label: 'Netherlands' },
  { cc: 'es', label: 'Spain' },
  { cc: 'at', label: 'Austria' },
  { cc: 'be', label: 'Belgium' },
  { cc: 'cz', label: 'Czechia' },
  { cc: 'hu', label: 'Hungary' },
  { cc: 'pl', label: 'Poland' },
];

/** Days in a month, from the calendar (not a table). */
function daysInMonth(year, monthIndex) {
  return new Date(year, monthIndex + 1, 0).getDate();
}

/**
 * Calendar month index 1..12 — the axis AGSI history is naturally read on
 * ("where was Germany in mid-March?"), and one that is identical in every year,
 * so no leap-year or 53-week offset can shift a comparison by a week.
 */
export function monthOfDay(date) {
  return dayOnly(date).getMonth() + 1;
}

function monthDayFraction(date) {
  const d = dayOnly(date);
  return (d.getDate() - 0.5) / daysInMonth(d.getFullYear(), d.getMonth());
}

/**
 * Build one country's usable series by joining the four AGSI series on date.
 *
 * The join is on the calendar day, not on row order: `getSeries` sorts each
 * series independently, and a day present in `full_pct` but missing from
 * `gas_in_storage` would otherwise shift every later row by one and silently
 * pair volume from the wrong day with a percentage.
 *
 * @param {Record<string, Array<{period: Date, value: number}>>} series
 * @returns {Array<{period: Date, pct: number|null, twh: number|null,
 *   injectionGwh: number|null, withdrawalGwh: number|null}>} ascending by date
 */
export function joinCountrySeries({ pct = [], gas = [], injection = [], withdrawal = [] }) {
  const byDay = new Map();
  const put = (rows, field) => {
    for (const r of rows || []) {
      const p = r.period instanceof Date ? dayOnly(r.period) : null;
      if (!p || !Number.isFinite(p.getTime())) continue;
      const v = Number(r.value);
      if (!Number.isFinite(v)) continue;
      const key = p.getTime();
      if (!byDay.has(key)) byDay.set(key, { period: p, pct: null, twh: null, injectionGwh: null, withdrawalGwh: null });
      byDay.get(key)[field] = v;
    }
  };
  put(pct, 'pct');
  put(gas, 'twh');
  put(injection, 'injectionGwh');
  put(withdrawal, 'withdrawalGwh');
  return [...byDay.values()].sort((a, b) => a.period - b.period);
}

/**
 * Working-gas capacity in TWh for one day, derived from the two series.
 *
 * `gas / pct × 100` is exact under AGSI's own definition of the two fields.
 * It is only computed when the percentage is far enough from zero that the
 * division is meaningful; a 0.2 % reading would otherwise imply a capacity
 * ten million TWh wide.
 *
 * @param {number|null} twh
 * @param {number|null} pct
 * @param {number} [minPct] floor below which the ratio is not trusted
 * @returns {number|null}
 */
export function capacityFromDay(twh, pct, minPct = 1) {
  if (!Number.isFinite(twh) || !Number.isFinite(pct)) return null;
  if (pct < minPct || twh <= 0) return null;
  return (twh / pct) * 100;
}

/**
 * Robust capacity per country: the median of the per-day ratios, and the
 * sample counted.
 *
 * A single day's ratio carries the reporting rounding of both fields; the
 * median over the full history does not, and it is insensitive to the one day
 * a country reports an anomalous pair. The maximum day-to-day spread is
 * returned as `spreadPct` so the UI can say how stable the derivation is
 * rather than implying it is exact.
 *
 * @param {Array<{pct: number|null, twh: number|null}>} rows
 * @param {{minPct?: number}} [opts]
 * @returns {{capacityTwh: number|null, n: number, spreadPct: number|null}}
 */
export function countryCapacity(rows, { minPct = 1 } = {}) {
  const caps = [];
  for (const r of rows || []) {
    const c = capacityFromDay(r.twh, r.pct, minPct);
    if (c != null) caps.push(c);
  }
  if (caps.length === 0) return { capacityTwh: null, n: 0, spreadPct: null };
  caps.sort((a, b) => a - b);
  const median = caps.length % 2 ? caps[(caps.length - 1) / 2]
    : (caps[caps.length / 2 - 1] + caps[caps.length / 2]) / 2;
  const spreadPct = ((caps[caps.length - 1] - caps[0]) / median) * 100;
  return { capacityTwh: median, n: caps.length, spreadPct };
}

/**
 * Build every country's view at once, including its derived capacity.
 *
 * @param {Record<string, Record<string, Array<{period: Date, value: number}>>>} perCountry
 * @returns {Array<{cc: string, label: string, rows: Array, capacityTwh: number|null,
 *   capacityN: number, capacitySpreadPct: number|null, latest: object|null}>}
 */
export function buildCountryViews(perCountry, countries = EU_COUNTRIES) {
  const out = [];
  for (const c of countries) {
    const s = perCountry?.[c.cc];
    if (!s) continue;
    const rows = joinCountrySeries(s);
    if (rows.length === 0) continue;
    const cap = countryCapacity(rows);
    let latest = null;
    for (let i = rows.length - 1; i >= 0; i--) {
      if (Number.isFinite(rows[i].pct)) { latest = rows[i]; break; }
    }
    out.push({
      cc: c.cc,
      label: c.label,
      rows,
      capacityTwh: cap.capacityTwh,
      capacityN: cap.n,
      capacitySpreadPct: cap.spreadPct,
      latest,
    });
  }
  return out;
}

/**
 * Panel aggregate: sum(TWh) over sum(derived capacity), per day.
 *
 * The previous panel weighted each country's % by its all-time maximum
 * `gas_in_storage`. That proxy is close (the probe measured a 0.03 pp drift on
 * the latest day) but it is a different quantity from capacity and it cannot
 * produce a TWh total at all. Summing volumes and dividing by the summed
 * derived capacity gives both, and a percentage that is exactly the weighted
 * mean of the parts.
 *
 * `nCountries` and `missing` are returned per day because the aggregate is a
 * mean over whoever reported: a late-joining country would otherwise change
 * the composition of the number without changing its label.
 *
 * @param {ReturnType<typeof buildCountryViews>} views
 * @returns {Array<{period: Date, pct: number, twh: number, capacityTwh: number,
 *   nCountries: number, missing: string[]}>}
 */
export function buildAggregate(views) {
  const byDay = new Map();
  for (const v of views) {
    const cap = v.capacityTwh;
    if (!Number.isFinite(cap) || cap <= 0) continue;
    for (const r of v.rows) {
      if (!Number.isFinite(r.twh) || !Number.isFinite(r.pct)) continue;
      const key = r.period.getTime();
      if (!byDay.has(key)) {
        byDay.set(key, { period: r.period, twh: 0, capacityTwh: 0, present: new Set() });
      }
      const slot = byDay.get(key);
      slot.twh += r.twh;
      slot.capacityTwh += cap;
      slot.present.add(v.cc);
    }
  }
  const out = [];
  for (const [, slot] of byDay) {
    if (slot.capacityTwh <= 0) continue;
    out.push({
      period: slot.period,
      twh: slot.twh,
      capacityTwh: slot.capacityTwh,
      pct: (slot.twh / slot.capacityTwh) * 100,
      nCountries: slot.present.size,
      missing: views.filter((v) => !slot.present.has(v.cc)).map((v) => v.cc),
    });
  }
  return out.sort((a, b) => a.period - b.period);
}

/**
 * Same-point-of-year normal over the prior years the bundle actually holds.
 *
 * Comparison is by **calendar month-day position** (day-of-year, so a leap year
 * still lines up in late December). For each prior year the nearest observation
 * within `toleranceDays` contributes; a year with no observation that close is
 * absent from `years` rather than interpolated.
 *
 * @param {Array<{period: Date, value: number}>} rows
 * @param {Date} asOf
 * @param {{lookbackYears?: number, toleranceDays?: number}} [opts]
 * @returns {{values: Array<{year: number, period: Date, value: number}>,
 *   years: number[], median: number|null, min: number|null, max: number|null,
 *   n: number}}
 */
export function positionalNormal(rows, asOf, { lookbackYears = 5, toleranceDays = 10 } = {}) {
  const d = dayOnly(asOf);
  const year = d.getFullYear();
  const minYear = year - lookbackYears;
  const doy = dayOfYear(d);
  const values = [];
  for (let y = year - 1; y >= minYear; y--) {
    let best = null;
    for (const r of rows || []) {
      const p = r.period instanceof Date ? r.period : null;
      if (!p || p.getFullYear() !== y) continue;
      if (!Number.isFinite(r.value)) continue;
      // day-of-year difference, wrapped through the year boundary
      let lag = Math.abs(dayOfYear(p) - doy);
      const len = isLeap(y) ? 366 : 365;
      lag = Math.min(lag, len - lag);
      if (lag > toleranceDays) continue;
      if (!best || lag < best.lag) best = { lag, row: r };
    }
    if (best) {
      values.push({ year: y, period: best.row.period, value: best.row.value });
    }
  }
  const sorted = values.map((v) => v.value).sort((a, b) => a - b);
  const median = sorted.length === 0 ? null
    : sorted.length % 2 ? sorted[(sorted.length - 1) / 2]
      : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2;
  return {
    values,
    years: values.map((v) => v.year),
    n: values.length,
    median,
    min: sorted.length ? sorted[0] : null,
    max: sorted.length ? sorted[sorted.length - 1] : null,
  };
}

function isLeap(y) {
  return (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
}

/** 1-based day of year for a local date. */
export function dayOfYear(date) {
  const d = dayOnly(date);
  const start = new Date(d.getFullYear(), 0, 1);
  return Math.round((d - start) / 86400000) + 1;
}

/**
 * Where a level ranks among the prior years at the same point of the year.
 * `better` = how many prior years were lower (0 means this is the lowest).
 */
export function rankAmongYears(value, values) {
  const vals = values.filter((v) => Number.isFinite(v));
  if (vals.length === 0 || !Number.isFinite(value)) return { rank: null, n: 0 };
  const below = vals.filter((v) => v < value).length;
  return { rank: below + 1, n: vals.length };
}

/**
 * Calendar-month envelope over prior years, for the "history by year" chart.
 *
 * Each month gets, over the observations of that month in prior years, the
 * min / median / max plus the years that contributed. This is the band the
 * country detail draws behind the current year's line; it is positional, not a
 * trend, and `nYears` is printed on screen.
 *
 * @param {Array<{period: Date, value: number}>} rows
 * @param {number} currentYear
 * @param {number} [nMonths]
 * @returns {Array<{month: number, min: number|null, median: number|null,
 *   max: number|null, years: number[], nYears: number}>}
 */
export function monthlyEnvelope(rows, currentYear, nMonths = 12) {
  const out = [];
  for (let m = 1; m <= nMonths; m++) {
    const vals = [];
    const years = new Set();
    for (const r of rows || []) {
      const p = r.period instanceof Date ? r.period : null;
      if (!p || p.getMonth() + 1 !== m) continue;
      if (p.getFullYear() >= currentYear) continue;
      if (!Number.isFinite(r.value)) continue;
      vals.push(r.value);
      years.add(p.getFullYear());
    }
    vals.sort((a, b) => a - b);
    const median = vals.length === 0 ? null
      : vals.length % 2 ? vals[(vals.length - 1) / 2]
        : (vals[vals.length / 2 - 1] + vals[vals.length / 2]) / 2;
    out.push({
      month: m,
      min: vals.length ? vals[0] : null,
      max: vals.length ? vals[vals.length - 1] : null,
      median,
      years: [...years].sort((a, b) => a - b),
      nYears: years.size,
    });
  }
  return out;
}

/**
 * Split a country's history into one array per calendar year, thinned to a
 * whole-year positional axis (`monthDayFraction`, 0..1) so every year is drawn
 * on the same horizontal scale.
 *
 * @param {Array<{period: Date, value: number}>} rows
 * @param {(row: object) => number} pick
 * @returns {Array<{year: number, points: Array<{x: number, period: Date, value: number}>, n: number}>}
 */
export function rowsByYear(rows, pick = (r) => r.pct) {
  const byYear = new Map();
  for (const r of rows || []) {
    const v = pick(r);
    if (!Number.isFinite(v)) continue;
    const y = r.period.getFullYear();
    if (!byYear.has(y)) byYear.set(y, []);
    byYear.get(y).push({ x: monthDayFraction(r.period), period: r.period, value: v });
  }
  return [...byYear.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([year, points]) => ({
      year,
      points: points.sort((a, b) => a.x - b.x),
      n: points.length,
    }));
}

/**
 * Net daily flow implied by consecutive storage readings, in GWh/d.
 *
 * The panel also uses GIE's own `injection` and `withdrawal` fields, which are
 * reported flows rather than differences and can therefore disagree with the
 * stock path in the last day of a reporting cycle. Where they do, the reported
 * fields win and this derived net is shown only as a cross-check; refusing to
 * print a contradiction is better than printing two "flows".
 */
export function stockImpliedFlow(rows) {
  const out = [];
  for (let i = 1; i < (rows || []).length; i++) {
    const a = rows[i - 1];
    const b = rows[i];
    if (!Number.isFinite(a.twh) || !Number.isFinite(b.twh)) continue;
    const days = Math.round((b.period - a.period) / 86400000);
    if (days <= 0) continue;
    out.push({
      period: b.period,
      gwhPerDay: ((b.twh - a.twh) * 1000) / days,
      days,
    });
  }
  return out;
}

/**
 * About how many days of EU consumption the stock covers.
 *
 * Volumetric: TWh ÷ daily consumption in TWh/d. Only the caller that has a
 * demand series may compute it, and the conversion factor is passed in rather
 * than embedded here, so the assumption is visible at the call site.
 */
export function daysOfCover(twh, demandGwhPerDay) {
  if (!Number.isFinite(twh) || !Number.isFinite(demandGwhPerDay) || demandGwhPerDay <= 0) return null;
  return (twh * 1000) / demandGwhPerDay;
}
