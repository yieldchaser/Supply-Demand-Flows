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
 * Methodological rules this module enforces:
 *
 *  1. **Capacity is read per day, not assumed constant.** GIE's `full` is
 *     `gasInStorage ÷ workingGasVolume` on that day, so `gas ÷ pct × 100` recovers
 *     the day's working-gas volume. Capacities move materially inside the window
 *     this bundle covers (Czechia ~36 → ~46 TWh, France ~134 → ~124 TWh,
 *     Belgium ~8.8 → ~7.6 TWh), so a single all-history median capacity biased
 *     the aggregate % and every pp → TWh conversion. Each day now carries its
 *     own capacity; days with a fill too low for the ratio to be stable borrow
 *     the nearest stable day's capacity.
 *
 *  2. **Deviations are computed in the unit they are printed in.** A TWh
 *     deviation is `stock − normal stock` on the TWh series; a pp deviation is
 *     `fill − normal fill` on the % series. Converting pp to TWh through a
 *     capacity is only exact when capacity never changed.
 *
 *  3. **"Normal" is the same calendar day in prior years, and its sample is
 *     printed.** AGSI history here starts 2021-01-01, so a normal for 2026 has
 *     at most five prior years, and fewer for earlier dates. Every normal
 *     returns its `years`; series that need a stable comparison require a
 *     minimum `n` and say so.
 *
 *  4. **Projections are analogs, not forecasts.** The end-of-season outlook
 *     applies each prior year's *observed* net change over the rest of the
 *     season to today's stock. It answers "if the remaining weeks repeat year
 *     X", with every analog listed, and makes no claim about which is likely.
 */

import { dayOnly } from './season.js';

const DAY_MS = 86400000;

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

/** Identifier of the synthetic 10-country total view. */
export const AGGREGATE_CC = 'eu10';

/**
 * EU-wide fill reference. Regulation (EU) 2022/1032 set a 90 % filling target;
 * the 2025 amendment lets it be met at any point between 1 October and
 * 1 December, with permitted deviations. National obligations differ, so the
 * panel draws this as a reference level, never as a per-country compliance test.
 */
export const FILL_TARGET_PCT = 90;

/** Below this fill the day's `gas ÷ pct` ratio is too sensitive to rounding. */
const CAPACITY_MIN_PCT = 3;

/* ------------------------------------------------------------------ */
/*  Calendar helpers                                                   */
/* ------------------------------------------------------------------ */

/** Days in a month, from the calendar (not a table). */
function daysInMonth(year, monthIndex) {
  return new Date(year, monthIndex + 1, 0).getDate();
}

function isLeap(y) {
  return (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
}

/** Calendar month index 1..12. */
export function monthOfDay(date) {
  return dayOnly(date).getMonth() + 1;
}

/** 1-based day of year for a local date. */
export function dayOfYear(date) {
  const d = dayOnly(date);
  const start = new Date(d.getFullYear(), 0, 1);
  return Math.round((d - start) / DAY_MS) + 1;
}

/**
 * Continuous position of a calendar date on a 0..1 January-to-December axis.
 * Each month is 1/12th of the axis and days are centred within it, so a given
 * month-day sits at the same x in leap and non-leap years.
 */
export function monthDayFraction(date) {
  const d = dayOnly(date);
  return (d.getMonth() + (d.getDate() - 0.5) / daysInMonth(d.getFullYear(), d.getMonth())) / 12;
}

/** Explicit alias for calendar position on the 0..1 axis. */
export const calendarFraction = monthDayFraction;

/**
 * Storage year a date belongs to: the year in which its injection season
 * started. 1 Apr 2026 – 31 Mar 2027 is storage year 2026 ("2026/27").
 *
 * European storage is a cycle that fills April–September and draws
 * October–March. On a January-to-December axis the winter being watched is
 * cut in two; on a storage-year axis it is one continuous line.
 */
export function storageYearOf(date) {
  const d = dayOnly(date);
  return d.getMonth() >= 3 ? d.getFullYear() : d.getFullYear() - 1;
}

/** "2026/27" style label. */
export function storageYearLabel(sy) {
  return `${sy}/${String((sy + 1) % 100).padStart(2, '0')}`;
}

/**
 * 0..1 position on the April-to-March storage-year axis, month-aligned like
 * `monthDayFraction` (each month 1/12th, days centred), so 1 March is the same
 * x whether or not the year has a 29 February.
 */
export function storageYearFraction(date) {
  const d = dayOnly(date);
  const m = (d.getMonth() + 9) % 12; // Apr → 0 … Mar → 11
  return (m + (d.getDate() - 0.5) / daysInMonth(d.getFullYear(), d.getMonth())) / 12;
}

/** First day of a storage year. */
export function storageYearStart(sy) {
  return new Date(sy, 3, 1);
}

/** Last day of a storage year. */
export function storageYearEnd(sy) {
  return new Date(sy + 1, 2, 31);
}

/**
 * Which half of the storage year a date falls in, and where that half ends.
 * Injection season: 1 Apr – 30 Sep. Withdrawal season: 1 Oct – 31 Mar.
 */
export function seasonOf(date) {
  const d = dayOnly(date);
  const m = d.getMonth();
  const y = d.getFullYear();
  if (m >= 3 && m <= 8) {
    return { kind: 'injection', start: new Date(y, 3, 1), end: new Date(y, 8, 30), endLabel: '30 Sep' };
  }
  const startYear = m >= 9 ? y : y - 1;
  return { kind: 'withdrawal', start: new Date(startYear, 9, 1), end: new Date(startYear + 1, 2, 31), endLabel: '31 Mar' };
}

/**
 * The same calendar day `k` years earlier. 29 February maps to 28 February in
 * a non-leap target year rather than spilling into March.
 */
export function sameDayYearsBack(date, k) {
  const d = dayOnly(date);
  const y = d.getFullYear() - k;
  const m = d.getMonth();
  return new Date(y, m, Math.min(d.getDate(), daysInMonth(y, m)));
}

/* ------------------------------------------------------------------ */
/*  Building the per-country and aggregate views                       */
/* ------------------------------------------------------------------ */

/**
 * Build one country's usable series by joining the four AGSI series on date.
 *
 * The join is on the calendar day, not on row order: a day present in
 * `full_pct` but missing from `gas_in_storage` would otherwise shift every
 * later row by one and silently pair volume from the wrong day with a percentage.
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
 * Attach a per-day capacity (`capTwh`) to joined rows.
 *
 * Days below `minPct` take the capacity of the nearest stable day (previous
 * first, then next) and are flagged `capFilled`, so nothing downstream divides
 * by a ratio that is mostly rounding noise.
 *
 * @param {Array} rows output of joinCountrySeries
 * @returns {Array} the same rows (copied) with `capTwh` and `capFilled`
 */
export function attachDailyCapacity(rows, { minPct = CAPACITY_MIN_PCT } = {}) {
  const out = (rows || []).map((r) => ({ ...r, capTwh: capacityFromDay(r.twh, r.pct, minPct), capFilled: false }));
  let last = null;
  for (const r of out) {
    if (r.capTwh != null) last = r.capTwh;
    else if (last != null) { r.capTwh = last; r.capFilled = true; }
  }
  let next = null;
  for (let i = out.length - 1; i >= 0; i--) {
    const r = out[i];
    if (r.capTwh != null && !r.capFilled) next = r.capTwh;
    else if (r.capTwh == null && next != null) { r.capTwh = next; r.capFilled = true; }
  }
  return out;
}

/**
 * Robust summary of a country's capacity history: median of the per-day
 * ratios plus first/latest stable values, so the UI can state how much the
 * capacity moved rather than implying a constant.
 *
 * @param {Array<{pct: number|null, twh: number|null}>} rows
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
  const med = median(caps);
  const spreadPct = ((caps[caps.length - 1] - caps[0]) / med) * 100;
  return { capacityTwh: med, n: caps.length, spreadPct };
}

/** Median of the stable capacities over the last `days` observations. */
function recentCapacity(rows, days = 30) {
  const caps = [];
  for (let i = rows.length - 1; i >= 0 && caps.length < days; i--) {
    if (rows[i].capTwh != null && !rows[i].capFilled) caps.push(rows[i].capTwh);
  }
  return caps.length ? median(caps.sort((a, b) => a - b)) : null;
}

/** First full storage year's median capacity, for the "capacity moved" note. */
function earliestCapacity(rows, days = 60) {
  const caps = [];
  for (let i = 0; i < rows.length && caps.length < days; i++) {
    if (rows[i].capTwh != null && !rows[i].capFilled) caps.push(rows[i].capTwh);
  }
  return caps.length ? median(caps.sort((a, b) => a - b)) : null;
}

function lastFinite(rows, field) {
  for (let i = rows.length - 1; i >= 0; i--) if (Number.isFinite(rows[i][field])) return rows[i];
  return null;
}

/**
 * Index a view's rows by local-midnight time for O(1) same-day lookups.
 * @returns {Map<number, object>}
 */
export function indexRows(rows) {
  const m = new Map();
  for (const r of rows || []) {
    if (r && r.period instanceof Date) m.set(dayOnly(r.period).getTime(), r);
  }
  return m;
}

function finalizeView(cc, label, rows, extra = {}) {
  const capacityTwh = recentCapacity(rows);
  const cap = countryCapacity(rows);
  return {
    cc,
    label,
    rows,
    index: indexRows(rows),
    capacityTwh,
    capacityFirstTwh: earliestCapacity(rows),
    capacityMedianTwh: cap.capacityTwh,
    capacityN: cap.n,
    capacitySpreadPct: cap.spreadPct,
    latest: lastFinite(rows, 'pct'),
    ...extra,
  };
}

/**
 * Build every country's view, each row carrying its own capacity.
 *
 * `capacityTwh` is the *current* capacity (median of the last 30 stable days);
 * `capacityFirstTwh` the earliest, so the UI can state how far it moved.
 *
 * @param {Record<string, Record<string, Array<{period: Date, value: number}>>>} perCountry
 */
export function buildCountryViews(perCountry, countries = EU_COUNTRIES) {
  const out = [];
  for (const c of countries) {
    const s = perCountry?.[c.cc];
    if (!s) continue;
    const joined = joinCountrySeries(s);
    if (joined.length === 0) continue;
    out.push(finalizeView(c.cc, c.label, attachDailyCapacity(joined)));
  }
  return out;
}

/**
 * Aggregate rows: Σ stock ÷ Σ that day's capacities, with summed flows.
 *
 * This is GIE's own aggregation rule, so the percentage is exactly the
 * capacity-weighted mean of the parts on that day. `nCountries`/`missing` are
 * returned per day because the aggregate is a sum over whoever reported.
 *
 * @param {ReturnType<typeof buildCountryViews>} views
 * @returns {Array<{period: Date, pct: number, twh: number, capTwh: number,
 *   capacityTwh: number, injectionGwh: number|null, withdrawalGwh: number|null,
 *   nCountries: number, missing: string[]}>}
 */
export function buildAggregate(views) {
  const byDay = new Map();
  for (const v of views) {
    for (const r of v.rows) {
      if (!Number.isFinite(r.twh) || !Number.isFinite(r.capTwh) || r.capTwh <= 0) continue;
      const key = r.period.getTime();
      if (!byDay.has(key)) {
        byDay.set(key, { period: r.period, twh: 0, capTwh: 0, inj: 0, wd: 0, nFlow: 0, present: new Set() });
      }
      const slot = byDay.get(key);
      slot.twh += r.twh;
      slot.capTwh += r.capTwh;
      if (r.injectionGwh != null || r.withdrawalGwh != null) {
        slot.inj += r.injectionGwh ?? 0;
        slot.wd += r.withdrawalGwh ?? 0;
        slot.nFlow += 1;
      }
      slot.present.add(v.cc);
    }
  }
  const out = [];
  for (const [, slot] of byDay) {
    if (slot.capTwh <= 0) continue;
    out.push({
      period: slot.period,
      twh: slot.twh,
      capTwh: slot.capTwh,
      capacityTwh: slot.capTwh,
      pct: (slot.twh / slot.capTwh) * 100,
      injectionGwh: slot.nFlow ? slot.inj : null,
      withdrawalGwh: slot.nFlow ? slot.wd : null,
      capFilled: false,
      nCountries: slot.present.size,
      missing: views.filter((v) => !slot.present.has(v.cc)).map((v) => v.cc),
    });
  }
  return out.sort((a, b) => a.period - b.period);
}

/**
 * The 10-country total as a view with the same shape as a country's, so every
 * chart and metric can be drawn for "the total" or "one country" alike.
 *
 * Days on which not every country reported are dropped from the view: a sum
 * over nine countries is a different quantity, and silently mixing the two
 * would put a step into the line that no physical flow produced.
 */
export function buildAggregateView(views) {
  const all = buildAggregate(views);
  const full = all.filter((r) => r.nCountries === views.length);
  return finalizeView(AGGREGATE_CC, `${views.length}-country total`, full, {
    isAggregate: true,
    droppedPartialDays: all.length - full.length,
  });
}

/**
 * Whether a prior-year comparison date falls before the selected baseline.
 * `minDate` (preferred) cuts on the exact day — e.g. 1 Apr 2022 for the
 * post-crisis baseline, so a January comparison does not pull in the
 * 2021/22 winter; `minYear` cuts on the calendar year.
 */
function beforeBaseline(date, minYear, minDate) {
  if (minDate instanceof Date && date < minDate) return true;
  return minYear != null && date.getFullYear() < minYear;
}

/* ------------------------------------------------------------------ */
/*  Normals                                                            */
/* ------------------------------------------------------------------ */

function median(sorted) {
  if (!sorted.length) return null;
  const n = sorted.length;
  return n % 2 ? sorted[(n - 1) / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
}

const pickPct = (r) => r?.value ?? r?.pct;

/**
 * Find the row for `date` in an index, allowing a small gap tolerance.
 * Exact day first, then ±1, ±2 … up to `toleranceDays`.
 */
function lookupNear(index, date, toleranceDays) {
  const t = dayOnly(date);
  const exact = index.get(t.getTime());
  if (exact) return exact;
  for (let k = 1; k <= toleranceDays; k++) {
    const a = index.get(new Date(t.getFullYear(), t.getMonth(), t.getDate() - k).getTime());
    if (a) return a;
    const b = index.get(new Date(t.getFullYear(), t.getMonth(), t.getDate() + k).getTime());
    if (b) return b;
  }
  return null;
}

/**
 * Same-day normal over prior years.
 *
 * For each of the `lookbackYears` prior years the observation on the same
 * calendar day (nearest within `toleranceDays` when that day is missing)
 * contributes; a year with nothing that close is absent from `years` rather
 * than interpolated.
 *
 * Accepts either rows or a prebuilt index (`indexRows`) as the first argument;
 * the index form is what the charts use, since they evaluate a normal for
 * every day of a year.
 *
 * @param {Array|Map} rowsOrIndex
 * @param {Date} asOf
 * @param {{lookbackYears?: number, toleranceDays?: number, pick?: Function, minYear?: number|null}} [opts]
 * @returns {{values: Array<{year: number, period: Date, value: number}>,
 *   years: number[], median: number|null, mean: number|null,
 *   min: number|null, max: number|null, n: number}}
 */
export function positionalNormal(rowsOrIndex, asOf, { lookbackYears = 5, toleranceDays = 1, pick = pickPct, minYear = null, minDate = null } = {}) {
  const index = rowsOrIndex instanceof Map ? rowsOrIndex : indexRows(rowsOrIndex);
  const d = dayOnly(asOf);
  const values = [];
  for (let k = 1; k <= lookbackYears; k++) {
    const target = sameDayYearsBack(d, k);
    if (beforeBaseline(target, minYear, minDate)) break;
    const row = lookupNear(index, target, toleranceDays);
    if (!row) continue;
    const v = pick(row);
    if (!Number.isFinite(v)) continue;
    values.push({ year: target.getFullYear(), period: row.period, value: v });
  }
  const sorted = values.map((v) => v.value).sort((a, b) => a - b);
  return {
    values,
    years: values.map((v) => v.year),
    n: values.length,
    median: median(sorted),
    mean: sorted.length ? sorted.reduce((s, x) => s + x, 0) / sorted.length : null,
    min: sorted.length ? sorted[0] : null,
    max: sorted.length ? sorted[sorted.length - 1] : null,
  };
}

/**
 * Where a level ranks among the prior years at the same point of the year.
 * `rank` 1 = lower than every prior year; `n + 1` = higher than all.
 */
export function rankAmongYears(value, values) {
  const vals = values.filter((v) => Number.isFinite(v));
  if (vals.length === 0 || !Number.isFinite(value)) return { rank: null, n: 0 };
  const below = vals.filter((v) => v < value).length;
  return { rank: below + 1, n: vals.length };
}

/**
 * Daily seasonal band for one storage year: for every day from 1 April to
 * 31 March, the min / median / max / mean of the same calendar day in prior
 * years. Drawn behind the year lines.
 *
 * This replaces a calendar-month envelope that pooled all thirty days of a
 * month across years: early-January and late-January levels (often 15 pp
 * apart in winter) were thrown into one bucket, so the band was wider than the
 * year-to-year spread it claimed to show, and its median disagreed with the
 * same-day "normal" printed next to it.
 *
 * @param {Map<number, object>} index  `indexRows(view.rows)`
 * @param {number} sy reference storage year
 * @param {{pick?: Function, lookbackYears?: number, minYear?: number|null, step?: number}} [opts]
 * @returns {Array<{period: Date, x: number, min: number, max: number, median: number, mean: number, n: number}>}
 */
export function seasonalBand(index, sy, { pick = pickPct, lookbackYears = 5, minYear = null, minDate = null, step = 1 } = {}) {
  const out = [];
  const start = storageYearStart(sy);
  const end = storageYearEnd(sy);
  for (let d = start; d <= end; d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + step)) {
    const nrm = positionalNormal(index, d, { pick, lookbackYears, minYear, minDate });
    if (nrm.n === 0) continue;
    out.push({ period: d, x: storageYearFraction(d), min: nrm.min, max: nrm.max, median: nrm.median, mean: nrm.mean, n: nrm.n });
  }
  return out;
}

/**
 * One line per storage year on the April-to-March axis.
 * @returns {Array<{sy: number, label: string, points: Array<{x: number, period: Date, value: number}>, n: number}>}
 */
export function storageYearTraces(rows, pick = pickPct) {
  const by = new Map();
  for (const r of rows || []) {
    const v = pick(r);
    if (!Number.isFinite(v) || !(r.period instanceof Date)) continue;
    const sy = storageYearOf(r.period);
    if (!by.has(sy)) by.set(sy, []);
    by.get(sy).push({ x: storageYearFraction(r.period), period: r.period, value: v });
  }
  return [...by.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([sy, points]) => ({ sy, label: storageYearLabel(sy), points: points.sort((a, b) => a.x - b.x), n: points.length }));
}

/**
 * Calendar-year traces on the January-to-December axis (kept for callers that
 * read history by calendar year).
 */
export function rowsByYear(rows, pick = pickPct) {
  const byYear = new Map();
  for (const r of rows || []) {
    const v = pick(r);
    if (!Number.isFinite(v)) continue;
    const p = r.period instanceof Date ? r.period : new Date(r.period);
    if (!Number.isFinite(p.getTime())) continue;
    const y = p.getFullYear();
    if (!byYear.has(y)) byYear.set(y, []);
    byYear.get(y).push({ x: monthDayFraction(p), period: p, value: v });
  }
  return [...byYear.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([year, points]) => ({ year, points: points.sort((a, b) => a.x - b.x), n: points.length }));
}

/**
 * Deviation from the same-day normal for every observation that has one with
 * at least `minN` prior years. Both units are computed on their own series.
 *
 * @returns {Array<{period: Date, pct: number, twh: number, normalPct: number,
 *   normalTwh: number, devPp: number, devTwh: number, n: number}>}
 */
export function deviationSeries(view, { lookbackYears = 5, minYear = null, minDate = null, minN = 3, stat = 'median', from = null } = {}) {
  const out = [];
  for (const r of view.rows) {
    if (from && r.period < from) continue;
    if (!Number.isFinite(r.pct) || !Number.isFinite(r.twh)) continue;
    const np = positionalNormal(view.index, r.period, { lookbackYears, minYear, minDate, pick: (x) => x.pct });
    if (np.n < minN) continue;
    const nt = positionalNormal(view.index, r.period, { lookbackYears, minYear, minDate, pick: (x) => x.twh });
    const normalPct = np[stat];
    const normalTwh = nt[stat];
    out.push({
      period: r.period,
      pct: r.pct,
      twh: r.twh,
      normalPct,
      normalTwh,
      devPp: r.pct - normalPct,
      devTwh: r.twh - normalTwh,
      n: np.n,
    });
  }
  return out;
}

/* ------------------------------------------------------------------ */
/*  Flows                                                              */
/* ------------------------------------------------------------------ */

/** Reported net flow for a row, GWh/d (injection − withdrawal), or null. */
export function netFlow(r) {
  if (!r || (r.injectionGwh == null && r.withdrawalGwh == null)) return null;
  return (r.injectionGwh ?? 0) - (r.withdrawalGwh ?? 0);
}

/**
 * Mean reported net flow over the `windowDays` days ending at `end`
 * (inclusive), from an index. Days without a report are skipped, and the
 * number actually averaged is returned so a short sample is visible.
 */
export function windowNet(index, end, windowDays = 7) {
  let sum = 0;
  let n = 0;
  const e = dayOnly(end);
  for (let k = 0; k < windowDays; k++) {
    const r = index.get(new Date(e.getFullYear(), e.getMonth(), e.getDate() - k).getTime());
    const v = netFlow(r);
    if (v == null) continue;
    sum += v;
    n += 1;
  }
  return n ? { avg: sum / n, n } : { avg: null, n: 0 };
}

/**
 * Trailing flow dynamics (GWh/d net, injection, withdrawal, momentum, weekly TWh rate).
 *
 * The regime threshold scales with the facility: a net flow counts as
 * injection or withdrawal only beyond ±0.05 % of working-gas capacity per day
 * (≈ ±125 GWh/d for Germany, ≈ ±4 GWh/d for Belgium). A fixed ±15 GWh/d called
 * Germany "injecting" on days that moved its fill by less than 0.01 pp.
 *
 * @param {Array} rows
 * @param {{windowDays?: number, capacityTwh?: number|null}} [opts]
 */
export function flowDynamics(rows, { windowDays = 7, capacityTwh = null } = {}) {
  if (!rows || rows.length === 0) return null;
  const valid = rows.filter((r) => netFlow(r) !== null);
  if (valid.length === 0) return null;

  const w = Math.min(windowDays, valid.length);
  const tail = valid.slice(-w);
  const prevTail = valid.slice(-2 * w, -w);

  const avg = (arr, f) => arr.reduce((s, r) => s + f(r), 0) / arr.length;
  const avgNet = avg(tail, netFlow);
  const avgInj = avg(tail, (r) => r.injectionGwh ?? 0);
  const avgWd = avg(tail, (r) => r.withdrawalGwh ?? 0);
  const prevAvgNet = prevTail.length ? avg(prevTail, netFlow) : null;

  const threshold = Number.isFinite(capacityTwh) && capacityTwh > 0 ? capacityTwh * 1000 * 0.0005 : 15;

  return {
    latestNetGwh: netFlow(valid[valid.length - 1]),
    avgNetGwh: avgNet,
    avgInjectionGwh: avgInj,
    avgWithdrawalGwh: avgWd,
    momentumGwh: prevAvgNet != null ? avgNet - prevAvgNet : null,
    rateTwhPerWeek: (avgNet * 7) / 1000,
    ppPerDay: Number.isFinite(capacityTwh) && capacityTwh > 0 ? (avgNet / (capacityTwh * 1000)) * 100 : null,
    regime: avgNet > threshold ? 'injection' : avgNet < -threshold ? 'withdrawal' : 'neutral',
    thresholdGwh: threshold,
    nDays: tail.length,
  };
}

/**
 * Is the current flow faster or slower than normal for the time of year?
 *
 * Compares the trailing `windowDays` mean net flow with the mean net flow
 * over the same calendar window in each prior year.
 *
 * @returns {{current: number|null, nCurrent: number, normal: ReturnType<typeof positionalNormal>,
 *   diffGwh: number|null}}
 */
export function flowPaceVsNormal(view, asOf, { windowDays = 7, lookbackYears = 5, minYear = null, minDate = null } = {}) {
  const cur = windowNet(view.index, asOf, windowDays);
  const values = [];
  for (let k = 1; k <= lookbackYears; k++) {
    const t = sameDayYearsBack(asOf, k);
    if (beforeBaseline(t, minYear, minDate)) break;
    const w = windowNet(view.index, t, windowDays);
    if (w.avg == null || w.n < Math.ceil(windowDays / 2)) continue;
    values.push({ year: t.getFullYear(), period: t, value: w.avg });
  }
  const sorted = values.map((v) => v.value).sort((a, b) => a - b);
  const normal = {
    values,
    years: values.map((v) => v.year),
    n: values.length,
    median: median(sorted),
    mean: sorted.length ? sorted.reduce((s, x) => s + x, 0) / sorted.length : null,
    min: sorted.length ? sorted[0] : null,
    max: sorted.length ? sorted[sorted.length - 1] : null,
  };
  return {
    current: cur.avg,
    nCurrent: cur.n,
    normal,
    diffGwh: cur.avg != null && normal.median != null ? cur.avg - normal.median : null,
  };
}

/**
 * Daily series of trailing-window net flow for the current window and the
 * prior-year band at the same dates — the flow chart's data.
 *
 * @returns {Array<{period: Date, net: number|null, avg: number|null,
 *   normMedian: number|null, normMin: number|null, normMax: number|null, n: number}>}
 */
export function flowSeriesWithNormal(view, from, to, { windowDays = 7, lookbackYears = 5, minYear = null, minDate = null } = {}) {
  const out = [];
  for (let d = dayOnly(from); d <= to; d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1)) {
    const r = view.index.get(d.getTime());
    if (!r) continue;
    const pace = flowPaceVsNormal(view, d, { windowDays, lookbackYears, minYear, minDate });
    out.push({
      period: d,
      net: netFlow(r),
      avg: pace.current,
      normMedian: pace.normal.median,
      normMin: pace.normal.min,
      normMax: pace.normal.max,
      n: pace.normal.n,
    });
  }
  return out;
}

/**
 * Net stock change since the start of the current season (1 Apr or 1 Oct),
 * against the same change in prior years, in TWh.
 */
export function seasonToDate(view, asOf, { lookbackYears = 5, minYear = null, minDate = null } = {}) {
  const season = seasonOf(asOf);
  const startRow = lookupNear(view.index, season.start, 2);
  const nowRow = lookupNear(view.index, asOf, 0);
  if (!startRow || !nowRow || !Number.isFinite(startRow.twh) || !Number.isFinite(nowRow.twh)) return null;
  const current = nowRow.twh - startRow.twh;
  const values = [];
  for (let k = 1; k <= lookbackYears; k++) {
    const s = sameDayYearsBack(season.start, k);
    const e = sameDayYearsBack(asOf, k);
    if (beforeBaseline(s, minYear, minDate)) break;
    const a = lookupNear(view.index, s, 2);
    const b = lookupNear(view.index, e, 2);
    if (!a || !b || !Number.isFinite(a.twh) || !Number.isFinite(b.twh)) continue;
    values.push({ year: e.getFullYear(), value: b.twh - a.twh });
  }
  const sorted = values.map((v) => v.value).sort((x, y) => x - y);
  return {
    season,
    startDate: startRow.period,
    currentTwh: current,
    normalMedianTwh: median(sorted),
    values,
    n: values.length,
  };
}

/* ------------------------------------------------------------------ */
/*  End-of-season analogs                                              */
/* ------------------------------------------------------------------ */

/**
 * End-of-season outlook by historical analog.
 *
 * For each prior year k with observations on both the same day as `asOf` and
 * the season's end date, the stock change it recorded over that interval is
 * applied to today's stock. Paths are clipped to [0, capacity] and flagged
 * when clipped, because a year that withdrew more than is now in storage
 * cannot be repeated.
 *
 * Caveat the panel prints: deliverability falls as storage empties and prices
 * respond to low stocks, so a low-stock year will not draw as hard as a
 * high-stock analog did. The analogs bound "what the flows of year X would do",
 * not what will happen.
 *
 * @param {object} view
 * @param {Date} asOf
 * @returns {null | {endDate: Date, endLabel: string, seasonKind: string,
 *   startTwh: number, capacityTwh: number,
 *   analogs: Array<{sy: number, label: string, deltaTwh: number, endTwh: number,
 *     endPct: number, clipped: boolean, path: Array<{period: Date, twh: number, pct: number}>}>,
 *   endNormal: ReturnType<typeof positionalNormal>,
 *   summary: {minPct: number, medianPct: number, maxPct: number, minTwh: number, medianTwh: number, maxTwh: number} | null}}
 */
export function analogProjection(view, asOfIn, { lookbackYears = 5, minYear = null, minDate = null, pathStep = 3 } = {}) {
  const asOf = dayOnly(asOfIn);
  const now = lookupNear(view.index, asOf, 0);
  if (!now || !Number.isFinite(now.twh)) return null;
  const season = seasonOf(asOf);
  const endDate = season.end;
  if (dayOnly(asOf) >= endDate) return null;
  const cap = Number.isFinite(now.capTwh) ? now.capTwh : view.capacityTwh;
  if (!Number.isFinite(cap) || cap <= 0) return null;

  const analogs = [];
  for (let k = 1; k <= lookbackYears; k++) {
    const d0 = sameDayYearsBack(asOf, k);
    if (beforeBaseline(d0, minYear, minDate)) break;
    const e0 = sameDayYearsBack(endDate, k);
    const a = lookupNear(view.index, d0, 2);
    const b = lookupNear(view.index, e0, 2);
    if (!a || !b || !Number.isFinite(a.twh) || !Number.isFinite(b.twh)) continue;
    let clipped = false;
    const path = [];
    const days = Math.round((endDate - dayOnly(asOf)) / DAY_MS);
    for (let i = 0; i <= days; i += pathStep) {
      const t = new Date(asOf.getFullYear(), asOf.getMonth(), asOf.getDate() + i);
      const h = lookupNear(view.index, sameDayYearsBack(t, k), 2);
      if (!h || !Number.isFinite(h.twh)) continue;
      let twh = now.twh + (h.twh - a.twh);
      if (twh < 0) { twh = 0; clipped = true; }
      if (twh > cap) { twh = cap; clipped = true; }
      path.push({ period: t, twh, pct: (twh / cap) * 100 });
    }
    let endTwh = now.twh + (b.twh - a.twh);
    if (endTwh < 0) { endTwh = 0; clipped = true; }
    if (endTwh > cap) { endTwh = cap; clipped = true; }
    path.push({ period: endDate, twh: endTwh, pct: (endTwh / cap) * 100 });
    const sy = storageYearOf(d0);
    analogs.push({ sy, label: storageYearLabel(sy), deltaTwh: b.twh - a.twh, endTwh, endPct: (endTwh / cap) * 100, clipped, path });
  }

  const endNormal = positionalNormal(view.index, endDate, { lookbackYears, minYear, minDate, pick: (r) => r.pct });
  let summary = null;
  if (analogs.length) {
    const p = analogs.map((x) => x.endPct).sort((x, y) => x - y);
    const t = analogs.map((x) => x.endTwh).sort((x, y) => x - y);
    summary = {
      minPct: p[0], medianPct: median(p), maxPct: p[p.length - 1],
      minTwh: t[0], medianTwh: median(t), maxTwh: t[t.length - 1],
    };
  }
  return {
    endDate,
    endLabel: season.endLabel,
    seasonKind: season.kind,
    startTwh: now.twh,
    startPct: now.pct,
    capacityTwh: cap,
    analogs,
    endNormal,
    summary,
  };
}

/* ------------------------------------------------------------------ */
/*  Composite metrics                                                  */
/* ------------------------------------------------------------------ */

/**
 * Everything the table and the KPI strip print for one view on one day.
 *
 * `asOf` may be a Date (that day, or the latest row if absent), a year number
 * (last row of that year) or null (latest row).
 */
export function countryStorageMetrics(view, asOf = null, { lookbackYears = 5, minYear = null, minDate = null } = {}) {
  if (!view) return null;
  const rows = view.rows || [];
  let latest = view.latest;
  if (asOf instanceof Date) {
    latest = view.index?.get(dayOnly(asOf).getTime()) ?? view.latest;
  } else if (typeof asOf === 'number') {
    const ym = rows.filter((r) => r.period && r.period.getFullYear() === asOf && Number.isFinite(r.pct));
    latest = ym.length ? ym[ym.length - 1] : view.latest;
  }
  if (!latest || !Number.isFinite(latest.pct)) return null;
  const index = view.index ?? indexRows(rows);

  const normal = positionalNormal(index, latest.period, { lookbackYears, minYear, minDate, pick: (r) => r.pct });
  const normalTwh = positionalNormal(index, latest.period, { lookbackYears, minYear, minDate, pick: (r) => r.twh });
  const deviationPp = normal.median != null ? latest.pct - normal.median : null;
  const deviationTwh = normalTwh.median != null && Number.isFinite(latest.twh) ? latest.twh - normalTwh.median : null;
  const rank = normal.n ? rankAmongYears(latest.pct, normal.values.map((v) => v.value)) : { rank: null, n: 0 };

  const yearAgoRow = lookupNear(index, sameDayYearsBack(latest.period, 1), 2);
  const yoyPp = yearAgoRow && Number.isFinite(yearAgoRow.pct) ? latest.pct - yearAgoRow.pct : null;
  const yoyTwh = yearAgoRow && Number.isFinite(yearAgoRow.twh) && Number.isFinite(latest.twh) ? latest.twh - yearAgoRow.twh : null;

  const upTo = rows.filter((r) => r.period <= latest.period);
  const cap = Number.isFinite(latest.capTwh) ? latest.capTwh : view.capacityTwh;
  const v = { ...view, index };

  return {
    cc: view.cc,
    label: view.label,
    capacityTwh: cap,
    latest: {
      period: latest.period,
      pct: latest.pct,
      twh: latest.twh,
      injectionGwh: latest.injectionGwh,
      withdrawalGwh: latest.withdrawalGwh,
    },
    normal,
    normalTwh,
    deviationPp,
    deviationTwh,
    rank,
    yoyPp,
    yoyTwh,
    flow7d: flowDynamics(upTo, { windowDays: 7, capacityTwh: cap }),
    flow14d: flowDynamics(upTo, { windowDays: 14, capacityTwh: cap }),
    pace7d: flowPaceVsNormal(v, latest.period, { windowDays: 7, lookbackYears, minYear, minDate }),
    seasonToDate: seasonToDate(v, latest.period, { lookbackYears, minYear, minDate }),
    projection: analogProjection(v, latest.period, { lookbackYears, minYear, minDate }),
  };
}

/**
 * Which countries account for the total's distance from normal.
 *
 * Uses the same-day prior-year **mean** in TWh, because means are additive:
 * Σ(country stock − country mean) equals (total stock − total mean) exactly
 * when every country reported in every compared year. Medians are not
 * additive, which is why the attribution is labelled "vs average" while the
 * headline deviation is "vs median". The residual is returned and the panel
 * prints it if it is not ~0.
 *
 * @returns {{rows: Array<{cc: string, label: string, devTwh: number, sharePct: number|null}>,
 *   totalDevTwh: number, aggregateDevTwh: number|null, residualTwh: number|null, years: number[]}}
 */
export function deviationAttribution(views, aggregateView, asOf, { lookbackYears = 5, minYear = null, minDate = null } = {}) {
  const rows = [];
  let total = 0;
  let years = null;
  for (const v of views) {
    const r = v.index.get(dayOnly(asOf).getTime());
    if (!r || !Number.isFinite(r.twh)) continue;
    const nrm = positionalNormal(v.index, asOf, { lookbackYears, minYear, minDate, pick: (x) => x.twh, toleranceDays: 0 });
    if (nrm.mean == null) continue;
    years = years ?? nrm.years;
    const dev = r.twh - nrm.mean;
    total += dev;
    rows.push({ cc: v.cc, label: v.label, devTwh: dev, years: nrm.years });
  }
  let aggregateDevTwh = null;
  if (aggregateView) {
    const r = aggregateView.index.get(dayOnly(asOf).getTime());
    const nrm = positionalNormal(aggregateView.index, asOf, { lookbackYears, minYear, minDate, pick: (x) => x.twh, toleranceDays: 0 });
    if (r && nrm.mean != null) aggregateDevTwh = r.twh - nrm.mean;
  }
  for (const r of rows) r.sharePct = total !== 0 ? (r.devTwh / total) * 100 : null;
  rows.sort((a, b) => a.devTwh - b.devTwh);
  return {
    rows,
    totalDevTwh: total,
    aggregateDevTwh,
    residualTwh: aggregateDevTwh == null ? null : total - aggregateDevTwh,
    years: years ?? [],
  };
}

/**
 * Net daily flow implied by consecutive storage readings, in GWh/d. Used as a
 * cross-check on the reported injection/withdrawal fields.
 */
export function stockImpliedFlow(rows) {
  const out = [];
  for (let i = 1; i < (rows || []).length; i++) {
    const a = rows[i - 1];
    const b = rows[i];
    if (!Number.isFinite(a.twh) || !Number.isFinite(b.twh)) continue;
    const days = Math.round((b.period - a.period) / DAY_MS);
    if (days <= 0) continue;
    out.push({ period: b.period, gwhPerDay: ((b.twh - a.twh) * 1000) / days, days });
  }
  return out;
}

/**
 * How well the reported flows reconcile with the stock path over the last
 * `days` observations: mean of (Δstock − reported net), in GWh/d. A large
 * value means one of the two series is revised or incomplete, and the panel
 * says so instead of printing two disagreeing "flows".
 */
export function flowReconciliation(rows, days = 30) {
  const tail = (rows || []).slice(-(days + 1));
  const implied = stockImpliedFlow(tail);
  const byT = indexRows(tail);
  let sum = 0;
  let abs = 0;
  let n = 0;
  for (const s of implied) {
    if (s.days !== 1) continue;
    const net = netFlow(byT.get(s.period.getTime()));
    if (net == null) continue;
    sum += s.gwhPerDay - net;
    abs += Math.abs(s.gwhPerDay - net);
    n += 1;
  }
  return n ? { meanResidualGwh: sum / n, meanAbsResidualGwh: abs / n, n } : null;
}
