/**
 * Seasonal-analytics primitives shared by every storage surface.
 *
 * Why this module exists
 * ----------------------
 * The storage charts used to answer "is this week high or low?" with an ISO-week
 * comparison. The EIA storage week ends on Friday, and its ISO week number
 * flips at an ISO-year boundary that is not the calendar boundary. Two storage
 * weeks in the same season could therefore land in different ISO weeks, and a
 * year with 52 vs 53 storage weeks shifted every subsequent week by one. The
 * fix is to compare on the axis these series actually live on: week-of-year
 * counted from the first storage week of each year (1..52/53), with no ISO
 * round-trip.
 *
 * Everything here is pure and synchronous so it can be unit-tested under Node.
 */

const DAY_MS = 86400000;

/** Strip the time-of-day, keeping local midnight. */
export function dayOnly(date) {
  const d = date instanceof Date ? date : new Date(date);
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

/** Whole days between two dates (b - a), time-of-day ignored. */
export function daysBetween(a, b) {
  return Math.round((dayOnly(b) - dayOnly(a)) / DAY_MS);
}

/** Add n days to a date, calendar-safe (no DST drift on local midnight). */
export function addDays(date, n) {
  const d = dayOnly(date);
  return new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);
}

/**
 * Monday-anchored season start: returns the Monday of the week containing
 * `date`. Used so a year's week 1 is a real week, never a 1-day stub.
 */
export function weekAnchor(date) {
  const d = dayOnly(date);
  const dow = (d.getDay() + 6) % 7; // Monday = 0
  return addDays(d, -dow);
}

/**
 * Week-of-season index, 1-based.
 *
 * The reference anchor is the first observation of the current year. The index
 * is the number of 7-day steps between the observation and that anchor, so
 * value 1 = anchor week, 2 = anchor + 7 days, and so on. Consistent across
 * years with 52 or 53 weeks, and monotone within a year by construction.
 *
 * @param {Date} date
 * @param {Date} anchor  first observation of the current year
 * @returns {number} 1..53 (0 or negative when the date precedes the anchor)
 */
export function seasonWeek(date, anchor) {
  const diff = daysBetween(anchor, date);
  return Math.floor(diff / 7) + 1;
}

/**
 * The observation that best represents the same point of the season in
 * another year: nearest calendar date (within `toleranceDays`) to
 * `date - yearDelta years`, restricted to rows in that target year.
 *
 * Calendar-date matching, not week-number matching: the EIA week is a Friday
 * label, and the Friday closest to "one year ago today" is the same storage
 * week as a matter of fact rather than as a matter of ISO convention.
 *
 * @param {Array<{period: Date, value: number}>} rows  sorted ascending
 * @param {Date} date
 * @param {number} yearDelta
 * @param {number} [toleranceDays=6]
 * @returns {{period: Date, value: number, lagDays: number}|null}
 */
export function matchYearAgo(rows, date, yearDelta = 1, toleranceDays = 6) {
  const d = dayOnly(date);
  const target = new Date(d.getFullYear() - yearDelta, d.getMonth(), d.getDate());
  let best = null;
  for (const r of rows) {
    // `daysBetween` compares whole calendar days, so `toleranceDays` is a day
    // count on both sides and never a millisecond subtraction.
    const lag = Math.abs(daysBetween(target, r.period));
    if (lag > toleranceDays) continue;
    // Strictly nearer wins; on an exact tie the earlier row keeps the match, so
    // a date-matched lookup is deterministic across input orderings.
    if (!best || lag < best.lagDays) best = { period: r.period, value: r.value, lagDays: lag };
  }
  return best;
}

/**
 * Rank a value against a reference population.
 *
 * @param {number} value
 * @param {number[]} population
 * @returns {{rank: number, n: number, pct: number}}  pct = share of the
 *   population at or below `value`, 0..100 (the classic "percentile rank").
 */
export function percentileRank(value, population) {
  const vals = population.filter((v) => Number.isFinite(v));
  if (vals.length === 0 || !Number.isFinite(value)) return { rank: null, n: 0, pct: null };
  const below = vals.filter((v) => v <= value).length;
  return { rank: below, n: vals.length, pct: (below / vals.length) * 100 };
}

/**
 * Per-week-of-season envelope across a lookback window of prior years.
 *
 * For each week index, collects every observed value from prior years and
 * summarises it. `years` lists which calendar years actually contributed, so a
 * tooltip can say "n=5 years (2021–2025)" instead of implying a full window.
 *
 * @param {Array<{period: Date, value: number}>} rows
 * @param {{anchor: Date, lookbackYears?: number, excludeYear?: number}} opts
 * @returns {Map<number, {min,max,mean,median,p10,p25,p75,p90,count,years:number[]}>}
 */
export function seasonalEnvelope(rows, { anchor, lookbackYears = 5, excludeYear } = {}) {
  const exclude = excludeYear ?? anchor.getFullYear();
  const cutoffYear = exclude - lookbackYears;
  const byWeek = new Map();
  const yearsByWeek = new Map();

  // A single global `anchor` only works if every year's first observation sits
  // on the same calendar day, which it does not: a leap year, or a year whose
  // first weekly Friday differs by a day, shifts year N's offset by one week.
  // Worse, the shift is negative, so `seasonWeek` returned a number < 1 and
  // *every* prior observation was dropped — an empty envelope, a blank band.
  // The season-week axis is therefore measured from each year's own first
  // observation, which is what `seasonWeek(date, yearAnchor[year])` means.
  const yearAnchors = new Map();
  for (const r of rows) {
    const period = r.period instanceof Date ? r.period : new Date(r.period);
    if (!Number.isFinite(period.getTime())) continue;
    const yr = period.getFullYear();
    if (yr >= exclude || yr < cutoffYear) continue;
    const first = yearAnchors.get(yr);
    if (!first || period < first) yearAnchors.set(yr, dayOnly(period));
  }

  for (const r of rows) {
    const period = r.period instanceof Date ? r.period : new Date(r.period);
    if (!Number.isFinite(period.getTime())) continue;
    const yr = period.getFullYear();
    if (yr >= exclude || yr < cutoffYear) continue;
    const v = Number(r.value);
    if (!Number.isFinite(v)) continue;
    const wk = seasonWeek(period, yearAnchors.get(yr));
    if (wk < 1) continue;
    if (!byWeek.has(wk)) { byWeek.set(wk, []); yearsByWeek.set(wk, new Set()); }
    byWeek.get(wk).push(v);
    yearsByWeek.get(wk).add(yr);
  }

  const out = new Map();
  for (const [wk, vals] of byWeek) {
    const sorted = [...vals].sort((a, b) => a - b);
    const q = (p) => quantileSorted(sorted, p);
    out.set(wk, {
      min: sorted[0],
      max: sorted[sorted.length - 1],
      mean: vals.reduce((s, v) => s + v, 0) / vals.length,
      median: q(0.5),
      p10: q(0.10),
      p25: q(0.25),
      p75: q(0.75),
      p90: q(0.90),
      count: vals.length,
      years: [...yearsByWeek.get(wk)].sort((a, b) => a - b),
    });
  }
  return out;
}

/**
 * Linear-interpolated quantile of an already-sorted array (Hyndman-Fan type 7,
 * the R/numpy default). The old index-picking version returned the 1st of 4
 * values as "p25", which reads as a percentile but is not one.
 */
export function quantileSorted(sorted, p) {
  if (!sorted || sorted.length === 0) return null;
  if (sorted.length === 1) return sorted[0];
  const pos = (sorted.length - 1) * p;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

/**
 * Quantile of an unsorted array.
 */
export function quantile(values, p) {
  const sorted = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  return quantileSorted(sorted, p);
}

/**
 * Ordinary least squares on (x, y): returns slope, intercept, r², n.
 * Used for refill/drawdown pace, so a pace is a regression over the last
 * weeks rather than a two-point difference that one revision-day can distort.
 *
 * @param {Array<{x: number, y: number}>} pts
 * @returns {{slope: number, intercept: number, r2: number, n: number}|null}
 */
export function ols(pts) {
  const clean = pts.filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y));
  const n = clean.length;
  if (n < 2) return null;
  const mx = clean.reduce((s, p) => s + p.x, 0) / n;
  const my = clean.reduce((s, p) => s + p.y, 0) / n;
  let sxx = 0, sxy = 0, syy = 0;
  for (const { x, y } of clean) {
    sxx += (x - mx) ** 2;
    sxy += (x - mx) * (y - my);
    syy += (y - my) ** 2;
  }
  if (sxx === 0) return null;
  const slope = sxy / sxx;
  const intercept = my - slope * mx;
  const r2 = syy === 0 ? 1 : (sxy * sxy) / (sxx * syy);
  return { slope, intercept, r2, n };
}

/**
 * The contiguous run of rows ending at the latest row whose values move in a
 * single direction. "Refill streak" / "drawdown streak" both read from this.
 *
 * @param {Array<{period: Date, value: number}>} rows sorted ascending
 * @param {{minWeeks?: number}} [opts]
 * @returns {{direction: 'up'|'down'|'flat', weeks: number, delta: number, start: Date|null, startValue: number|null}}
 */
export function currentStreak(rows, { minWeeks = 1 } = {}) {
  if (!rows || rows.length < 2) {
    return { direction: 'flat', weeks: 0, delta: 0, start: null, startValue: null };
  }
  let i = rows.length - 1;
  let dir = 0;
  while (i > 0) {
    const d = rows[i].value - rows[i - 1].value;
    if (d === 0) break;
    const s = d > 0 ? 1 : -1;
    if (dir === 0) dir = s;
    else if (s !== dir) break;
    i -= 1;
  }
  const weeks = rows.length - 1 - i;
  if (weeks < minWeeks) {
    return { direction: 'flat', weeks: 0, delta: 0, start: null, startValue: null };
  }
  const startRow = rows[i];
  const endRow = rows[rows.length - 1];
  return {
    direction: dir > 0 ? 'up' : 'down',
    weeks,
    delta: endRow.value - startRow.value,
    start: startRow.period,
    startValue: startRow.value,
  };
}

/**
 * Season-adjusted normal for one week: the mean of the same season-week in
 * prior years, falling back to the nearest populated week (edges of the year
 * are thin for daily series and empty for the first week of a partial history).
 */
export function envelopeLookup(envelope, wk) {
  if (!envelope) return null;
  const hit = envelope.get(wk);
  if (hit) return hit;
  for (let d = 1; d <= 3; d++) {
    const lo = envelope.get(wk - d);
    if (lo) return lo;
    const hi = envelope.get(wk + d);
    if (hi) return hi;
  }
  return null;
}

/**
 * Week-of-year index used for storage charts.
 *
 * EIA weekly storage is labelled by the week-ending Friday. The ISO week number
 * of that Friday is not a stable season index: it jumps by one between years
 * that start on different weekdays, and a 53-week ISO year shifts every later
 * week. Taking the ISO week of `published Friday − 6 days` (the Monday that
 * starts the storage week) removes most of that, and clamping at 53 keeps the
 * x-domain fixed.
 *
 * @param {Date|string} date  any date within the storage week
 * @returns {number} 1..53
 */
export function isoWeekFromDate(date) {
  const d = dayOnly(date);
  const monday = addDays(d, -((d.getDay() + 6) % 7));
  const iso = isoWeekNumber(monday);
  return Math.min(Math.max(iso, 1), 53);
}

/** ISO-8601 week number for a date (Monday-based, week 1 contains Jan 4). */
export function isoWeekNumber(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
}
