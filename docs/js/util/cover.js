/**
 * Days-of-Cover: how many days of *typical* demand the gas in the ground can
 * meet.
 *
 * Two corrections over the first implementation:
 *
 *  1. The denominator is season-matched. Dividing April storage by January
 *     burn overstates nothing and understates nothing in April — it answers the
 *     wrong question. Cover is computed against the demand of the same calendar
 *     month in prior years, averaged over every available year so one mild
 *     winter cannot set the normal.
 *
 *  2. A percentage is not always the right unit. The UK NBP convention is
 *     "days of gas in storage", and the industry uses it because comparable
 *     percentages hide real volume: 56% of Germany's caverns is not 56% of
 *     France's. `computeStorageCover` therefore also returns the Bcf/d demand
 *     it used, so a reader can audit the division.
 */

import {
  seasonalEnvelope,
  envelopeLookup,
  isoWeekFromDate,
  ols,
  currentStreak,
  matchYearAgo,
  percentileRank,
  dayOnly,
  daysBetween,
  seasonWeek,
} from './season.js';

/**
 * @param {Array<{period: Date, value: number}>} storageRows  weekly Bcf
 * @param {Array<{period: Date, value: number}>} consumptionRows  monthly MMCF
 * @returns {Array<{period, storage, dailyConsumption, daysOfCover, monthsUsed}>}
 */
export function computeStorageCover(storageRows, consumptionRows, anchor = null) {
  if (!storageRows?.length || !consumptionRows?.length) return [];

  // month (0-11) → [{year, bcfPerDay}]
  const byMonth = new Map();
  for (const r of consumptionRows) {
    const d = r.period instanceof Date ? r.period : new Date(r.period);
    const year = d.getFullYear();
    const month = d.getMonth();
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const dailyBcf = Number(r.value) / 1000 / daysInMonth; // MMCF/month → Bcf/day
    if (!(dailyBcf > 0)) continue;
    if (!byMonth.has(month)) byMonth.set(month, []);
    byMonth.get(month).push({ year, dailyBcf });
  }
  if (byMonth.size === 0) return [];

  const seasonNormal = (month, maxYear) => {
    const obs = (byMonth.get(month) || []).filter((o) => o.year < maxYear);
    const use = obs.length ? obs : (byMonth.get(month) || []);
    if (use.length === 0) return null;
    return {
      dailyBcf: use.reduce((s, o) => s + o.dailyBcf, 0) / use.length,
      monthsUsed: use.length,
    };
  };

  return storageRows
    .map((s) => {
      const sd = s.period instanceof Date ? s.period : new Date(s.period);
      const normal = seasonNormal(sd.getMonth(), sd.getFullYear());
      if (!normal) return null;
      const storage = Number(s.value);
      if (!Number.isFinite(storage)) return null;
      return {
        period: sd,
        storage,
        dailyConsumption: normal.dailyBcf,
        daysOfCover: storage / normal.dailyBcf,
        monthsUsed: normal.monthsUsed,
        week: isoWeekFromDate(sd),
        seasonWeek: anchor ? seasonWeek(sd, anchor) : null,
        denominatorSource: 'season-normal',
      };
    })
    .filter((r) => r !== null && Number.isFinite(r.daysOfCover));
}

/**
 * Five-year envelope of days-of-cover by week-of-season.
 *
 * Cover is a ratio, so comparing it requires the same denominator convention in
 * prior years as in the current one. `computeStorageCover` already applies the
 * season-month normal, so this is a summarisation, not a second normalisation.
 *
 * @param {Array<{period: Date, daysOfCover: number}>} coverRows
 * @param {number} [lookbackYears=5]
 * @param {Date} [anchor]  first observation of the current year; defaults to
 *   the first cover row of the latest calendar year present in the data.
 * @returns {Map<number, {min,max,mean,median,p10,p25,p75,p90,count,years:number[]}>}
 */
export function computeCoverEnvelope(coverRows, lookbackYears = 5, anchor = null) {
  if (!coverRows?.length) return new Map();
  const rows = coverRows.map((r) => ({
    period: r.period instanceof Date ? r.period : new Date(r.period),
    value: Number(r.daysOfCover),
  })).filter((r) => Number.isFinite(r.value));
  if (rows.length === 0) return new Map();

  const currentYear = Math.max(...rows.map((r) => r.period.getFullYear()));
  const firstThisYear = rows.find((r) => r.period.getFullYear() === currentYear);
  const ref = anchor || firstThisYear?.period || rows[0].period;

  return seasonalEnvelope(rows, { anchor: ref, lookbackYears, excludeYear: currentYear });
}

/**
 * Classify the latest cover value against its 5-year envelope.
 *
 * Labels are deliberately about *position in the historical distribution*, not
 * about a market call: "low" on 1 July is normal, "comfortable" on 1 January is
 * not. Reporting a rank and a band keeps the claim falsifiable.
 *
 * @returns {{label: string, pctRank: number|null, envelope: object|null, week: number, vsMean: number|null, years: number[]}}
 */
export function classifyCover(latest, envelope) {
  const empty = { label: 'unknown', pctRank: null, envelope: null, week: null, vsMean: null, years: [] };
  if (!latest) return empty;
  const period = latest.period instanceof Date ? latest.period : new Date(latest.period);
  const wk = isoWeekFromDate(period);
  const env = envelopeLookup(envelope, wk);
  if (!env) return { ...empty, week: wk };

  const value = Number(latest.daysOfCover);
  const pctRank = env.max === env.min
    ? 50
    : ((value - env.min) / (env.max - env.min)) * 100;

  const label =
    pctRank < 10 ? 'critical' :
    pctRank < 30 ? 'low' :
    pctRank > 90 ? 'comfortable' :
    pctRank > 70 ? 'elevated' :
    'normal';

  return {
    label,
    pctRank,
    envelope: env,
    week: wk,
    vsMean: value - env.mean,
    years: env.years || [],
  };
}

/* ================================================================== */
/*  Refill / drawdown analytics                                        */
/* ================================================================== */

/**
 * Pace of the current storage swing, in Bcf/day.
 *
 * The last `windowWeeks` observations are regressed on time (OLS) so a single
 * revision day cannot define the pace. The week-over-week number is reported
 * alongside it because that is what the EIA report headlines; the regression
 * is what says whether the swing is persistent.
 *
 * @param {Array<{period: Date, value: number}>} rows sorted ascending, Bcf
 * @param {{windowWeeks?: number}} [opts]
 * @returns {{bcfPerDay: number|null, r2: number|null, n: number, windowBcf: number|null,
 *            streak: object, daysToSeasonPeak: number|null, targetBcf: number|null}}
 */
export function computePace(rows, { windowWeeks = 6, envelope = null } = {}) {
  const clean = (rows || []).filter((r) => Number.isFinite(Number(r.value)));
  if (clean.length < 2) {
    return { bcfPerDay: null, r2: null, n: 0, windowBcf: null, streak: currentStreak(clean || []), daysToSeasonPeak: null, targetBcf: null };
  }
  const tail = clean.slice(-Math.min(windowWeeks, clean.length));
  const t0 = dayOnly(tail[0].period);
  const pts = tail.map((r) => ({ x: daysBetween(t0, r.period), y: Number(r.value) }));
  const fit = ols(pts);

  const streak = currentStreak(clean);
  const latest = clean[clean.length - 1];

  // Where the season normally turns: the highest mean inside the envelope, and
  // the first week past the current point that reaches within 1% of it.
  let targetBcf = null;
  let daysToSeasonPeak = null;
  if (envelope && envelope.size) {
    const entries = [...envelope.entries()].sort((a, b) => a[0] - b[0]);
    const peak = entries.reduce((best, cur) => (cur[1].mean > best[1].mean ? cur : best), entries[0]);
    targetBcf = peak[1].mean;
    const wkNow = isoWeekFromDate(latest.period);
    const nextPeak = entries.find(([wk]) => wk >= wkNow && wk[1].mean >= peak[1].mean * 0.99);
    if (nextPeak && fit && fit.slope !== 0) {
      const weeksAhead = nextPeak[0] - wkNow;
      daysToSeasonPeak = Math.max(0, Math.round(weeksAhead * 7));
    }
    if (!nextPeak) daysToSeasonPeak = 0;
  }

  return {
    bcfPerDay: fit ? fit.slope : null,
    r2: fit ? fit.r2 : null,
    n: tail.length,
    windowBcf: Number(latest.value) - Number(tail[0].value),
    streak,
    daysToSeasonPeak,
    targetBcf,
  };
}

/**
 * Season-to-date comparison: how far the current year's net injection or
 * withdrawal has diverged from the same window in prior years.
 *
 * The window is [first observation of the year, latest observation], matched to
 * each prior year by calendar date — not by week number, which drifts.
 *
 * @param {Array<{period: Date, value: number}>} rows sorted ascending
 * @param {{lookbackYears?: number}} [opts]
 * @returns {{start: Date|null, end: Date|null, netChange: number|null,
 *            prior: Array<{year: number, netChange: number, startValue: number, endValue: number}>,
 *            mean: number|null, median: number|null, rank: number|null, n: number}}
 */
export function seasonToDateDelta(rows, { lookbackYears = 5 } = {}) {
  const clean = (rows || []).filter((r) => Number.isFinite(Number(r.value)));
  const empty = { start: null, end: null, netChange: null, prior: [], mean: null, median: null, rank: null, n: 0 };
  if (clean.length < 2) return empty;

  const currentYear = clean[clean.length - 1].period.getFullYear();
  const start = clean.find((r) => r.period.getFullYear() === currentYear)?.period ?? null;
  const end = clean[clean.length - 1].period;
  if (!start) return empty;

  const netChange = Number(clean[clean.length - 1].value) - Number(clean.find((r) => r.period.getFullYear() === currentYear).value);

  const prior = [];
  for (let y = currentYear - 1; y >= currentYear - lookbackYears; y--) {
    const rowsY = clean.filter((r) => r.period.getFullYear() === y);
    if (rowsY.length < 2) continue;
    const startY = rowsY.find((r) => r.period.getMonth() === start.getMonth() && r.period.getDate() === start.getDate())
      || matchYearAgo(rowsY, start, 0, 6);
    const endY = matchYearAgo(rowsY, end, 0, 6);
    if (!startY || !endY) continue;
    prior.push({
      year: y,
      netChange: Number(endY.value) - Number(startY.value),
      startValue: Number(startY.value),
      endValue: Number(endY.value),
    });
  }

  const vals = prior.map((p) => p.netChange);
  const sorted = [...vals].sort((a, b) => a - b);
  const median = sorted.length
    ? (sorted.length % 2 ? sorted[(sorted.length - 1) / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2)
    : null;

  return {
    start,
    end,
    netChange,
    prior,
    mean: vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : null,
    median,
    rank: percentileRank(netChange, vals).pct,
    n: vals.length,
  };
}

/**
 * Smallest system-wide demand and storage thresholds are not constants here:
 * they come from the observed extremes of the series, so the "we could run out
 * of gas" framing is driven by data rather than by a remembered number.
 *
 * Returns the storage level that corresponds to a chosen emergency-response
 * duration, given the season-normal demand used for cover. This is the standard
 * way to read storage: "X days of typical burn", not "Y percent full".
 *
 * @param {number} days
 * @param {number} dailyConsumptionBcf
 * @returns {number} Bcf
 */
export function storageForDays(days, dailyConsumptionBcf) {
  if (!Number.isFinite(days) || !Number.isFinite(dailyConsumptionBcf)) return null;
  return days * dailyConsumptionBcf;
}
