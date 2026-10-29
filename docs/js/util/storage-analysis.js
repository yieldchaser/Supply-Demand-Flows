/**
 * Storage analysis — every number the weekly storage surface prints.
 *
 * Pure and synchronous: no DOM, no `d3`, no `Date.now()`. The panel draws what
 * this module returns and the Node tests assert the same values, so a change in
 * methodology has to pass a test rather than quietly change a chart.
 *
 * Two decisions that are easy to get wrong, and are therefore fixed here:
 *
 *  1. **Sample size is first-class.** The old panel said "5-year average" for
 *     whatever happened to be in the window. EIA's own bibliography counts a
 *     week whose Friday lands one day differently in a prior year, so a
 *     "5-year" normal is routinely n=5, n=4 or (early in a year) n=3. Every
 *     normal here returns its `years` array and the UI is required to print it.
 *
 *  2. **Nothing is annualised from a two-point difference.** Pace is a
 *     regression slope over a window (`ols`), and the horizon is derived from
 *     the same regression the pace came from — extrapolating with one number
 *     while displaying another is how dashboards lie.
 *
 * Units: values are Bcf as delivered by EIA (weekly working gas, week-ending
 * Friday). Rates are Bcf/d = ΔBcf ÷ (days between the two storage weeks).
 */

import { parsePeriod } from './period.js';
import {
  seasonalEnvelope,
  envelopeLookup,
  percentileRank,
  ols,
  currentStreak,
  daysBetween,
  dayOnly,
  addDays,
  matchYearAgo,
  quantile,
} from './season.js';
import { summarize } from './stats.js';

/** Nominal ten-year maximum; the industry convention for `% of max`. */
export const NOMINAL_MAX_BCF = 4_000;

/** The five weekly EIA regions; South Central is split salt / non-salt. */
export const STORAGE_REGIONS = [
  { key: 'east', label: 'East', seriesId: 'storage_east' },
  { key: 'midwest', label: 'Midwest', seriesId: 'storage_midwest' },
  { key: 'mountain', label: 'Mountain', seriesId: 'storage_mountain' },
  { key: 'pacific', label: 'Pacific', seriesId: 'storage_pacific' },
  { key: 'south_central', label: 'South Central', seriesId: 'storage_south_central' },
  { key: 'sc_salt', label: 'SC · salt', seriesId: 'storage_sc_salt' },
  { key: 'sc_nonsalt', label: 'SC · non-salt', seriesId: 'storage_sc_nonsalt' },
];

export const L48_SERIES_ID = 'storage_lower48';

/**
 * Parse EIA storage rows into a per-period record of every series.
 *
 * @param {Array<{series_id: string, period: string, value: any}>} rows
 * @returns {{periods: Array<{period: Date, key: string, values: Record<string, number>}>,
 *            dropped: number, duplicateKeys: number}}
 */
export function buildStorageSeries(rows) {
  const byKey = new Map();
  let dropped = 0;
  let duplicateKeys = 0;
  for (const row of rows || []) {
    const period = parsePeriod(row.period);
    const value = Number(row.value);
    if (!period || !Number.isFinite(value)) {
      dropped += 1;
      continue;
    }
    const key = `${period.getFullYear()}-${String(period.getMonth() + 1).padStart(2, '0')}-${String(period.getDate()).padStart(2, '0')}`;
    if (!byKey.has(key)) byKey.set(key, { period, key, values: {} });
    const bucket = byKey.get(key);
    // A duplicate (series, period) pair is a data defect, not an average:
    // surface the count and keep the last writer, exactly like the parquet merge.
    if (Object.prototype.hasOwnProperty.call(bucket.values, row.series_id)) duplicateKeys += 1;
    bucket.values[row.series_id] = value;
  }
  return {
    periods: [...byKey.values()].sort((a, b) => a.period - b.period),
    dropped,
    duplicateKeys,
  };
}

/**
 * Check the two additive identities EIA's regional split guarantees.
 *
 * Guessing here is a known bug class: the previous panel took `max(value per
 * period)` as "the Lower 48 total", which silently picks whichever region is
 * largest if the aggregate row is ever missing. Tolerances are 1 Bcf because
 * EIA rounds each region to whole Bcf independently.
 *
 * @param {ReturnType<typeof buildStorageSeries>['periods']} periods
 * @returns {{checked: number, toleranceBcf: number,
 *            saltPlusNonsalt: Array<{key: string, residual: number}>,
 *            regionsSumL48: Array<{key: string, residual: number}>}}
 */
export function assertStorageInvariants(periods) {
  const TOL = 1;
  const saltPlusNonsalt = [];
  const regionsSumL48 = [];
  let checked = 0;
  for (const { key, values } of periods || []) {
    const sc = values.storage_south_central;
    const salt = values.storage_sc_salt;
    const nonsalt = values.storage_sc_nonsalt;
    if ([sc, salt, nonsalt].every(Number.isFinite)) {
      const residual = sc - (salt + nonsalt);
      if (Math.abs(residual) > TOL) saltPlusNonsalt.push({ key, residual });
    }
    const l48 = values[L48_SERIES_ID];
    const regionVals = STORAGE_REGIONS.filter((r) => !r.key.startsWith('sc_'))
      .map((r) => values[r.seriesId]);
    if (Number.isFinite(l48) && regionVals.every(Number.isFinite)) {
      checked += 1;
      const residual = regionVals.reduce((s, v) => s + v, 0) - l48;
      if (Math.abs(residual) > TOL) regionsSumL48.push({ key, residual });
    }
  }
  return { checked, toleranceBcf: TOL, saltPlusNonsalt, regionsSumL48 };
}

/**
 * Every distinct reported level in a series, ascending, time-ordered.
 * `assertStorageInvariants` calls this "violations" without knowing whether a
 * residual is a rounding artifact (1–2 Bcf) or a real break; the history keeps
 * the size so a reader can judge.
 */
export function weekSeries(periods, seriesId = L48_SERIES_ID) {
  return (periods || [])
    .filter((p) => Number.isFinite(p.values[seriesId]))
    .map((p) => ({ period: p.period, key: p.key, value: p.values[seriesId] }));
}

/**
 * Season classification and completeness for a week-ending date.
 *
 * The convention is the one the rest of the observatory draws: injection runs
 * April–October, withdrawal November–March. Progress is measured in whole
 * storage weeks from the season's first month, so it is an integer out of ~31
 * and never depends on the day of the month the week label lands on.
 *
 * @param {Date} date
 * @returns {{season: 'injection'|'withdrawal', progressPct: number, weeksElapsed: number,
 *            weeksTotal: number, seasonStart: Date, seasonEnd: Date}}
 */
export function seasonProgress(date) {
  const d = dayOnly(date);
  const month = d.getMonth() + 1;
  const injection = month >= 4 && month <= 10;
  const startYear = injection || month >= 11 ? d.getFullYear() : d.getFullYear() - 1;
  const seasonStart = new Date(startYear, injection ? 3 : 10, 1);
  const seasonEnd = new Date(injection ? startYear : startYear + 1, injection ? 10 : 3, 1);
  const days = daysBetween(seasonStart, seasonEnd);
  const elapsed = Math.min(Math.max(daysBetween(seasonStart, d), 0), days);
  return {
    season: injection ? 'injection' : 'withdrawal',
    progressPct: days > 0 ? (elapsed / days) * 100 : 0,
    weeksElapsed: Math.floor(elapsed / 7),
    weeksTotal: Math.round(days / 7),
    seasonStart,
    seasonEnd,
  };
}

/**
 * The normal, the extremes and the current position for one week of season.
 *
 * `lookbackYears` is a *window*, not a promise: the returned object carries the
 * years that actually contributed, so the label can say `mean of n=4 prior
 * years` instead of `5-year average`.
 *
 * @param {Array<{period: Date, value: number}>} rows ascending
 * @param {Date} asOf
 * @param {{lookbackYears?: number}} [opts]
 */
export function seasonNormal(rows, asOf, { lookbackYears = 5 } = {}) {
  const envelope = seasonalEnvelope(rows, { anchor: dayOnly(asOf), lookbackYears });
  const key = seasonWeekOf(rows, asOf);
  const entry = envelopeLookup(envelope, key);
  if (!entry) return { weekOfSeason: key, n: 0, years: [], mean: null, median: null, min: null, max: null, p10: null, p90: null };
  return {
    weekOfSeason: key,
    n: entry.count,
    years: entry.years,
    mean: entry.mean,
    median: entry.median,
    min: entry.min,
    max: entry.max,
    p10: entry.p10,
    p90: entry.p90,
  };
}

/**
 * Week-of-season index for `date`, measured from the first observation of that
 * date's own year — the same anchor `seasonalEnvelope` uses per year.
 */
export function seasonWeekOf(rows, date) {
  const d = dayOnly(date);
  const year = d.getFullYear();
  let anchor = null;
  for (const r of rows || []) {
    const p = r.period instanceof Date ? r.period : parsePeriod(r.period);
    if (!p || p.getFullYear() !== year) continue;
    if (!anchor || p < anchor) anchor = p;
  }
  if (!anchor) return 0;
  return Math.floor(daysBetween(anchor, d) / 7) + 1;
}

/**
 * Current level vs its normal, plus the level's rank among the same week of
 * season in prior years and among the whole history of that week-of-season.
 *
 * `pctOfMax` uses the nominal 10-year maximum (industry convention, not an
 * observation): a reader who wants the observed maximum gets `observedMaxBcf`.
 */
export function environmentComparison(rows, asOf) {
  const latest = rows[rows.length - 1];
  if (!latest) return null;
  const value = latest.value;
  const normal = seasonNormal(rows, asOf);
  const sameWeek = priorSameWeekValues(rows, asOf);
  const rank = percentileRank(value, sameWeek.map((x) => x.value));
  const observedMax = rows.reduce((m, r) => Math.max(m, r.value), -Infinity);
  const observedMin = rows.reduce((m, r) => Math.min(m, r.value), Infinity);
  return {
    period: latest.period,
    valueBcf: value,
    normal,
    vsNormalBcf: normal.mean == null ? null : value - normal.mean,
    vsNormalPct: normal.mean ? ((value - normal.mean) / normal.mean) * 100 : null,
    vsMedianBcf: normal.median == null ? null : value - normal.median,
    pctOfMax: (value / NOMINAL_MAX_BCF) * 100,
    observedMaxBcf: Number.isFinite(observedMax) ? observedMax : null,
    observedMinBcf: Number.isFinite(observedMin) ? observedMin : null,
    rankPct: rank.pct,
    rankN: rank.n,
  };
}

/**
 * Values from prior years at the same point of the season — the ranking
 * population behind "Xth of n".
 *
 * Matching is by **calendar date**, not by week-of-season index. Every EIA row
 * is a Friday, so "the Friday within a week of 12 September 2025" is the same
 * storage week by fact. Index matching looked equivalent and was not: a
 * 53-week year shifts every later index by one, and a ±1 tolerance then
 * quietly admits a week from a different season (2019 crept into a 2021–2025
 * window that way, making an n=5 label describe five of six years).
 *
 * @param {Array<{period: Date, value: number, key?: string}>} rows ascending
 * @param {Date} asOf
 * @param {{lookbackYears?: number, toleranceDays?: number}} [opts]
 * @returns {Array<{year: number, period: Date, value: number}>}
 */
export function priorSameWeekValues(rows, asOf, { lookbackYears = 5, toleranceDays = 7 } = {}) {
  const d = dayOnly(asOf);
  const year = d.getFullYear();
  const minYear = year - lookbackYears;
  const candidates = (rows || []).filter((r) => {
    const y = r.period.getFullYear();
    return y < year && y >= minYear;
  });
  const out = [];
  for (let y = year - 1; y >= minYear; y--) {
    const inYear = candidates.filter((r) => r.period.getFullYear() === y);
    let best = null;
    for (const r of inYear) {
      const lag = Math.abs(daysBetween(r.period, d) - (y === year - 1 ? 365 : (year - y) * 365.25));
      if (lag > toleranceDays) continue;
      if (!best || lag < best.lag) best = { lag, row: r };
    }
    if (best) out.push({ year: y, period: best.row.period, value: best.row.value });
  }
  return out;
}

/**
 * Change over a window ending at the latest week, in Bcf and Bcf/d.
 *
 * @param {Array<{period: Date, value: number}>} rows ascending
 * @param {number} weeks
 * @returns {{startKey: string, endKey: string, startValue: number, endValue: number,
 *            deltaBcf: number, days: number, rate: number}|null}
 */
export function windowChange(rows, weeks) {
  if (!rows || rows.length < 2) return null;
  const end = rows[rows.length - 1];
  const idx = Math.max(0, rows.length - 1 - weeks);
  const start = rows[idx];
  const days = daysBetween(start.period, end.period);
  if (days <= 0) return null;
  return {
    startKey: start.key ?? null,
    endKey: end.key ?? null,
    startValue: start.value,
    endValue: end.value,
    deltaBcf: end.value - start.value,
    days,
    rate: (end.value - start.value) / days,
  };
}

/**
 * Pace of the seasonal net flow, as a regression over the last `windowWeeks`
 * changes rather than a one-week diff.
 *
 * A single EIA week is a noisy estimate of a rate: revisions and holiday weeks
 * move it by ±20 Bcf. The slope over four weeks is the same number with a
 * standard error attached, which is why it is returned.
 *
 * @param {Array<{period: Date, value: number}>} rows ascending
 * @param {{windowWeeks?: number}} [opts]
 */
export function flowPace(rows, { windowWeeks = 4 } = {}) {
  if (!rows || rows.length < 2) return null;
  const tail = rows.slice(-(windowWeeks + 1));
  const origin = tail[0].period;
  const pts = tail.map((r) => ({ x: daysBetween(origin, r.period), y: r.value }));
  const fit = ols(pts);
  if (!fit) return null;
  const residuals = pts.map((p) => p.y - (fit.intercept + fit.slope * p.x));
  const dof = pts.length - 2;
  const s2 = dof > 0 ? residuals.reduce((s, e) => s + e * e, 0) / dof : 0;
  const meanX = pts.reduce((s, p) => s + p.x, 0) / pts.length;
  const sxx = pts.reduce((s, p) => s + (p.x - meanX) ** 2, 0);
  const se = sxx > 0 ? Math.sqrt(s2 / sxx) : null;
  const thisWeek = rows[rows.length - 1].value - rows[rows.length - 2].value;
  const weekDays = daysBetween(rows[rows.length - 2].period, rows[rows.length - 1].period);
  return {
    bePerDay: fit.slope,
    seBePerDay: se,
    r2: fit.r2,
    n: fit.n,
    thisWeekBePerDay: weekDays > 0 ? thisWeek / weekDays : null,
    windowBcf: rows[rows.length - 1].value - tail[0].value,
  };
}

/**
 * A drift-free linear projection of the level plus an empirical residual band.
 *
 * Not a forecast model and must never be labelled one: it is the straight line
 * through the last `windowWeeks` weeks with a band given by the *dispersion of
 * what actually happened* over the same horizon in the trailing 5 years. The
 * band therefore already contains the seasonal curvature the straight line
 * ignores — which is exactly the quantity a reader needs to discount it.
 *
 * @param {Array<{period: Date, value: number}>} rows ascending
 * @param {{windowWeeks?: number, horizonWeeks?: number, historyYears?: number}} [opts]
 * @returns {null | {
 *   asOf: Date, horizonWeeks: number, targetDate: Date,
 *   modelBcf: number, slopeBePerDay: number, weekNascent: number,
 *   residualQ10: number|null, residualQ50: number|null, residualQ90: number|null,
 *   empiricalN: number, empiricalYears: number[], sameWeekLastYear: number|null,
 *   riskToPeakBcf: number|null, riskToPeakDate: Date|null, riskToPeakResidual: number|null,
 *   endsSeasonAbove2750Bcf: boolean|null, pastPeakBcf: number|null, pastPeakDate: Date|null
 * }}
 */
export function projectLevel(rows, { windowWeeks = 4, horizonWeeks = 8, historyYears = 5 } = {}) {
  if (!rows || rows.length < 3) return null;
  const fitRows = rows.slice(-(windowWeeks + 1));
  const origin = fitRows[0].period;
  const fit = ols(fitRows.map((r) => ({ x: daysBetween(origin, r.period), y: r.value })));
  if (!fit) return null;

  const latest = rows[rows.length - 1];
  const cutoff = latest.period.getFullYear() - historyYears;
  const targetDate = addDays(latest.period, horizonWeeks * 7);
  // Anchor the projection on the *fitted* level at the latest week so a single
  // stale print does not become the baseline; the difference is shown to the
  // reader instead of hidden.
  const fittedNow = fit.intercept + fit.slope * daysBetween(origin, latest.period);
  const modelBcf = fittedNow + fit.slope * horizonWeeks * 7;

  // Empirical band: what storage actually did from this same point of the
  // calendar in prior years.
  //
  // An earlier version took the *regression residual* of every historical week
  // in the window. That is wrong here: a local 4-week slope flat-lined over a
  // turn in the season implies enormous "residuals", and the band came out at
  // ±950 Bcf, wide enough to contain any forecast and therefore useless. What a
  // reader needs is the dispersion of the realised 8-week change at this slot
  // of the year, one observation per year — n≈5, printed as such.
  const slots = priorSameWeekValues(rows, latest.period).filter((x) => x.year >= cutoff);
  const realised = [];
  for (const slot of slots) {
    const idx = rows.findIndex((r) => r === slot.period || (r.period.getTime() === slot.period.getTime()));
    if (idx < 0 || idx + horizonWeeks >= rows.length) continue;
    realised.push({ year: slot.year, deltaBcf: rows[idx + horizonWeeks].value - slot.value });
  }

  const sameWeek = matchYearAgo(rows, latest.period, 1, 6);

  // Distance to the seasonal peak, measured from the *history* of this same
  // week of the year: the median and the worst exceedance of this slot's level
  // over the following `horizonWeeks`. A point estimate of the peak would be a
  // forecast; this is a distribution of what the peak has actually done from
  // this point of the calendar, which is the quantity a trader discounts.
  const riskRows = rows.filter((r) => r.period < latest.period && r.period.getFullYear() >= cutoff);
  const risk = [];
  for (let i = 0; i < riskRows.length; i++) {
    const base = riskRows[i];
    const sameSlot = Math.abs(daysBetween(base.period, latest.period) - 365) <= 20
      || Math.abs(daysBetween(base.period, latest.period) - 366) <= 20;
    if (!sameSlot) continue;
    const ahead = riskRows.slice(i, i + horizonWeeks + 1);
    if (ahead.length < 2) continue;
    const peak = ahead.reduce((m, r) => Math.max(m, r.value), -Infinity);
    risk.push({ residual: peak - base.value, year: base.period.getFullYear() });
  }

  let pastPeakBcf = null;
  let pastPeakDate = null;
  const seasonStartMonth = 0;
  for (const r of rows) {
    if (r.period.getFullYear() !== latest.period.getFullYear()) continue;
    if (r.period < new Date(latest.period.getFullYear(), seasonStartMonth, 1)) continue;
    if (pastPeakBcf == null || r.value > pastPeakBcf) { pastPeakBcf = r.value; pastPeakDate = r.period; }
  }

  return {
    asOf: latest.period,
    horizonWeeks,
    targetDate,
    modelBcf,
    fittedNowBcf: fittedNow,
    lastPrintBcf: latest.value,
    slopeBePerDay: fit.slope,
    r2: fit.r2,
    windowN: fit.n,
    empiricalN: realised.length,
    empiricalYears: realised.map((r) => r.year),
    empiricalDeltas: realised,
    residualMin: realised.length ? Math.min(...realised.map((r) => r.deltaBcf)) : null,
    residualQ10: realised.length >= 3 ? quantile(realised.map((r) => r.deltaBcf), 0.10) : null,
    residualQ50: realised.length ? quantile(realised.map((r) => r.deltaBcf), 0.50) : null,
    residualQ90: realised.length >= 3 ? quantile(realised.map((r) => r.deltaBcf), 0.90) : null,
    residualMax: realised.length ? Math.max(...realised.map((r) => r.deltaBcf)) : null,
    sameWeekLastYear: sameWeek ? sameWeek.value : null,
    sameWeekLastYearLagDays: sameWeek ? sameWeek.lagDays : null,
    riskN: risk.length,
    riskToPeakBcf: risk.length ? risk.reduce((s, r) => s + r.residual, 0) / risk.length : null,
    riskToPeakBcfMedian: risk.length >= 3 ? quantile(risk.map((r) => r.residual), 0.5) : null,
    riskToPeakBcfP90: risk.length >= 3 ? quantile(risk.map((r) => r.residual), 0.9) : null,
    endsSeasonAbove2750Bcf: realised.length >= 3 ? modelBcf + quantile(realised.map((r) => r.deltaBcf), 0.10) > 2750 : null,
    pastPeakBcf,
    pastPeakDate,
  };
}

/**
 * "At this rate, when does X happen?" — with the crossing solved on the same
 * regression the pace came from, and the remaining distance reported so the
 * reader can see how much extrapolation is being asked for.
 *
 * @param {Array<{period: Date, value: number}>} rows ascending
 * @param {number} targetBcf
 * @param {{windowWeeks?: number}} [opts]
 * @returns {null | {rate: number, daysToTarget: number, targetDate: Date, distanceBcf: number,
 *                   direction: 'up'|'down'|'flat'}}
 */
export function pointOfWay(rows, targetBcf, { windowWeeks = 4 } = {}) {
  const pace = flowPace(rows, { windowWeeks });
  const latest = rows[rows.length - 1];
  if (!pace || pace.bePerDay == null) return null;
  const distance = targetBcf - latest.value;
  const rate = pace.bePerDay;
  const moving = rate > 0.05 ? 'up' : rate < -0.05 ? 'down' : 'flat';
  if (moving === 'flat' || Math.sign(distance) !== Math.sign(rate)) {
    return { rate, weeks: null, daysToTarget: null, targetDate: null, distanceBcf: distance, direction: moving };
  }
  const days = distance / rate;
  return {
    rate,
    weeks: days / 7,
    daysToTarget: days,
    targetDate: addDays(latest.period, Math.round(days)),
    distanceBcf: distance,
    direction: moving,
  };
}

/**
 * Region-level table: level, change, and (where the history supports it) the
 * same-week normal. Each row is independently checkable against EIA's regional
 * table, which is why the change columns are first.
 */
export function regionTable(periods, { regions = STORAGE_REGIONS } = {}) {
  const out = [];
  const latest = periods[periods.length - 1];
  if (!latest) return out;
  for (const region of regions) {
    const rows = weekSeries(periods, region.seriesId);
    if (!rows.length) continue;
    const level = latest.values[region.seriesId];
    if (!Number.isFinite(level)) continue;
    const wow = windowChange(rows, 1);
    const four = windowChange(rows, 4);
    const normal = seasonNormal(rows, latest.period);
    out.push({
      key: region.key,
      label: region.label,
      seriesId: region.seriesId,
      levelBcf: level,
      wowBcf: wow ? wow.deltaBcf : null,
      wowRate: wow ? wow.rate : null,
      wowDays: wow ? wow.days : null,
      fourWeekBcf: four ? four.deltaBcf : null,
      normalMeanBcf: normal.mean,
      normalN: normal.n,
      vsNormalBcf: normal.mean == null ? null : level - normal.mean,
      vsNormalPct: normal.mean ? ((level - normal.mean) / normal.mean) * 100 : null,
      shareOfL48Pct: latest.values[L48_SERIES_ID]
        ? (level / latest.values[L48_SERIES_ID]) * 100
        : null,
    });
  }
  return out;
}

/**
 * Sum of the latest week-over-week changes across the regions that report one.
 *
 * This is what makes the regional table checkable: the column must add up to
 * the L48 change, and where it misses by a couple of Bcf the residual is EIA's
 * independent per-region rounding rather than a defect — `residualVsL48Bcf`
 * reports the size so the reader can see which it is.
 *
 * @param {ReturnType<typeof regionTable>} regions
 * @param {number|null} l48WowBcf
 */
export function regionChangeSum(regions, l48WowBcf) {
  const contributing = (regions || []).filter((r) => r.wowBcf != null);
  const total = contributing.reduce((s, r) => s + r.wowBcf, 0);
  return {
    n: contributing.length,
    sumBcf: total,
    buildBcf: contributing.filter((r) => r.wowBcf > 0).reduce((s, r) => s + r.wowBcf, 0),
    drawBcf: contributing.filter((r) => r.wowBcf < 0).reduce((s, r) => s + r.wowBcf, 0),
    nBuild: contributing.filter((r) => r.wowBcf > 0).length,
    nDraw: contributing.filter((r) => r.wowBcf < 0).length,
    residualVsL48Bcf: Number.isFinite(l48WowBcf) ? total - l48WowBcf : null,
  };
}

/**
 * Week-over-week change per region, aligned to a zero line: which regions built
 * and which drew in the latest report, in Bcf and Bcf/d.
 */
export function regionFlowBars(periods) {
  return regionTable(periods)
    .filter((r) => r.wowBcf != null)
    .map((r) => ({ key: r.key, label: r.label, deltaBcf: r.wowBcf, rateBePerDay: r.wowRate }));
}

/**
 * The full analysis bundle for the panel. One call, so the chart, the KPIs and
 * the tests all describe the same snapshot.
 *
 * @param {Array} rawRows - bundle.sources.eia_storage.data
 * @param {{horizonWeeks?: number, lookbackYears?: number}} [opts]
 */
export function analyseStorage(rawRows, { horizonWeeks = 8, lookbackYears = 5 } = {}) {
  const { periods, dropped, duplicateKeys } = buildStorageSeries(rawRows);
  const l48 = weekSeries(periods);
  if (l48.length === 0) return { periods, l48, dropped, duplicateKeys, usable: false };

  const latest = l48[l48.length - 1];
  const invariants = assertStorageInvariants(periods);
  const environment = environmentComparison(l48, latest.period);
  const pace = flowPace(l48);
  const projection = projectLevel(l48, { horizonWeeks });
  const streak = currentStreak(l48);
  const season = seasonProgress(latest.period);
  const regions = regionTable(periods);
  const summary = summarize(l48.map((r) => r.value));

  // Revisions vs EIA's own printed change: the bundle carries no vintage of the
  // previous release, so the honest statement is what the panel *can* check —
  // the implied seasonal position vs the same slot in prior years.
  const sameSlotHistory = priorSameWeekValues(l48, latest.period);

  return {
    periods,
    l48,
    dropped,
    duplicateKeys,
    usable: true,
    latest,
    environment,
    pace,
    projection,
    streak,
    season,
    regions,
    regionChange: regionChangeSum(regions, windowChange(l48, 1)?.deltaBcf ?? null),
    regionFills: computeRegionSeasonFill(periods, regions),
    sameSlotHistory,
    sample: {
      weeks: l48.length,
      firstKey: l48[0].key,
      lastKey: latest.key,
      meanBcf: summary.mean,
      minBcf: summary.min,
      maxBcf: summary.max,
    },
    invariants,
  };
}

/**
 * Seasonal fill per region: level as a share of the region's own observed
 * range, and the same-week normal it is being compared with.
 *
 * This is the answer to "we only see one number, 50% full" — the share is
 * meaningless without the reference. Two references are printed: the observed
 * max (what this region has physically held in the window) and the nominal
 * 4,000 Bcf L48 convention scaled by the region's share of the L48 max.
 */
export function computeRegionSeasonFill(periods, regions = regionTable(periods)) {
  const out = [];
  for (const region of regions) {
    const rows = weekSeries(periods, region.seriesId);
    if (rows.length < 2) continue;
    const max = rows.reduce((m, r) => Math.max(m, r.value), -Infinity);
    const min = rows.reduce((m, r) => Math.min(m, r.value), Infinity);
    const normal = seasonNormal(rows, rows[rows.length - 1].period);
    out.push({
      key: region.key,
      label: region.label,
      levelBcf: rows[rows.length - 1].value,
      observedMaxBcf: max,
      observedMinBcf: min,
      pctOfObservedMax: max > 0 ? (rows[rows.length - 1].value / max) * 100 : null,
      normalMeanBcf: normal.mean,
      normalN: normal.n,
      vsNormalBcf: normal.mean == null ? null : rows[rows.length - 1].value - normal.mean,
      priorYearSameWeekBcf: matchYearAgo(rows, rows[rows.length - 1].period, 1, 6)?.value ?? null,
    });
  }
  return out;
}

export { matchYearAgo, percentileRank, ols, currentStreak, seasonalEnvelope };
