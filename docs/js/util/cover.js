/**
 * Days-of-Cover analytical utilities.
 *
 * Compute Days-of-Cover series by joining weekly storage (Bcf) with monthly
 * consumption (MMcf → Bcf/day via piecewise-constant interpolation).
 *
 * Why two cadences: EIA storage publishes weekly, consumption publishes monthly.
 * We interpolate each month's total into a daily burn rate and apply it to every
 * storage week that falls within that month. Weeks past the latest known
 * consumption month forward-carry the most recent monthly rate.
 */

/**
 * @param {Array<{period: Date, value: number}>} storageRows  - Weekly Bcf
 * @param {Array<{period: Date, value: number}>} consumptionRows - Monthly MMcf
 * @returns {Array<{period, storage, dailyConsumption, daysOfCover, extrapolated}>}
 */
export function computeStorageCover(storageRows, consumptionRows) {
  if (!storageRows || storageRows.length === 0) return [];
  if (!consumptionRows || consumptionRows.length === 0) return [];

  // Build piecewise-constant daily rate table from monthly MMcf totals.
  const dailyByMonth = consumptionRows
    .map((r) => {
      const d = r.period instanceof Date ? r.period : new Date(r.period);
      const year = d.getFullYear();
      const month = d.getMonth(); // 0-indexed
      const daysInMonth = new Date(year, month + 1, 0).getDate();
      const valueBcf = Number(r.value) / 1000; // MMcf → Bcf
      return {
        monthStart: new Date(year, month, 1),
        monthEnd:   new Date(year, month + 1, 0, 23, 59, 59),
        dailyBcf:   valueBcf / daysInMonth,
      };
    })
    .filter((m) => m.dailyBcf > 0)
    .sort((a, b) => a.monthStart - b.monthStart);

  if (dailyByMonth.length === 0) return [];

  const earliestConsumption = dailyByMonth[0].monthStart;
  const latestMonthRecord   = dailyByMonth[dailyByMonth.length - 1];

  return storageRows
    .filter((s) => {
      const d = s.period instanceof Date ? s.period : new Date(s.period);
      return d >= earliestConsumption;
    })
    .map((s) => {
      const sd = s.period instanceof Date ? s.period : new Date(s.period);

      // Find the month window containing this storage date.
      let match = dailyByMonth.find((m) => sd >= m.monthStart && sd <= m.monthEnd);
      let extrapolated = false;
      if (!match) {
        // Forward-carry: storage date is beyond latest known consumption month
        match = latestMonthRecord;
        extrapolated = true;
      }

      const dailyConsumption = match.dailyBcf;
      const storage = Number(s.value);
      const daysOfCover = dailyConsumption > 0 ? storage / dailyConsumption : null;

      return { period: sd, storage, dailyConsumption, daysOfCover, extrapolated };
    })
    .filter((r) => r.daysOfCover !== null && !isNaN(r.daysOfCover));
}

/**
 * Compute per-ISO-week seasonal envelope (min/max/median) from prior-year data.
 *
 * @param {Array<{period: Date, daysOfCover: number}>} coverRows
 * @param {number} lookbackYears
 * @returns {Map<number, {min, max, median, count}>}
 */
export function computeCoverEnvelope(coverRows, lookbackYears = 5) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const cutoff = new Date(currentYear - lookbackYears, 0, 1);

  const filtered = coverRows.filter(
    (r) => r.period >= cutoff && r.period.getFullYear() < currentYear
  );

  const byWeek = new Map();
  for (const r of filtered) {
    const wk = isoWeekFromDate(r.period);
    if (!byWeek.has(wk)) byWeek.set(wk, []);
    byWeek.get(wk).push(r.daysOfCover);
  }

  const envelope = new Map();
  for (const [wk, values] of byWeek) {
    if (values.length === 0) continue;
    const sorted = [...values].sort((a, b) => a - b);
    envelope.set(wk, {
      min:    sorted[0],
      max:    sorted[sorted.length - 1],
      median: sorted[Math.floor(sorted.length / 2)],
      count:  values.length,
    });
  }
  return envelope;
}

/**
 * Classify latest cover value against its seasonal envelope.
 * Position within envelope: <10% = critical, <30% = low, <70% = normal,
 * <90% = elevated, ≥90% = comfortable.
 *
 * @param {{period: Date, daysOfCover: number}} latest
 * @param {Map<number, {min, max, median, count}>} envelope
 * @returns {{label: string, envelope: object|null}}
 */
export function classifyCover(latest, envelope) {
  if (!latest || latest.daysOfCover == null) return { label: 'normal', envelope: null };
  const wk  = isoWeekFromDate(latest.period);
  const env = envelope.get(wk);
  if (!env) return { label: 'normal', envelope: null };

  const range = env.max - env.min;
  const pos   = range > 0 ? (latest.daysOfCover - env.min) / range : 0.5;

  let label;
  if (pos < 0.10)      label = 'critical';
  else if (pos < 0.30) label = 'low';
  else if (pos < 0.70) label = 'normal';
  else if (pos < 0.90) label = 'elevated';
  else                 label = 'comfortable';

  return { label, envelope: env };
}

/** ISO week number (1-52/53) from a Date object. */
export function isoWeekFromDate(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
}
