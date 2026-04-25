/**
 * Days-of-Cover analytical utilities.
 *
 * Compute Days-of-Cover series by joining weekly storage (Bcf) with monthly
 * consumption (MMcf → Bcf/day), using SEASONALLY-MATCHED consumption as the
 * divisor — same calendar month from the prior year.
 *
 * Why: dividing April storage by January consumption (peak heating) gives a
 * wildly low cover number — different seasons have 40-60% different burn rates.
 * The correct question is "how many days of TYPICAL APRIL demand does this cover?"
 * Prior-year same-month is the best available forward-typical-demand proxy.
 */

/**
 * @param {Array<{period: Date, value: number}>} storageRows  - Weekly Bcf
 * @param {Array<{period: Date, value: number}>} consumptionRows - Monthly MMcf
 * @returns {Array<{period, storage, dailyConsumption, daysOfCover, denominatorSource}>}
 */
export function computeStorageCover(storageRows, consumptionRows) {
  if (!storageRows?.length || !consumptionRows?.length) return [];

  // Build {`${year}-${month}`: Bcf/day} lookup (month is 0-indexed)
  const byYearMonth = new Map();
  for (const r of consumptionRows) {
    const d = r.period instanceof Date ? r.period : new Date(r.period);
    const year = d.getFullYear();
    const month = d.getMonth(); // 0-indexed
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const dailyBcf = (Number(r.value) / 1000) / daysInMonth; // MMcf/month → Bcf/day
    if (dailyBcf > 0) byYearMonth.set(`${year}-${month}`, dailyBcf);
  }

  if (byYearMonth.size === 0) return [];

  // Pre-compute per-calendar-month historical average across ALL years (fallback)
  const byMonthAvg = new Map();
  for (let m = 0; m < 12; m++) {
    const values = [];
    for (const [key, v] of byYearMonth) {
      const mm = Number(key.split('-')[1]);
      if (mm === m) values.push(v);
    }
    if (values.length > 0) {
      byMonthAvg.set(m, values.reduce((s, v) => s + v, 0) / values.length);
    }
  }

  return storageRows
    .map((s) => {
      const sd = s.period instanceof Date ? s.period : new Date(s.period);
      const month = sd.getMonth(); // 0-indexed
      const priorYearKey = `${sd.getFullYear() - 1}-${month}`;

      // Prefer prior-year same-month; fall back to all-history average for that month
      let dailyConsumption;
      let denominatorSource;
      if (byYearMonth.has(priorYearKey)) {
        dailyConsumption = byYearMonth.get(priorYearKey);
        denominatorSource = 'prior-year';
      } else if (byMonthAvg.has(month)) {
        dailyConsumption = byMonthAvg.get(month);
        denominatorSource = 'historical-avg';
      } else {
        return null; // no seasonal data for this month at all — skip
      }

      const storage = Number(s.value);
      const daysOfCover = storage / dailyConsumption;
      return { period: sd, storage, dailyConsumption, daysOfCover, denominatorSource };
    })
    .filter((r) => r !== null && isFinite(r.daysOfCover));
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
