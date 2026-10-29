/**
 * Period parsing — one place, one rule.
 *
 * Every series in the bundle carries a `period` string. The EIA weekly storage
 * series is labelled by a date (`2026-09-11`, the week-ending Friday), daily
 * series by a date, and monthly series by `YYYY-MM`. The previous code did
 * `new Date(period)` everywhere, which parses a bare `YYYY-MM-DD` as *UTC*
 * midnight. In any timezone west of Greenwich that lands on the previous local
 * day, so a Friday-labelled storage week rendered as Thursday, a month-boundary
 * observation jumped into the previous month, and two charts labelling the same
 * observation disagreed about its date.
 *
 * `parsePeriod` builds date-only strings in *local* time and keeps the
 * hh:mm shape for anything that carries a time. Invalid input returns `null`;
 * callers must treat that as a gap rather than silently plotting an
 * `Invalid Date` as NaN.
 */

const DATE_ONLY = /^(\d{4})-(\d{2})-(\d{2})$/;
const MONTH_ONLY = /^(\d{4})-(\d{2})$/;
const YEAR_ONLY = /^(\d{4})$/;

/**
 * Parse a bundle period into a local `Date`.
 *
 * @param {string|number|Date|null|undefined} period
 * @returns {Date|null} `null` when the value is absent or unparseable.
 */
export function parsePeriod(period) {
  if (period instanceof Date) {
    return Number.isFinite(period.getTime()) ? period : null;
  }
  if (period === null || period === undefined) return null;
  if (typeof period === 'number') {
    const d = new Date(period);
    return Number.isFinite(d.getTime()) ? d : null;
  }

  const text = String(period).trim();
  if (!text) return null;

  let m = DATE_ONLY.exec(text);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));

  m = MONTH_ONLY.exec(text);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, 1);

  m = YEAR_ONLY.exec(text);
  if (m) return new Date(Number(m[1]), 0, 1);

  // Full ISO timestamps carry a real instant; keep it. A *date-only* string is
  // handled above so it never becomes UTC midnight.
  const d = new Date(text);
  return Number.isFinite(d.getTime()) ? d : null;
}

/**
 * `YYYY-MM-DD` of a local date, for display and for stable keys.
 */
export function formatPeriodKey(date) {
  const d = parsePeriod(date);
  if (!d) return null;
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${d.getFullYear()}-${mm}-${dd}`;
}

/**
 * Parse a whole row set once, dropping rows whose period cannot be read and
 * reporting how many were dropped. Consumers use `.dropped` to label the gap
 * instead of quietly shrinking the sample.
 *
 * @template T
 * @param {T[]} rows
 * @param {(row: T) => any} [getPeriod]
 * @returns {{rows: Array<T & {period: Date}>, dropped: number}}
 */
export function withParsedPeriods(rows, getPeriod = (r) => r.period) {
  const out = [];
  let dropped = 0;
  for (const row of rows || []) {
    const d = parsePeriod(getPeriod(row));
    if (!d) { dropped += 1; continue; }
    out.push({ ...row, period: d });
  }
  return { rows: out, dropped };
}
