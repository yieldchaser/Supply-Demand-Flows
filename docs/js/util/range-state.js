/**
 * Shared Time Range State and URL Parameter Synchronizer.
 *
 * Provides date-range parsing, preset calculation, history span checking,
 * and URL search parameter sync for observatory time series panels.
 *
 * Vanilla JS — zero TypeScript in executable code.
 */

export const RANGE_PRESETS = ['30d', '90d', '1y', 'all'];

/**
 * Parse range from URL search params or fallback to default 'all'.
 *
 * @param {string} [searchStr] - window.location.search or custom query string
 * @returns {{preset: string, start: string|null, end: string|null, isCustom: boolean}}
 */
export function parseRangeFromQuery(searchStr = '') {
  const params = new URLSearchParams(searchStr);
  const raw = (params.get('range') || 'all').trim().toLowerCase();

  if (RANGE_PRESETS.includes(raw)) {
    return { preset: raw, start: null, end: null, isCustom: false };
  }

  // Custom format: YYYY-MM-DD..YYYY-MM-DD
  const parts = raw.split('..');
  if (parts.length === 2 && /^\d{4}-\d{2}-\d{2}$/.test(parts[0]) && /^\d{4}-\d{2}-\d{2}$/.test(parts[1])) {
    return { preset: 'custom', start: parts[0], end: parts[1], isCustom: true };
  }

  return { preset: 'all', start: null, end: null, isCustom: false };
}

/**
 * Serialize range state to URL search param string.
 *
 * @param {string} preset
 * @param {string|null} [start]
 * @param {string|null} [end]
 * @returns {string} e.g. "range=90d" or "range=2026-05-01..2026-08-01"
 */
export function formatRangeQueryParam(preset, start = null, end = null) {
  if (preset === 'custom' && start && end) {
    return `range=${start}..${end}`;
  }
  return `range=${preset || 'all'}`;
}

/**
 * Compute the effective date filter interval from a preset relative to anchor date.
 *
 * @param {string} preset - '30d', '90d', '1y', 'all'
 * @param {string|Date} anchorDate - latest available period (string YYYY-MM-DD or Date)
 * @returns {{startDateStr: string|null, endDateStr: string|null}}
 */
export function computePresetInterval(preset, anchorDate) {
  if (preset === 'all' || !anchorDate) {
    return { startDateStr: null, endDateStr: null };
  }

  const end = new Date(typeof anchorDate === 'string' ? `${anchorDate}T00:00:00Z` : anchorDate);
  let days = 0;
  if (preset === '30d') days = 30;
  else if (preset === '90d') days = 90;
  else if (preset === '1y') days = 365;
  else return { startDateStr: null, endDateStr: null };

  const start = new Date(end.getTime() - days * 86400000);
  const startStr = start.toISOString().slice(0, 10);
  const endStr = end.toISOString().slice(0, 10);
  return { startDateStr: startStr, endDateStr: endStr };
}

/**
 * Check whether a requested start date precedes a source's earliest available history.
 * Protects against dishonest silent clipping.
 *
 * @param {string|null} reqStartStr
 * @param {string} sourceEarliestStr
 * @param {string} sourceLabel
 * @returns {string|null} Caveat message if range exceeds history, else null
 */
export function checkRangeExceedsHistory(reqStartStr, sourceEarliestStr, sourceLabel) {
  if (!reqStartStr || !sourceEarliestStr) return null;
  if (reqStartStr < sourceEarliestStr) {
    return `Selected window starts ${reqStartStr}, but ${sourceLabel} history begins ${sourceEarliestStr}. Display shows available data from ${sourceEarliestStr}.`;
  }
  return null;
}

/**
 * Filter an array of time series objects by date range.
 *
 * @template T
 * @param {T[]} items
 * @param {string|null} startStr - inclusive YYYY-MM-DD
 * @param {string|null} endStr - inclusive YYYY-MM-DD
 * @param {string} [dateKey='dateStr'] - key holding YYYY-MM-DD string
 * @returns {T[]}
 */
export function filterSeriesByRange(items, startStr, endStr, dateKey = 'dateStr') {
  if (!items || !Array.isArray(items)) return [];
  if (!startStr && !endStr) return items;

  return items.filter((item) => {
    const d = item[dateKey];
    if (!d) return false;
    if (startStr && d < startStr) return false;
    if (endStr && d > endStr) return false;
    return true;
  });
}
