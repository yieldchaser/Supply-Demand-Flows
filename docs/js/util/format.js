/**
 * Formatting utilities for the Blue Tide dashboard.
 * All numeric displays use these — no inline formatting anywhere else.
 */

/**
 * Format Bcf value with sign and commas.
 * @param {number} n
 * @returns {string} e.g. "+1,234 Bcf" or "−42 Bcf"
 */
export function formatBcf(n) {
  const sign = n > 0 ? '+' : n < 0 ? '−' : '';
  return `${sign}${Math.abs(n).toLocaleString('en-US')} Bcf`;
}

/**
 * Format MMcf value with commas.
 * @param {number} n
 * @returns {string} e.g. "3,365,900 MMcf"
 */
export function formatMMcf(n) {
  return `${n.toLocaleString('en-US')} MMcf`;
}

/**
 * Format rig count.
 * @param {number} n
 * @returns {string} e.g. "543 rigs"
 */
export function formatRigs(n) {
  return `${n.toLocaleString('en-US')} rigs`;
}

/**
 * Format a date as relative human time.
 * @param {Date} date
 * @returns {string} e.g. "2 days ago", "3 hours ago", "just now"
 */
export function formatRelativeTime(date) {
  const now = Date.now();
  const diffMs = now - date.getTime();

  if (diffMs < 0) return 'just now';

  const seconds = Math.floor(diffMs / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0)    return `${days}d ago`;
  if (hours > 0)   return `${hours}h ago`;
  if (minutes > 0) return `${minutes}m ago`;
  return 'just now';
}

/**
 * Format a Date to ISO string (date portion only).
 * @param {Date} date
 * @returns {string} e.g. "2026-04-17"
 */
export function formatIsoDate(date) {
  return date.toISOString().slice(0, 10);
}

/**
 * Classify the vintage of a data source by asking "when is the NEXT print due?"
 *
 * Why: comparing today to the LATEST publish trips false-stale for sources with
 * long inter-publish gaps (monthly EIA data). The honest question is whether
 * the next print is overdue.
 *
 * @param {string} sourceKey - 'eia_storage' | 'eia_supply' | 'baker_hughes_weekly'
 * @param {string} latestPeriod - 'YYYY-MM-DD' (weekly) or 'YYYY-MM' (monthly)
 * @returns {{
 *   ageDays: number,                  // days since LATEST published (display only)
 *   daysUntilNext: number,            // negative means overdue
 *   freshness: 'fresh' | 'stale' | 'critical',
 *   tooltip: string,
 *   labelText: string
 * }}
 */
export function classifyVintage(sourceKey, latestPeriod) {
  const RULES = {
    eia_storage: {
      // EIA weekly storage: report covers period ending Friday F, publishes Thursday F+6.
      // Next report covers period ending F+7, publishes Thursday F+13.
      // Fresh if today < expected_next_publish + 1d grace.
      // Stale if 1-7 days past, critical if 8+ days past.
      periodLength: 7,
      publishLag: 6,
      graceDays: 1,
      criticalAfterOverdueDays: 7,
      parsePeriod: (s) => new Date(s + 'T00:00:00Z'),
    },
    eia_supply: {
      // EIA monthly supply: report covers month M, publishes ~60 days after M-end.
      // Next report covers M+1, publishes ~90 days after M-end.
      // Fresh if today < expected_next_publish + 7d grace (EIA often slips).
      // Stale if 7-30 days past, critical if 30+ days past.
      periodLength: 30,
      publishLag: 60,
      graceDays: 7,
      criticalAfterOverdueDays: 30,
      parsePeriod: (s) => new Date(s + '-01T00:00:00Z'),
    },
    baker_hughes_weekly: {
      // BH weekly: published Friday for that Friday's date. Lag 0.
      // Next report = next Friday.
      periodLength: 7,
      publishLag: 0,
      graceDays: 1,
      criticalAfterOverdueDays: 7,
      parsePeriod: (s) => new Date(s + 'T00:00:00Z'),
    },
  };

  const rule = RULES[sourceKey];
  if (!rule) {
    return {
      ageDays: 0, daysUntilNext: 0, freshness: 'fresh',
      tooltip: 'Unknown source', labelText: 'UNKNOWN',
    };
  }

  const periodDate = rule.parsePeriod(latestPeriod);
  const latestPublishDate = new Date(periodDate.getTime() + rule.publishLag * 86400000);
  const nextExpectedPublishDate = new Date(latestPublishDate.getTime() + rule.periodLength * 86400000);

  const now = new Date();
  const ageDays = Math.max(0, Math.floor((now.getTime() - latestPublishDate.getTime()) / 86400000));
  const daysUntilNext = Math.floor((nextExpectedPublishDate.getTime() - now.getTime()) / 86400000);

  let freshness;
  if (daysUntilNext >= -rule.graceDays) {
    freshness = 'fresh';
  } else if (daysUntilNext >= -(rule.graceDays + rule.criticalAfterOverdueDays)) {
    freshness = 'stale';
  } else {
    freshness = 'critical';
  }

  const tooltip =
    `Latest period: ${latestPeriod}` +
    ` · Published ${latestPublishDate.toISOString().slice(0, 10)}` +
    ` · Next due ${nextExpectedPublishDate.toISOString().slice(0, 10)}` +
    (daysUntilNext >= 0
      ? ` · Next print in ${daysUntilNext}d`
      : ` · Next print overdue by ${-daysUntilNext}d`);

  const labelText =
    freshness === 'fresh' ? `LATEST · ${formatAgeShort(ageDays)}` :
    freshness === 'stale' ? `STALE · ${-daysUntilNext}d overdue` :
    `OVERDUE · ${-daysUntilNext}d`;

  return { ageDays, daysUntilNext, freshness, tooltip, labelText };
}

function formatAgeShort(days) {
  if (days === 0) return 'today';
  if (days === 1) return '1d';
  if (days < 30) return `${days}d`;
  const weeks = Math.floor(days / 7);
  if (weeks < 8) return `${weeks}w`;
  const months = Math.floor(days / 30);
  return `${months}mo`;
}
