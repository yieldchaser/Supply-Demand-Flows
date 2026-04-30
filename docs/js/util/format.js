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
      kind: 'weekly',
      publishLag: 6,           // period date Friday → published Thursday +6d
      cadenceDays: 7,
      graceDays: 1,
      criticalAfterOverdueDays: 7,
    },
    eia_supply: {
      kind: 'monthly_eia_ngm',
      // EIA Natural Gas Monthly publishes on the last day of (period_month + 2).
      // E.g. Jan 2026 data published Mar 31 2026; Feb 2026 data due ~Apr 30 2026.
      // Stale 14d after expected publish; critical 30d after that.
      graceDays: 14,
      criticalAfterOverdueDays: 30,
    },
    baker_hughes_weekly: {
      kind: 'weekly',
      publishLag: 0,           // period date IS Friday publish day
      cadenceDays: 7,
      graceDays: 1,
      criticalAfterOverdueDays: 7,
    },
    gie_agsi: {
      kind: 'daily',
      publishLag: 1,           // yesterday's gas day published today
      cadenceDays: 1,
      graceDays: 1,
      criticalAfterOverdueDays: 3,
    },
    eia_lng: {
      kind: 'monthly_eia_ngm',
      // EIA LNG export data publishes ~3-5 days into the following month.
      // Uses the same monthly_eia_ngm kind but with a tighter publish lag
      // (5 days after month end rather than end-of-month+2 like NGM).
      // Approximated here with the same NGM formula; graceDays covers the slippage.
      graceDays: 5,
      criticalAfterOverdueDays: 30,
    },
  };

  const rule = RULES[sourceKey];
  if (!rule) {
    return {
      ageDays: 0, daysUntilNext: 0, freshness: 'fresh',
      tooltip: 'Unknown source', labelText: 'UNKNOWN',
    };
  }

  let latestPublishDate;
  let nextExpectedPublishDate;

  if (rule.kind === 'weekly' || rule.kind === 'daily') {
    const periodDate = new Date(latestPeriod + 'T00:00:00Z');
    latestPublishDate = new Date(periodDate.getTime() + rule.publishLag * 86400000);
    nextExpectedPublishDate = new Date(latestPublishDate.getTime() + rule.cadenceDays * 86400000);
  } else if (rule.kind === 'monthly_eia_ngm') {
    // latestPeriod is 'YYYY-MM' — the data month
    const [yStr, mStr] = latestPeriod.split('-');
    const year = parseInt(yStr, 10);
    const month = parseInt(mStr, 10);  // 1-12
    // IMPORTANT: Date.UTC month is 0-indexed (Jan=0), but period month is 1-indexed
    // (Jan=1 from parseInt('01')). To get end-of-month-N in 1-indexed terms, pass N
    // to Date.UTC with day=0 — "day 0 of month N" = last day of month N-1 in 0-indexed.
    // For period Jan (month=1): end of March = Date.UTC(year, 1+2, 0) = Date.UTC(year, 3, 0) ✓
    latestPublishDate = new Date(Date.UTC(year, month + 2, 0));
    // For period Jan (month=1): end of April = Date.UTC(year, 1+3, 0) = Date.UTC(year, 4, 0) ✓
    // Date.UTC handles month overflow automatically (Dec=12: 12+3=15 → end of Mar next year).
    nextExpectedPublishDate = new Date(Date.UTC(year, month + 3, 0));
  }

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
