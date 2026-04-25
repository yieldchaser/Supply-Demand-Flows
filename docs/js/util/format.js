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
 * Classify the vintage of a data source against its real publish cadence.
 *
 * Why: comparing today to `latest_period` is misleading. EIA Supply for period
 * 2026-01 was published in early March 2026, so on April 24 it is ~7 weeks
 * since the publish date, NOT 16 weeks. Each source has its own publish lag.
 *
 * @param {string} sourceKey - one of: 'eia_storage', 'eia_supply', 'baker_hughes_weekly'
 * @param {string} latestPeriod - the source's most recent period string
 *   - 'YYYY-MM-DD' for weekly sources
 *   - 'YYYY-MM' for monthly sources
 * @returns {{
 *   ageDays: number,           // days since the EXPECTED publish, not the period
 *   freshness: 'fresh' | 'stale' | 'critical',
 *   tooltip: string,           // human-readable explanation
 *   labelText: string          // short pill text e.g. "FRESH · 1d" or "STALE · 12d"
 * }}
 */
export function classifyVintage(sourceKey, latestPeriod) {
  const SOURCE_RULES = {
    eia_storage: {
      // EIA publishes weekly storage Thursdays for the prior Friday's date.
      // So latest_period = '2026-04-17' (Fri) -> published Thu 2026-04-23.
      // Publish lag from period: 6 days.
      // Cadence: weekly. Fresh until next expected Thursday + 1d grace.
      publishLagDays: 6,
      cadenceDays: 7,
      freshGraceDays: 2,
      criticalDays: 14,
      parsePeriod: (s) => new Date(s + 'T00:00:00Z'),
    },
    eia_supply: {
      // EIA publishes monthly supply ~30-60 days after month-close.
      // So latest_period = '2026-01' -> published ~early March 2026.
      // Publish lag from period start: ~60 days conservatively.
      // Cadence: monthly. Fresh through the next expected publish + 14d grace.
      publishLagDays: 60,
      cadenceDays: 30,
      freshGraceDays: 14,
      criticalDays: 75,
      parsePeriod: (s) => new Date(s + '-01T00:00:00Z'),
    },
    baker_hughes_weekly: {
      // Baker Hughes publishes Fridays at noon CT. latest_period = '2026-04-17'
      // is the Friday it was published. Publish lag from period: 0 days.
      // Cadence: weekly. Fresh until next Friday + 1d grace.
      publishLagDays: 0,
      cadenceDays: 7,
      freshGraceDays: 2,
      criticalDays: 14,
      parsePeriod: (s) => new Date(s + 'T00:00:00Z'),
    },
  };

  const rule = SOURCE_RULES[sourceKey];
  if (!rule) {
    return { ageDays: 0, freshness: 'fresh', tooltip: 'Unknown source', labelText: 'UNKNOWN' };
  }

  const periodDate = rule.parsePeriod(latestPeriod);
  const expectedPublishDate = new Date(periodDate.getTime() + rule.publishLagDays * 86400000);
  const now = new Date();
  const daysSincePublish = Math.floor((now.getTime() - expectedPublishDate.getTime()) / 86400000);

  // Negative means we're not yet past the expected publish date — definitely fresh
  const ageDays = Math.max(0, daysSincePublish);

  let freshness;
  if (ageDays <= rule.cadenceDays + rule.freshGraceDays) {
    freshness = 'fresh';
  } else if (ageDays <= rule.criticalDays) {
    freshness = 'stale';
  } else {
    freshness = 'critical';
  }

  const tooltip =
    `Period: ${latestPeriod} · Expected publish: ${expectedPublishDate.toISOString().slice(0, 10)}` +
    ` · ${ageDays} day(s) since publish`;

  const labelText =
    freshness === 'fresh' ? `LATEST · ${formatAgeShort(ageDays)}` :
    freshness === 'stale' ? `STALE · ${formatAgeShort(ageDays)}` :
    `OVERDUE · ${formatAgeShort(ageDays)}`;

  return { ageDays, freshness, tooltip, labelText };
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
