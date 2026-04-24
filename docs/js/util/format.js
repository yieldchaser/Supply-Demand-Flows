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
 * Classify data vintage freshness based on age and expected cadence.
 * @param {number} ageDays — number of days since last data point
 * @param {number} cadenceDays — expected publish cadence in days
 * @returns {'fresh' | 'stale' | 'critical'}
 */
export function classifyVintage(ageDays, cadenceDays) {
  // Fresh: within cadence + 1 day buffer
  if (ageDays <= cadenceDays + 1) return 'fresh';
  // Critical: exceeds 2x cadence
  if (ageDays > cadenceDays * 2) return 'critical';
  // Stale: in between
  return 'stale';
}

/**
 * Get cadence in days for a given source key.
 * @param {string} sourceKey
 * @returns {number}
 */
export function getCadenceDays(sourceKey) {
  const cadences = {
    eia_storage: 7,
    eia_supply: 30,
    baker_hughes_weekly: 7,
  };
  return cadences[sourceKey] ?? 7;
}
