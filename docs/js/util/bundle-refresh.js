/**
 * Which panels a newer data bundle actually changes.
 *
 * The observatory fetches its bundle once at boot from `manifest.json`'s hashed
 * URL. The publisher rewrites `manifest.json` only after it has written every
 * shard (see `publishers/export_dashboard_json.py`), so a reader who opened the
 * page before a publish keeps reading yesterday's numbers, with a vintage pill
 * that still says "today".
 *
 * `main.js` re-fetches the manifest on idle and on tab focus. If its hash moved,
 * the pages that read that data are re-run in place. Two facts decide what gets
 * re-run:
 *
 *   - **Which panels read a source.** Each panel is registered in `main.js` with
 *     the bundle sources it reads (`SECTIONS`), so a panel is only re-run when one
 *     of *its* sources moved. Refreshing the whole page costs a full re-render of
 *     every chart; refreshing nothing leaves a stale number under a live badge.
 *   - **A blocked inbound series blocks its consumers.** If a panel renders an
 *     empty state - "history not available", because the producer started
 *     publishing a series this bundle lacks - that empty state is the *fixed*
 *     thing, and a newer bundle may well have the series. The dependency record
 *     alone cannot see it, so the panel marks itself (`data-blocked`) and the
 *     next refresh always re-runs it once.
 *
 * Pure, so it can be unit-tested under Node without a DOM.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/**
 * @typedef {Object} RefreshPlan
 * @property {string[]} moved     sources whose latest_period or row count changed
 * @property {string[]} sections  section keys to re-render, in registry order
 * @property {string[]} blocked   sections that rendered an empty 'not available'
 *                                state since the last refresh, so their blank
 *                                state is retried once even if nothing moved
 */

/**
 * Compare two bundle source headers and list the ones that changed.
 *
 * A shard whose latest_period or row_count differs describes different data:
 * the publisher only rewrites a shard when its source actually moved.
 *
 * @param {Record<string, {latest_period?: string, row_count?: number}|undefined>} before
 * @param {Record<string, {latest_period?: string, row_count?: number}|undefined>} after
 * @returns {string[]} source keys
 */
/**
 * Row count from a source header or a parsed shard, whichever shape it is.
 * @param {{rows?: number, row_count?: number, data?: unknown[]}|undefined} shard
 * @returns {number|null}
 */
function shardRows(shard) {
  if (!shard) return null;
  if (typeof shard.row_count === 'number') return shard.row_count;
  if (typeof shard.rows === 'number') return shard.rows;
  if (Array.isArray(shard.data)) return shard.data.length;
  return null;
}

export function movedSources(before, after) {
  const prev = before && before.sources ? before.sources : before || {};
  const next = after && after.sources ? after.sources : after || {};
  const moved = [];
  for (const [key, nextShard] of Object.entries(next)) {
    const prevShard = prev[key];
    if (!prevShard) {
      // A source the old bundle did not carry at all is new data.
      moved.push(key);
      continue;
    }
    const nextPeriod = nextShard && nextShard.latest_period;
    const prevPeriod = prevShard.latest_period;
    if (nextPeriod !== prevPeriod) {
      moved.push(key);
      continue;
    }
    // A revision (same latest period, corrected rows) still changes numbers.
    // Compare row counts only when both sides are index headers: the live
    // bundle's sources hold data and row_count, the index holds rows, so a
    // mismatched pair must not read as a change.
    const nextRows = shardRows(nextShard);
    const prevRows = shardRows(prevShard);
    if (nextRows !== null && prevRows !== null && nextRows !== prevRows) moved.push(key);
  }
  for (const key of Object.keys(prev)) {
    if (!(key in next)) moved.push(key);
  }
  return moved;
}

/**
 * Sections to re-render for a set of moved sources.
 *
 * @param {{[section: string]: string|string[]}} sections  section key -> sources
 * @param {string[]} moved
 * @returns {string[]} section keys, in registry order
 */
export function sectionsForSources(sections, moved) {
  const hit = new Set(moved || []);
  if (!hit.size) return [];
  const out = [];
  for (const [name, keys] of Object.entries(sections || {})) {
    const list = Array.isArray(keys) ? keys : [keys];
    if (list.some((k) => hit.has(k))) out.push(name);
  }
  return out;
}

/**
 * Build the refresh plan.
 *
 * `blocked` is the set of section keys whose rendered output said "this data is
 * not here". They are re-run even when their sources did not move, because the
 * fix for a blocked panel is often the presence of data, not a new period, and
 * re-running an empty panel is cheap.
 *
 * @param {{sections: {[section: string]: string|string[]}, before: any, after: any,
 *   blocked?: Iterable<string>}} opts
 * @returns {RefreshPlan}
 */
export function planRefresh({ sections, before, after, blocked = [] }) {
  const moved = movedSources(before, after);
  const byData = new Set(sectionsForSources(sections, moved));
  const blockedKeys = [...(blocked || [])];
  const all = new Set([...byData, ...blockedKeys]);
  // Registry order keeps the render sequence stable across refreshes; a blocked
  // section the registry does not know about still gets its one retry, last.
  const ordered = [
    ...Object.keys(sections || {}).filter((name) => all.has(name)),
    ...blockedKeys.filter((name) => !(name in (sections || {}))),
  ];
  return { moved, sections: ordered, blocked: blockedKeys };
}

/**
 * Whether a manifest names a later build than the one the page is drawing.
 *
 * The bundle hash covers the source shards; the health sidecar and coverage
 * aggregates ride in the bundle but not in its hash. A publish that only
 * refreshed those therefore keeps the hash and moves `generated_at`, which is
 * the only evidence such a publish left.
 *
 * @param {string|null} manifestGeneratedAt
 * @param {string|null} loadedGeneratedAt
 * @returns {boolean}
 */
export function isNewerGeneration(manifestGeneratedAt, loadedGeneratedAt) {
  if (!manifestGeneratedAt || !loadedGeneratedAt) return false;
  const a = Date.parse(manifestGeneratedAt);
  const b = Date.parse(loadedGeneratedAt);
  // Unparseable timestamps compare as equal: re-running cheap chrome on every
  // check for a malformed date would be worse than missing one publish.
  if (Number.isNaN(a) || Number.isNaN(b)) return false;
  return a > b;
}

/**
 * Whether the loaded bundle is behind the manifest the page was served with.
 *
 * @param {string|null} manifestHash  hash from the last manifest read
 * @param {string|null} loadedHash    hash of the bundle currently drawn
 * @returns {boolean}
 */
export function isNewerBundle(manifestHash, loadedHash) {
  if (!manifestHash) return false;
  // With no recorded bundle hash (a partial boot) there is nothing to compare,
  // so nothing is treated as new; the page will reload on next visit anyway.
  if (!loadedHash) return false;
  return String(manifestHash) !== String(loadedHash);
}
