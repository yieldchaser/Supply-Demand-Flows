/**
 * Bundle Loader — fetches manifest.json → index → core sources at boot;
 * remaining sources load lazily per-panel.
 *
 * Why: the bundle URL changes per publish (hash-versioned for cache-busting).
 * Measured client cost of parsing the whole 60 MB bundle in one go was
 * ~2.6 s JSON.parse + ~500 MB heap @4x CPU throttle (2026-08-25,
 * scripts/measure_bundle_parse.py) — most of it for sources (GIE, enbridge
 * raw TETCO, gulf_south) whose panels sit far down the page. The loader now:
 *
 *   1. fetches manifest.json (always fresh) → index.{hash}.json
 *   2. parses CORE sources immediately (small; power above-the-fold panels)
 *   3. exposes getSource(key): awaits that source's shard on first access,
 *      caches the parsed result, and fires listeners so affected panels can
 *      re-render when a late shard lands.
 *
 * Failure modes:
 *   - manifest.json 404 → boot error with clear message
 *   - index/shard 404 / malformed JSON → error surfaced to the caller;
 *     panels already rendered from other sources are unaffected
 *   - Transient CDN error (5xx) → retried up to 3 times with linear backoff
 *   - No index in manifest (old publish) → falls back to monolithic bundle
 */

const MANIFEST_URL = './data/manifest.json';

/**
 * Fetch with automatic retry on transient failures.
 *
 * - Retries on network errors and HTTP 5xx responses.
 * - Fails fast on HTTP 4xx (permanent client-side errors).
 * - Applies linear backoff: 1s after attempt 1, 2s after attempt 2, etc.
 *
 * @param {string} url
 * @param {RequestInit} options
 * @param {number} maxAttempts
 * @param {number} backoffMs - delay multiplier in ms (backoffMs * attempt)
 * @returns {Promise<Response>}
 */
async function fetchWithRetry(url, options = {}, maxAttempts = 3, backoffMs = 1000) {
  let lastError;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      // Fail fast on 4xx — these are permanent errors (wrong URL, auth, etc.)
      if (response.status < 500) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }
      // 5xx — transient server / CDN propagation error, will retry
      lastError = new Error(`HTTP ${response.status} for ${url}`);
    } catch (err) {
      lastError = err;
      // Re-throw immediately on 4xx (thrown above, not a network error)
      if (err.message && err.message.startsWith('HTTP 4')) throw err;
    }
    if (attempt < maxAttempts - 1) {
      await new Promise((resolve) => setTimeout(resolve, backoffMs * (attempt + 1)));
    }
  }
  throw lastError;
}

/** Attach vintage helper shared by boot and lazy paths.
 *
 * @param {any} bundle
 * @returns {any} same bundle with helpers attached
 */
function attachHelpers(bundle) {
  if (bundle.sourceVintage) return bundle;
  bundle.generatedAt = new Date(bundle.generated_at);

  /**
   * Returns vintage info for a source key.
   * @param {string} sourceKey
   * @returns {{ latest: Date, ageDays: number } | null}
   */
  bundle.sourceVintage = (sourceKey) => {
    const src = bundle.sources?.[sourceKey];
    if (!src || !src.latest_period) return null;
    const latest = new Date(src.latest_period);
    const ageDays = Math.floor((Date.now() - latest.getTime()) / 86400000);
    return { latest, ageDays };
  };
  return bundle;
}

/**
 * Boot: manifest → index → parse only core sources.
 *
 * @returns {Promise<any>} bundle-like object (sources partially populated)
 */
export async function loadBundle() {
  const manifestResp = await fetchWithRetry(MANIFEST_URL, { cache: 'no-cache' });
  if (!manifestResp.ok) {
    throw new Error(`Manifest fetch failed: ${manifestResp.status}`);
  }
  const manifest = await manifestResp.json();

  // Legacy fallback: an older publish has no index_url — parse everything.
  if (!manifest.index_url) {
    console.warn('[bluetide] no index in manifest — falling back to full-bundle parse');
    const bundleUrl = `./data/${manifest.bundle_url}?v=${manifest.hash}`;
    const bundle = await (await fetchWithRetry(bundleUrl)).json();
    return attachHelpers({ ...bundle, hash: manifest.hash });
  }

  const indexUrl = `./data/${manifest.index_url}?v=${manifest.hash}`;
  let index;
  try {
    index = await (await fetchWithRetry(indexUrl)).json();
  } catch (err) {
    // Shards not deployed (e.g. gitignored build artifacts on Pages) — fall
    // back to the monolithic bundle.json, which is always tracked + served.
    console.warn(
      `[bluetide] index shard ${manifest.index_url} unavailable — falling back to full-bundle parse`
    );
    const bundleUrl = `./data/${manifest.bundle_url}?v=${manifest.hash}`;
    const bundle = await (await fetchWithRetry(bundleUrl)).json();
    return attachHelpers({ ...bundle, hash: manifest.hash });
  }

  /** @type {any} */
  const bundle = {
    generated_at: index.generated_at,
    hash: index.hash,
    sources: {},
    health: {},
    __index: index,
    __pending: new Map(),
    __listeners: new Map(),
  };

  const t0 = performance.now();
  await Promise.all(
    index.core.map(async (key) => {
      const meta = index.sources[key];
      const resp = await fetchWithRetry(`./data/${meta.file}?v=${index.hash}`);
      bundle.sources[key] = await resp.json();
    })
  );
  console.info(
    `[bluetide] boot: ${index.core.length} core sources parsed in ${(performance.now() - t0).toFixed(0)} ms`
    + ` (${Object.keys(index.sources).length - index.core.length} more lazy)`
  );
  return attachHelpers(bundle);
}

/**
 * Lazily ensure one source is parsed. Resolves immediately when present;
 * otherwise fetches + parses its shard once, caches, notifies listeners.
 *
 * @param {any} bundle
 * @param {string} sourceKey
 * @returns {Promise<void>} resolves when bundle.sources[sourceKey] exists
 */
export async function ensureSource(bundle, sourceKey) {
  if (bundle.sources?.[sourceKey]) return;
  const index = bundle.__index;
  if (!index || !index.sources?.[sourceKey]) {
    throw new Error(`ensureSource: unknown source '${sourceKey}'`);
  }
  const inflight = bundle.__pending.get(sourceKey);
  if (inflight) return inflight;

  const task = (async () => {
    const meta = index.sources[sourceKey];
    const t0 = performance.now();
    const resp = await fetchWithRetry(`./data/${meta.file}?v=${index.hash}`);
    bundle.sources[sourceKey] = await resp.json();
    console.info(`[bluetide] lazy-loaded '${sourceKey}' in ${(performance.now() - t0).toFixed(0)} ms`);
    const listeners = bundle.__listeners.get(sourceKey);
    if (listeners) listeners.forEach((fn) => fn());
    bundle.__pending.delete(sourceKey);
  })();
  bundle.__pending.set(sourceKey, task);
  return task;
}

/**
 * Register a callback fired when a lazily-loaded source lands (for panels
 * that rendered a placeholder before their data arrived).
 *
 * @param {any} bundle
 * @param {string|string[]} sourceKeys
 * @param {Function} fn
 */
export function onSourceReady(bundle, sourceKeys, fn) {
  const keys = Array.isArray(sourceKeys) ? sourceKeys : [sourceKeys];
  keys.forEach((key) => {
    const list = bundle.__listeners.get(key) || [];
    list.push(fn);
    bundle.__listeners.set(key, list);
  });
}
