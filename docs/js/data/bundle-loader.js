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
 *
 * Every fallback names the STABLE `bundle.json`, never a hashed file: Pages
 * deploys only what git tracks, and the hash-suffixed artifacts are gitignored
 * build output that exists on the publisher's disk and nowhere else. A stale
 * committed manifest that outlives its shards would otherwise 404 in the
 * fallback as well and blank the whole page instead of one panel.
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
        const err = new Error(`HTTP ${response.status} for ${url}`);
        err.status = response.status;
        throw err;
      }
      // 5xx — transient server / CDN propagation error, will retry
      lastError = new Error(`HTTP ${response.status} for ${url}`);
      lastError.status = response.status;
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

/**
 * Fetch one data file, recovering from a publish whose shards are not there.
 *
 * The publisher keeps only the current bundle hash plus a two-hash rollback
 * window (`_prune_stale_bundles` in publishers/export_dashboard_json.py). A tab
 * that booted before those publishes and then scrolls to a chapter it had not
 * opened yet asks for `src.gulf_south.<old>.json` — a 404 that used to leave
 * that panel permanently empty. A 404 on a hashed file means the bundle expired,
 * not that the data is gone, so take that source from the stable `bundle.json`;
 * `ensureSource` then rebases on the live publish before it needs anything else.
 *
 * @param {string} url
 * @param {any} bundle  whose `__index` is invalidated
 * @param {string} key  source key, for logging
 * @returns {Promise<Response>}
 */
async function fetchShard(url, bundle, key) {
  try {
    return await fetchWithRetry(url);
  } catch (err) {
    if (err && err.status !== 404) throw err;
    console.warn(`[bluetide] shard for '${key}' is gone — taking it from the stable bundle`);
    bundle.__index = null;
    bundle.__expired = true;
    throw err;
  }
}

/**
 * Split a source out of the stable monolithic bundle.
 *
 * `bundle.json` is the one data artifact git tracks, so it is the one that is
 * always on the deployed site. It is ~90 MB, so this is the last resort — and
 * once fetched it is cached on the bundle, so recovering several sources from
 * the same publish parses it once.
 *
 * @param {any} bundle
 * @param {string} sourceKey
 * @returns {Promise<any|null>} that source's entry, or null when the stable
 *   bundle does not exist or does not carry it
 */
async function sourceFromStableBundle(bundle, sourceKey) {
  if (!bundle.__stable) {
    bundle.__stable = (async () => {
      try {
        const resp = await fetchWithRetry('./data/bundle.json', { cache: 'no-cache' });
        return await resp.json();
      } catch (err) {
        console.warn('[bluetide] stable bundle.json unavailable', err && err.message);
        return null;
      }
    })();
  }
  const stable = await bundle.__stable;
  return (stable && stable.sources && stable.sources[sourceKey]) || null;
}

/**
 * Recover a source from the stable bundle into `bundle.sources`.
 *
 * @param {any} bundle
 * @param {string} sourceKey
 * @returns {Promise<boolean>} whether the source is now present
 */
async function recoverFromStableBundle(bundle, sourceKey) {
  const entry = await sourceFromStableBundle(bundle, sourceKey);
  if (!entry) return false;
  bundle.sources[sourceKey] = entry;
  console.info(`[bluetide] '${sourceKey}' recovered from the stable bundle.json`);
  return true;
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
 * Read the manifest without loading any data. Always fetched fresh.
 *
 * The page re-checks this to notice a publish that happened after boot; the
 * loader itself only reads it once, so `main.js` needs the same fetch with the
 * same caching rules.
 *
 * @returns {Promise<{hash: string, index_url?: string, bundle_url: string, generated_at?: string}>}
 */
export async function readManifest() {
  const resp = await fetchWithRetry(MANIFEST_URL, { cache: 'no-cache' });
  if (!resp.ok) throw new Error(`Manifest fetch failed: ${resp.status}`);
  return resp.json();
}

/**
 * Load the bundle a manifest names, caching by hash.
 *
 * A refresh passes the hash already on disk; two tabs noticing the same publish
 * must not parse the same shards twice.
 *
 * @param {{hash: string, index_url?: string, bundle_url: string}} manifest
 * @param {Map<string, Promise<any>>} [cache]
 * @returns {Promise<any>}
 */
export function loadBundleFromManifest(manifest, cache = new Map()) {
  const key = String(manifest && manifest.hash);
  if (cache.has(key)) return cache.get(key);
  const task = loadFrom(manifest);
  cache.set(key, task);
  // A failed parse must not pin the failure: the next check retries.
  task.catch(() => cache.delete(key));
  return task;
}

/**
 * Boot: manifest → index → parse only core sources.
 *
 * @returns {Promise<any>} bundle-like object (sources partially populated)
 */
export async function loadBundle() {
  const manifest = await readManifest();
  return loadFrom(manifest);
}

/**
 * @param {any} manifest
 * @returns {Promise<any>}
 */
async function loadFrom(manifest) {
  // The hashed files are gitignored build output. Where only the stable
  // bundle.json is deployed, a manifest that names anything else is stale
  // (a commit landing ahead of the Publish Dashboard Bundle run), so fetch the
  // stable bundle first — a 404 there means the manifest is current.
  if (!manifest.index_url) {
    console.warn('[bluetide] no index in manifest — falling back to full-bundle parse');
    const bundle = await (await fetchWithRetry('./data/bundle.json', { cache: 'no-cache' })).json();
    return attachHelpers({ ...bundle, hash: manifest.hash });
  }

  const indexUrl = `./data/${manifest.index_url}?v=${manifest.hash}`;
  let index;
  try {
    index = await (await fetchWithRetry(indexUrl)).json();
  } catch (err) {
    // Shards not deployed — once with the stable bundle.json, once by parsing
    // the bundle a manifest naming live shards would point at.
    const plannedUrl = `./data/${manifest.bundle_url}?v=${manifest.hash}`;
    let bundle;
    try {
      console.warn(
        `[bluetide] index shard ${manifest.index_url} unavailable — falling back to stable bundle.json`
      );
      bundle = await (await fetchWithRetry('./data/bundle.json', { cache: 'no-cache' })).json();
    } catch (stableErr) {
      console.warn(
        `[bluetide] stable bundle.json unavailable — falling back to ${manifest.bundle_url}`
      );
      bundle = await (await fetchWithRetry(plannedUrl)).json();
      return attachHelpers({ ...bundle, hash: manifest.hash });
    }
    const wrapped = attachHelpers({ ...bundle, hash: manifest.hash });
    // Booters read source metadata off `__index`; give the sharded path an
    // equivalent view of what this bundle actually carries.
    wrapped.__index = {
      hash: manifest.hash,
      sources: Object.fromEntries(
        Object.entries(bundle.sources || {}).map(([key, entry]) => [
          key,
          {
            rows: entry.row_count,
            latest_period: entry.latest_period,
            file: `bundle.json#${key}`,
          },
        ])
      ),
    };
    return wrapped;
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
  if (!bundle.__index || !bundle.__index.sources?.[sourceKey]) {
    // A bundle may be expired: a shard it named has since been pruned. Its
    // index no longer knows the key, so ask the live publish — unless the
    // stable bundle already carries the source, which needs no rebase.
    if (bundle.__expired) {
      if (!(await recoverFromStableBundle(bundle, sourceKey))) {
        await rebaseOnCurrentPublish(bundle);
      }
    } else {
      throw new Error(`ensureSource: unknown source '${sourceKey}'`);
    }
  }
  if (bundle.sources?.[sourceKey]) {
    notifyListeners(bundle, sourceKey);
    return;
  }
  const index = bundle.__index;
  if (!index || !index.sources?.[sourceKey]) {
    throw new Error(`ensureSource: unknown source '${sourceKey}' in the live publish`);
  }
  const inflight = bundle.__pending.get(sourceKey);
  if (inflight) return inflight;

  const task = (async () => {
    const meta = index.sources[sourceKey];
    const t0 = performance.now();
    try {
      let resp;
      try {
        resp = await fetchShard(`./data/${meta.file}?v=${index.hash}`, bundle, sourceKey);
      } catch (err) {
        if (err?.status !== 404) throw err;
        // The shard was not deployed: the index is stale, so take this source
        // from the stable bundle (or the live publish, when shards exist)
        // rather than leaving this panel empty for the rest of the session.
        if (await recoverFromStableBundle(bundle, sourceKey)) {
          notifyListeners(bundle, sourceKey);
          return;
        }
        await rebaseOnCurrentPublish(bundle);
        const meta2 = bundle.__index?.sources?.[sourceKey];
        if (!meta2) throw err;
        try {
          resp = await fetchWithRetry(`./data/${meta2.file}?v=${bundle.__index.hash}`);
        } catch (err2) {
          if (!(await recoverFromStableBundle(bundle, sourceKey))) throw err2;
          notifyListeners(bundle, sourceKey);
          return;
        }
      }
      bundle.sources[sourceKey] = await resp.json();
      console.info(`[bluetide] lazy-loaded '${sourceKey}' in ${(performance.now() - t0).toFixed(0)} ms`);
      notifyListeners(bundle, sourceKey);
    } finally {
      bundle.__pending.delete(sourceKey);
    }
  })();
  bundle.__pending.set(sourceKey, task);
  return task;
}

/**
 * Fire the callbacks waiting on a lazily-loaded source.
 *
 * @param {any} bundle
 * @param {string} sourceKey
 */
function notifyListeners(bundle, sourceKey) {
  const listeners = bundle.__listeners && bundle.__listeners.get(sourceKey);
  if (listeners) listeners.forEach((fn) => fn());
}

/**
 * Point an expired bundle at the publish the manifest names now.
 *
 * Only the index is replaced: sources already parsed stay, so panels keep the
 * numbers they drew and a reader never sees a blank page because a shard was
 * pruned. `main.js` follows up with its own refresh plan, which re-runs the
 * panels whose sources actually moved.
 *
 * @param {any} bundle
 * @returns {Promise<void>}
 */
async function rebaseOnCurrentPublish(bundle) {
  const manifest = await readManifest();
  // A manifest from before indexes is stale until the publisher catches up.
  if (!manifest.index_url) {
    console.warn('[bluetide] rebase: manifest has no index — keeping the current index');
    return;
  }
  const resp = await fetchWithRetry(`./data/${manifest.index_url}?v=${manifest.hash}`, { cache: 'no-cache' });
  const index = await resp.json();
  bundle.__index = index;
  bundle.__expired = false;
  console.info(`[bluetide] rebased on publish ${index.hash} (was ${bundle.hash})`);
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
    if (!bundle.__listeners) bundle.__listeners = new Map();
    const list = bundle.__listeners.get(key) || [];
    list.push(fn);
    bundle.__listeners.set(key, list);
  });
}
