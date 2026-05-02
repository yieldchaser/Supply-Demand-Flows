/**
 * Bundle Loader — fetches manifest.json → hashed bundle.
 *
 * Why: the bundle URL changes per publish (hash-versioned for cache-busting).
 * We fetch manifest.json first (always fresh), then the bundle it points to.
 *
 * Failure modes:
 *   - manifest.json 404 → boot error with clear message
 *   - bundle 404 / malformed JSON → boot error, preserve manifest context
 *   - Transient CDN error (5xx) → retried up to 3 times with linear backoff
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

export async function loadBundle() {
  const manifestResp = await fetchWithRetry(MANIFEST_URL, { cache: 'no-cache' });
  if (!manifestResp.ok) {
    throw new Error(`Manifest fetch failed: ${manifestResp.status}`);
  }
  const manifest = await manifestResp.json();
  const bundleUrl = `./data/${manifest.bundle_url}?v=${manifest.hash}`;
  const bundleResp = await fetchWithRetry(bundleUrl, { cache: 'default' });
  if (!bundleResp.ok) {
    throw new Error(`Bundle fetch failed: ${bundleResp.status} at ${bundleUrl}`);
  }
  const bundle = await bundleResp.json();

  // Enrich with parsed timestamps + helpers
  bundle.generatedAt = new Date(bundle.generated_at);
  console.info(`[bluetide] Loaded bundle ${manifest.hash}, generated ${manifest.generated_at}`);
  bundle.hash = manifest.hash;

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
