/**
 * Bundle Loader — fetches manifest.json → hashed bundle.
 *
 * Why: the bundle URL changes per publish (hash-versioned for cache-busting).
 * We fetch manifest.json first (always fresh), then the bundle it points to.
 *
 * Failure modes:
 *   - manifest.json 404 → boot error with clear message
 *   - bundle 404 / malformed JSON → boot error, preserve manifest context
 */

const MANIFEST_URL = './data/manifest.json';

export async function loadBundle() {
  const manifestResp = await fetch(MANIFEST_URL, { cache: 'no-cache' });
  if (!manifestResp.ok) {
    throw new Error(`Manifest fetch failed: ${manifestResp.status}`);
  }
  const manifest = await manifestResp.json();
  const bundleUrl = `./data/${manifest.bundle_url}`;

  const bundleResp = await fetch(bundleUrl, { cache: 'force-cache' });
  if (!bundleResp.ok) {
    throw new Error(`Bundle fetch failed: ${bundleResp.status} at ${bundleUrl}`);
  }
  const bundle = await bundleResp.json();

  // Enrich with parsed timestamps + helpers
  bundle.generatedAt = new Date(bundle.generated_at);
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
