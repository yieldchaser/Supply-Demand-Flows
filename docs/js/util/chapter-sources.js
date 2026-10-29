/**
 * Chapter sources — load exactly the shards a chapter's export reads.
 *
 * Why this exists:
 *   The chapter export used to call `loadBundle()`, which boots a *second*
 *   bundle: manifest, index, and the three CORE shards. That is not the bundle
 *   the page is showing — `main.js` owns the live one — and it is not even the
 *   bundle the chapter reads. A reader exporting from the Macro chapter refetched
 *   storage and supply data they had already downloaded and never fetched the
 *   GIE history that chapter is about. If the publish changed mid-session, the
 *   download also disagreed with the screen it was downloaded from.
 *
 *   So the exporter gets the live bundle plus a source loader built on the same
 *   `ensureSource` the panels use: the export fetches only what its own chapter
 *   declares in `Chapter.sources`, and re-uses anything already parsed.
 *
 * Failure modes:
 *   - `bundle` null (the page boot never happened, or the export is exercised
 *     outside the app) -> the caller falls back to `loadBundle()` itself.
 *   - A source shard fails to load -> `report.loadFailed` names it; the export
 *     still ships every source it did get rather than refusing the whole file,
 *     matching how a failing panel does not blank the page.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/**
 * Make every source a chapter declares available on `bundle`.
 *
 * @param {any} bundle  live bundle from bundle-loader
 * @param {{ensureSource: Function}} loader  bundle-loader module
 * @param {{sources?: string[]}} chapter
 * @returns {Promise<{loaded: string[], already: string[], loadFailed: string[]}>}
 */
export async function ensureChapterSources(bundle, loader, chapter) {
  const keys = Array.isArray(chapter?.sources) ? chapter.sources : [];
  const loaded = [];
  const already = [];
  const loadFailed = [];

  await Promise.all(
    keys.map(async (key) => {
      if (bundle?.sources?.[key]) {
        already.push(key);
        return;
      }
      try {
        await loader.ensureSource(bundle, key);
        loaded.push(key);
      } catch (err) {
        loadFailed.push(key);
        console.error(`[chapter-export] source '${key}' unavailable:`, err);
      }
    }),
  );

  // Stable order regardless of which fetch resolved first.
  const order = (a, b) => keys.indexOf(a) - keys.indexOf(b);
  return {
    loaded: loaded.sort(order),
    already: already.sort(order),
    loadFailed: loadFailed.sort(order),
  };
}
