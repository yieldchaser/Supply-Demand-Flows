/**
 * Chapter export — download the data behind what you are currently looking at.
 *
 * Two kinds of payload, deliberately separated:
 *
 *   1. TIMESERIES. The curated rows of every source the chapter reads, plus the
 *      per-source metadata the publisher shipped (latest period, row count,
 *      generated_at). This is the honest artifact: it is exactly the bundle
 *      rows, not a re-derived figure, so a reader can reproduce any number on
 *      screen with pandas and check it.
 *
 *   2. DERIVED. The view models the panels actually compute — fleet summary,
 *      supply/demand balance, transatlantic divergence, downtime events. These
 *      carry their own filter notes, because a derived number without the rule
 *      that produced it is a fabricated number waiting to happen ("observatory,
 *      not oracle").
 *
 * The file is self-describing: a `provenance` block names the bundle hash, the
 * build timestamp, the chapter, and the request time. A CSV with no provenance
 * cannot be audited; neither can one of these.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/**
 * @param {any} bundle
 * @param {string} key   bundle source key
 * @returns {{rows: unknown[], meta: Object|null}}
 */
export function sourceRows(bundle, key) {
  const src = bundle?.sources?.[key];
  if (!src) return { rows: [], meta: null };
  return {
    rows: Array.isArray(src.data) ? src.data : [],
    meta: {
      latest_period: src.latest_period ?? null,
      rows: Array.isArray(src.data) ? src.data.length : 0,
      generated_at: src.generated_at ?? null,
    },
  };
}

/**
 * Build the full export payload for one chapter.
 *
 * @param {any} bundle
 * @param {{id: string, label: string, sources: string[]}} chapter
 * @param {Object<string, Function>} [derived]  chapter id -> () => object
 * @param {Date} [now]
 * @param {{loaded?: string[], already?: string[], loadFailed?: string[]}} [report]
 *   per-source load outcome from chapter-sources.js, carried into provenance so
 *   a missing source reads as a named failure rather than an empty series
 * @returns {Object} JSON-serialisable payload
 */
export function buildChapterExport(bundle, chapter, derived = {}, now = new Date(), report = null) {
  const meta = bundle?.meta || bundle?.manifest || {};
  const sources = {};
  (chapter.sources || []).forEach((key) => {
    sources[key] = sourceRows(bundle, key);
  });

  const derivedBlock = {};
  const fn = derived[chapter.id];
  if (typeof fn === 'function') {
    try {
      derivedBlock[chapter.id] = fn();
    } catch (err) {
      // A failed derivation must not cost the reader the raw rows underneath it.
      derivedBlock[chapter.id] = { error: String(err && err.message ? err.message : err) };
    }
  }

  const rowCount = Object.values(sources).reduce((n, s) => n + s.rows.length, 0);
  return {
    schema: 'bluetide.chapter-export/1',
    provenance: {
      chapter: chapter.id,
      chapter_label: chapter.label,
      exported_at: now.toISOString(),
      bundle_hash: meta.hash ?? null,
      bundle_generated_at: meta.generated_at ?? null,
      rows: rowCount,
      source_load: report
        ? { fetched: report.loaded ?? [], cached: report.already ?? [], failed: report.loadFailed ?? [] }
        : null,
    },
    /// Filters that apply to the derived blocks. Empty for a raw dump on purpose.
    filter_notes: [
      'timeseries[n].rows are the published bundle rows, unfiltered.',
      'derived[*] blocks are view models computed in the browser; each states its own basis.',
      'units: value is Dth/d (raw). MMcf/d = Dth/d ÷ 1.025 ÷ 1000.',
    ],
    sources: Object.fromEntries(
      Object.entries(sources).map(([k, v]) => [k, { meta: v.meta, rows: v.rows }]),
    ),
    derived: derivedBlock,
  };
}

/**
 * Filename for a chapter export. Date-stamped so two downloads never overwrite
 * each other and so the bundle vintage is visible in a folder listing.
 *
 * @param {string} chapterId
 * @param {Date} now
 * @returns {string}
 */
export function exportFilename(chapterId, now = new Date()) {
  return `bluetide_${chapterId}_${now.toISOString().slice(0, 10)}.json`;
}
