/**
 * Per-chart snapshots — "download the data behind this chart".
 *
 * The chapter export (chapter-export.js) ships the rows a chapter reads; this is
 * the narrower question a reader actually asks in front of one chart: *what are
 * the plotted points?* Ranges are filtered, closures roll up, envelopes are
 * derived — the rows that went into the SVG are not the rows in the bundle.
 *
 * Panels register what they plotted. A single delegated click handler on
 * `document` turns any `[data-chart-export]` element (usually a small button in
 * the panel header) into a download, so no panel needs its own listener, and a
 * chart that re-renders on range change simply re-registers.
 *
 * Registry is per-page-lifetime and keyed by chart id; re-rendering replaces the
 * previous snapshot rather than piling up stale ones.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/** @type {Map<string, {label: string, rows: unknown[], meta: Object}>} */
const registry = new Map();

/**
 * Register (or replace) the snapshot for one chart.
 *
 * @param {string} id     stable chart id, matching the button's data-chart-export
 * @param {string} label  filename stem, e.g. 'storage-envelope'
 * @param {unknown[]} rows rows exactly as drawn
 * @param {Object} meta   range/filters/basis, free-form; surfaced in the file
 * @returns {{key: string}}
 */
export function registerChartSnapshot(id, label, rows, meta = {}) {
  registry.set(id, { label, rows: Array.isArray(rows) ? rows : [], meta });
  return { key: id };
}

/**
 * Markup for the header button. Renders nothing when the chart is not
 * registered, so a panel can call it unconditionally.
 *
 * @param {string} id
 * @param {string} [text]
 * @returns {string}
 */
export function chartExportButton(id, text = '↓ CSV') {
  if (!registry.has(id)) return '';
  return `<button type="button" class="chart-export-btn" data-chart-export="${id}" title="Download the plotted points as CSV">${text}</button>`;
}

/**
 * Build a CSV string. Header row is the union of keys across rows, in first-seen
 * order, so heterogeneous rows (some with an optional field) stay aligned.
 *
 * @param {unknown[]} rows
 * @returns {string}
 */
export function toCsv(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return '';
  const flat = rows.map((r) => (r && typeof r === 'object' ? r : { value: r }));
  const keys = [];
  const seen = new Set();
  flat.forEach((r) => {
    Object.keys(r).forEach((k) => {
      if (!seen.has(k)) {
        seen.add(k);
        keys.push(k);
      }
    });
  });
  const cell = (v) => {
    if (v == null) return '';
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [keys.join(',')];
  flat.forEach((r) => lines.push(keys.map((k) => cell(r[k])).join(',')));
  return lines.join('\n');
}

/**
 * Full file text: provenance comments first (a CSV without provenance cannot be
 * audited), then the header and rows.
 *
 * @param {{label: string, rows: unknown[], meta: Object}} snap
 * @param {Date} [now]
 * @returns {string}
 */
export function snapshotToCsv(snap, now = new Date()) {
  const meta = snap.meta || {};
  const head = [
    `# bluetide chart export — ${snap.label}`,
    `# exported_at: ${now.toISOString()}`,
    `# rows: ${snap.rows.length}`,
  ];
  Object.entries(meta).forEach(([k, v]) => {
    if (v == null || typeof v === 'object') return;
    head.push(`# ${k}: ${v}`);
  });
  head.push('#');
  return `${head.join('\n')}\n${toCsv(snap.rows)}\n`;
}

/**
 * Trigger a browser download of the registered chart.
 *
 * @param {string} id
 * @param {Document} doc
 * @param {Date} [now]
 * @returns {boolean} whether anything was written
 */
export function downloadChartSnapshot(id, doc, now = new Date()) {
  const snap = registry.get(id);
  if (!snap) return false;
  const text = snapshotToCsv(snap, now);
  const blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = doc.createElement('a');
  a.href = url;
  a.download = `bluetide_${snap.label}_${now.toISOString().slice(0, 10)}.csv`;
  a.style.display = 'none';
  doc.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  return true;
}

/**
 * Install the delegated handler once.
 *
 * @param {Document} doc
 * @returns {void}
 */
export function initChartExports(doc) {
  if (!doc || doc.__bluetideChartExports) return;
  doc.__bluetideChartExports = true;
  doc.addEventListener('click', (ev) => {
    const target = /** @type {HTMLElement|null} */ (ev.target);
    const btn = target && target.closest ? target.closest('[data-chart-export]') : null;
    if (!btn) return;
    ev.preventDefault();
    downloadChartSnapshot(btn.getAttribute('data-chart-export'), doc);
  });
}

/** @returns {number} registered chart count (tests + debugging) */
export function registeredCount() {
  return registry.size;
}
