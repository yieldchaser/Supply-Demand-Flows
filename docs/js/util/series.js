/**
 * Series utilities — query helpers for the long-format curated data schema.
 * Schema: source | series_id | series_name | period | value | unit | region | ingested_at
 */

/**
 * Filter rows from a source by series_id, returning chronologically sorted records.
 * @param {Array} rows - bundle.sources[sourceKey].data
 * @param {string} seriesId - exact series_id match
 * @returns {Array<{period: Date, value: number, ...}>}
 */
export function getSeries(rows, seriesId) {
  return rows
    .filter((r) => r.series_id === seriesId)
    .map((r) => ({ ...r, period: new Date(r.period), value: Number(r.value) }))
    .sort((a, b) => a.period - b.period);
}

/**
 * Group rows by series_id, return a Map<series_id, sorted-rows>.
 */
export function groupBySeriesId(rows) {
  const map = new Map();
  for (const r of rows) {
    if (!map.has(r.series_id)) map.set(r.series_id, []);
    map.get(r.series_id).push(r);
  }
  for (const arr of map.values()) {
    for (const item of arr) {
      item.period = new Date(item.period);
      item.value = Number(item.value);
    }
    arr.sort((a, b) => a.period - b.period);
  }
  return map;
}

/**
 * For a weekly series, compute week-of-year statistics for a band overlay.
 * Returns Map<weekNumber, {min, max, mean, p25, p75, count}> across prior years.
 *
 * Uses all data from non-current years. The `lookbackYears` limit is applied
 * only as a soft cap — if less data is available (e.g., bundle only covers
 * 1 year), all available prior-year data is still used so the envelope isn't
 * empty for weeks not covered by the prior calendar year's window.
 */
export function computeWeeklyEnvelope(rows, lookbackYears = 5) {
  const now = new Date();
  const currentYear = now.getFullYear();
  // Soft lower cutoff — only exclude data older than lookbackYears
  const cutoff = new Date(currentYear - lookbackYears, 0, 1);
  // Use all rows from prior calendar years within the lookback window
  const filtered = rows.filter((r) => {
    const yr = r.period instanceof Date ? r.period.getFullYear() : new Date(r.period).getFullYear();
    return yr < currentYear && r.period >= cutoff;
  });

  const byWeek = new Map();
  for (const r of filtered) {
    const wk = isoWeek(r.period instanceof Date ? r.period : new Date(r.period));
    if (!byWeek.has(wk)) byWeek.set(wk, []);
    byWeek.get(wk).push(Number(r.value));
  }

  const envelope = new Map();
  for (const [wk, vals] of byWeek) {
    const sorted = [...vals].sort((a, b) => a - b);
    envelope.set(wk, {
      min: sorted[0],
      max: sorted[sorted.length - 1],
      mean: vals.reduce((s, v) => s + v, 0) / vals.length,
      p25: sorted[Math.floor(sorted.length * 0.25)],
      p75: sorted[Math.floor(sorted.length * 0.75)],
      count: vals.length,
    });
  }
  return envelope;
}

/**
 * ISO week number (1-53) for a given date.
 */
export function isoWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
}

/**
 * Compute year-over-year deltas for a series.
 * Returns rows enriched with .yoyDelta and .yoyPct fields.
 */
export function computeYoY(rows) {
  return rows.map((r) => {
    const yearAgo = new Date(r.period.getFullYear() - 1, r.period.getMonth(), r.period.getDate());
    const match = rows
      .filter((p) => Math.abs(p.period - yearAgo) < 7 * 86400000)
      .sort((a, b) => Math.abs(a.period - yearAgo) - Math.abs(b.period - yearAgo))[0];
    return {
      ...r,
      yoyDelta: match ? r.value - match.value : null,
      yoyPct: match && match.value !== 0 ? (r.value - match.value) / match.value : null,
    };
  });
}
