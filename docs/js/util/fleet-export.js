/**
 * Fleet export — the numbers behind the LNG card grid, as rows.
 *
 * The fleet overview renders one card per terminal from `terminalSummary()`.
 * That summary is the *only* place where the headline meter, the parity rule
 * (incomplete days dropped), the operating level and the outage classification
 * are stitched together — so a reader who wants to check a card number has no
 * way to do it from the chart snapshots alone. This module exposes exactly the
 * same summaries as flat rows, and additionally the per-day fleet aggregate.
 *
 * Deliberately thin: it computes nothing new. If a number is here, it is the
 * number the card shows, because it came from the same function.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

import { terminalSummary } from './lng-fleet-data.js';
import { LNG_TERMINALS } from './lng-terminals.js';

/**
 * One row per registered terminal.
 *
 * Non-operational terminals and terminals whose source is absent still produce
 * a row, with `ok: false` — silently omitting them would make the export look
 * like a complete fleet when it is not.
 *
 * @param {Object} bundle
 * @returns {Array<Object>}
 */
export function fleetSummaryRows(bundle) {
  return Object.values(LNG_TERMINALS).map((t) => {
    let s;
    try {
      s = terminalSummary(bundle, t);
    } catch (err) {
      s = { ok: false, error: String(err && err.message ? err.message : err) };
    }
    const operational = t.operational !== false;
    return {
      terminal_id: t.id,
      terminal: t.display,
      nameplate_mmcf_d: t.nameplate,
      signal: t.signal || 'sq',
      operational,
      ok: Boolean(s.ok),
      latest_mmcf_d: s.ok ? Number(s.latest.toFixed(1)) : null,
      utilization_pct: s.ok ? Number(s.utilPct.toFixed(1)) : null,
      wow_delta_mmcf_d: s.wow ? Number(s.wow.delta.toFixed(1)) : null,
      wow_pct: s.wow ? Number(s.wow.pct.toFixed(1)) : null,
      outage: s.outage ? s.outage.type : null,
      outage_days: s.outage ? s.outage.duration : null,
      days_of_history: s.days ?? null,
      first_day: s.daily && s.daily.length ? s.daily[0].dateStr : null,
      last_day: s.daily && s.daily.length ? s.daily[s.daily.length - 1].dateStr : null,
      error: s.error || null,
    };
  });
}

/**
 * Daily fleet total across every terminal that has a series.
 *
 * This is the sum of each terminal's *own* daily series, aligned on gas day.
 * It is NOT the headline aggregate the fleet panel draws (that one enforces
 * per-feed parity and uses a 60-day complete-day median); it is the audit trail
 * underneath it, so both are shipped and the difference is stated rather than
 * left for the reader to discover.
 *
 * @param {Object} bundle
 * @returns {Array<{date: string, mmcf_d: number, terminals_reporting: number}>}
 */
export function fleetDailyRows(bundle) {
  /** @type {Map<string, {sum: number, n: number}>} */
  const byDay = new Map();
  Object.values(LNG_TERMINALS).forEach((t) => {
    if (t.operational === false) return;
    let daily;
    try {
      daily = terminalSummary(bundle, t).daily || [];
    } catch {
      daily = [];
    }
    daily.forEach((d) => {
      const cur = byDay.get(d.dateStr) || { sum: 0, n: 0 };
      cur.sum += Number(d.value) || 0;
      cur.n += 1;
      byDay.set(d.dateStr, cur);
    });
  });
  return Array.from(byDay, ([date, v]) => ({
    date,
    mmcf_d: Number(v.sum.toFixed(1)),
    terminals_reporting: v.n,
  })).sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
}

/**
 * Everything the fleet card grid is built from.
 *
 * @param {Object} bundle
 * @param {{terminal?: string|null, range?: string|null}} [view]
 * @returns {Object} JSON-serialisable block for the chapter export
 */
export function fleetExport(bundle, view = {}) {
  const rows = fleetSummaryRows(bundle);
  const daily = fleetDailyRows(bundle);
  const operational = rows.filter((r) => r.operational && r.ok);
  return {
    basis: {
      summary: 'terminalSummary() — headline meter per terminal, incomplete parity days dropped',
      daily_total: 'sum of each terminal\'s own daily series, aligned on gas day (no parity filter)',
      units: 'MMcf/d = Dth/d ÷ 1.025 ÷ 1000',
      ...(view.terminal ? { selected_terminal: view.terminal } : {}),
      ...(view.range ? { range: view.range } : {}),
    },
    totals: {
      terminals: rows.length,
      reporting: operational.length,
      latest_mmcf_d: Number(operational.reduce((n, r) => n + (r.latest_mmcf_d || 0), 0).toFixed(1)),
      nameplate_mmcf_d: operational.reduce((n, r) => n + (r.nameplate_mmcf_d || 0), 0),
    },
    terminals: rows,
    daily,
  };
}
