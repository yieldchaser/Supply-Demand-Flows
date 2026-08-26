/**
 * LNG metrics utilities — conversion calculations, nameplate capacities, and utilization level logic.
 */

/**
 * Registry of state-side LNG nameplate capacities in MMcf/d.
 *
 * THESE ARE THE DENOMINATORS FOR EVERY UTILIZATION FIGURE ON THE DASHBOARD.
 * Each value MUST carry a FERC/operator-filing source (see per-key JSDoc).
 * DO NOT CHANGE A NAMEPLATE WITHOUT: (a) citing the filing in the JSDoc, and
 * (b) a commit message that states the before→after and why. A silent
 * denominator change corrupts every utilization % silently (2026-08-26 lesson:
 * a report carried 2,260 for Freeport and 2,400 for Golden Pass that were
 * never in this registry — typos, but they show how easily the denominator
 * drifts). Authoritative reconciliation:
 *   - freeport_lng 2,100   — FERC CP12-509 (FERC-authorized base capacity)
 *   - cameron_lng 2,000   — FERC CP13-25
 *   - sabine_pass 4,500   — FERC CP11-72 (Cheniere cites 4.7 Bcf/d incl. T5/6)
 *   - corpus_christi 2,400 — FERC CP12-507
 *   - plaquemines 3,400   — FERC CP17-66
 *   - calcasieu_pass 1,300 — FERC CP15-550
 *   - cove_point 750      — FERC CP13-113
 *   - golden_pass 2,600   — FERC CP14-517 (EIA: ~18 MTPA ≈ 2.6 Bcf/d)
 *   - port_arthur 1,900   — FERC CP17-20
 *
 * @type {Object<string, number>}
 */
export const LNG_NAMEPLATE_MMCFD = {
  /** Source: Freeport LNG Expansion, L.P. FERC Docket No. CP12-509 */
  freeport_lng: 2100,
  /** Source: Sempra Cameron LNG Terminal FERC Docket No. CP13-25 */
  cameron_lng: 2000,
  /** Source: Cheniere Sabine Pass LNG Terminal FERC Docket No. CP11-72 */
  sabine_pass: 4500,
  /** Source: Cheniere Corpus Christi LNG Terminal FERC Docket No. CP12-507 */
  corpus_christi: 2400,
  /** Source: Venture Global Plaquemines LNG FERC Docket No. CP17-66 */
  plaquemines: 3400,
  /** Source: Venture Global Calcasieu Pass LNG FERC Docket No. CP15-550 */
  calcasieu_pass: 1300,
  /** Source: Berkshire Hathaway Cove Point LNG FERC Docket No. CP13-113 */
  cove_point: 750,
  /** Source: Golden Pass LNG Terminal FERC Docket No. CP14-517 */
  golden_pass: 2600,
  /** Source: Sempra Port Arthur LNG Terminal FERC Docket No. CP17-20 */
  port_arthur: 1900,
};

/**
 * Registry of LNG terminals and their pipeline delivery meters.
 * Used to retrieve pipeline data in a terminal-agnostic manner.
 *
 * @type {Object<string, {pipeline: string, loc_id: number, loc_name: string}>}
 */
export const LNG_METERS = {
  freeport_lng: {
    pipeline: 'gulf_south',
    loc_id: 24329,
    loc_name: 'Stratton Ridge (To Freeport Lng)',
  },
};

/**
 * Convert raw Decatherms per day (Dth/d or MMBtu/d) to million cubic feet per day (MMcf/d).
 * Conversion factor: 1.025 HHV.
 *
 * @param {number} dth
 * @returns {number} MMcf/d value
 */
export function dth_to_mmcf(dth) {
  if (dth == null || isNaN(dth)) return 0;
  return dth / 1.025 / 1000;
}

/**
 * Determine the utilization level metadata based on percentage.
 *
 * @param {number} percentage - value from 0 to 100 (or higher)
 * @returns {{level: string, label: string, colorClass: string}} level info
 */
export function get_utilization_level(percentage) {
  if (percentage < 40) {
    return {
      level: 'low',
      label: 'low',
      colorClass: 'utilization-gray',
    };
  } else if (percentage <= 75) {
    return {
      level: 'normal',
      label: 'normal',
      colorClass: 'utilization-green',
    };
  } else if (percentage <= 92) {
    return {
      level: 'high',
      label: 'high',
      colorClass: 'utilization-amber',
    };
  } else {
    return {
      level: 'saturation',
      label: 'near saturation',
      colorClass: 'utilization-red',
    };
  }
}

/**
 * Calculate the Week-over-Week delta.
 *
 * @param {number} latestValue - the value on the latest gas day
 * @param {string} latestCycleCode - the cycle code (e.g. 'id3')
 * @param {Date|string} latestDate - the latest gas day date
 * @param {Object} rowsByDate - map of date strings to cycle maps
 * @returns {{ valueText: string, deltaProps: {value: string, kind: string}|null, helpText: string }}
 */
export function calculate_wow_delta(latestValue, latestCycleCode, latestDate, rowsByDate) {
  const dateObj = new Date(latestDate);
  const targetDate = new Date(dateObj);
  targetDate.setDate(targetDate.getDate() - 7);
  
  // Format target date as YYYY-MM-DD manually to avoid timezone offset shifts
  const y = targetDate.getFullYear();
  const m = String(targetDate.getMonth() + 1).padStart(2, '0');
  const d = String(targetDate.getDate()).padStart(2, '0');
  const targetDateStr = `${y}-${m}-${d}`;
  
  const targetCycles = rowsByDate[targetDateStr];

  if (!targetCycles || targetCycles[latestCycleCode] === undefined) {
    return {
      valueText: '—',
      deltaProps: null,
      helpText: 'No 7-day-ago same-cycle data',
    };
  }

  const val7DaysAgo = targetCycles[latestCycleCode];
  const wowDelta = latestValue - val7DaysAgo;
  const wowPct = val7DaysAgo > 0 ? (wowDelta / val7DaysAgo) * 100 : 0;
  
  const valueText = `${wowDelta >= 0 ? '+' : ''}${wowDelta.toFixed(0)} MMcf/d`;
  const deltaProps = {
    value: `${wowPct >= 0 ? '+' : ''}${wowPct.toFixed(1)}%`,
    kind: wowDelta > 0 ? 'bullish' : wowDelta < 0 ? 'bearish' : 'neutral',
  };
  const helpText = `vs same cycle on ${targetDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;

  return { valueText, deltaProps, helpText };
}

/**
 * Determine if history is sufficient to render prior year and envelope lines.
 *
 * @param {number} totalDays - the number of days of history available
 * @returns {boolean} true if history is sufficient
 */
export function should_show_envelope(totalDays) {
  return totalDays >= 30;
}

