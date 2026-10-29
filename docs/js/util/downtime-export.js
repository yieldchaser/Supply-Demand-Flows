/**
 * Downtime export — the Section 8 view model as rows.
 *
 * The reliability chapter's claim is falsifiable: "these are the gas days this
 * terminal was down, and here is the rule that said so". A reader checking that
 * claim needs three things together — the raw daily series, the classification
 * rule, and the resulting events — because any one alone lets a threshold look
 * like a fact.
 *
 * This module ships all three, straight from the same builder the panel renders,
 * so the export cannot drift from the screen.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

import {
  buildDowntimeViewModel,
  DOWNTIME_CONF,
  DEPRESSED_PCT,
  DEPRESSED_DAYS,
  BASELINE_WINDOW,
  FLOOR_PCT,
} from './lng-downtime.js';

/**
 * Classification thresholds, as shipped with every export.
 *
 * A downtime list without these cannot be audited: "12 events" means nothing
 * until the reader knows a day counts as depressed below 60% of a 30-day
 * trailing median for 5 consecutive days.
 *
 * @returns {Object}
 */
export function downtimeRules() {
  return {
    baseline: `trailing ${BASELINE_WINDOW}-day median of non-zero days`,
    depressed_pct: DEPRESSED_PCT,
    depressed_days_to_flag: DEPRESSED_DAYS,
    floor_pct_of_baseline: FLOOR_PCT,
    note:
      'A day is uninformed when the meter posts zero or sits at/below floor% of its ' +
      'baseline. Runs of uninformed days are classified OFFLINE; sustained soft days ' +
      'DEPRESSED. Per-terminal honesty notes state what the meters cannot see.',
  };
}

/**
 * One terminal's downtime picture.
 *
 * @param {Object} bundle
 * @param {string} key  DOWNTIME_CONF key
 * @returns {Object|null}
 */
export function downtimeExportOne(bundle, key) {
  const vm = buildDowntimeViewModel(bundle, key);
  if (!vm) return null;
  const conf = vm.conf;
  return {
    terminal: conf.label,
    nameplate_mmcf_d: conf.nameplate,
    coverage_note: conf.honesty,
    status: vm.status,
    span: vm.daily.length ? { first_day: vm.daily[0].dateStr, last_day: vm.daily[vm.daily.length - 1].dateStr, days: vm.daily.length } : null,
    // Event shape as detectDowntime emits it: the first day of the run plus the
    // detail string that states the rule it fired under.
    events: vm.events.map((e) => ({
      type: e.type,
      first_day: e.date,
      duration_days: e.duration,
      detail: e.detail ?? null,
    })),
    daily: vm.daily.map((d) => ({
      date: d.dateStr,
      mmcf_d: Number(Number(d.value).toFixed(1)),
      posted_zero: Boolean(d.postedZero),
    })),
  };
}

/**
 * Every terminal's downtime picture, keyed by terminal.
 *
 * @param {Object} bundle
 * @returns {Object} JSON-serialisable block for the chapter export
 */
export function downtimeExport(bundle) {
  const out = {};
  Object.keys(DOWNTIME_CONF).forEach((key) => {
    try {
      out[key] = downtimeExportOne(bundle, key);
    } catch (err) {
      // Keep the other terminals exportable; an exception is data, not a crash.
      out[key] = { error: String(err && err.message ? err.message : err) };
    }
  });
  return { rules: downtimeRules(), terminals: out };
}
