/**
 * Multi-Terminal Feedgas Comparison Utility with Mandatory Coverage Caveats.
 *
 * Normalizes daily feedgas series across multiple selected LNG terminals
 * for comparison in both absolute MMcf/d and normalized percentage of nameplate.
 * Strictly guards against naive comparison deception by surfacing coverage tiers
 * and invisible feedgas fractions inline.
 *
 * Vanilla JS — zero TypeScript in executable code.
 */

import { LNG_TERMINALS } from './lng-terminals.js';
import { dth_to_mmcf } from './lng-metrics.js';
import { buildDailyTotal, DOWNTIME_CONF } from './lng-downtime.js';

/**
 * Build comparison dataset across a list of terminal keys.
 *
 * @param {Object} bundle - loaded dashboard bundle
 * @param {string[]} terminalKeys - array of terminal ids (e.g. ['freeport', 'cove_point'])
 * @returns {{
 *   dates: string[],
 *   terminals: Array<{
 *     id: string,
 *     label: string,
 *     nameplate: number,
 *     coveragePct: number,
 *     coverageKind: string,
 *     coverageNote: string,
 *     isPartial: boolean,
 *     series: Object<string, { mmcf: number, pctNameplate: number }>
 *   }>,
 *   caveats: string[]
 * }}
 */
/**
 * Determine if a terminal is measured-partial (pipeline visibility gap).
 *
 * @param {Object} t - terminal config from LNG_TERMINALS
 * @returns {boolean}
 */
export function isTerminalPartial(t) {
  if (!t || t.operational === false) return false;
  // Golden Pass is 100% plant gate intake undergoing commissioning ramp, not partial pipeline visibility
  if (t.id === 'golden_pass') return false;
  // Authoritative rule: expectedCoveragePct < 90% indicates partial pipeline visibility
  return typeof t.expectedCoveragePct === 'number' && t.expectedCoveragePct < 90.0;
}

/**
 * Derive an honest caveat string for any terminal.
 *
 * @param {Object} t - terminal config from LNG_TERMINALS
 * @returns {string|null}
 */
export function getTerminalCaveat(t) {
  if (!t) return null;
  if (t.operational === false) {
    return `${t.display}: ${t.statusText || 'Not operational'}`;
  }
  if (t.coverageNote) {
    return `${t.display} (~${t.expectedCoveragePct}%): ${t.coverageNote}`;
  }
  if (isTerminalPartial(t)) {
    return `${t.display} (~${t.expectedCoveragePct}%): Measured-partial coverage (~${t.expectedCoveragePct}% of nameplate). Interstate-visible feeds only.`;
  }
  if (t.expectedCoveragePct > 105.0) {
    return `${t.display} (~${t.expectedCoveragePct}%): Running above stated nameplate capacity (~${t.expectedCoveragePct}% utilization).`;
  }
  return null;
}

/**
 * Build comparison dataset across a list of terminal keys.
 *
 * @param {Object} bundle - loaded dashboard bundle
 * @param {string[]} terminalKeys - array of terminal ids (e.g. ['freeport', 'cove_point'])
 * @returns {{
 *   dates: string[],
 *   terminals: Array<{
 *     id: string,
 *     label: string,
 *     nameplate: number,
 *     coveragePct: number,
 *     coverageKind: string,
 *     coverageNote: string,
 *     isPartial: boolean,
 *     series: Object<string, { mmcf: number, pctNameplate: number }>
 *   }>,
 *   caveats: string[]
 * }}
 */
export function buildTerminalComparison(bundle, terminalKeys) {
  const selectedKeys = (terminalKeys || []).filter((k) => LNG_TERMINALS[k]);
  const dateSet = new Set();
  const terminalData = [];
  const caveats = [];

  selectedKeys.forEach((key) => {
    const t = LNG_TERMINALS[key];
    const conf = DOWNTIME_CONF[key];
    if (!t || !conf) return;

    const daily = buildDailyTotal(bundle, conf);
    const series = {};

    daily.forEach((d) => {
      dateSet.add(d.dateStr);
      const mmcf = d.value;
      const pct = t.nameplate > 0 ? (mmcf / t.nameplate) * 100.0 : 0.0;
      series[d.dateStr] = { mmcf, pctNameplate: pct };
    });

    const isPartial = isTerminalPartial(t);
    const caveat = getTerminalCaveat(t);
    if (caveat) {
      caveats.push(caveat);
    }

    const coverageKind = isPartial
      ? 'measured-partial'
      : t.id === 'golden_pass'
      ? 'commissioning-ramp'
      : t.expectedCoveragePct > 105
      ? 'above-nameplate'
      : 'measured';

    const termDates = Object.keys(series).sort();
    const firstDate = termDates.length > 0 ? termDates[0] : null;
    const lastDate = termDates.length > 0 ? termDates[termDates.length - 1] : null;
    const dayCount = termDates.length;

    terminalData.push({
      id: key,
      label: t.display,
      nameplate: t.nameplate,
      coveragePct: t.expectedCoveragePct,
      coverageKind,
      coverageNote: t.coverageNote || '',
      isPartial,
      firstDate,
      lastDate,
      dayCount,
      span: { firstDate, lastDate, dayCount },
      series,
    });
  });

  // Emit caveat when selected terminals' spans differ by more than 2x (§02.3)
  const validTerminals = terminalData.filter((t) => t.dayCount > 0);
  if (validTerminals.length >= 2) {
    const sorted = [...validTerminals].sort((a, b) => a.dayCount - b.dayCount);
    const minT = sorted[0];
    const maxT = sorted[sorted.length - 1];
    if (maxT.dayCount > 2 * minT.dayCount) {
      caveats.push(
        `${minT.label} is known from ${minT.firstDate} (${minT.dayCount.toLocaleString('en-US')} days); ${maxT.label} from ${maxT.firstDate} (${maxT.dayCount.toLocaleString('en-US')} days). Comparisons before ${minT.firstDate} include ${maxT.label} only.`
      );
    }
  }

  const dates = [...dateSet].sort();
  const spans = Object.fromEntries(
    terminalData.map((t) => [t.id, { firstDate: t.firstDate, lastDate: t.lastDate, dayCount: t.dayCount }])
  );

  return {
    dates,
    terminals: terminalData,
    spans,
    caveats,
  };
}
