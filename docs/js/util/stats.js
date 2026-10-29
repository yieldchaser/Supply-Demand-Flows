/**
 * Statistical primitives with an explicit sample size.
 *
 * Every storage/flow claim in the dashboard is of the form "X vs its 5-year
 * normal". That sentence is only honest if the code knows how many observations
 * actually went into the normal, so every summary here carries `n`, and
 * quartiles are suppressed entirely below MIN_QUARTILE_N rather than being
 * computed from two points and printed with the same confidence as five years.
 *
 * Pure and synchronous so it can be unit-tested under Node.
 */

import { quantile } from './season.js';

/** Below this many observations a quartile is noise dressed as precision. */
export const MIN_QUARTILE_N = 3;

/**
 * Summary of a numeric sample. Non-finite entries are dropped, not coerced.
 *
 * @param {number[]} values
 * @returns {{n:number, mean:number, median:number, min:number, max:number,
 *            p10:number|null, p25:number|null, p75:number|null, p90:number|null,
 *            sum:number, sd:number}|null} null only when nothing remains
 */
export function summarize(values) {
  const vals = (values || []).filter((v) => Number.isFinite(v));
  const n = vals.length;
  if (n === 0) return null;
  const sorted = [...vals].sort((a, b) => a - b);
  const sum = vals.reduce((s, v) => s + v, 0);
  const mean = sum / n;
  const variance = n > 1 ? vals.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1) : 0;
  const q = (p) => (n >= MIN_QUARTILE_N ? quantile(sorted, p) : null);
  return {
    n,
    sum,
    mean,
    median: quantile(sorted, 0.5),
    min: sorted[0],
    max: sorted[n - 1],
    sd: Math.sqrt(variance),
    p10: q(0.10),
    p25: q(0.25),
    p75: q(0.75),
    p90: q(0.90),
  };
}

export { quantile };
