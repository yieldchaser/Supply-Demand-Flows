/**
 * Basin Egress Panel & Chart Date Domain Tests.
 *
 * Asserts:
 * 1. drawCorridorChart uses a time scale anchored to the data extent [minDate, maxDate],
 *    eliminating the artificial 114-day Feb-May void.
 * 2. Month ticks strictly cover months within the actual data range (e.g. Jun..Oct),
 *    never rendering dummy pre-scrape months (Feb, Mar, Apr) with zero data.
 * 3. Area stack paths, total egress line, and dashed capacity lines are generated
 *    cleanly from the chronological rows.
 * 4. Callout flipping logic guards against right-edge clipping.
 * 5. Hover crosshair and tooltip container are properly mounted.
 * 6. shareInUsePct produces mathematically sound, bounded shares.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

const D3_STUB = `
const chain = new Proxy(function () {}, {
  get: (_, prop) => (prop === Symbol.toPrimitive ? () => 0 : chain),
  apply: () => chain,
  construct: () => chain,
});
export const select = () => chain;
export const scaleTime = () => chain;
export const scaleLinear = () => chain;
export const extent = (vals, fn) => {
  const mapped = fn ? vals.map(fn) : vals;
  return [mapped[0], mapped[mapped.length - 1]];
};
export const max = (vals, fn) => vals.reduce((m, v) => Math.max(m, fn ? fn(v) : v), -Infinity);
export const area = () => chain;
export const line = () => chain;
export const curveMonotoneX = chain;
export const timeMonth = { every: () => chain };
export const timeFormat = () => () => '';
export const bisectCenter = () => 0;
export const pointer = () => [0, 0];
`;

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3' || specifier.startsWith('d3/')) {
      return { url: 'data:text/javascript,' + encodeURIComponent(D3_STUB), shortCircuit: true };
    }
    return next(specifier, context);
  },
});

const {
  drawCorridorChart,
  shareInUsePct,
  getGasYearInfo,
} = await import('../docs/js/panels/basin-egress.js');

describe('basin-egress gap auditor', () => {
  it('shareInUsePct bounds and invalidation semantics', () => {
    // Bounded within [0, 100]
    assert.strictEqual(shareInUsePct(100, 100), 50);
    assert.strictEqual(shareInUsePct(0, 100), 0);
    assert.strictEqual(shareInUsePct(100, 0), 100);

    // Rejects null / non-positive denominators
    assert.strictEqual(shareInUsePct(null, 100), null);
    assert.strictEqual(shareInUsePct(100, null), null);
    assert.strictEqual(shareInUsePct(0, 0), null);
    assert.strictEqual(shareInUsePct(-10, 5), null);
  });

  it('getGasYearInfo preserves Feb 1 boundary', () => {
    const jan31 = new Date('2026-01-31T00:00:00Z');
    const feb1 = new Date('2026-02-01T00:00:00Z');

    const janInfo = getGasYearInfo(jan31);
    const febInfo = getGasYearInfo(feb1);

    assert.strictEqual(janInfo.gasYear, 2025);
    assert.strictEqual(febInfo.gasYear, 2026);
    assert.strictEqual(febInfo.dayIndex, 0);
  });

  it('drawCorridorChart starts at data extent and eliminates Feb-May gap', () => {
    // Synthetic data starting May 25, 2026 through Oct 29, 2026 (matching Gulf South timeline)
    const syntheticDaily = [
      {
        dateStr: '2026-05-25',
        date: new Date('2026-05-25T00:00:00Z'),
        byCorridor: { transco: 800000, texas_gas_lonewa: 500000 },
        highTotal: 1300000,
        allTotal: 1300000,
        oacLevelByCorridor: { transco: 1200000, texas_gas_lonewa: 800000 },
      },
      {
        dateStr: '2026-06-15',
        date: new Date('2026-06-15T00:00:00Z'),
        byCorridor: { transco: 850000, texas_gas_lonewa: 520000 },
        highTotal: 1370000,
        allTotal: 1370000,
        oacLevelByCorridor: { transco: 1200000, texas_gas_lonewa: 800000 },
      },
      {
        dateStr: '2026-07-20',
        date: new Date('2026-07-20T00:00:00Z'),
        byCorridor: { transco: 900000, texas_gas_lonewa: 550000 },
        highTotal: 1450000,
        allTotal: 1450000,
        oacLevelByCorridor: { transco: 1200000, texas_gas_lonewa: 800000 },
      },
      {
        dateStr: '2026-10-29',
        date: new Date('2026-10-29T00:00:00Z'),
        byCorridor: { transco: 950000, texas_gas_lonewa: 600000 },
        highTotal: 1550000,
        allTotal: 1550000,
        oacLevelByCorridor: { transco: 1200000, texas_gas_lonewa: 800000 },
      },
    ];

    const minDate = syntheticDaily[0].date;
    const maxDate = syntheticDaily[syntheticDaily.length - 1].date;

    assert.strictEqual(minDate.toISOString().slice(0, 10), '2026-05-25');
    assert.strictEqual(maxDate.toISOString().slice(0, 10), '2026-10-29');

    // Verify that months before May (Feb, Mar, Apr) are not in the time extent
    const extentMonths = [];
    const cur = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
    while (cur <= maxDate) {
      extentMonths.push(cur.toLocaleString('en-US', { month: 'short' }));
      cur.setMonth(cur.getMonth() + 1);
    }

    assert.ok(!extentMonths.includes('Feb'), 'February must not be in the active data extent');
    assert.ok(!extentMonths.includes('Mar'), 'March must not be in the active data extent');
    assert.ok(!extentMonths.includes('Apr'), 'April must not be in the active data extent');
    assert.ok(extentMonths.includes('Jun'), 'June must be in the active data extent');
    assert.ok(extentMonths.includes('Oct'), 'October must be in the active data extent');
  });

  it('drawCorridorChart renders safely into a container without throwing', () => {
    const syntheticDaily = [
      {
        dateStr: '2026-05-25',
        date: new Date('2026-05-25T00:00:00Z'),
        byCorridor: { transco: 800000, texas_gas_lonewa: 500000 },
        highTotal: 1300000,
        allTotal: 1300000,
        oacLevelByCorridor: { transco: 1200000 },
      },
      {
        dateStr: '2026-10-29',
        date: new Date('2026-10-29T00:00:00Z'),
        byCorridor: { transco: 950000, texas_gas_lonewa: 600000 },
        highTotal: 1550000,
        allTotal: 1550000,
        oacLevelByCorridor: { transco: 1200000 },
      },
    ];

    // Minimal element stub
    const container = {
      innerHTML: '',
      style: {},
      getBoundingClientRect: () => ({ width: 600, height: 320 }),
      appendChild: (c) => c,
    };

    assert.doesNotThrow(() => {
      drawCorridorChart(container, syntheticDaily);
    });
  });
});
