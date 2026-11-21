/**
 * LNG total panel: monthly volumes are turned into per-day rates so a short
 * month is not read as a fall in exports, and the trailing 12-month rate is a
 * day-weighted mean over 12 consecutive months only.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3') return { url: 'data:text/javascript,export const x=1;', shortCircuit: true };
    return next(specifier, context);
  },
});
const { buildLngMonthly } = await import('../docs/js/panels/lng-total.js');

const months = (n, start = [2025, 0], bcfd = 15) => Array.from({ length: n }, (_, i) => {
  const period = new Date(start[0], start[1] + i, 1);
  const days = new Date(period.getFullYear(), period.getMonth() + 1, 0).getDate();
  return { period, value: bcfd * days };
});

describe('buildLngMonthly', () => {
  it('a constant per-day rate reads as constant across short and long months', () => {
    const out = buildLngMonthly(months(14));
    for (const r of out) assert.ok(Math.abs(r.bcfd - 15) < 1e-9, `${r.period.toISOString()} ${r.bcfd}`);
    const feb = out.find((r) => r.period.getMonth() === 1);
    assert.equal(feb.days, 28);
    assert.ok(feb.bcf < out[0].bcf, 'the Bcf total still shows the short month');
  });

  it('12-month rate starts at the twelfth month and is day-weighted', () => {
    const out = buildLngMonthly(months(13));
    assert.equal(out[10].ma12, null);
    assert.ok(Math.abs(out[11].ma12 - 15) < 1e-9);
  });

  it('does not stretch the 12-month window across a gap', () => {
    const rows = months(14);
    rows.splice(5, 1); // drop one month
    const out = buildLngMonthly(rows);
    // Only 8 consecutive months follow the gap, so no 12-month window exists.
    assert.ok(out.every((r) => r.ma12 == null), 'a window spanning a missing month was averaged');
    const full = buildLngMonthly(months(30).filter((_, i) => i !== 5));
    assert.equal(full[15].ma12, null, 'last window still containing the gap');
    assert.ok(full[16].ma12 != null, 'first full window after the gap');
  });
});
