/**
 * Basin cards and movers: momentum is judged against the basin's size, and
 * the 12-week ranking ignores basins too small for a percentage to mean much.
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
const { sharpestMovers } = await import('../docs/js/panels/basin-deep.js');
const { basinMomentum } = await import('../docs/js/panels/basins.js');

const m = (label, latest, d12w) => ({ label, latest, d12w, flavor: 'gas' });

describe('sharpestMovers', () => {
  it('ranks by change relative to the starting count, and drops tiny bases', () => {
    const { up, down } = sharpestMovers([
      m('Big', 275, 8), // +3%
      m('Mid', 35, 8), // +30%
      m('Tiny', 2, 1), // base 1: excluded
      m('Fall', 50, -10), // −17%
      m('Flat', 20, 0),
    ]);
    assert.deepEqual(up.map((r) => r.label), ['Mid', 'Big']);
    assert.deepEqual(down.map((r) => r.label), ['Fall']);
    assert.equal(Math.round(up[0].pct12w), 30);
    assert.equal(up[0].base12w, 27);
  });
});

describe('basinMomentum', () => {
  const series = (vals) => vals.map((value, i) => ({ period: new Date(2026, 0, 1 + 7 * i), value }));

  it('+3 on 275 rigs is flat; +3 on 23 rigs is a move', () => {
    assert.equal(basinMomentum(series([272, 272, 272, 272, 272, 275])).dir, 'flat');
    assert.equal(basinMomentum(series([23, 23, 23, 23, 23, 26])).dir, 'up');
    assert.equal(basinMomentum(series([56, 56, 56, 56, 56, 53])).dir, 'down');
  });

  it('reports the 12-week range it was drawn from', () => {
    const r = basinMomentum(series([50, 52, 49, 51, 50, 50]));
    assert.equal(r.lo, 49);
    assert.equal(r.hi, 52);
  });
});
