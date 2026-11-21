/**
 * Hero chart: days before a terminal's first sustained flow are pre-service,
 * not zero feedgas, and must not become the floor of the prior-years range.
 */
import assert from 'node:assert/strict';
import { it } from 'node:test';
import { registerHooks } from 'node:module';

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3') return { url: 'data:text/javascript,export const x=1;', shortCircuit: true };
    return next(specifier, context);
  },
});
const { trimPreService } = await import('../docs/js/panels/lng-feedgas.js');
const rows = (vals) => vals.map((value, i) => ({ date: new Date(2024, 3, 1 + i), value }));

it('drops the zero run before first sustained flow, keeps later outages', () => {
  const out = trimPreService(rows([0, 0, 0, 5, 0, 900, 1200, 1500, 0, 1600]), 3400);
  // A one-day blip (5) is not service; three days >= 2% of nameplate is.
  assert.deepEqual(out.map((r) => r.value), [900, 1200, 1500, 0, 1600]);
});

it('leaves a series that starts in service alone', () => {
  const r = rows([1000, 1100, 1200, 0, 900]);
  assert.equal(trimPreService(r, 2000), r);
});
