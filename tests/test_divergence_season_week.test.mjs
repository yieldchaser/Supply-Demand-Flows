/**
 * Transatlantic divergence — comparison axis.
 *
 * The card used to compare the latest reading against `computeWeeklyEnvelope`,
 * an ISO-week-keyed normal. For the live bundle the neighbouring ISO weeks
 * disagreed by more than two percentage points (week 35 → +7.3%, week 37 →
 * +2.5%, week 38 → −0.1%), so the headline depended on which Friday happened to
 * carry the latest label. These tests pin the comparison to the same
 * week-of-season axis the storage panel uses, and pin a missing side to `null`
 * rather than a confident "neutral".
 *
 * The panel imports `d3` (for `rollup`/`max` only). Rather than depending on a
 * bundled copy, install a minimal stub when the real package is absent, so this
 * file runs under plain `node --test` in a fresh checkout.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

const D3_STUB = `
export function rollup(rows, reduce, keyFn) {
  const groups = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  const out = new Map();
  for (const [key, group] of groups) out.set(key, reduce(group));
  return out;
}
export function max(rows, accessor) {
  let best;
  for (const row of rows) {
    const v = accessor(row);
    if (v == null) continue;
    if (best === undefined || v > best) best = v;
  }
  return best;
}
`;

assert.equal(typeof registerHooks, 'function', 'needs Node >= 22.15 (module registerHooks)');
registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3' || specifier.startsWith('d3/')) {
      return { url: 'data:text/javascript,' + encodeURIComponent(D3_STUB), shortCircuit: true };
    }
    return next(specifier, context);
  },
});

const { buildDivergenceModel, divergenceExport } = await import('../docs/js/panels/divergence.js');

/** `count` weekly Fridays from 3 January of `startYear`, value from `fn`. */
function fridays(startYear, count, fn) {
  const rows = [];
  for (let i = 0; i < count; i += 1) {
    const d = new Date(startYear, 0, 3 + i * 7);
    // Use the local-calendar components, not toISOString(): in IST that label
    // is the previous UTC day, and every year would then start one day short.
    const day = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    rows.push({ period: d, iso: day, value: fn(d, i) });
  }
  return rows;
}

/**
 * Minimal bundle: a US weekly series and ten EU countries held at a flat % full,
 * so the EU aggregate is exactly that constant and its own seasonal signal is 0.
 */
function makeBundle({ usLatest = 3000, usPrior = 2000, euPct = 50 } = {}) {
  const eiaRows = [];
  for (let y = 2021; y <= 2026; y += 1) {
    for (const r of fridays(y, 40, () => (y === 2026 ? usLatest : usPrior))) {
      eiaRows.push({ series_id: 'storage_lower48', period: r.iso, value: r.value });
    }
  }

  const ccs = ['de', 'fr', 'it', 'nl', 'es', 'at', 'be', 'cz', 'hu', 'pl'];
  const gieRows = [];
  for (const cc of ccs) {
    for (const r of fridays(2021, 40 * 6, () => euPct)) {
      gieRows.push({ series_id: `gie_storage_${cc}_gas_in_storage`, period: r.iso, value: 100 });
      gieRows.push({ series_id: `gie_storage_${cc}_full_pct`, period: r.iso, value: r.value });
    }
  }

  return {
    sources: {
      eia_storage: { data: eiaRows },
      gie_agsi: { data: gieRows },
    },
  };
}

describe('buildDivergenceModel — week-of-season comparison', () => {
  it('reports the week-of-season and sample size behind each normal', () => {
    const model = buildDivergenceModel(makeBundle());
    assert.ok(model, 'model should exist when both sources are present');
    assert.equal(model.usNormal.weekOfSeason, 40, 'latest Friday is the 40th of its year');
    assert.equal(model.usNormal.n, 5);
    assert.ok(model.euNormal, 'EU normal should be found');
    assert.ok(model.euNormal.n >= 3 && model.euNormal.n <= 5, `EU normal n=${model.euNormal.n}`);
  });

  it('measures US deviation against the prior-year normal, not a neighbouring week', () => {
    const model = buildDivergenceModel(makeBundle({ usLatest: 3000, usPrior: 2000 }));
    assert.ok(Math.abs(model.us_signal - 50) < 1e-9, '3000 vs a 2000 normal is +50%');
  });

  it('reports a flat EU series as exactly on its normal', () => {
    const model = buildDivergenceModel(makeBundle({ euPct: 50 }));
    assert.ok(Math.abs(model.eu_signal) < 1e-9);
  });

  it('treats a missing side as unavailable rather than "at normal"', () => {
    const bundle = makeBundle();
    assert.equal(
      buildDivergenceModel({ sources: { eia_storage: bundle.sources.eia_storage, gie_agsi: { data: [] } } }),
      null,
      'missing EU is unavailable, not zero',
    );
    assert.equal(
      buildDivergenceModel({ sources: { eia_storage: { data: [] }, gie_agsi: bundle.sources.gie_agsi } }),
      null,
      'missing US is unavailable, not zero',
    );
  });

  it('exports the comparison basis and normal size with the numbers', () => {
    const exp = divergenceExport(buildDivergenceModel(makeBundle()));
    assert.ok(exp.basis.includes('week-of-season'));
    assert.equal(exp.normal.us.n, 5);
    // US +50pp, EU flat → spread +50 pp on the num US% − num EUpp scale.
    assert.ok(Math.abs(exp.spread_pp - 50) < 1e-9);
    assert.equal(exp.us.period, '2026-10-03');
  });
});
