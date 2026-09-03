/**
 * Registry Parity Guard.
 *
 * Verifies that the set of counted feed stems derived from LNG_TERMINALS
 * equals the set derived from DOWNTIME_CONF (case-insensitively) for every
 * terminal configured in DOWNTIME_CONF (Prompt X §04).
 *
 * Runs via built-in `node --test` with zero dependencies.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';
import { DOWNTIME_CONF } from '../docs/js/util/lng-downtime.js';

test('Registry parity: counted feed stems match between LNG_TERMINALS and DOWNTIME_CONF', () => {
  const dtKeys = Object.keys(DOWNTIME_CONF);
  assert.strictEqual(dtKeys.length, 8, 'Expected 8 operational terminals in DOWNTIME_CONF');

  for (const [termKey, dtConf] of Object.entries(DOWNTIME_CONF)) {
    const regTerm = LNG_TERMINALS[termKey];
    assert.ok(regTerm, `Terminal ${termKey} in DOWNTIME_CONF must exist in LNG_TERMINALS`);

    // Stems counted in Section 8 / DOWNTIME_CONF (non-context, non-comparison)
    const dtStems = new Set(
      (dtConf.feeds || [])
        .filter((f) => !f.context && f.kind !== 'comparison' && f.kind !== 'context')
        .map((f) => f.stem.toLowerCase())
    );

    // Stems counted in Section 5 / LNG_TERMINALS (non-context, non-comparison, non-proxy)
    const regStems = new Set(
      (regTerm.feeds || [])
        .filter((f) => f.kind !== 'context' && f.kind !== 'comparison' && f.kind !== 'proxy')
        .map((f) => f.series.toLowerCase())
    );

    assert.ok(regStems.size > 0, `${termKey} has no counted feed stems in LNG_TERMINALS`);
    assert.ok(dtStems.size > 0, `${termKey} has no counted feed stems in DOWNTIME_CONF`);

    assert.deepStrictEqual(
      Array.from(regStems).sort(),
      Array.from(dtStems).sort(),
      `Counted feed stem mismatch for ${termKey}:\n` +
      `  LNG_TERMINALS (S5): ${JSON.stringify(Array.from(regStems).sort())}\n` +
      `  DOWNTIME_CONF (S8): ${JSON.stringify(Array.from(dtStems).sort())}`
    );
  }
});
