/**
 * Anti-Rot Guard for Terminal Coverage Claims.
 *
 * Verifies that structured coverage metadata in LNG_TERMINALS matches verified
 * empirical baselines within stated tolerance, preventing regression to flattering falsehoods.
 *
 * Runs via built-in `node --test` with zero dependencies.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';

test('Anti-rot coverage guard: all terminals carry structured coverage metadata', () => {
  const terminals = Object.values(LNG_TERMINALS);
  assert.strictEqual(terminals.length, 9, 'All 9 terminals must be present');

  terminals.forEach((t) => {
    assert.ok(typeof t.expectedCoveragePct === 'number', `${t.id} missing expectedCoveragePct`);
    assert.ok(typeof t.expectedMedianMmcf === 'number', `${t.id} missing expectedMedianMmcf`);
    assert.ok(typeof t.coverageTolerancePct === 'number', `${t.id} missing coverageTolerancePct`);

    if (t.operational === false) {
      assert.strictEqual(t.expectedCoveragePct, 0);
      assert.strictEqual(t.expectedMedianMmcf, 0);
    } else {
      assert.ok(t.expectedCoveragePct > 0, `${t.id} expectedCoveragePct must be positive`);
      assert.ok(t.expectedMedianMmcf > 0, `${t.id} expectedMedianMmcf must be positive`);
      assert.ok(t.coverageTolerancePct > 0, `${t.id} coverageTolerancePct must be positive`);

      // Derived consistency: expectedMedianMmcf / nameplate ~ expectedCoveragePct
      const calculatedPct = (t.expectedMedianMmcf / t.nameplate) * 100;
      assert.ok(
        Math.abs(calculatedPct - t.expectedCoveragePct) < 0.5,
        `${t.id}: calculatedPct (${calculatedPct.toFixed(1)}%) diverges from expectedCoveragePct (${t.expectedCoveragePct}%)`
      );
    }
  });
});

test('Anti-rot coverage guard: Freeport strictly guards against the 80% flattering falsehood', () => {
  const fp = LNG_TERMINALS.freeport;
  // True empirical median over 100-day overlap is 52.9% (1,111.5 MMcf/d)
  assert.strictEqual(fp.expectedCoveragePct, 52.9);
  assert.strictEqual(fp.expectedMedianMmcf, 1111.5);
  assert.strictEqual(fp.nameplate, 2100);

  // Demonstrate that the guard fails on a deliberately wrong claim (80%)
  const simulatedWrongClaim = 80.0;
  const error = Math.abs(simulatedWrongClaim - fp.expectedCoveragePct);
  assert.ok(
    error > fp.coverageTolerancePct,
    `Guard must reject 80% claim (error ${error.toFixed(1)}% exceeds tolerance ${fp.coverageTolerancePct}%)`
  );
});

test('Anti-rot coverage guard: verified 60-day empirical table consistency', () => {
  // Ground-truth 60-day median measurements from Brief L:
  const measurements = {
    plaquemines: { median: 3820.9, pct: 112.4 },
    calcasieu:   { median: 1605.8, pct: 123.5 },
    corpus_christi: { median: 2384.7, pct: 99.4 },
    sabine_pass: { median: 1365.2, pct: 30.3 },
    cameron:     { median: 1458.6, pct: 72.9 },
    golden_pass: { median: 330.4,  pct: 12.7 },
    cove_point:  { median: 728.5,  pct: 97.1 },
    freeport:    { median: 1111.5, pct: 52.9 }, // 100-day dual-feed baseline
  };

  Object.entries(measurements).forEach(([termId, m]) => {
    const t = LNG_TERMINALS[termId];
    assert.ok(t, `Terminal ${termId} not found in registry`);

    const diffPct = Math.abs(t.expectedCoveragePct - m.pct);
    assert.ok(
      diffPct <= t.coverageTolerancePct,
      `${termId}: registry coverage (${t.expectedCoveragePct}%) drifts from measured (${m.pct}%) beyond tolerance (${t.coverageTolerancePct}%)`
    );
  });
});
