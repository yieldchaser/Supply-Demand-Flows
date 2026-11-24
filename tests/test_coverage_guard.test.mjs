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

test('Anti-rot coverage guard: Freeport rejects the "whole plant visible" falsehood', () => {
  const fp = LNG_TERMINALS.freeport;
  // Re-measured 2026-11-20 over the last 100 operating days: 76.1% (1,597.3
  // MMcf/d). The earlier 52.9% was a degraded Jun–Aug 2026 run. The unposted
  // KMTP lateral (~400–450 MMcf/d) caps visible coverage near 78–80%.
  assert.strictEqual(fp.expectedCoveragePct, 76.1);
  assert.strictEqual(fp.expectedMedianMmcf, 1597.3);
  assert.strictEqual(fp.nameplate, 2100);

  // A claim that the meters see the whole plant must fail the tolerance.
  const simulatedWrongClaim = 100.0;
  const error = Math.abs(simulatedWrongClaim - fp.expectedCoveragePct);
  assert.ok(
    error > fp.coverageTolerancePct,
    `Guard must reject a 100% claim (error ${error.toFixed(1)}% exceeds tolerance ${fp.coverageTolerancePct}%)`
  );
});

test('Anti-rot coverage guard: verified 60-day empirical table consistency', () => {
  // Ground-truth 60-day median measurements from Brief L, re-measured 2026-10-28
  // over the live curated window (cove_point/corpus_christi had outrun theirs).
  const measurements = {
    plaquemines: { median: 4186.1, pct: 123.1 },
    calcasieu:   { median: 1688.1, pct: 129.9 },
    corpus_christi: { median: 2561.0, pct: 106.7 },
    sabine_pass: { median: 1365.2, pct: 30.3 },
    cameron:     { median: 1458.6, pct: 72.9 },
    golden_pass: { median: 686.0,  pct: 26.4 },
    cove_point:  { median: 837.7,  pct: 111.7 }, // re-measured 2026-11-24 (100 operating days)
    freeport:    { median: 1597.3, pct: 76.1 }, // 100 operating days to 2026-11-20 (tests/test_coverage_guard.py re-measures)
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
