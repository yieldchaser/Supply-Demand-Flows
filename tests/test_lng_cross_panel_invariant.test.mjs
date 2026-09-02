/**
 * Cross-Panel Invariant Test Suite.
 *
 * Proves mathematically that Section 5 (Hero Feedgas), Section 7 (Fleet Overview),
 * and Section 8 (Terminal Downtime) produce IDENTICAL daily flow totals for any terminal.
 *
 * Runs via built-in `node --test` with zero dependencies.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';
import { DOWNTIME_CONF, buildDailyTotal } from '../docs/js/util/lng-downtime.js';
import { buildMultiFeedData } from '../docs/js/util/lng-feedgas-data.js';
import { terminalSummary } from '../docs/js/util/lng-fleet-data.js';

// Build synthetic multi-feed bundle matching Freeport EBB conditions
function createTestBundle() {
  return {
    sources: {
      gulf_south: {
        data: [
          // 2026-07-14: Full flow
          { series_id: 'gulf_south_sq_24329_d_timely', period: '2026-07-14', value: 1000000 },
          { series_id: 'gulf_south_sq_24329_d_id3',    period: '2026-07-14', value: 1062590 },
          // 2026-07-15: Acute dip
          { series_id: 'gulf_south_sq_24329_d_timely', period: '2026-07-15', value: 500000 },
          { series_id: 'gulf_south_sq_24329_d_id3',    period: '2026-07-15', value: 145963 },
          // 2026-07-16: Recovery partial
          { series_id: 'gulf_south_sq_24329_d_id3',    period: '2026-07-16', value: 600282 },
        ],
      },
      enbridge: {
        data: [
          // 2026-07-14: TETCO at 0 (covered by Gulf South)
          { series_id: 'tetco_sq_79999_d_timely', period: '2026-07-14', value: 0 },
          { series_id: 'tetco_sq_79999_d_latec',  period: '2026-07-14', value: 0 },
          // 2026-07-15: latec correction cycle supersedes timely
          { series_id: 'tetco_sq_79999_d_timely', period: '2026-07-15', value: 119321 },
          { series_id: 'tetco_sq_79999_d_late',   period: '2026-07-15', value: 117996 },
          { series_id: 'tetco_sq_79999_d_latec',  period: '2026-07-15', value: 148887 },
          // Automated hourly snapshots carrying 0.0 must be ignored in all panels!
          { series_id: 'tetco_sq_79999_d_id0900', period: '2026-07-15', value: 0 },
          { series_id: 'tetco_sq_79999_d_id1200', period: '2026-07-15', value: 0 },
          { series_id: 'tetco_sq_79999_d_id2300', period: '2026-07-15', value: 0 },
          // 2026-07-16:
          { series_id: 'tetco_sq_79999_d_timely', period: '2026-07-16', value: 35739 },
          { series_id: 'tetco_sq_79999_d_latec',  period: '2026-07-16', value: 35739 },
        ],
      },
    },
  };
}

test('Cross-panel invariant: Section 5, 7, and 8 compute identical Freeport daily totals', () => {
  const bundle = createTestBundle();

  // Section 5 (Hero Feedgas)
  const sec5 = buildMultiFeedData(bundle, LNG_TERMINALS.freeport);
  const sec5Map = new Map(sec5.dailySeries.map((d) => [d.dateStr, d.value]));

  // Section 7 (Fleet Overview summary)
  const sec7 = terminalSummary(bundle, LNG_TERMINALS.freeport);
  assert.ok(sec7.ok, 'Section 7 terminalSummary must be ok');
  const sec7Map = new Map(sec7.daily.map((d) => [d.dateStr, d.value]));

  // Section 8 (Terminal Downtime)
  const sec8 = buildDailyTotal(bundle, DOWNTIME_CONF.freeport);
  const sec8Map = new Map(sec8.map((d) => [d.dateStr, d.value]));

  // 1. All three panels must cover the exact same dates
  assert.strictEqual(sec5Map.size, sec8Map.size, 'Section 5 vs 8 date count mismatch');
  assert.strictEqual(sec7Map.size, sec8Map.size, 'Section 7 vs 8 date count mismatch');

  for (const [dateStr, sec8Val] of sec8Map.entries()) {
    const sec5Val = sec5Map.get(dateStr);
    const sec7Val = sec7Map.get(dateStr);

    assert.ok(sec5Val !== undefined, `Date ${dateStr} missing in Section 5`);
    assert.ok(sec7Val !== undefined, `Date ${dateStr} missing in Section 7`);

    // Invariant assertion: exact float equality (or < 1e-6)
    assert.ok(
      Math.abs(sec5Val - sec8Val) < 1e-6,
      `Mismatch on ${dateStr}: Sec 5 (${sec5Val}) !== Sec 8 (${sec8Val})`
    );
    assert.ok(
      Math.abs(sec7Val - sec8Val) < 1e-6,
      `Mismatch on ${dateStr}: Sec 7 (${sec7Val}) !== Sec 8 (${sec8Val})`
    );
  }

  // 2. Specific key date assertions:
  // On 2026-07-15: GS id3 (145,963) + TETCO latec (148,887) = 294,850 Dth -> 287.6585 MMcf/d
  const val0715 = sec8Map.get('2026-07-15');
  const expectedMmcf0715 = (145963 + 148887) / 1.025 / 1000;
  assert.ok(
    Math.abs(val0715 - expectedMmcf0715) < 1e-6,
    `2026-07-15 value ${val0715} did not match expected ${expectedMmcf0715}`
  );

  // Hourly id2300 (0.0) did NOT overwrite latec (148,887)
  assert.ok(val0715 > 280, 'Hourly snapshot zero wrongly overwrote latec');
});

test('Partial-day trap: incomplete trailing day is suppressed from total flow', () => {
  const bundle = createTestBundle();
  // Simulate partial newest day: TETCO files 2026-07-17, Gulf South has NOT filed
  bundle.sources.enbridge.data.push({
    series_id: 'tetco_sq_79999_d_latec',
    period: '2026-07-17',
    value: 214430, // 209.2 MMcf/d
  });

  // Section 5
  const sec5 = buildMultiFeedData(bundle, LNG_TERMINALS.freeport);
  const sec5Dates = sec5.dailySeries.map((d) => d.dateStr);
  assert.ok(!sec5Dates.includes('2026-07-17'), 'Section 5 must suppress incomplete trailing day');

  // Section 7
  const sec7 = terminalSummary(bundle, LNG_TERMINALS.freeport);
  const sec7Dates = sec7.daily.map((d) => d.dateStr);
  assert.ok(!sec7Dates.includes('2026-07-17'), 'Section 7 daily must suppress incomplete trailing day');
  assert.strictEqual(sec7.daily[sec7.daily.length - 1].dateStr, '2026-07-16');

  // Section 8
  const sec8 = buildDailyTotal(bundle, DOWNTIME_CONF.freeport);
  const sec8Dates = sec8.map((d) => d.dateStr);
  assert.ok(!sec8Dates.includes('2026-07-17'), 'Section 8 must suppress incomplete trailing day');
});

test('Genuinely-zero feed: pipe posting 0 while sibling flows is a complete day and is NOT suppressed', () => {
  const bundle = createTestBundle();
  // Both feeds file for 2026-07-17: TETCO files 0, Gulf South files 1,025,000 Dth
  bundle.sources.enbridge.data.push({
    series_id: 'tetco_sq_79999_d_latec',
    period: '2026-07-17',
    value: 0,
  });
  bundle.sources.gulf_south.data.push({
    series_id: 'gulf_south_sq_24329_d_id3',
    period: '2026-07-17',
    value: 1025000,
  });

  const sec5 = buildMultiFeedData(bundle, LNG_TERMINALS.freeport);
  const sec5Point = sec5.dailySeries.find((d) => d.dateStr === '2026-07-17');
  assert.ok(sec5Point !== undefined, 'Complete day with zero feed must be included in Section 5');
  assert.ok(Math.abs(sec5Point.value - 1000) < 1e-6, `Expected ~1000 MMcf/d, got ${sec5Point.value}`);

  const sec8 = buildDailyTotal(bundle, DOWNTIME_CONF.freeport);
  const sec8Point = sec8.find((d) => d.dateStr === '2026-07-17');
  assert.ok(sec8Point !== undefined, 'Complete day with zero feed must be included in Section 8');
  assert.strictEqual(sec8Point.feedsPosted, 2);
  assert.strictEqual(sec8Point.postedZero, false);
});

test('Real outage: both feeds posting 0 is complete and sets postedZero', () => {
  const bundle = createTestBundle();
  bundle.sources.enbridge.data.push({
    series_id: 'tetco_sq_79999_d_latec',
    period: '2026-07-17',
    value: 0,
  });
  bundle.sources.gulf_south.data.push({
    series_id: 'gulf_south_sq_24329_d_id3',
    period: '2026-07-17',
    value: 0,
  });

  const sec8 = buildDailyTotal(bundle, DOWNTIME_CONF.freeport);
  const sec8Point = sec8.find((d) => d.dateStr === '2026-07-17');
  assert.ok(sec8Point !== undefined);
  assert.strictEqual(sec8Point.value, 0);
  assert.strictEqual(sec8Point.feedsPosted, 2);
  assert.strictEqual(sec8Point.postedZero, true);
});
