/**
 * Node.js test suite for interactive observatory utilities:
 * - Shared date range & URL query state (P4)
 * - Self-describing data export with provenance (P7)
 * - Multi-terminal comparison with mandatory coverage caveats (P6)
 *
 * Runs via built-in `node --test` with ZERO dependencies.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import {
  parseRangeFromQuery,
  formatRangeQueryParam,
  computePresetInterval,
  checkRangeExceedsHistory,
  filterSeriesByRange,
} from '../docs/js/util/range-state.js';

import { formatCsvWithProvenance, serializeSvgToString } from '../docs/js/util/export-data.js';
import {
  buildTerminalComparison,
  isTerminalPartial,
  getTerminalCaveat,
} from '../docs/js/util/terminal-comparison.js';
import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';

test('P4: Shared time range parses presets and custom intervals from URL query', () => {
  assert.deepStrictEqual(parseRangeFromQuery('?range=30d'), {
    preset: '30d', start: null, end: null, isCustom: false,
  });
  assert.deepStrictEqual(parseRangeFromQuery('?range=90d'), {
    preset: '90d', start: null, end: null, isCustom: false,
  });
  assert.deepStrictEqual(parseRangeFromQuery('?range=all'), {
    preset: 'all', start: null, end: null, isCustom: false,
  });
  assert.deepStrictEqual(parseRangeFromQuery('?range=2026-06-01..2026-08-31'), {
    preset: 'custom', start: '2026-06-01', end: '2026-08-31', isCustom: true,
  });
  assert.deepStrictEqual(parseRangeFromQuery(''), {
    preset: 'all', start: null, end: null, isCustom: false,
  });
});

test('P4: Shared time range serializes state to URL parameters', () => {
  assert.strictEqual(formatRangeQueryParam('30d'), 'range=30d');
  assert.strictEqual(formatRangeQueryParam('custom', '2026-06-01', '2026-08-31'), 'range=2026-06-01..2026-08-31');
});

test('P4: Shared time range calculates correct preset interval relative to anchor', () => {
  const { startDateStr, endDateStr } = computePresetInterval('30d', '2026-09-01');
  assert.strictEqual(endDateStr, '2026-09-01');
  assert.strictEqual(startDateStr, '2026-08-02');
});

test('P4: Range exceeding history surfaces honest caveat rather than silent clipping', () => {
  const caveat = checkRangeExceedsHistory('2025-01-01', '2026-05-25', 'Gulf South');
  assert.ok(caveat !== null, 'Must return caveat when requested start precedes history');
  assert.ok(caveat.includes('Gulf South history begins 2026-05-25'), 'Must state source history boundary');

  const noCaveat = checkRangeExceedsHistory('2026-06-01', '2026-05-25', 'Gulf South');
  assert.strictEqual(noCaveat, null, 'Must return null when range is within history');
});

test('P4: filterSeriesByRange filters items strictly within bounds', () => {
  const items = [
    { dateStr: '2026-07-01', value: 100 },
    { dateStr: '2026-07-15', value: 120 },
    { dateStr: '2026-08-01', value: 140 },
  ];
  const filtered = filterSeriesByRange(items, '2026-07-10', '2026-07-20');
  assert.strictEqual(filtered.length, 1);
  assert.strictEqual(filtered[0].dateStr, '2026-07-15');
});

test('P7: formatCsvWithProvenance includes mandatory metadata, cycle rules, and caveats', () => {
  const rows = [
    { date: '2026-08-01', value_mmcf_d: 1120.5, value_dth_d: 1148512 },
    { date: '2026-08-02', value_mmcf_d: 1115.0, value_dth_d: 1142875 },
  ];
  const csv = formatCsvWithProvenance('Freeport LNG Feedgas', rows, {
    terminal: 'Freeport LNG',
    coverageNote: '52.9% coverage (KMTP intrastate feed missing)',
  });

  assert.ok(csv.includes('# Series: Freeport LNG Feedgas'));
  assert.ok(csv.includes('# Coverage Caveat: 52.9% coverage (KMTP intrastate feed missing)'));
  assert.ok(csv.includes('# Conversion Formula: MMcf/d = Dth/d / 1.025 / 1000.0'));
  assert.ok(csv.includes('# Cycle Precedence: NAESB'));
  assert.ok(csv.includes('date,value_mmcf_d,value_dth_d'));
  assert.ok(csv.includes('2026-08-01,1120.50,1148512'));
});

test('P6: buildTerminalComparison normalizes series and surfaces partial-coverage warnings', () => {
  const mockBundle = {
    sources: {
      gulf_south: {
        data: [{ series_id: 'gulf_south_sq_24329_d_timely', period: '2026-08-01', value: 500000 }],
      },
      enbridge: {
        data: [{ series_id: 'tetco_sq_79999_d_timely', period: '2026-08-01', value: 600000 }],
      },
      bhe: {
        data: [{ series_id: 'cpl_sq_10001_d_timely', period: '2026-08-01', value: 750000 }],
      },
    },
  };

  const comp = buildTerminalComparison(mockBundle, ['freeport', 'cove_point']);
  assert.strictEqual(comp.terminals.length, 2);

  const freeport = comp.terminals.find((t) => t.id === 'freeport');
  const cove = comp.terminals.find((t) => t.id === 'cove_point');

  assert.strictEqual(freeport.isPartial, true);
  assert.strictEqual(cove.isPartial, false);
  assert.ok(comp.caveats.length > 0, 'Must surface caveats for partial-coverage terminal');
  assert.ok(comp.caveats.some((c) => c.includes('Freeport')), 'Must include Freeport caveat');
});

test('§01 coverage anti-rot: all 9 terminals carry honest partial-ness and derivable caveats', () => {
  const terminalIds = Object.keys(LNG_TERMINALS);
  assert.strictEqual(terminalIds.length, 9, 'All 9 terminals must be present');

  const expectedPartial = {
    freeport: true,        // 52.9%
    sabine_pass: true,     // 30.3%
    cameron: true,         // 72.9%
    golden_pass: false,    // 12.7% (100% plant gate intake, Train 1 commissioning ramp)
    cove_point: false,     // 97.1%
    corpus_christi: false, // 99.4%
    plaquemines: false,    // 112.4% (running above nameplate)
    calcasieu: false,      // 123.5% (running above nameplate)
    port_arthur: false,    // non-operational
  };

  terminalIds.forEach((id) => {
    const t = LNG_TERMINALS[id];
    assert.ok(typeof t.expectedCoveragePct === 'number', `${id} must have numeric expectedCoveragePct`);
    assert.ok(typeof t.nameplate === 'number', `${id} must have numeric nameplate`);

    const isPartial = isTerminalPartial(t);
    assert.strictEqual(
      isPartial,
      expectedPartial[id],
      `Terminal ${id} partial-ness mismatch: got ${isPartial}, expected ${expectedPartial[id]}`
    );

    const caveat = getTerminalCaveat(t);
    if (isPartial || t.id === 'golden_pass' || t.operational === false || t.expectedCoveragePct > 105) {
      assert.ok(caveat !== null && caveat.length > 0, `Terminal ${id} must emit a caveat`);
      assert.ok(caveat.includes(t.display), `Caveat for ${id} must mention terminal display name`);
    }
  });
});

