/**
 * Chart export + chapter export contract.
 *
 * Two invariants here, both cheap to break silently:
 *   1. A CSV must survive awkward values (commas, quotes, newlines, Date, null)
 *      and keep columns aligned when rows have different keys — a misaligned
 *      export is worse than no export, because it looks authoritative.
 *   2. A chapter export must still hand back the raw rows when a derived block
 *      throws, and must carry provenance. The observatory's whole claim is that
 *      every number is traceable.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import {
  registerChartSnapshot,
  chartExportButton,
  toCsv,
  snapshotToCsv,
  registeredCount,
} from '../docs/js/util/chart-snapshot.js';
import { buildChapterExport, exportFilename, sourceRows } from '../docs/js/util/chapter-export.js';

test('chart export: button only renders for a registered chart', () => {
  assert.equal(chartExportButton('nope-not-registered'), '', 'unregistered chart offers no button');
  registerChartSnapshot('storage-envelope', 'storage-envelope', [{ bcf: 3800 }], { source: 'eia_storage' });
  const html = chartExportButton('storage-envelope');
  assert.match(html, /data-chart-export="storage-envelope"/);
  assert.equal(registeredCount(), 1, 're-registering the same id replaces, never duplicates');
  registerChartSnapshot('storage-envelope', 'storage-envelope', [{ bcf: 3810 }], {});
  assert.equal(registeredCount(), 1);
});

test('chart export: CSV quoting, column union, and Date formatting', () => {
  const csv = toCsv([
    { date: '2026-09-01', note: 'build, 60 Bcf', value: 1 },
    { date: '2026-09-02', quote: 'said "full"', value: 2 },
    { date: new Date(Date.UTC(2026, 8, 3)), value: null },
  ]);
  const lines = csv.split('\n');
  assert.equal(lines[0], 'date,note,value,quote', 'key order is first-seen across all rows');
  assert.equal(lines[1], '2026-09-01,"build, 60 Bcf",1,');
  assert.equal(lines[2], '2026-09-02,,2,"said ""full"""');
  assert.equal(lines[3], '2026-09-03,,,');
});

test('chart export: file carries provenance lines before the header', () => {
  registerChartSnapshot('demo', 'demo', [{ a: 1 }], { source: 'gulf_south', basis: 'latest cycle wins', nested: { skip: 1 } });
  const text = snapshotToCsv({ label: 'demo', rows: [{ a: 1 }], meta: { source: 'gulf_south', nested: {} } }, new Date('2026-09-19T12:00:00Z'));
  const lines = text.trimEnd().split('\n');
  assert.match(lines[0], /# bluetide chart export — demo/);
  assert.ok(lines.includes('# source: gulf_south'));
  assert.equal(lines.some((l) => l.startsWith('# nested')), false, 'non-scalar meta is dropped, not stringified');
  assert.equal(lines[lines.length - 2], 'a');
  assert.equal(lines[lines.length - 1], '1');
});

test('chapter export: keeps raw rows when a derived block throws', () => {
  const bundle = {
    meta: { hash: 'abc123', generated_at: '2026-09-18T00:00:00Z' },
    sources: { gulf_south: { data: [{ series_id: 'x', period: '2026-09-01', value: 1 }], latest_period: '2026-09-01' } },
  };
  const chapter = { id: 'lng', label: 'LNG', sources: ['gulf_south', 'missing'] };
  const payload = buildChapterExport(bundle, chapter, { lng: () => { throw new Error('boom'); } }, new Date('2026-09-19T00:00:00Z'));

  assert.equal(payload.schema, 'bluetide.chapter-export/1');
  assert.equal(payload.provenance.chapter, 'lng');
  assert.equal(payload.provenance.bundle_hash, 'abc123');
  assert.equal(payload.provenance.rows, 1);
  assert.equal(payload.sources.gulf_south.rows.length, 1);
  assert.equal(payload.sources.missing.meta, null, 'absent source is reported, not invented');
  assert.match(payload.derived.lng.error, /boom/);
  // Must be serialisable: a Date or Map slipping in would silently produce a
  // broken download at click time, not at build time.
  assert.doesNotThrow(() => JSON.stringify(payload));
});

test('chapter export: filenames are date-stamped and source rows are honestly counted', () => {
  assert.equal(exportFilename('reliability', new Date('2026-09-19T23:59:00Z')), 'bluetide_reliability_2026-09-19.json');
  assert.deepEqual(sourceRows({ sources: {} }, 'x'), { rows: [], meta: null });
});
