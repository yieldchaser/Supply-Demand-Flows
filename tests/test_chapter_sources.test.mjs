/**
 * Chapter export: source scoping.
 *
 * The download for a chapter must fetch exactly that chapter's sources, on top
 * of whatever the page already parsed — not a second full boot, and not every
 * shard in the bundle. These tests pin that contract, plus the provenance line
 * that tells a reader which sources were fetched fresh, which were already on
 * the page, and which failed.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { ensureChapterSources } from '../docs/js/util/chapter-sources.js';
import { buildChapterExport } from '../docs/js/util/chapter-export.js';
import { CHAPTERS } from '../docs/js/components/chapter-nav.js';

test('chapter sources: only the chapter\'s own shards are fetched', async () => {
  const requested = [];
  const loader = {
    async ensureSource(bundle, key) {
      requested.push(key);
      bundle.sources[key] = { data: [] };
    },
  };
  const bundle = { sources: {} };
  const chapter = CHAPTERS.find((c) => c.id === 'macro');
  const report = await ensureChapterSources(bundle, loader, chapter);

  assert.deepEqual(requested.sort(), [...chapter.sources].sort());
  assert.deepEqual(report.loaded.sort(), [...chapter.sources].sort());
  assert.deepEqual(report.loadFailed, []);
  assert.deepEqual(report.already, []);
});

test('chapter sources: already-parsed sources are reused, not refetched', async () => {
  const requested = [];
  const loader = {
    async ensureSource(_bundle, key) {
      requested.push(key);
    },
  };
  const bundle = { sources: { eia_storage: { data: [{ value: 1 }] } } };
  const report = await ensureChapterSources(bundle, loader, { sources: ['eia_storage', 'eia_supply'] });

  assert.deepEqual(requested, ['eia_supply']);
  assert.deepEqual(report.already, ['eia_storage']);
});

test('chapter sources: a failed shard is named, and the rest still land', async () => {
  const loader = {
    async ensureSource(bundle, key) {
      if (key === 'gie_agsi') throw new Error('404');
      bundle.sources[key] = { data: [] };
    },
  };
  const report = await ensureChapterSources({ sources: {} }, loader, { sources: ['eia_storage', 'gie_agsi'] });
  assert.deepEqual(report.loadFailed, ['gie_agsi']);
  assert.deepEqual(report.loaded, ['eia_storage']);
});

test('chapter sources: a chapter with no sources declared is a no-op', async () => {
  const report = await ensureChapterSources({ sources: {} }, null, {});
  assert.deepEqual(report, { loaded: [], already: [], loadFailed: [] });
});

test('chapter export: provenance records the source load outcome', () => {
  const bundle = { sources: {} };
  const chapter = { id: 'lng', label: 'LNG', sources: ['gulf_south'] };
  const payload = buildChapterExport(bundle, chapter, {}, new Date('2026-10-28T00:00:00Z'), {
    loaded: ['gulf_south'],
    already: [],
    loadFailed: [],
  });
  assert.equal(payload.schema, 'bluetide.chapter-export/1');
  assert.deepEqual(payload.provenance.source_load.fetched, ['gulf_south']);
  assert.equal(payload.provenance.exported_at, '2026-10-28T00:00:00.000Z');

  // Without a report the field is explicitly null rather than absent, so a
  // reader can tell "not recorded" from "nothing fetched".
  assert.equal(buildChapterExport(bundle, chapter).provenance.source_load, null);
});
