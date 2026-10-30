/**
 * Bundle loader fallback contract.
 *
 * The hash-suffixed artifacts (`index.{h}.json`, `bundle.{h}.json`,
 * `src.{source}.{h}.json`) are gitignored build output: a Pages deployment only
 * carries what git tracks, so on the live site they appear only once the
 * "Publish Dashboard Bundle" run after a commit has finished. A commit can
 * therefore land with a manifest naming shards that are not deployed yet.
 *
 * That used to blank the whole page: boot fell back from the missing index to
 * the missing hashed bundle. The loader now falls back to the stable
 * `bundle.json`, which git tracks and Pages always serves, and recovers a lazy
 * source from it when a shard 404s.
 *
 * Runs with built-in `node --test`; `fetch` is stubbed, no DOM required.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { loadBundle, ensureSource, onSourceReady } from '../docs/js/data/bundle-loader.js';

const MANIFEST = {
  bundle_url: 'bundle.deadbeef.json',
  index_url: 'index.deadbeef.json',
  generated_at: '2026-10-30T06:00:39.223830+00:00',
  hash: 'deadbeef',
};

const SERVED = {
  './data/manifest.json': MANIFEST,
  './data/bundle.json': {
    generated_at: '2026-10-29T11:00:00+00:00',
    sources: {
      eia_storage: { latest_period: '2026-10-23', row_count: 2, data: [] },
      gie_agsi: { latest_period: '2026-10-29', row_count: 1, data: [{ cc: 'DE' }] },
    },
  },
};

/**
 * Serve the listed URLs; everything else 404s, as a deploy missing build
 * output does.
 *
 * @param {Record<string, any>} served
 * @returns {{urls: string[]}} the requested URLs, in order
 */
function stubFetch(served) {
  const urls = [];
  globalThis.fetch = async (url) => {
    const path = String(url).split('?')[0];
    urls.push(path);
    if (!(path in served)) {
      return new Response('not found', { status: 404 });
    }
    return new Response(JSON.stringify(served[path]), { status: 200 });
  };
  return { urls };
}

test('boot falls back to the stable bundle when the manifest names undeployed shards', async () => {
  const { urls } = stubFetch(SERVED);
  const bundle = await loadBundle();
  assert.deepEqual(Object.keys(bundle.sources).sort(), ['eia_storage', 'gie_agsi']);
  assert.equal(bundle.sources.eia_storage.row_count, 2);
  // One attempt each: the hashed files are permanent 404s, not transient 5xx.
  assert.deepEqual(urls, ['./data/manifest.json', './data/index.deadbeef.json', './data/bundle.json']);
});

test('boot still parses the hashed bundle when the index exists', async () => {
  const { urls } = stubFetch({
    ...SERVED,
    './data/index.deadbeef.json': {
      generated_at: MANIFEST.generated_at,
      hash: 'deadbeef',
      core: [],
      sources: {},
    },
  });
  const bundle = await loadBundle();
  assert.equal(bundle.hash, 'deadbeef');
  assert.deepEqual(urls, ['./data/manifest.json', './data/index.deadbeef.json']);
});

test('a lazy shard that was never deployed is recovered from the stable bundle', async () => {
  const { urls } = stubFetch({
    ...SERVED,
    './data/index.deadbeef.json': {
      generated_at: MANIFEST.generated_at,
      hash: 'deadbeef',
      core: [],
      sources: { gie_agsi: { file: 'src.gie_agsi.deadbeef.json', rows: 1 } },
    },
  });
  const bundle = await loadBundle();
  let notified = 0;
  onSourceReady(bundle, 'gie_agsi', () => {
    notified += 1;
  });
  await ensureSource(bundle, 'gie_agsi');
  assert.deepEqual(bundle.sources.gie_agsi.data, [{ cc: 'DE' }]);
  assert.equal(notified, 1, 'panels waiting on the shard must be told it landed');
  // The shard is tried once (a 4xx fails fast, no retries), then the stable
  // bundle supplies it — and is parsed once, not per source.
  assert.equal(urls.filter((u) => u === './data/src.gie_agsi.deadbeef.json').length, 1);
  assert.equal(urls.filter((u) => u === './data/bundle.json').length, 1);
});

test('a source the index never listed comes from the stable bundle, not an error', async () => {
  stubFetch({
    ...SERVED,
    './data/index.deadbeef.json': {
      generated_at: MANIFEST.generated_at,
      hash: 'deadbeef',
      core: [],
      sources: {},
    },
  });
  const bundle = await loadBundle();
  // Pretend a shard 404 marked the bundle expired, as a pruned publish does.
  bundle.__expired = true;
  await ensureSource(bundle, 'gie_agsi');
  assert.deepEqual(bundle.sources.gie_agsi.data, [{ cc: 'DE' }]);
});
