/**
 * EU storage panel: data contracts of the view model the panel renders, the
 * chapter export projection, and copy hygiene of the panel module.
 */

import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { describe, it, before } from 'node:test';
import { registerHooks } from 'node:module';

// The panel imports d3 from a CDN import map; Node has no import map, so
// resolve 'd3' to an empty stub. Only the pure model functions are exercised.
registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3') return { url: 'data:text/javascript,export const timeFormat=()=>()=>"";', shortCircuit: true };
    return next(specifier, context);
  },
});

const PANEL = new URL('../docs/js/panels/eu-storage.js', import.meta.url);
const code = readFileSync(PANEL, 'utf8');
const bundleUrl = new URL('../docs/data/bundle.json', import.meta.url);
const haveBundle = existsSync(bundleUrl);

describe('EU panel model and export', { skip: !haveBundle && 'bundle not built' }, () => {
  let mod;
  let model;
  before(async () => {
    mod = await import(PANEL.href);
    const bundle = JSON.parse(readFileSync(bundleUrl, 'utf8'));
    model = mod.buildEuStorageModel(bundle);
  });

  it('builds the 10-country total first, then the ten member states', () => {
    assert.equal(model.entities.length, 11);
    assert.equal(model.entities[0].cc, 'eu10');
    assert.ok(model.entities[0].isAggregate);
    assert.ok(model.latestDate instanceof Date);
  });

  it('exports one row per entity with the figures the table prints', () => {
    for (const key of Object.keys(mod.EU_BASELINES)) {
      const out = mod.euStorageExport(model, key);
      assert.equal(out.entities.length, 11);
      assert.match(out.basis, /same calendar day/);
      for (const e of out.entities) {
        assert.ok(Number.isFinite(e.fill_pct), `${e.cc} fill`);
        assert.ok(Number.isFinite(e.dev_pp), `${e.cc} dev`);
        assert.ok(e.rank_low_to_high >= 1 && e.rank_low_to_high <= e.rank_of);
      }
    }
  });

  it('adds GIE\'s EU-wide aggregate as its own entity when the bundle carries it', () => {
    const bundle = JSON.parse(readFileSync(bundleUrl, 'utf8'));
    const src = bundle.sources.gie_agsi;
    // Stand-in EU series: Germany's rows relabelled, scaled to 4x capacity.
    const extra = src.data.filter((r) => r.series_id.startsWith('gie_storage_de_')).map((r) => ({
      ...r,
      series_id: r.series_id.replace('gie_storage_de_', 'gie_storage_eu_'),
      value: r.series_id.endsWith('_full_pct') ? r.value : r.value * 4,
    }));
    const m = mod.buildEuStorageModel({ sources: { gie_agsi: { ...src, data: [...src.data, ...extra] } } });
    assert.equal(m.entities.length, 12);
    assert.ok(m.entities[1].isEuWide);
    assert.ok(m.coverage > 0.9 && m.coverage < 1.1, `coverage ${m.coverage}`);
    assert.equal(m.views.length, 10, 'EU-wide rows never enter the 10-country arithmetic');
  });

  it('post-2022 baseline uses fewer prior years than the 5-year baseline', () => {
    const a = mod.euStorageExport(model, '5y').entities[0];
    const b = mod.euStorageExport(model, 'post2022').entities[0];
    assert.ok(b.normal_years.length <= a.normal_years.length);
    assert.ok(b.normal_years.every((y) => y >= 2022));
  });
});

describe('Panel module hygiene', () => {
  it('has no conversational filler', () => {
    for (const f of ['delve into', 'as an ai', 'tapestry', 'supercharge', 'dive deep', 'let us explore']) {
      assert.ok(!code.toLowerCase().includes(f), f);
    }
  });

  it('has no emoji', () => {
    assert.ok(!/[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u.test(code));
  });

  it('does not use double dashes in visible prose', () => {
    const literals = code.match(/`([^`]+)`/g) || [];
    const bad = [];
    for (const tpl of literals) {
      for (const m of tpl.match(/>([^<`$]+)</g) || []) {
        const text = m.slice(1, -1).trim();
        if (text && !text.startsWith('!--') && text.includes('--')) bad.push(text);
      }
    }
    assert.deepEqual(bad, []);
  });

  it('offers selection, view, unit and baseline controls', () => {
    for (const s of ['data-role="entity"', 'data-group="view"', 'data-group="unit"', 'data-group="baseline"',
      'data-value="season"', 'data-value="years"', 'data-value="timeline"', 'data-value="pct"', 'data-value="twh"']) {
      assert.ok(code.includes(s), s);
    }
  });

  it('does not restyle shared tooltip classes from the EU stylesheet', () => {
    const css = readFileSync(new URL('../docs/css/components/eu-storage.css', import.meta.url), 'utf8');
    assert.ok(!/^\s*\.chart-tooltip\b/m.test(css), '.chart-tooltip must not be restyled globally');
    assert.ok(!/^\s*\.tt-(row|label|title)\b/m.test(css), '.tt-* must not be restyled globally');
  });
});
