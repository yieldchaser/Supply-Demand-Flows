/**
 * Chapter export wiring contract.
 *
 * The chapter export is assembled in a few pieces that can silently come apart:
 * the button is built by chapter-nav, mounted by the inline script in
 * docs/index.html, and the files themselves come from util/*-export.js. Nothing
 * at runtime complains when one of those is missing — the reader just sees no
 * button, or gets an empty chapter. These assertions fail when the wiring, the
 * declared provenance, or the honesty of a derived block regresses.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { CHAPTERS, buildExportButton, syncExportButton } from '../docs/js/components/chapter-nav.js';
import { fleetExport, fleetSummaryRows } from '../docs/js/util/fleet-export.js';
import { downtimeExport, downtimeRules } from '../docs/js/util/downtime-export.js';
import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';
import { DOWNTIME_CONF } from '../docs/js/util/lng-downtime.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HTML = fs.readFileSync(path.join(__dirname, '..', 'docs', 'index.html'), 'utf8');

test('chapter export: every chapter declares bundle provenance', () => {
  CHAPTERS.forEach((c) => {
    assert.ok(Array.isArray(c.sources) && c.sources.length > 0, `chapter "${c.id}" declares no sources`);
  });
  // The declared keys must be the keys the exporters actually read, or the
  // downloaded file would claim provenance it does not contain.
  assert.ok(CHAPTERS.find((c) => c.id === 'lng').sources.includes('gulf_south'), 'LNG reads Gulf South rows');
  assert.ok(CHAPTERS.find((c) => c.id === 'macro').sources.includes('gie_agsi'), 'Macro reads GIE rows');
});

test('chapter export: the download button renders and relabels per chapter', () => {
  // chapter-nav is browser code; a minimal element shim is enough to exercise
  // the markup and attribute contract without a DOM implementation dependency.
  const el = () => ({
    type: '',
    className: '',
    title: '',
    innerHTML: '',
    attrs: {},
    setAttribute(k, v) {
      this.attrs[k] = String(v);
    },
    getAttribute(k) {
      return this.attrs[k] ?? null;
    },
  });
  globalThis.document = { createElement: el };
  try {
    const btn = buildExportButton('LNG');
    checkButton(btn);
  } finally {
    delete globalThis.document;
  }
});

function checkButton(btn) {
  assert.match(btn.className, /chapter-export/);
  assert.match(btn.innerHTML, /Download LNG data/, 'the label names the chapter, not "current"');
  assert.match(btn.getAttribute('aria-label'), /LNG chapter/);
  syncExportButton(btn, 'Macro & Europe');
  assert.match(btn.getAttribute('aria-label'), /Macro & Europe chapter/);
  // syncExportButton must be a no-op for an absent button (partial mounts).
  assert.doesNotThrow(() => syncExportButton(null, 'x'));
  // Markup injection guard: a label is interpolated into innerHTML.
  globalThis.document = { createElement: () => ({ innerHTML: '', setAttribute() {}, getAttribute: () => null }) };
  assert.equal(buildExportButton('<img onerror=1>').innerHTML.includes('<img'), false);
  delete globalThis.document;
}

test('chapter export: index.html wires the button, loads the bundle, and un-hides the bar', () => {
  assert.match(HTML, /id="chapter-nav"[^>]*hidden/, 'kept hidden in the authored markup for the no-JS path');
  assert.match(
    HTML,
    /getElementById\('chapter-nav'\)\?\.removeAttribute\('hidden'\)/,
    'a hidden bar would mount the export button inside an invisible container',
  );
  assert.match(HTML, /buildExportButton\(/, 'button is built by chapter-nav');
  assert.match(HTML, /chapter-export\.js/, 'inline script imports the exporter');
  assert.match(HTML, /loadBundle\(\)/, 'downloads come from the real bundle, not a re-fetch of one source');
  assert.match(HTML, /fleetExport\(/, 'LNG chapter ships the fleet view model');
  assert.match(HTML, /downtimeExport\(/, 'reliability chapter ships the downtime view model');
});

test('fleet export: one row per registered terminal, absences reported not invented', () => {
  const bundle = { sources: {} };
  const rows = fleetSummaryRows(bundle);
  assert.equal(rows.length, Object.keys(LNG_TERMINALS).length, 'every terminal appears');
  rows.forEach((r) => {
    assert.equal(r.ok, false, 'no source means not-ok, not zero volume');
    assert.equal(r.latest_mmcf_d, null, 'absence must never render as 0');
  });
  const exp = fleetExport(bundle);
  assert.equal(exp.totals.terminals, rows.length);
  assert.equal(exp.totals.latest_mmcf_d, 0);
  assert.ok(exp.basis.summary.includes('terminalSummary'), 'states which function produced the numbers');
  assert.doesNotThrow(() => JSON.stringify(exp));
  assert.equal(exp.daily.length, 0);
});

test('fleet export: daily totals are the sum of terminal series on shared gas days', () => {
  // Freeport via its Gulf South headline meter, two days.
  const gulf = [
    { series_id: 'gulf_south_sq_24329_d_timely', period: '2026-09-01', value: 1100000 },
    { series_id: 'gulf_south_sq_24329_d_timely', period: '2026-09-02', value: 1200000 },
  ];
  const bundle = { sources: { gulf_south: { data: gulf } } };
  const rows = fleetSummaryRows(bundle);
  const fp = rows.find((r) => r.terminal_id === 'freeport');
  assert.equal(fp.ok, true);
  assert.equal(fp.last_day, '2026-09-02');
  assert.equal(fp.first_day, '2026-09-01');
  assert.equal(fp.days_of_history, 2);
  assert.equal(fp.latest_mmcf_d, 1170.7, '1,200,000 Dth/d = 1,170.7 MMcf/d');

  const daily = fleetExport(bundle).daily;
  assert.deepEqual(daily.map((d) => d.date), ['2026-09-01', '2026-09-02']);
  assert.equal(daily[1].terminals_reporting, 1);
  assert.ok(daily[1].mmcf_d > 0);
});

test('downtime export: ships the rule with the events', () => {
  const rules = downtimeRules();
  assert.equal(rules.depressed_days_to_flag, 5);
  assert.ok(rules.floor_pct_of_baseline > 0 && rules.floor_pct_of_baseline < 1);
  assert.ok(rules.baseline.includes('median'), 'thresholds are stated, not implied');

  const exp = downtimeExport({ sources: {} });
  assert.deepEqual(Object.keys(exp.terminals).sort(), Object.keys(DOWNTIME_CONF).sort());
  Object.values(exp.terminals).forEach((t) => {
    assert.ok(t.status, 'a terminal with no data still states its status');
    assert.ok(t.coverage_note, 'the honesty note travels with the numbers');
    assert.equal(t.events.length, 0);
  });
  assert.doesNotThrow(() => JSON.stringify(exp));
});
