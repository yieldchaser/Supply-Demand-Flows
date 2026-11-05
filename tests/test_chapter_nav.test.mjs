/**
 * Chapter navigation contract.
 *
 * The dashboard was one 18-panel scroll. It is now six chapters. These tests pin
 * the two things that restructuring can silently break:
 *   1. No panel is lost, duplicated, or left outside a chapter — a panel stranded
 *      outside every `data-chapter` would be permanently hidden once a chapter is
 *      selected, and the page would still "pass" every other test.
 *   2. Routing resolves a deep link, a query param, and a stored choice, and
 *      falls back rather than throwing on a malformed hash.
 *
 * Everything is read from the real docs/index.html and the real module, so the
 * test fails when the page and the navigation disagree.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { CHAPTERS, resolveChapter, applyChapter } from '../docs/js/components/chapter-nav.js';
import { SECTION_ORDER, sectionDomId } from '../docs/js/components/chapter-sections.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HTML = fs.readFileSync(path.join(__dirname, '..', 'docs', 'index.html'), 'utf8');
const CSS = fs.readFileSync(path.join(__dirname, '..', 'docs', 'css', 'components', 'chapter-nav.css'), 'utf8');

test('Chapter nav: no panel is dropped, duplicated, or stranded outside a chapter', () => {
  const ids = [...HTML.matchAll(/id="(panel-[a-z0-9-]+)"/g)].map((m) => m[1]);
  assert.ok(ids.length >= 18, `expected the full panel set, found ${ids.length}`);
  assert.deepEqual(
    ids.filter((id, i) => ids.indexOf(id) !== i),
    [],
    'a duplicated panel id would make the second mount silently no-op',
  );

  // Split the body at chapter boundaries; every panel must fall inside one.
  const body = HTML.slice(HTML.indexOf('chapter-root'));
  const parts = body.split(/data-chapter="([a-z]+)"/);
  // parts = [before, chapterId, content, chapterId, content, ...]
  assert.ok(parts.length >= 3, 'no data-chapter containers found in index.html');
  assert.equal(parts[0].includes('id="panel-'), false, 'a panel sits before the first chapter');

  const seen = new Set();
  for (let i = 1; i < parts.length; i += 2) seen.add(parts[i]);
  for (const c of CHAPTERS) {
    assert.ok(seen.has(c.id), `chapter "${c.id}" is declared in JS but missing from index.html`);
  }
  assert.equal(seen.size, CHAPTERS.length, 'index.html has a chapter the nav does not know about');
});

test('Chapter nav: deep links, query params, stored choice, and malformed input', () => {
  assert.equal(resolveChapter('', '', null), 'overview', 'default chapter');
  assert.equal(resolveChapter('', '#chapter=lng', null), 'lng', 'hash deep link');
  assert.equal(resolveChapter('?chapter=basins', '', null), 'basins', 'query param');
  assert.equal(resolveChapter('', '', 'reliability'), 'reliability', 'stored choice');
  assert.equal(resolveChapter('', '#chapter=nope', null), 'overview', 'unknown chapter falls back');
  assert.equal(resolveChapter('', '#', null), 'overview', 'empty hash falls back');
  assert.equal(resolveChapter('', '#chapter=', null), 'overview', 'empty value falls back');
});

test('Chapter nav: applyChapter shows exactly one chapter', () => {
  // Minimal element shim: applyChapter also sets role/aria-controls on the
  // shown section and the tab that controls it, so the shim has to support
  // attributes rather than only reading data-chapter.
  const make = (id) => {
    const attrs = { 'data-chapter': id };
    return {
      getAttribute: (k) => (k in attrs ? attrs[k] : null),
      setAttribute: (k, v) => { attrs[k] = String(v); },
      hasAttribute: (k) => k in attrs,
      removeAttribute: (k) => { delete attrs[k]; },
      hidden: false,
      tabIndex: -1,
      id,
    };
  };
  const nodes = CHAPTERS.map((c) => make(c.id));
  const root = { querySelectorAll: () => nodes, querySelector: () => null };
  const shown = applyChapter(root, 'flows');
  assert.deepEqual(
    shown.map((n) => n.id),
    ['flows'],
  );
  assert.deepEqual(
    nodes.filter((n) => !n.hidden).map((n) => n.id),
    ['flows'],
  );
});

test('Chapter nav: zero TypeScript syntax in executable docs/js code', () => {
  // Project rule: vanilla JS only in docs/. JSDoc comments are fine; real TS
  // annotations are not, because they break rendering silently in the browser.
  const file = path.join(__dirname, '..', 'docs', 'js', 'components', 'chapter-nav.js');
  const violations = fs
    .readFileSync(file, 'utf8')
    .split('\n')
    .map((line, i) => ({ line, n: i + 1 }))
    .filter(({ line }) => !/^\s*(\*|\/\*\*|\/\/)/.test(line))
    .filter(({ line }) =>
      /:\s*(string|number|boolean|any)\b|\binterface\s+[A-Z]|\bas\s+(string|number|HTMLElement)\b/.test(
        line,
      ),
    );
  assert.deepEqual(violations, []);
});

test('Chapter nav: 7 primary chapters declared with Europe elevated', () => {
  const ids = CHAPTERS.map((c) => c.id);
  assert.deepEqual(ids, ['overview', 'basins', 'lng', 'flows', 'reliability', 'europe', 'macro']);

  const europe = CHAPTERS.find((c) => c.id === 'europe');
  assert.equal(europe.label, 'Europe');
  assert.equal(europe.blurb, '10 countries, 5-yr norms, daily flows');
  assert.deepEqual(europe.sources, ['gie_agsi', 'eia_storage']);

  const macro = CHAPTERS.find((c) => c.id === 'macro');
  assert.equal(macro.label, 'Macro & Trade');
  assert.equal(macro.blurb, 'EIA export totals & destination shares');
  assert.deepEqual(macro.sources, ['eia_lng_exports']);
});

test('Chapter nav: CSS specificity defect fixed for absolute view isolation', () => {
  assert.match(
    CSS,
    /\.dashboard\s*>\s*\.chapter\[hidden\],\s*\.chapter\[hidden\]\s*\{\s*display:\s*none\s*!important;\s*\}/,
    'enforces display: none !important on hidden dashboard chapters to avoid specificity override',
  );
});

test('Chapter nav: chapter sections configuration and sectionDomId parity', () => {
  // Overview has no divergence
  assert.ok(!SECTION_ORDER.overview.some((s) => s.id === 'overview-divergence'));
  assert.ok(!SECTION_ORDER.overview.some((s) => s.panels?.includes('panel-divergence')));

  // Europe section order
  const europeSectionIds = SECTION_ORDER.europe.map((s) => s.id);
  assert.deepEqual(europeSectionIds, ['europe-divergence', 'europe-storage', 'europe-method']);
  assert.ok(SECTION_ORDER.europe.find((s) => s.id === 'europe-divergence').panels.includes('panel-divergence'));
  assert.ok(SECTION_ORDER.europe.find((s) => s.id === 'europe-storage').panels.includes('panel-eu-storage'));

  // Macro section order
  const macroSectionIds = SECTION_ORDER.macro.map((s) => s.id);
  assert.deepEqual(macroSectionIds, ['exports-total', 'exports-shares']);
  assert.ok(!macroSectionIds.includes('europe-storage'));
  assert.ok(!macroSectionIds.includes('europe-method'));

  // sectionDomId format for europe
  assert.equal(sectionDomId('europe', 'europe-divergence'), 'sec-europe-divergence');
  assert.equal(sectionDomId('europe', 'europe-storage'), 'sec-europe-storage');
  assert.equal(sectionDomId('europe', 'europe-method'), 'sec-europe-method');
});

test('Chapter nav: index.html chapters DOM separation', () => {
  const chapters = {};
  const chunks = HTML.split(/<div class="chapter" data-chapter="([a-z]+)">/);
  for (let i = 1; i < chunks.length; i += 2) {
    chapters[chunks[i]] = chunks[i + 1];
  }

  // Overview must not have panel-divergence
  assert.ok(!chapters.overview.includes('id="panel-divergence"'), 'Overview does not contain panel-divergence');

  // Europe must have panel-divergence and panel-eu-storage
  assert.ok(chapters.europe.includes('id="panel-divergence"'), 'Europe contains panel-divergence');
  assert.ok(chapters.europe.includes('id="panel-eu-storage"'), 'Europe contains panel-eu-storage');

  // Macro must only have panel-lng-total and panel-lng-shares
  assert.ok(chapters.macro.includes('id="panel-lng-total"'), 'Macro contains panel-lng-total');
  assert.ok(chapters.macro.includes('id="panel-lng-shares"'), 'Macro contains panel-lng-shares');
  assert.ok(!chapters.macro.includes('id="panel-eu-storage"'), 'Macro does not contain panel-eu-storage');
  assert.ok(!chapters.macro.includes('id="panel-divergence"'), 'Macro does not contain panel-divergence');
});
