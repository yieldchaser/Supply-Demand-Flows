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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HTML = fs.readFileSync(path.join(__dirname, '..', 'docs', 'index.html'), 'utf8');

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
