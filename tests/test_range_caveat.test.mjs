/**
 * Test suite for Prompt Y §04 / Y3:
 * Range exceeding history caveat on LNG panels (Section 5, 7, 8).
 *
 * Proves that selecting a range (such as '1y') against a source's actual
 * history surfaces an honest caveat stating the real span and start date
 * (e.g. "showing 101 days; this source begins 2026-05-25") rather than
 * silently clipping. All dates/counts are supplied by the caller — derived
 * from the data actually rendered — so this suite never hardcodes "today".
 *
 * Runs via built-in `node --test`.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import {
  checkRangeExceedsHistory,
  computePresetInterval,
  computeSeriesDateRange,
  parseRangeFromQuery,
  applyRangeCaveat,
} from '../docs/js/util/range-state.js';

const LATEST = '2026-09-02';
const EARLIEST = '2026-05-25';
const DAY_COUNT = 101;

test('Y3: checkRangeExceedsHistory includes actual span and source start date', () => {
  const { startDateStr } = computePresetInterval('1y', LATEST);
  assert.strictEqual(startDateStr, '2025-09-02');

  const caveat = checkRangeExceedsHistory(startDateStr, EARLIEST, 'LNG feedgas', DAY_COUNT);
  assert.ok(caveat !== null, 'Caveat must be returned when 1y exceeds history');
  assert.ok(
    caveat.includes(`showing ${DAY_COUNT} days; this source begins ${EARLIEST}`),
    `Caveat must state exact span and start date. Got: "${caveat}"`
  );
});

test('Y3: LNG range caveat fires for 1y preset and does not fire for 30d preset', () => {
  // 1y preset exceeds the source's history -> must fire
  const { startDateStr: start1y } = computePresetInterval('1y', LATEST);
  const caveat1y = checkRangeExceedsHistory(start1y, EARLIEST, 'LNG feedgas', DAY_COUNT);
  assert.ok(caveat1y !== null);
  assert.ok(caveat1y.includes(`showing ${DAY_COUNT} days; this source begins ${EARLIEST}`));

  // 30d preset is within the source's history -> must NOT fire
  const { startDateStr: start30d } = computePresetInterval('30d', LATEST);
  const caveat30d = checkRangeExceedsHistory(start30d, EARLIEST, 'LNG feedgas', DAY_COUNT);
  assert.strictEqual(caveat30d, null);
});

test('Y3: computeSeriesDateRange derives earliest/latest/dayCount from rows, not literals', () => {
  const rows = [
    { period: '2026-05-25', value: 1 },
    { period: '2026-05-26', value: 2 },
    { period: '2026-05-26', value: 2.5 }, // duplicate day (e.g. different cycle) must not double-count
    { period: '2026-09-02', value: 3 },
  ];
  const range = computeSeriesDateRange(rows, 'period');
  assert.deepStrictEqual(range, {
    earliestStr: '2026-05-25',
    latestStr: '2026-09-02',
    dayCount: 3,
  });

  assert.strictEqual(computeSeriesDateRange([], 'period'), null, 'empty rows must yield null, not throw');
  assert.strictEqual(computeSeriesDateRange(null, 'period'), null, 'missing rows must yield null, not throw');
});

test('Y3: applyRangeCaveat injects caveat banner for LNG panels when 1y range is queried, using supplied seriesInfo', () => {
  class MockElement {
    constructor(id) {
      this.id = id;
      this.children = [];
      this.innerHTML = '';
      this.style = {};
      this.className = '';
    }
    querySelector(selector) {
      if (selector === '.panel-caveat--range') {
        return this.children.find((c) => c.className && c.className.includes('panel-caveat--range')) || null;
      }
      return null;
    }
    prepend(child) {
      this.children.unshift(child);
    }
    appendChild(child) {
      this.children.push(child);
    }
  }

  const prevDoc = globalThis.document;
  try {
    globalThis.document = {
      createElement: (tag) => new MockElement(tag),
    };

    const seriesInfo = { earliestStr: EARLIEST, latestStr: LATEST, dayCount: DAY_COUNT, sourceLabel: 'LNG feedgas' };

    const panelEl = new MockElement('panel-lng-feedgas');
    const caveatText = applyRangeCaveat(panelEl, 'lng-feedgas', '?range=1y', seriesInfo);

    assert.ok(caveatText !== null, 'applyRangeCaveat must return caveat text on 1y range');
    assert.ok(
      caveatText.includes(`showing ${DAY_COUNT} days; this source begins ${EARLIEST}`),
      'Caveat text must state exact span and start date'
    );

    const caveatEl = panelEl.querySelector('.panel-caveat--range');
    assert.ok(caveatEl !== null, 'Caveat DOM element must be prepended to panelEl');
    assert.ok(
      caveatEl.innerHTML.includes(`showing ${DAY_COUNT} days; this source begins ${EARLIEST}`),
      'Caveat element innerHTML must include exact span and start date'
    );

    // 30d should not inject caveat
    const panelEl30 = new MockElement('panel-lng-feedgas');
    const noCaveat = applyRangeCaveat(panelEl30, 'lng-feedgas', '?range=30d', seriesInfo);
    assert.strictEqual(noCaveat, null);
    assert.strictEqual(panelEl30.querySelector('.panel-caveat--range'), null);
  } finally {
    globalThis.document = prevDoc;
  }
});

test('Y3: applyRangeCaveat reflects a different start date and day count when seriesInfo changes', () => {
  class MockElement {
    constructor(id) {
      this.id = id;
      this.children = [];
      this.innerHTML = '';
      this.style = {};
      this.className = '';
    }
    querySelector(selector) {
      if (selector === '.panel-caveat--range') {
        return this.children.find((c) => c.className && c.className.includes('panel-caveat--range')) || null;
      }
      return null;
    }
    prepend(child) {
      this.children.unshift(child);
    }
    appendChild(child) {
      this.children.push(child);
    }
  }

  const prevDoc = globalThis.document;
  try {
    globalThis.document = {
      createElement: (tag) => new MockElement(tag),
    };

    const seriesInfo = { earliestStr: '2025-06-01', latestStr: '2026-01-01', dayCount: 215, sourceLabel: 'LNG feedgas' };
    const panelEl = new MockElement('panel-lng-feedgas');
    const caveatText = applyRangeCaveat(panelEl, 'lng-feedgas', '?range=1y', seriesInfo);

    assert.ok(caveatText !== null);
    assert.ok(
      caveatText.includes('showing 215 days; this source begins 2025-06-01'),
      `Caveat must reflect the supplied seriesInfo, not any hardcoded value. Got: "${caveatText}"`
    );
    assert.ok(!caveatText.includes('101 days'), 'Caveat must not contain a stale hardcoded day count');
    assert.ok(!caveatText.includes('2026-05-25'), 'Caveat must not contain a stale hardcoded start date');
  } finally {
    globalThis.document = prevDoc;
  }
});

test('Y3: applyRangeCaveat does not inject a caveat and does not throw when the panel has no data', () => {
  class MockElement {
    constructor(id) {
      this.id = id;
      this.children = [];
      this.innerHTML = '';
      this.style = {};
      this.className = '';
    }
    querySelector() {
      return null;
    }
    prepend(child) {
      this.children.unshift(child);
    }
  }

  const panelEl = new MockElement('panel-lng-feedgas');
  assert.doesNotThrow(() => {
    const result = applyRangeCaveat(panelEl, 'lng-feedgas', '?range=1y', null);
    assert.strictEqual(result, null);
  });
  assert.strictEqual(panelEl.children.length, 0);
});
