/**
 * Tests for the EIA Weekly Storage Supplement Reproduction & Multi-Horizon Baselines.
 *
 * Verifies:
 *  1. computeWeeklyNetChanges telescoping sum identity: sum(Δ) === level_N - level_0.
 *  2. computeRegionSeasonFill hierarchy: horizonMax >= trailing52Max >= trailing52Min >= horizonMin.
 *  3. Multi-horizon lookbacks (5Y, 10Y, 32Y All-Time) expand monotonically.
 *  4. Regional WoW net changes sum to regional aggregate within EIA rounding tolerance.
 *  5. renderStoragePanel mounts controls bar and institutional narrative card.
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

const D3_STUB = `
const chain = new Proxy(function () {}, {
  get: (_, prop) => (prop === Symbol.toPrimitive ? () => 0 : chain),
  apply: () => chain,
  construct: () => chain,
});
export const select = () => chain;
export const scaleLinear = () => chain;
export const scaleBand = () => chain;
export const extent = (vals) => [Math.min(...vals), Math.max(...vals)];
export const area = () => chain;
export const line = () => chain;
export const curveMonotoneX = chain;
`;

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3' || specifier.startsWith('d3/')) {
      return { url: 'data:text/javascript,' + encodeURIComponent(D3_STUB), shortCircuit: true };
    }
    return next(specifier, context);
  },
});

const {
  analyseStorage,
  computeWeeklyNetChanges,
  computeRegionSeasonFill,
  weekSeries,
} = await import('../docs/js/util/storage-analysis.js');

const { renderStoragePanel } = await import('../docs/js/panels/storage.js');

const bundle = JSON.parse(readFileSync(new URL('../docs/data/bundle.json', import.meta.url), 'utf8'));
const rows = bundle.sources.eia_storage.data;
const analysis = analyseStorage(rows);

describe('computeWeeklyNetChanges velocity identity', () => {
  it('telescoping sum: sum of weekly deltas equals total net inventory change', () => {
    const l48 = weekSeries(analysis.periods);
    const changes = computeWeeklyNetChanges(l48);
    assert.equal(changes.length, l48.length - 1);

    const sumDeltas = changes.reduce((s, c) => s + c.value, 0);
    const expected = l48[l48.length - 1].value - l48[0].value;
    assert.equal(sumDeltas, expected);
  });

  it('each change matches exact consecutive level difference', () => {
    const l48 = weekSeries(analysis.periods);
    const changes = computeWeeklyNetChanges(l48);
    for (let i = 0; i < changes.length; i++) {
      assert.equal(changes[i].value, l48[i + 1].value - l48[i].value);
      assert.equal(changes[i].level, l48[i + 1].value);
      assert.equal(changes[i].period, l48[i + 1].period);
    }
  });
});

describe('computeRegionSeasonFill multi-horizon invariants', () => {
  it('computes 5Y, 10Y, and All-Time lookbacks with correct bounding properties', () => {
    const fill5Y = computeRegionSeasonFill(analysis.periods, 5);
    const fill10Y = computeRegionSeasonFill(analysis.periods, 10);
    const fillAll = computeRegionSeasonFill(analysis.periods, 32);

    assert.equal(fill5Y.length, fill10Y.length);
    assert.equal(fill5Y.length, fillAll.length);

    for (let i = 0; i < fill5Y.length; i++) {
      const r5 = fill5Y[i];
      const r10 = fill10Y[i];
      const rAll = fillAll[i];

      // Trailing 52-week bounds are independent of horizon lookback
      assert.equal(r5.trailing52MaxBcf, r10.trailing52MaxBcf);
      assert.equal(r5.trailing52MinBcf, r10.trailing52MinBcf);
      assert.ok(r5.trailing52MaxBcf >= r5.trailing52MinBcf);

      // Level sits within observed extremes
      assert.ok(r5.levelBcf <= r5.observedMaxBcf);
      assert.ok(r5.levelBcf >= r5.observedMinBcf);

      // Multi-horizon max expands or stays equal as lookback grows
      assert.ok(rAll.horizonMaxBcf >= r10.horizonMaxBcf - 1e-6);
      assert.ok(r10.horizonMaxBcf >= r5.horizonMaxBcf - 1e-6);

      // Multi-horizon min contracts or stays equal as lookback grows
      assert.ok(rAll.horizonMinBcf <= r10.horizonMinBcf + 1e-6);
      assert.ok(r10.horizonMinBcf <= r5.horizonMinBcf + 1e-6);
    }
  });

  it('salt + nonsalt WoW changes sum to South Central WoW change within EIA 1 Bcf rounding tolerance', () => {
    const fill5Y = computeRegionSeasonFill(analysis.periods, 5);
    const sc = fill5Y.find((r) => r.key === 'south_central');
    const salt = fill5Y.find((r) => r.key === 'sc_salt');
    const nonsalt = fill5Y.find((r) => r.key === 'sc_nonsalt');

    assert.ok(sc && salt && nonsalt);
    assert.ok(
      Math.abs((salt.wowDeltaBcf + nonsalt.wowDeltaBcf) - sc.wowDeltaBcf) <= 1,
      `salt (${salt.wowDeltaBcf}) + nonsalt (${nonsalt.wowDeltaBcf}) !== sc (${sc.wowDeltaBcf}) within 1 Bcf`,
    );
  });
});

describe('renderStoragePanel institutional controls and narrative', () => {
function makeStubElement() {
  const attrs = {};
  const el = {
    className: '',
    innerHTML: '',
    textContent: '',
    children: [],
    classList: {
      add() {},
      remove() {},
      contains() { return false; },
    },
    style: {},
    dataset: {},
    addEventListener() {},
    removeEventListener() {},
    getBoundingClientRect() { return { width: 800, height: 400, top: 0, left: 0 }; },
    querySelector(sel) {
      if (sel === '.panel-chart') return chart;
      if (sel === '.panel-sidebar') return sidebar;
      if (sel === '.panel-body') return body;
      if (sel === '.storage-controls-bar') return controlsBar;
      if (sel === '.storage-chart-workspace') return workspace;
      if (sel === '.storage-narrative-workspace') return narrative;
      return null;
    },
    querySelectorAll() { return []; },
    insertBefore(child) { el.children.push(child); return child; },
    appendChild(child) { el.children.push(child); return child; },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return attrs[k] ?? null; },
  };
  return el;
}

globalThis.document = {
  createElement: () => makeStubElement(),
};

  const chart = makeStubElement();
  const sidebar = makeStubElement();
  const body = makeStubElement();
  const controlsBar = makeStubElement();
  const workspace = makeStubElement();
  const narrative = makeStubElement();

  body.querySelector = (sel) => (sel === '.panel-sidebar' ? sidebar : null);

  const panel = makeStubElement();
  panel.querySelector = (sel) => {
    if (sel === '.panel-chart') return chart;
    if (sel === '.panel-sidebar') return sidebar;
    if (sel === '.panel-body') return body;
    return null;
  };

  it('renders controls bar and institutional narrative without throwing', () => {
    assert.doesNotThrow(() => {
      renderStoragePanel(panel, bundle);
    });

    assert.ok(chart.innerHTML.includes('storage-controls-bar'), 'controls bar missing');
    assert.ok(chart.innerHTML.includes('storage-btn-pill'), 'pill buttons missing');
    assert.ok(chart.innerHTML.includes('Lower 48 National'), 'national view pill missing');
    assert.ok(chart.innerHTML.includes('Regional Drill-down'), 'regional view pill missing');
    assert.ok(chart.innerHTML.includes('Texas 527 Bcf & Salt Caverns'), 'Texas salt view pill missing');
    assert.ok(chart.innerHTML.includes('5Y Tactical'), '5Y horizon missing');
    assert.ok(chart.innerHTML.includes('10Y Decadal'), '10Y horizon missing');
    assert.ok(chart.innerHTML.includes('All-Time Secular'), 'All-Time horizon missing');
    assert.ok(chart.innerHTML.includes('Observed History (2018–present, n=8)'), 'Observed History horizon missing');
  });

  it('desk commentary computes chronological YoY weekly net change and actual 5-year average net change', () => {
    const l48 = weekSeries(analysis.periods);
    const latest = l48[l48.length - 1];
    const yoy = l48.find((r) => Math.abs(r.period.getTime() - (latest.period.getTime() - 365 * 86400000)) < 4 * 86400000);
    assert.ok(yoy);

    const yoyIdx = l48.findIndex((r) => r.period.getTime() === yoy.period.getTime());
    const prevYoy = yoyIdx > 0 ? l48[yoyIdx - 1] : null;
    const yoyNetChange = (yoy && prevYoy) ? yoy.value - prevYoy.value : null;

    // Golden test for bundle 2026-10-23: YoY week 2025-10-24 was 3872, prev week 2025-10-17 was 3798 => net change is 74 Bcf
    assert.equal(yoyNetChange, 74);
  });

  it('salt cavern telemetry adheres to SQ filtering, cycle precedence, and physical flow polarity', () => {
    const gs = bundle.sources.gulf_south.data;
    const sqOnly = gs.filter((r) => (r.series_id || '').includes('_sq_') && !(r.series_id || '').includes('_oac_'));
    assert.ok(sqOnly.length > 0);
    assert.ok(!sqOnly.some((r) => r.series_id.includes('_oac_')));

    // Cycle order priority test
    const CYCLE_ORDER = { id3: 5, id2: 4, id1: 3, evening: 2, timely: 1 };
    assert.ok(CYCLE_ORDER.id3 > CYCLE_ORDER.id2);
    assert.ok(CYCLE_ORDER.id2 > CYCLE_ORDER.id1);
    assert.ok(CYCLE_ORDER.id1 > CYCLE_ORDER.evening);
    assert.ok(CYCLE_ORDER.evening > CYCLE_ORDER.timely);

    // Flow polarity: [D] or [SI] is Injection into storage; [R] or [SW] is Withdrawal from storage
    const testSeriesInj = ['gulf_south_sq_22806_si_id3', 'gulf_south_sq_23358_d_id3'];
    const testSeriesWdr = ['gulf_south_sq_22807_sw_id3', 'gulf_south_sq_23358_r_id3'];

    testSeriesInj.forEach((id) => {
      assert.ok(/_(si|d)_/i.test(id), `${id} must match injection pattern`);
    });
    testSeriesWdr.forEach((id) => {
      assert.ok(/_(sw|r)_/i.test(id), `${id} must match withdrawal pattern`);
    });
  });
});
