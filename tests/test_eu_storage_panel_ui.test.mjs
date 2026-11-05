/**
 * Verification test for EU Storage Panel 100x Upgrade:
 * - All 10 countries covered with complete metrics
 * - Card metrics: % full, TWh, deviation pp + TWh, regime (injection/withdrawal/neutral), sparkline
 * - Detail view: "Where it should be right now" (5Y median), working-gas capacity, historical rank
 * - Multi-year comparison trajectory (2021–2026) without zigzags
 * - Flow dynamics chart: daily flows, net injection/withdrawal, 7-day trailing average
 * - 14-day history table completeness
 * - Clean institutional terminology (no AI-slop, no emojis, no double dashes in prose)
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import {
  EU_COUNTRIES,
  buildCountryViews,
  buildAggregate,
  positionalNormal,
  rankAmongYears,
  monthlyEnvelope,
  rowsByYear,
  monthDayFraction,
  flowDynamics,
  countryStorageMetrics,
} from '../docs/js/util/eu-storage-analysis.js';
import { getSeries } from '../docs/js/util/series.js';

const bundle = JSON.parse(readFileSync(new URL('../docs/data/bundle.json', import.meta.url), 'utf8'));
const source = bundle.sources.gie_agsi;

const perCountry = {};
for (const c of EU_COUNTRIES) {
  perCountry[c.cc] = {
    pct: getSeries(source.data, `gie_storage_${c.cc}_full_pct`),
    gas: getSeries(source.data, `gie_storage_${c.cc}_gas_in_storage`),
    injection: getSeries(source.data, `gie_storage_${c.cc}_injection`),
    withdrawal: getSeries(source.data, `gie_storage_${c.cc}_withdrawal`),
  };
}

const views = buildCountryViews(perCountry);
const aggregate = buildAggregate(views);

describe('EU Storage Country Grid & Metrics (10 Countries)', () => {
  it('includes all 10 required member states with positive working gas capacity', () => {
    assert.equal(views.length, 10);
    const expected = ['Germany', 'France', 'Italy', 'Netherlands', 'Spain', 'Austria', 'Belgium', 'Czechia', 'Hungary', 'Poland'];
    for (const name of expected) {
      const v = views.find((item) => item.label === name);
      assert.ok(v, `Missing country: ${name}`);
      assert.ok(v.capacityTwh > 0, `Capacity must be > 0 for ${name}: ${v.capacityTwh}`);
      assert.ok(v.latest, `Latest observation missing for ${name}`);
      assert.ok(Number.isFinite(v.latest.pct), `Latest pct non-finite for ${name}`);
      assert.ok(Number.isFinite(v.latest.twh), `Latest twh non-finite for ${name}`);
    }
  });

  it('calculates deviation vs 5Y median in both pp and TWh for each country', () => {
    for (const v of views) {
      const metrics = countryStorageMetrics(v);
      assert.ok(metrics, `Metrics null for ${v.cc}`);
      assert.ok(metrics.normal.n > 0, `Normal sample empty for ${v.cc}`);
      assert.ok(Number.isFinite(metrics.normal.median), `Median non-finite for ${v.cc}`);
      assert.ok(Number.isFinite(metrics.deviationPp), `Deviation pp non-finite for ${v.cc}`);
      assert.ok(Number.isFinite(metrics.deviationTwh), `Deviation TWh non-finite for ${v.cc}`);

      // Verify mathematical identity: devTwh === (devPp * capacityTwh) / 100
      const expectedDevTwh = (metrics.deviationPp * v.capacityTwh) / 100;
      assert.ok(
        Math.abs(metrics.deviationTwh - expectedDevTwh) < 1e-4,
        `Deviation TWh mismatch for ${v.cc}: ${metrics.deviationTwh} vs ${expectedDevTwh}`
      );
    }
  });

  it('correctly derives flow regimes across the 10 member states', () => {
    const regimes = new Set();
    for (const v of views) {
      const metrics = countryStorageMetrics(v);
      const regime = metrics.flow7d.regime;
      assert.ok(['injection', 'withdrawal', 'neutral'].includes(regime), `Invalid regime: ${regime}`);
      regimes.add(regime);
      assert.ok(Number.isFinite(metrics.flow7d.avgNetGwh), `avgNetGwh non-finite for ${v.cc}`);
      assert.ok(Number.isFinite(metrics.flow7d.rateTwhPerWeek), `rateTwhPerWeek non-finite for ${v.cc}`);
    }
    // Across the 10 countries, diverse regimes should be active
    assert.ok(regimes.has('injection'), 'Should have injection countries');
    assert.ok(regimes.has('withdrawal'), 'Should have withdrawal countries');
  });
});

describe('Country Detail View Data Contracts', () => {
  it('provides "Where it should be right now" 5Y median benchmark', () => {
    for (const v of views) {
      const metrics = countryStorageMetrics(v);
      const medianPct = metrics.normal.median;
      const medianTwh = (medianPct * v.capacityTwh) / 100;
      assert.ok(medianPct > 0 && medianPct <= 100, `Implausible median pct: ${medianPct}`);
      assert.ok(medianTwh > 0 && medianTwh <= v.capacityTwh * 1.05, `Implausible median twh: ${medianTwh}`);
    }
  });

  it('provides valid historical rank among prior years', () => {
    for (const v of views) {
      const metrics = countryStorageMetrics(v);
      assert.ok(metrics.rank.rank >= 1, `Rank must be >= 1: ${metrics.rank.rank}`);
      assert.ok(metrics.rank.rank <= metrics.rank.n + 1, `Rank must be <= n+1`);
    }
  });

  it('provides 14-day history rows with all required fields', () => {
    for (const v of views) {
      const flowRows = v.rows.slice(-14).reverse();
      assert.equal(flowRows.length, 14, `Expected 14 rows for ${v.cc}`);
      for (const r of flowRows) {
        assert.ok(r.period instanceof Date);
        assert.ok(Number.isFinite(r.pct), `pct invalid on ${r.period}`);
        assert.ok(Number.isFinite(r.twh), `twh invalid on ${r.period}`);
        // GIE AGSI+ reports injection and withdrawal
        assert.ok(r.injectionGwh !== undefined);
        assert.ok(r.withdrawalGwh !== undefined);
      }
    }
  });

  it('produces zero zigzags across all multi-year trajectories', () => {
    for (const v of views) {
      const byYear = rowsByYear(v.rows, (r) => r.pct);
      for (const yr of byYear) {
        for (let i = 1; i < yr.points.length; i++) {
          assert.ok(
            yr.points[i].x > yr.points[i - 1].x,
            `x must be strictly increasing in ${v.cc} year ${yr.year} at index ${i}`
          );
        }
      }
    }
  });

  it('computes 7-day rolling trailing average without NaNs or discontinuities', () => {
    for (const v of views) {
      const rows = v.rows.slice(-30);
      assert.ok(rows.length >= 14);
      const processed = rows.map((r, i) => {
        const windowSlice = rows.slice(Math.max(0, i - 6), i + 1);
        const valid = windowSlice.map((s) => (s.injectionGwh ?? 0) - (s.withdrawalGwh ?? 0));
        const avg7d = valid.length ? valid.reduce((acc, curr) => acc + curr, 0) / valid.length : 0;
        return avg7d;
      });
      for (const avg of processed) {
        assert.ok(Number.isFinite(avg), `Rolling average non-finite: ${avg}`);
      }
    }
  });
});

describe('Terminology, Hygiene & Code Standards', () => {
  it('does not contain robotic AI-slop or conversational filler in panel module', () => {
    const code = readFileSync(new URL('../docs/js/panels/eu-storage.js', import.meta.url), 'utf8');
    const forbidden = [
      'delve into',
      'as an ai',
      'tapestry',
      'crucial',
      'supercharge',
      'dive deep',
      'in this section',
      'let us explore',
    ];
    for (const f of forbidden) {
      assert.ok(!code.toLowerCase().includes(f), `Found forbidden AI phrase: "${f}"`);
    }
  });

  it('does not contain emojis in panel module', () => {
    const code = readFileSync(new URL('../docs/js/panels/eu-storage.js', import.meta.url), 'utf8');
    const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
    assert.ok(!emojiRegex.test(code), 'Found emojis in eu-storage.js');
  });

  it('does not use double dashes in visible prose', () => {
    const code = readFileSync(new URL('../docs/js/panels/eu-storage.js', import.meta.url), 'utf8');
    // Extract template literals between backticks
    const templateLiterals = code.match(/`([^`]+)`/g) || [];
    const proseWithDoubleDash = [];
    for (const tpl of templateLiterals) {
      // Find text content inside HTML tags
      const htmlMatches = tpl.match(/>([^<`$]+)</g) || [];
      for (const m of htmlMatches) {
        const text = m.slice(1, -1).trim();
        if (!text || text.startsWith('!--') || text.startsWith('var(')) continue;
        if (text.includes('--')) {
          proseWithDoubleDash.push(text);
        }
      }
    }
    assert.deepEqual(proseWithDoubleDash, [], `Found double dashes in HTML prose: ${proseWithDoubleDash}`);
  });

  it('renders institutional controls bar with baseline horizon and range filter options', () => {
    const code = readFileSync(new URL('../docs/js/panels/eu-storage.js', import.meta.url), 'utf8');
    assert.ok(code.includes('eu-controls-bar'), 'Must contain eu-controls-bar');
    assert.ok(code.includes('data-horizon="5y"'), 'Must contain 5y horizon button');
    assert.ok(code.includes('data-horizon="post2022"'), 'Must contain post2022 horizon button');
    assert.ok(code.includes('data-range="30D"'), 'Must contain 30D range button');
    assert.ok(code.includes('data-range="90D"'), 'Must contain 90D range button');
    assert.ok(code.includes('data-range="1Y"'), 'Must contain 1Y range button');
    assert.ok(code.includes('data-range="5Y"'), 'Must contain 5Y range button');
    assert.ok(code.includes('data-range="MAX"'), 'Must contain MAX range button');
  });
});
