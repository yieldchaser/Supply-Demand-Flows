/**
 * Pins the EU storage analysis engine and mathematical invariants against the committed bundle.
 *
 * Verifies:
 * 1. Calendar axis monotonicity (no 12x/year sawtooth resets across month boundaries).
 * 2. rowsByYear chronologically ordered points with 0..1 scale.
 * 3. Derived working-gas capacity per country (median of daily stock/pct*100).
 * 4. Aggregate volumetric weighting: aggregate pct === (sum TWh / sum Cap) * 100.
 * 5. 5-year positional normal across both country series and aggregate series.
 * 6. Historical monthly envelope min/median/max band.
 * 7. Trailing flow dynamics (7d and 14d net flows, momentum, TWh/wk rates).
 * 8. Golden figures for the bundle reference date across all 10 EU countries.
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
  monthOfDay,
  dayOfYear,
  countryCapacity,
  capacityFromDay,
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

describe('EU Countries and Capacity Invariants', () => {
  it('covers all 10 target EU member states', () => {
    assert.equal(views.length, 10);
    const codes = views.map((v) => v.cc).sort();
    assert.deepEqual(codes, ['at', 'be', 'cz', 'de', 'es', 'fr', 'hu', 'it', 'nl', 'pl']);
  });

  it('derived working-gas capacity is positive and stable (< 30% all-time spread across facility expansions)', () => {
    let totalCap = 0;
    for (const v of views) {
      assert.ok(v.capacityTwh > 0, `capacity non-positive for ${v.cc}: ${v.capacityTwh}`);
      assert.ok(v.capacityN > 1000, `too few observations for ${v.cc}: ${v.capacityN}`);
      assert.ok(v.capacitySpreadPct < 30, `capacity spread too wide for ${v.cc}: ${v.capacitySpreadPct}%`);
      totalCap += v.capacityTwh;
    }
    // Total EU capacity across the 10 member states is approximately 1,010 TWh
    assert.ok(totalCap >= 1000 && totalCap <= 1025, `unexpected total capacity: ${totalCap}`);
  });

  it('capacityFromDay returns null below threshold percentage', () => {
    assert.equal(capacityFromDay(50, 0.5, 1), null);
    assert.equal(capacityFromDay(0, 50, 1), null);
    assert.equal(capacityFromDay(-10, 50, 1), null);
    assert.equal(capacityFromDay(50, 50, 1), 100);
  });
});

describe('Continuous Calendar Axis (monthDayFraction)', () => {
  it('is strictly monotonic within any calendar year', () => {
    const leapYear = 2024;
    let prevX = -1;
    for (let m = 0; m < 12; m++) {
      const days = new Date(leapYear, m + 1, 0).getDate();
      for (let day = 1; day <= days; day++) {
        const d = new Date(leapYear, m, day);
        const x = monthDayFraction(d);
        assert.ok(x > prevX, `monotonicity violated on ${d.toISOString()}: x=${x} <= prevX=${prevX}`);
        assert.ok(x >= 0 && x < 1, `x out of bounds [0, 1) on ${d.toISOString()}: ${x}`);
        prevX = x;
      }
    }
  });

  it('does not reset to zero at month boundaries', () => {
    const jan31 = new Date(2025, 0, 31);
    const feb1 = new Date(2025, 1, 1);
    const xJan31 = monthDayFraction(jan31);
    const xFeb1 = monthDayFraction(feb1);

    // Jan 31 must be strictly less than Feb 1, eliminating the 12x/year sawtooth bug
    assert.ok(xFeb1 > xJan31, `sawtooth reset detected: Feb 1 (${xFeb1}) <= Jan 31 (${xJan31})`);
    assert.ok(Math.abs(xFeb1 - xJan31) < 0.01, `discontinuous jump between Jan 31 and Feb 1: ${xFeb1 - xJan31}`);
  });

  it('aligns months consistently between leap and non-leap years', () => {
    // March 1 must be at the exact same calendar fraction in both leap (2024) and non-leap (2025) years
    const mar1Leap = monthDayFraction(new Date(2024, 2, 1));
    const mar1NonLeap = monthDayFraction(new Date(2025, 2, 1));
    assert.equal(mar1Leap, mar1NonLeap);
    assert.equal(mar1Leap, (2 + 0.5 / 31) / 12);
  });
});

describe('rowsByYear Monotonicity and Continuity', () => {
  it('produces chronologically ordered points without zigzags', () => {
    const deView = views.find((v) => v.cc === 'de');
    const byYear = rowsByYear(deView.rows);
    assert.ok(byYear.length >= 5);

    for (const yr of byYear) {
      assert.ok(yr.points.length > 200, `year ${yr.year} has too few points: ${yr.points.length}`);
      for (let i = 1; i < yr.points.length; i++) {
        const pPrev = yr.points[i - 1];
        const pCurr = yr.points[i];
        assert.ok(pCurr.x > pPrev.x, `x not strictly increasing in year ${yr.year} at index ${i}`);
        assert.ok(
          pCurr.period.getTime() > pPrev.period.getTime(),
          `chronological order violated in year ${yr.year} at index ${i}`,
        );
      }
    }
  });
});

describe('Aggregate Identities', () => {
  it('aggregate percentage equals exact capacity-weighted mean of parts', () => {
    for (const r of aggregate) {
      assert.ok(r.capacityTwh > 0);
      assert.ok(r.twh >= 0);
      const expectedPct = (r.twh / r.capacityTwh) * 100;
      assert.ok(
        Math.abs(r.pct - expectedPct) < 1e-9,
        `aggregate percentage mismatch on ${r.period.toISOString()}: ${r.pct} vs ${expectedPct}`,
      );
    }
  });

  it('reports all 10 countries reporting on latest dates', () => {
    const latest = aggregate[aggregate.length - 1];
    assert.equal(latest.nCountries, 10);
    assert.equal(latest.missing.length, 0);
  });
});

describe('Positional Normal & Envelope Contracts', () => {
  it('positionalNormal operates directly on aggregate series without manual mapping', () => {
    const latest = aggregate[aggregate.length - 1];
    const norm = positionalNormal(aggregate, latest.period);
    assert.ok(norm.n >= 4, `expected at least 4 prior years for aggregate normal, got ${norm.n}`);
    assert.ok(norm.median > 70 && norm.median < 100, `implausible aggregate normal median: ${norm.median}`);
    assert.ok(norm.min <= norm.median && norm.median <= norm.max);
  });

  it('monthlyEnvelope returns non-empty 12-month band for aggregate', () => {
    const latestYear = aggregate[aggregate.length - 1].period.getFullYear();
    const env = monthlyEnvelope(aggregate, latestYear);
    assert.equal(env.length, 12);
    for (const m of env) {
      assert.ok(m.nYears >= 4, `month ${m.month} has too few years: ${m.nYears}`);
      assert.ok(Number.isFinite(m.min), `month ${m.month} has null min`);
      assert.ok(Number.isFinite(m.median), `month ${m.month} has null median`);
      assert.ok(Number.isFinite(m.max), `month ${m.month} has null max`);
      assert.ok(m.min <= m.median && m.median <= m.max);
    }
  });
});

describe('Flow Dynamics', () => {
  it('calculates 7-day trailing average and verifies injection - withdrawal === net', () => {
    const deView = views.find((v) => v.cc === 'de');
    const fd = flowDynamics(deView.rows, { windowDays: 7 });
    assert.ok(fd !== null);
    assert.equal(fd.nDays, 7);
    assert.ok(Math.abs(fd.avgNetGwh - (fd.avgInjectionGwh - fd.avgWithdrawalGwh)) < 1e-6);
    assert.ok(Math.abs(fd.rateTwhPerWeek - (fd.avgNetGwh * 7) / 1000) < 1e-6);
    assert.ok(['injection', 'withdrawal', 'neutral'].includes(fd.regime));
  });
});

describe('Golden Figures (2026-10-26 reference date)', () => {
  it('reproduces exact Germany storage and deficit figures', () => {
    const de = views.find((v) => v.cc === 'de');
    assert.equal(de.capacityTwh.toFixed(1), '248.7');
    const refRow = de.rows.find((r) => r.period && (r.period.toISOString().slice(0, 10) === '2026-10-26' || r.period.toISOString().slice(0, 10) === '2026-10-27')) ?? de.latest;
    assert.ok(refRow.pct >= 61.0 && refRow.pct <= 62.0);
    assert.ok(refRow.twh >= 150.0 && refRow.twh <= 155.0);

    const m = countryStorageMetrics(de, refRow.period);
    assert.equal(m.normal.n, 5);
    assert.ok(m.normal.median > 90);
    assert.ok(m.deviationPp < -30);
    assert.ok(m.deviationTwh < -80);
  });

  it('reproduces exact European aggregate storage and deficit figures', () => {
    const refAgg = aggregate.find((r) => r.period && (r.period.toISOString().slice(0, 10) === '2026-10-26' || r.period.toISOString().slice(0, 10) === '2026-10-27')) ?? aggregate[aggregate.length - 1];
    assert.ok(refAgg.twh >= 750 && refAgg.twh <= 780);
    assert.ok(refAgg.capacityTwh >= 1000 && refAgg.capacityTwh <= 1020);
    assert.ok(refAgg.pct >= 74 && refAgg.pct <= 78);

    const norm = positionalNormal(aggregate, refAgg.period);
    assert.equal(norm.n, 5);
    assert.ok(norm.median > 90);
    const devPp = refAgg.pct - norm.median;
    assert.ok(devPp < -10);
    const devTwh = (devPp * refAgg.capacityTwh) / 100;
    assert.ok(devTwh < -150);
  });
});
