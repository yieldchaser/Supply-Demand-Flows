/**
 * Season / storage-primitive tests.
 *
 * These pin the rules the arbiter accepted:
 *  - the reference year comes from the series, never the wall clock;
 *  - the sample size is counted and reported, never asserted;
 *  - quantiles are interpolated, so a 4-year window cannot masquerade as p25;
 *  - a date-only period is a calendar date, not UTC midnight.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { referenceYear } from '../docs/js/util/series.js';
import {
  percentileRank,
  quantileSorted,
  seasonalEnvelope,
} from '../docs/js/util/season.js';
import { parsePeriod } from '../docs/js/util/period.js';

function weeklySeries(startYear, endYear, valueFn) {
  const rows = [];
  for (let y = startYear; y <= endYear; y += 1) {
    for (let wk = 0; wk < 52; wk += 1) {
      const period = new Date(y, 0, 3 + wk * 7); // Fridays from early Jan
      rows.push({
        series_id: 'storage_lower48',
        series_name: 'Weekly Lower 48 States Natural Gas Working Underground Storage - Lower 48',
        period: period.toISOString().slice(0, 10),
        value: valueFn(y, wk),
        unit: 'Bcf',
      });
    }
  }
  return rows;
}

describe('referenceYear', () => {
  it('takes the year of the latest observation, not the current year', () => {
    const rows = weeklySeries(2018, 2019, () => 1000);
    assert.equal(referenceYear(rows), 2019);
  });

  it('accepts Date rows too', () => {
    assert.equal(referenceYear([{ period: new Date(2021, 5, 1) }]), 2021);
  });

  it('returns null for an empty series', () => {
    assert.equal(referenceYear([]), null);
  });
});

describe('weekly envelope is season-keyed, not ISO-keyed', () => {
  it('no longer exports an ISO-week envelope', async () => {
    // The ISO-keyed band compared a Friday reading against a normal from the
    // neighbouring week of the season and switched under the reader between
    // years. `seasonalEnvelope` is the only envelope now; reintroducing an
    // ISO-keyed one would put the two back out of step.
    const series = await import('../docs/js/util/series.js');
    assert.equal(series.computeWeeklyEnvelope, undefined);
    assert.equal(series.isoWeek, undefined);
  });

  it('keeps the interpolated-quartile rule on the surviving envelope', () => {
    // prior values 1000,1100,1200,1300 -> p25 = 1075 (not the 2nd order stat)
    const rows = [];
    for (let y = 2015; y <= 2018; y += 1) {
      for (let wk = 0; wk < 52; wk += 1) {
        rows.push({ period: new Date(y, 0, 3 + wk * 7), value: 1000 + (y - 2015) * 100 });
      }
    }
    const env = seasonalEnvelope(rows, { anchor: new Date(2019, 0, 4) });
    const first = env.values().next().value;
    assert.equal(first.count, 4);
    assert.ok(Math.abs(first.p25 - 1075) < 1e-6, `p25 was ${first.p25}`);
    assert.ok(Math.abs(first.p75 - 1225) < 1e-6, `p75 was ${first.p75}`);
  });
});

describe('quantileSorted (type 7)', () => {
  it('interpolates the quartiles of four values', () => {
    assert.equal(quantileSorted([10, 20, 30, 40], 0.25), 17.5);
    assert.equal(quantileSorted([10, 20, 30, 40], 0.5), 25);
    assert.equal(quantileSorted([10, 20, 30, 40], 0.75), 32.5);
  });

  it('degenerates correctly at the edges', () => {
    assert.equal(quantileSorted([5], 0.25), 5);
    assert.equal(quantileSorted([1, 2], 0), 1);
    assert.equal(quantileSorted([1, 2], 1), 2);
  });

  it('returns null on an empty sample instead of NaN', () => {
    assert.equal(quantileSorted([], 0.5), null);
  });
});

describe('seasonalEnvelope', () => {
  const anchor = new Date(2026, 0, 2); // Friday 2026-01-02
  const rows = weeklySeries(2015, 2026, (y, wk) => 1000 + (y - 2015) * 100 + wk);

  it('reports which years contributed, so a label can never claim a window it lacks', () => {
    const env = seasonalEnvelope(rows, { anchor, lookbackYears: 5, excludeYear: 2026 });
    const wk = env.get(20);
    assert.ok(wk, 'week 20 must exist');
    assert.deepEqual(wk.years, [2021, 2022, 2023, 2024, 2025]);
    assert.equal(wk.count, 5);
  });

  it('never lets the reference year into its own envelope', () => {
    const env = seasonalEnvelope(rows, { anchor, lookbackYears: 5, excludeYear: 2026 });
    for (const [wkIndex, e] of env) {
      assert.ok(
        e.max < 1000 + (2026 - 2015) * 100 + wkIndex,
        `week ${wkIndex} leaked the reference year`,
      );
    }
  });

  it('honours lookbackYears', () => {
    const env = seasonalEnvelope(rows, { anchor, lookbackYears: 2, excludeYear: 2026 });
    assert.equal(env.get(20).count, 2);
  });

  it('is empty for an empty series', () => {
    assert.equal(seasonalEnvelope([], { anchor }).size, 0);
  });
});

describe('percentileRank', () => {
  it('reports the share of the population at or below the value', () => {
    const r = percentileRank(1200, [1000, 1100, 1200, 1300]);
    assert.equal(r.n, 4);
    assert.equal(r.pct, 75);
  });

  it('returns null rather than 0% for a value it cannot rank', () => {
    assert.equal(percentileRank(NaN, [1, 2]).pct, null);
  });
});

describe('parsePeriod', () => {
  it('a Friday label stays on the same calendar day in every timezone', () => {
    const p = parsePeriod('2026-09-11');
    assert.equal(p.getFullYear(), 2026);
    assert.equal(p.getMonth(), 8);
    assert.equal(p.getDate(), 11);
  });

  it('keeps a month boundary intact', () => {
    const p = parsePeriod('2026-09-01');
    assert.equal(p.getMonth(), 8);
    assert.equal(p.getDate(), 1);
  });

  it('handles YYYY-MM buckets as the first of the month', () => {
    const p = parsePeriod('2026-06');
    assert.equal(p.getMonth(), 5);
    assert.equal(p.getDate(), 1);
  });

  it('passes Date instances through by identity', () => {
    const src = new Date(2026, 8, 11);
    assert.equal(parsePeriod(src), src);
  });

  it('returns null for empty or unparseable input, never an Invalid Date', () => {
    assert.equal(parsePeriod(''), null);
    assert.equal(parsePeriod(null), null);
    assert.equal(parsePeriod(undefined), null);
    assert.equal(parsePeriod('not-a-date'), null);
  });
});
