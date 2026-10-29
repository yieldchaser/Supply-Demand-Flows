/**
 * Pins the analytical primitives the arbiter ruled in.
 *
 * Each test here corresponds to a defect found in the debate in
 * `scratch/debate/`. They are written against the *behaviour* a gas desk would
 * assume, not against the current implementation, so they fail on the old code.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { parsePeriod } from '../docs/js/util/period.js';
import { quantile, summarize } from '../docs/js/util/stats.js';

function d(y, m, day) {
  return new Date(y, m - 1, day);
}

describe('parsePeriod', () => {
  it('reads a date-only period as a calendar date in every timezone', () => {
    // new Date('2026-09-11') is UTC midnight; west of Greenwich getMonth() is
    // then the PREVIOUS day, which shifts month buckets and week-of-year.
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

  it('handles month buckets (YYYY-MM) as the first of the month', () => {
    const p = parsePeriod('2026-06');
    assert.equal(p.getFullYear(), 2026);
    assert.equal(p.getMonth(), 5);
    assert.equal(p.getDate(), 1);
  });

  it('passes Date instances through untouched (identity)', () => {
    const src = d(2026, 9, 11);
    assert.equal(parsePeriod(src), src);
  });

  it('accepts a full ISO timestamp as the instant it denotes', () => {
    // A real timestamp is an instant, not a label: it must not be reinterpreted
    // in local time. (Date-only strings are the ones that need local parsing.)
    const p = parsePeriod('2026-09-11T23:30:00Z');
    assert.equal(p.toISOString(), '2026-09-11T23:30:00.000Z');
  });

  it('returns null for empty / unparseable input rather than an Invalid Date', () => {
    assert.equal(parsePeriod(''), null);
    assert.equal(parsePeriod(null), null);
    assert.equal(parsePeriod(undefined), null);
    assert.equal(parsePeriod('not-a-date'), null);
  });
});

describe('quantile', () => {
  it('interpolates (type-7) instead of taking an order statistic', () => {
    // The old code returned sorted[floor(n*0.25)] = sorted[1] = 20 and called it
    // p25. The population p25 of 10,20,30,40 is 17.5.
    const vals = [10, 20, 30, 40];
    assert.equal(quantile(vals, 0.25), 17.5);
    assert.equal(quantile(vals, 0.5), 25);
    assert.equal(quantile(vals, 0.75), 32.5);
  });

  it('uses the whole sample, sorted or not', () => {
    assert.equal(quantile([40, 10, 30, 20], 0.5), 25);
  });

  it('degenerates correctly at the edges', () => {
    assert.equal(quantile([5], 0.25), 5);
    assert.equal(quantile([1, 2], 0), 1);
    assert.equal(quantile([1, 2], 1), 2);
  });

  it('returns null on an empty sample instead of NaN', () => {
    assert.equal(quantile([], 0.5), null);
  });
});

describe('summarize', () => {
  it('always reports n, so a label can never claim a sample it does not have', () => {
    const s = summarize([1, 2, 3]);
    assert.equal(s.n, 3);
    assert.equal(s.mean, 2);
    assert.equal(s.median, 2);
  });

  it('suppresses quartiles below n=3 but keeps min/max', () => {
    const s = summarize([10, 40]);
    assert.equal(s.n, 2);
    assert.equal(s.p25, null);
    assert.equal(s.p75, null);
    assert.equal(s.min, 10);
    assert.equal(s.max, 40);
  });

  it('ignores non-finite values instead of poisoning the mean', () => {
    const s = summarize([1, NaN, 3, Infinity, null]);
    assert.equal(s.n, 2);
    assert.equal(s.mean, 2);
  });

  it('returns a null summary for an empty sample', () => {
    assert.equal(summarize([]), null);
  });
});
