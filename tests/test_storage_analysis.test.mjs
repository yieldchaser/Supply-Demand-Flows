/**
 * Pins the storage analysis engine against the committed bundle.
 *
 * The point of these tests is not that the functions run; it is that the exact
 * figures the panel prints are reproducible and defensible. Each assertion is a
 * number a reader could check against EIA's own weekly report, or an identity
 * EIA's regional split guarantees. If a methodology change moves one of these,
 * the test fails and the change has to be argued for.
 *
 * Regenerate the bundle first (the parquet → JSON export) if these fail after a
 * data refresh: the *numbers* are expected to move, the *relationships* are not.
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import {
  analyseStorage,
  assertStorageInvariants,
  buildStorageSeries,
  flowPace,
  priorSameWeekValues,
  projectLevel,
  seasonProgress,
  windowChange,
} from '../docs/js/util/storage-analysis.js';

const bundle = JSON.parse(readFileSync(new URL('../docs/data/bundle.json', import.meta.url), 'utf8'));
const rows = bundle.sources.eia_storage.data;
const analysis = analyseStorage(rows);

function d(y, m, day) {
  return new Date(y, m - 1, day);
}

describe('buildStorageSeries', () => {
  it('one record per week, ascending, no dropped or duplicated rows', () => {
    assert.equal(analysis.dropped, 0);
    assert.equal(analysis.duplicateKeys, 0);
    const keys = analysis.periods.map((p) => p.key);
    assert.equal(new Set(keys).size, keys.length);
    assert.deepEqual([...keys].sort(), keys);
  });

  it('every week-ending date is a Friday', () => {
    for (const p of analysis.periods) assert.equal(p.period.getDay(), 5, `not a Friday: ${p.key}`);
  });
});

describe('EIA additive identities', () => {
  it('salt + non-salt equals South Central in every week', () => {
    // Not approximate: salt and non-salt are printed as the two halves of a
    // published total, so any residual at all is a pipeline defect.
    assert.equal(analysis.invariants.saltPlusNonsalt.length, 0);
  });

  it('five regions sum to Lower 48 within EIA’s own rounding of each region', () => {
    // Five independently rounded integers can sum 2–3 Bcf off the separately
    // rounded total. Anything above the tolerance is a real break, not rounding.
    assert.ok(analysis.invariants.checked > 400);
    assert.ok(
      analysis.invariants.regionsSumL48.every((v) => Math.abs(v.residual) <= 3),
      'residual exceeded EIA rounding',
    );
  });

  it('validation tolerance is stated, not implied', () => {
    assert.equal(analysis.invariants.toleranceBcf, 1);
  });
});

describe('environment vs normal', () => {
  it('reports the sample size of the normal and the years behind it', () => {
    const { normal } = analysis.environment;
    assert.ok(normal.n >= 3);
    assert.equal(normal.years.length, normal.n);
    assert.ok(normal.years.every((y) => y < analysis.latest.period.getFullYear()));
  });

  it('ranking population matches the normal’s population', () => {
    // Both describe "the same week in prior years". If they disagree, one of
    // them is matching on a different axis (this caught a 53-week-year bug).
    assert.equal(analysis.environment.rankN, analysis.environment.normal.n);
  });

  it('pct-of-max is against the nominal convention, and says so', () => {
    assert.ok(analysis.environment.pctOfMax > 0 && analysis.environment.pctOfMax < 100);
    assert.equal(analysis.environment.pctOfMax, (analysis.environment.valueBcf / 4000) * 100);
  });
});

describe('pace', () => {
  it('is a regression slope in Bcf/d with a standard error', () => {
    const p = analysis.pace;
    assert.ok(Number.isFinite(p.bePerDay));
    assert.ok(p.seBePerDay > 0);
    assert.ok(p.r2 > 0 && p.r2 <= 1);
    assert.ok(p.n >= 5);
  });

  it('this-week rate is the raw difference over the actual day gap', () => {
    const tail = analysis.l48.slice(-2);
    const days = Math.round((tail[1].period - tail[0].period) / 86400000);
    assert.equal(days, 7);
    assert.equal(
      analysis.pace.thisWeekBePerDay,
      (tail[1].value - tail[0].value) / days,
    );
  });

  it('refuses to report a slope from fewer than two points', () => {
    assert.equal(flowPace([{ period: d(2026, 1, 2), value: 3000 }]), null);
  });
});

describe('projection', () => {
  it('the empirical band is a realised seasonal change, one per year', () => {
    const p = analysis.projection;
    assert.equal(p.empiricalN, p.empiricalYears.length);
    assert.ok(p.empiricalN >= 3);
    // A band that contains every possible outcome is not information. Realised
    // 8-week changes this late in the injection season are a few hundred Bcf.
    const spread = p.residualQ90 - p.residualQ10;
    assert.ok(spread < 900, `band too wide to be informative: ${spread}`);
    assert.ok(spread > 20, `band implausibly narrow: ${spread}`);
  });

  it('the horizon is the same regression used for the pace', () => {
    const p = analysis.projection;
    assert.equal(p.slopeBePerDay, analysis.pace.bePerDay);
    const days = Math.round((p.targetDate - p.asOf) / 86400000);
    assert.equal(days, p.horizonWeeks * 7);
  });

  it('is anchored on the fitted level, and discloses the last print', () => {
    const p = analysis.projection;
    assert.ok(Math.abs(p.fittedNowBcf - p.lastPrintBcf) < 30);
  });

  it('returns null rather than extrapolating from nothing', () => {
    assert.equal(projectLevel(analysis.l48.slice(0, 2)), null);
  });
});

describe('season position', () => {
  it('April–October is injection, November–March is withdrawal', () => {
    assert.equal(seasonProgress(d(2026, 4, 1)).season, 'injection');
    assert.equal(seasonProgress(d(2026, 10, 30)).season, 'injection');
    assert.equal(seasonProgress(d(2026, 11, 6)).season, 'withdrawal');
    assert.equal(seasonProgress(d(2027, 2, 5)).season, 'withdrawal');
    assert.equal(seasonProgress(d(2026, 3, 20)).season, 'withdrawal');
  });

  it('progress is monotone within a season and bounded', () => {
    const a = seasonProgress(d(2026, 5, 1));
    const b = seasonProgress(d(2026, 8, 1));
    assert.ok(b.progressPct > a.progressPct);
    assert.ok(a.progressPct >= 0 && b.progressPct <= 100);
    assert.ok(a.weeksTotal >= 30 && a.weeksTotal <= 32);
  });
});

describe('windowChange', () => {
  it('uses the real day gap, not a 7-day assumption', () => {
    const rows4 = analysis.l48.slice(-5);
    const w = windowChange(rows4, 4);
    assert.equal(w.deltaBcf, rows4[4].value - rows4[0].value);
    assert.equal(w.days, 28);
    assert.equal(w.rate, w.deltaBcf / 28);
  });

  it('returns null when there is nothing to compare', () => {
    assert.equal(windowChange(analysis.l48.slice(-1), 1), null);
  });
});

describe('prior years', () => {
  it('matches calendar-adjacent Fridays, one per year, inside the window', () => {
    const ref = d(2026, 9, 11);
    const pop = priorSameWeekValues(analysis.l48, ref, { lookbackYears: 5 });
    assert.ok(pop.length <= 5);
    assert.ok(pop.length >= 4);
    assert.equal(new Set(pop.map((p) => p.year)).size, pop.length);
    for (const p of pop) {
      assert.equal(p.period.getDay(), 5);
      assert.ok(p.year >= 2021 && p.year < 2026);
    }
  });
});

describe('region table', () => {
  it('each region is internally consistent with its own week change', () => {
    for (const r of analysis.regions) {
      assert.ok(Number.isFinite(r.levelBcf));
      assert.ok(Math.abs(r.wowRate * r.wowDays - r.wowBcf) < 1e-6);
    }
  });

  it('region shares of Lower 48 sum to about 100%', () => {
    const shares = analysis.regions.filter((r) => !r.key.startsWith('sc_'));
    const total = shares.reduce((s, r) => s + r.shareOfL48Pct, 0);
    assert.ok(Math.abs(total - 100) < 0.5, `shares sum to ${total}`);
  });

  it('salt and non-salt together reproduce the South Central level', () => {
    const byKey = Object.fromEntries(analysis.regions.map((r) => [r.key, r]));
    assert.equal(byKey.sc_salt.levelBcf + byKey.sc_nonsalt.levelBcf, byKey.south_central.levelBcf);
  });
});

describe('golden figures (bundle 2026-09-11)', () => {
  // These are the numbers the panel shows. They are asserted so a refactor
  // cannot silently change what the dashboard reports.
  const isFresh = analysis.latest?.key === '2026-09-11';
  it('latest Lower 48 level', () => {
    if (!isFresh) return;
    assert.equal(analysis.latest.value, 3298);
  });
  it('vs 5-year mean and median', () => {
    if (!isFresh) return;
    assert.equal(analysis.environment.normal.mean, 3203.4);
    assert.equal(analysis.environment.normal.median, 3269);
    assert.ok(Math.abs(analysis.environment.vsNormalBcf - 94.6) < 0.01);
    assert.ok(Math.abs(analysis.environment.vsMedianBcf - 29) < 1e-9);
  });
  it('pace and projection', () => {
    if (!isFresh) return;
    assert.ok(Math.abs(analysis.pace.bePerDay - 4.685714) < 1e-5);
    assert.ok(Math.abs(analysis.projection.modelBcf - 3551.8) < 0.01);
    assert.equal(analysis.streak.weeks, 25);
  });
});

describe('assertStorageInvariants on synthetic breakage', () => {
  it('catches a salt/non-salt split that does not close', () => {
    const bad = buildStorageSeries([
      { series_id: 'storage_south_central', period: '2026-01-02', value: 1000 },
      { series_id: 'storage_sc_salt', period: '2026-01-02', value: 200 },
      { series_id: 'storage_sc_nonsalt', period: '2026-01-02', value: 700 },
    ]);
    const res = assertStorageInvariants(bad.periods);
    assert.equal(res.saltPlusNonsalt.length, 1);
    assert.equal(res.saltPlusNonsalt[0].residual, 100);
  });
});
