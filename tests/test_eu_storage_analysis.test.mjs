/**
 * EU storage analysis engine: methodology pinned on synthetic fixtures with
 * known answers, plus invariants that must hold on the live bundle whatever
 * day it was built.
 *
 * The previous suite pinned "golden" figures (e.g. Germany capacity 248.7 TWh)
 * read off one bundle; every new AGSI day moved them and the build failed on
 * data, not on logic. Values that depend on the data vintage are now asserted
 * as identities or ranges; exact numbers are asserted only on fixtures.
 */

import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { describe, it } from 'node:test';

import {
  EU_COUNTRIES,
  AGGREGATE_CC,
  buildCountryViews,
  buildAggregate,
  buildAggregateView,
  attachDailyCapacity,
  capacityFromDay,
  positionalNormal,
  rankAmongYears,
  seasonalBand,
  storageYearOf,
  storageYearFraction,
  storageYearLabel,
  seasonOf,
  sameDayYearsBack,
  monthDayFraction,
  flowDynamics,
  flowPaceVsNormal,
  analogProjection,
  deviationAttribution,
  deviationSeries,
  countryStorageMetrics,
  indexRows,
  seasonToDate,
} from '../docs/js/util/eu-storage-analysis.js';
import { getSeries } from '../docs/js/util/series.js';

const DAY = 86400000;
const d = (y, m, day) => new Date(y, m - 1, day);

/** Daily rows from `from` to `to` with a generator for each field. */
function synth(from, to, gen) {
  const out = [];
  for (let t = from; t <= to; t = new Date(t.getFullYear(), t.getMonth(), t.getDate() + 1)) {
    out.push({ period: t, ...gen(t) });
  }
  return out;
}
/** A view built the way buildCountryViews builds one, from synthetic rows. */
function viewFrom(cc, rows) {
  const series = { pct: [], gas: [], injection: [], withdrawal: [] };
  for (const r of rows) {
    if (r.pct != null) series.pct.push({ period: r.period, value: r.pct });
    if (r.twh != null) series.gas.push({ period: r.period, value: r.twh });
    if (r.injectionGwh != null) series.injection.push({ period: r.period, value: r.injectionGwh });
    if (r.withdrawalGwh != null) series.withdrawal.push({ period: r.period, value: r.withdrawalGwh });
  }
  return buildCountryViews({ [cc]: series }, [{ cc, label: cc.toUpperCase() }])[0];
}

describe('Calendar: storage year axis', () => {
  it('assigns April–March storage years and labels them', () => {
    assert.equal(storageYearOf(d(2026, 3, 31)), 2025);
    assert.equal(storageYearOf(d(2026, 4, 1)), 2026);
    assert.equal(storageYearOf(d(2027, 1, 15)), 2026);
    assert.equal(storageYearLabel(2026), '2026/27');
    assert.equal(storageYearLabel(2099), '2099/00');
  });

  it('is strictly monotonic from 1 Apr to 31 Mar, in [0, 1)', () => {
    let prev = -1;
    for (let t = d(2023, 4, 1); t <= d(2024, 3, 31); t = new Date(t.getFullYear(), t.getMonth(), t.getDate() + 1)) {
      const x = storageYearFraction(t);
      assert.ok(x > prev && x >= 0 && x < 1, `${t.toDateString()} x=${x}`);
      prev = x;
    }
  });

  it('puts 1 March at the same x in leap and non-leap storage years', () => {
    assert.equal(storageYearFraction(d(2024, 3, 1)), storageYearFraction(d(2025, 3, 1)));
    assert.equal(monthDayFraction(d(2024, 3, 1)), monthDayFraction(d(2025, 3, 1)));
  });

  it('splits seasons at 1 Apr and 1 Oct', () => {
    assert.equal(seasonOf(d(2026, 9, 30)).kind, 'injection');
    assert.equal(seasonOf(d(2026, 10, 1)).kind, 'withdrawal');
    assert.equal(seasonOf(d(2027, 2, 1)).end.getTime(), d(2027, 3, 31).getTime());
    assert.equal(seasonOf(d(2026, 11, 18)).start.getTime(), d(2026, 10, 1).getTime());
  });

  it('maps 29 February to 28 February in a non-leap year', () => {
    assert.equal(sameDayYearsBack(d(2024, 2, 29), 1).getTime(), d(2023, 2, 28).getTime());
    assert.equal(sameDayYearsBack(d(2024, 2, 29), 4).getTime(), d(2020, 2, 29).getTime());
  });
});

describe('Capacity is read per day', () => {
  it('derives capacity from gas ÷ pct and refuses near-empty days', () => {
    assert.equal(capacityFromDay(50, 50, 1), 100);
    assert.equal(capacityFromDay(50, 0.5, 1), null);
    assert.equal(capacityFromDay(0, 50, 1), null);
  });

  it('follows a capacity step instead of averaging it away', () => {
    // Capacity 100 TWh in 2023, 120 TWh from 2024; the facility is always 50% full.
    const rows = synth(d(2023, 1, 1), d(2024, 12, 31), (t) => {
      const cap = t.getFullYear() < 2024 ? 100 : 120;
      return { pct: 50, twh: cap / 2 };
    });
    const v = viewFrom('xx', rows);
    assert.ok(Math.abs(v.capacityTwh - 120) < 1e-9, 'current capacity is the recent one');
    assert.ok(Math.abs(v.capacityFirstTwh - 100) < 1e-9, 'earliest capacity retained for the note');
    assert.ok(v.rows.every((r) => Math.abs(r.capTwh - (r.period.getFullYear() < 2024 ? 100 : 120)) < 1e-9));
  });

  it('borrows the nearest stable capacity on days too empty to divide', () => {
    const rows = attachDailyCapacity([
      { period: d(2024, 1, 1), pct: 2, twh: 2.4 },
      { period: d(2024, 1, 2), pct: 40, twh: 48 },
      { period: d(2024, 1, 3), pct: 1, twh: 1.1 },
    ]);
    assert.equal(rows[0].capTwh, 120);
    assert.equal(rows[0].capFilled, true);
    assert.equal(rows[2].capTwh, 120);
    assert.equal(rows[1].capFilled, false);
  });

  it('aggregates as Σ stock ÷ Σ that day’s capacity', () => {
    const a = viewFrom('aa', synth(d(2024, 1, 1), d(2024, 1, 10), () => ({ pct: 50, twh: 50 })));
    const b = viewFrom('bb', synth(d(2024, 1, 1), d(2024, 1, 10), (t) => ({ pct: t.getDate() <= 5 ? 100 : 25, twh: t.getDate() <= 5 ? 300 : 100 })));
    const agg = buildAggregate([a, b]);
    // Day 1: (50 + 300) / (100 + 300); day 10: (50 + 100) / (100 + 400)
    assert.ok(Math.abs(agg[0].pct - 87.5) < 1e-9);
    assert.ok(Math.abs(agg[9].pct - 30) < 1e-9);
    assert.equal(agg[0].nCountries, 2);
  });

  it('drops days on which a country is missing from the total view', () => {
    const a = viewFrom('aa', synth(d(2024, 1, 1), d(2024, 1, 10), () => ({ pct: 50, twh: 50 })));
    const b = viewFrom('bb', synth(d(2024, 1, 1), d(2024, 1, 8), () => ({ pct: 50, twh: 50 })));
    const total = buildAggregateView([a, b]);
    assert.equal(total.cc, AGGREGATE_CC);
    assert.equal(total.rows.length, 8);
    assert.equal(total.droppedPartialDays, 2);
  });
});

describe('Normals', () => {
  // Five prior years whose fill on a given day is 10·(year − 2019): 20, 30, 40, 50, 60.
  const rows = synth(d(2021, 1, 1), d(2026, 12, 31), (t) => ({ pct: 10 * (t.getFullYear() - 2019), twh: (10 * (t.getFullYear() - 2019)) * 2 }));
  const v = viewFrom('nn', rows);

  it('uses the same calendar day in each prior year and reports n and years', () => {
    const n = positionalNormal(v.index, d(2026, 11, 18), { pick: (r) => r.pct });
    assert.equal(n.n, 5);
    assert.deepEqual(n.years, [2025, 2024, 2023, 2022, 2021]);
    assert.equal(n.median, 40);
    assert.equal(n.mean, 40);
    assert.equal(n.min, 20);
    assert.equal(n.max, 60);
  });

  it('cuts the post-2022 baseline on 1 April 2022, not on the calendar year', () => {
    const minDate = d(2022, 4, 1);
    const jan = positionalNormal(v.index, d(2027, 1, 15), { pick: (r) => r.pct, minDate });
    assert.deepEqual(jan.years, [2026, 2025, 2024, 2023], 'Jan 2022 belongs to the 2021/22 winter and is excluded');
    const nov = positionalNormal(v.index, d(2026, 11, 18), { pick: (r) => r.pct, minDate });
    assert.deepEqual(nov.years, [2025, 2024, 2023, 2022]);
  });

  it('builds a daily band over a whole storage year', () => {
    const band = seasonalBand(v.index, 2026, { pick: (r) => r.pct });
    assert.equal(band.length, 365);
    assert.ok(band.every((b) => b.min <= b.median && b.median <= b.max));
    // Jan–Mar 2027 compares with Jan–Mar 2022..2026 (fill 30..70)
    const jan = band.find((b) => b.period.getMonth() === 0);
    assert.equal(jan.median, 50);
  });

  it('ranks among prior years (1 = lowest)', () => {
    assert.deepEqual(rankAmongYears(10, [20, 30, 40]), { rank: 1, n: 3 });
    assert.deepEqual(rankAmongYears(35, [20, 30, 40]), { rank: 3, n: 3 });
    assert.deepEqual(rankAmongYears(99, [20, 30, 40]), { rank: 4, n: 3 });
  });

  it('computes pp and TWh deviations on their own series', () => {
    const m = countryStorageMetrics(v, d(2026, 11, 18));
    assert.equal(m.deviationPp, 70 - 40);
    assert.equal(m.deviationTwh, 140 - 80);
    assert.equal(m.yoyPp, 10);
  });

  it('deviation series only starts once n ≥ minN', () => {
    const ds = deviationSeries(v, { minN: 3 });
    assert.ok(ds[0].period >= d(2023, 12, 31), 'one-day gap tolerance at most');
    assert.ok(ds.every((r) => r.n >= 3));
  });
});

describe('Flows', () => {
  it('scales the regime threshold with capacity', () => {
    const rows = synth(d(2024, 1, 1), d(2024, 1, 14), () => ({ injectionGwh: 60, withdrawalGwh: 0 }));
    assert.equal(flowDynamics(rows, { capacityTwh: 250 }).regime, 'neutral', '60 GWh/d is < 0.05%/d of 250 TWh');
    assert.equal(flowDynamics(rows, { capacityTwh: 8 }).regime, 'injection', '60 GWh/d is > 0.05%/d of 8 TWh');
  });

  it('keeps injection − withdrawal = net and the weekly TWh rate consistent', () => {
    const rows = synth(d(2024, 1, 1), d(2024, 1, 14), (t) => ({ injectionGwh: t.getDate(), withdrawalGwh: 100 }));
    const fd = flowDynamics(rows, { windowDays: 7 });
    assert.ok(Math.abs(fd.avgNetGwh - (fd.avgInjectionGwh - fd.avgWithdrawalGwh)) < 1e-9);
    assert.ok(Math.abs(fd.rateTwhPerWeek - (fd.avgNetGwh * 7) / 1000) < 1e-9);
  });

  it('compares the 7-day flow with the same window in prior years', () => {
    const rows = synth(d(2021, 1, 1), d(2026, 12, 31), (t) => ({
      pct: 50, twh: 50, injectionGwh: 0, withdrawalGwh: t.getFullYear() === 2026 ? 300 : 100 * (t.getFullYear() - 2020),
    }));
    const v = viewFrom('ff', rows);
    const p = flowPaceVsNormal(v, d(2026, 11, 18));
    assert.equal(p.current, -300);
    assert.equal(p.normal.n, 5);
    assert.equal(p.normal.median, -300);
    assert.equal(p.diffGwh, 0);
  });
});

describe('End-of-season analogs', () => {
  // Stock falls 1 TWh/day from 1 Oct in every year, so from 18 Nov to 31 Mar
  // each analog withdraws exactly the number of days in that interval.
  const rows = synth(d(2021, 1, 1), d(2026, 11, 18), (t) => {
    const sy = storageYearOf(t);
    const since = Math.round((t - new Date(sy, 9, 1)) / DAY);
    const twh = since >= 0 ? 400 - since : 400;
    return { pct: twh / 5, twh };
  });
  const v = viewFrom('pp', rows);

  it('applies each prior season’s remaining change to today’s stock', () => {
    const p = analogProjection(v, d(2026, 11, 18));
    assert.equal(p.endDate.getTime(), d(2027, 3, 31).getTime());
    assert.equal(p.analogs.length, 5);
    for (const a of p.analogs) {
      const daysThen = Math.round((new Date(a.sy + 1, 2, 31) - new Date(a.sy, 10, 18)) / DAY);
      assert.equal(a.deltaTwh, -daysThen);
      assert.equal(a.endTwh, p.startTwh - daysThen);
      assert.equal(a.clipped, false);
    }
    assert.ok(p.summary.minPct <= p.summary.medianPct && p.summary.medianPct <= p.summary.maxPct);
  });

  it('clips at empty and flags it', () => {
    const low = rows.map((r) => (r.period.getTime() === d(2026, 11, 18).getTime() ? { ...r, twh: 50, pct: 10 } : r));
    const p = analogProjection(viewFrom('lo', low), d(2026, 11, 18));
    assert.ok(p.analogs.every((a) => a.clipped && a.endTwh === 0));
  });

  it('season-to-date compares the change since 1 Oct with prior years', () => {
    const s = seasonToDate(v, d(2026, 11, 18));
    assert.equal(s.currentTwh, -48);
    assert.equal(s.normalMedianTwh, -48);
  });
});

describe('Attribution adds up', () => {
  it('country deviations from the same-day mean sum to the total’s', () => {
    const mk = (cc, k) => viewFrom(cc, synth(d(2021, 1, 1), d(2026, 11, 18), (t) => {
      const twh = k * (t.getFullYear() - 2015) + (t.getDate() % 7);
      return { pct: twh / (k * 0.2), twh };
    }));
    const views = [mk('aa', 3), mk('bb', 7), mk('cc', 11)];
    const total = buildAggregateView(views);
    const att = deviationAttribution(views, total, d(2026, 11, 18));
    assert.equal(att.rows.length, 3);
    assert.ok(Math.abs(att.residualTwh) < 1e-9, `residual ${att.residualTwh}`);
    assert.ok(Math.abs(att.rows.reduce((s, r) => s + r.sharePct, 0) - 100) < 1e-9);
  });
});

/* ------------------------------------------------------------------ */
/*  Invariants on the live bundle (skipped when it has not been built) */
/* ------------------------------------------------------------------ */

const bundleUrl = new URL('../docs/data/bundle.json', import.meta.url);
const haveBundle = existsSync(bundleUrl);

describe('Live bundle invariants', { skip: !haveBundle && 'bundle not built' }, () => {
  const bundle = haveBundle ? JSON.parse(readFileSync(bundleUrl, 'utf8')) : null;
  const source = bundle?.sources?.gie_agsi;
  const per = {};
  for (const c of EU_COUNTRIES) {
    per[c.cc] = {
      pct: getSeries(source?.data, `gie_storage_${c.cc}_full_pct`),
      gas: getSeries(source?.data, `gie_storage_${c.cc}_gas_in_storage`),
      injection: getSeries(source?.data, `gie_storage_${c.cc}_injection`),
      withdrawal: getSeries(source?.data, `gie_storage_${c.cc}_withdrawal`),
    };
  }
  const views = haveBundle ? buildCountryViews(per) : [];
  const total = haveBundle ? buildAggregateView(views) : null;

  it('covers the ten member states with plausible capacities', () => {
    assert.deepEqual(views.map((v) => v.cc).sort(), ['at', 'be', 'cz', 'de', 'es', 'fr', 'hu', 'it', 'nl', 'pl']);
    const sum = views.reduce((s, v) => s + v.capacityTwh, 0);
    assert.ok(sum > 900 && sum < 1150, `total capacity ${sum}`);
    assert.ok(Math.abs(total.capacityTwh - sum) / sum < 0.01, 'total view capacity = Σ countries');
  });

  it('total % is exactly Σ stock ÷ Σ capacity on every day', () => {
    for (const r of total.rows) {
      assert.ok(Math.abs(r.pct - (r.twh / r.capTwh) * 100) < 1e-9);
      assert.equal(r.nCountries, views.length);
    }
  });

  it('per-day capacity reproduces the reported % full', () => {
    for (const v of views) {
      for (const r of v.rows.slice(-120)) {
        if (r.capFilled || !Number.isFinite(r.pct)) continue;
        assert.ok(Math.abs((r.twh / r.capTwh) * 100 - r.pct) < 1e-6, `${v.cc} ${r.period.toDateString()}`);
      }
    }
  });

  it('attribution residual vs the total is ~0', () => {
    const att = deviationAttribution(views, total, total.latest.period);
    assert.ok(Math.abs(att.residualTwh) < 1e-6, `residual ${att.residualTwh}`);
  });

  it('metrics are complete and internally consistent for every entity', () => {
    for (const v of [total, ...views]) {
      const m = countryStorageMetrics(v);
      assert.ok(m, v.cc);
      assert.ok(m.normal.n >= 1 && m.normal.n <= 5);
      assert.ok(m.rank.rank >= 1 && m.rank.rank <= m.rank.n + 1);
      assert.equal(m.deviationPp, m.latest.pct - m.normal.median);
      assert.equal(m.deviationTwh, m.latest.twh - m.normalTwh.median);
      assert.ok(['injection', 'withdrawal', 'neutral'].includes(m.flow7d.regime));
      if (m.projection) {
        for (const a of m.projection.analogs) assert.ok(a.endPct >= 0 && a.endPct <= 100.0001);
      }
    }
  });
});
