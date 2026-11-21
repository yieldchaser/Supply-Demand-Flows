/**
 * EU supply/demand analysis: route mix, Russian-origin classification, the US
 * LNG link and demand cover — on fixtures with known answers, plus invariants
 * on the live bundle.
 */

import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { describe, it } from 'node:test';

import {
  ROUTES,
  TWH_PER_BCF,
  isRussianOn,
  buildRouteIndex,
  supplyMix,
  rollingRouteSeries,
  monthlyEnergy,
  usLngToEu,
  transatlanticLng,
  consumptionMap,
  sumConsumption,
  winterDemandRate,
  annualDemand,
  demandCover,
} from '../docs/js/util/eu-supply-analysis.js';
import { getSeries } from '../docs/js/util/series.js';

const d = (y, m, day) => new Date(y, m - 1, day);
function days(from, to, f) {
  const out = [];
  for (let t = from; t <= to; t = new Date(t.getFullYear(), t.getMonth(), t.getDate() + 1)) out.push({ period: t, value: f(t) });
  return out;
}

describe('Route mix', () => {
  const two = [{ id: 'norway', label: 'N' }, { id: 'russia_nordstream', label: 'R' }];
  const series = {
    norway: days(d(2021, 1, 1), d(2026, 11, 19), () => 100),
    russia_nordstream: days(d(2021, 1, 1), d(2026, 11, 19), (t) => (t < d(2022, 9, 1) ? 300 : 0)),
  };
  const idx = buildRouteIndex((id) => series[id], two);

  it('shares sum to 100 and compare with 2021', () => {
    const mix = supplyMix(idx, d(2026, 11, 19), { routes: two });
    assert.equal(mix.total, 100);
    assert.equal(mix.rows.find((r) => r.id === 'norway').share, 100);
    assert.equal(mix.rows.find((r) => r.id === 'russia_nordstream').share2021, 75);
    assert.equal(mix.russianShare, 0);
    assert.equal(mix.russianShare2021, 75);
  });

  it('only counts days every route reported', () => {
    const gap = { ...series, norway: series.norway.filter((r) => r.period.getTime() !== d(2026, 11, 18).getTime()) };
    const i2 = buildRouteIndex((id) => gap[id], two);
    assert.equal(i2.days.length, idx.days.length - 1);
  });

  it('rolling series stacks to the total', () => {
    const rows = rollingRouteSeries(idx, { routes: two, from: d(2026, 11, 1) });
    for (const r of rows) assert.equal(r.total, r.norway + r.russia_nordstream);
  });
});

describe('Russian-origin classification is date-aware', () => {
  it('counts the Ukraine border as Russian transit only before 2025', () => {
    assert.equal(isRussianOn('ukraine', d(2024, 12, 31)), true);
    assert.equal(isRussianOn('ukraine', d(2025, 1, 1)), false);
    assert.equal(isRussianOn('russia_turkstream', d(2026, 1, 1)), true);
    assert.equal(isRussianOn('lng', d(2021, 1, 1)), false);
  });
});

describe('US LNG link', () => {
  it('sums EU-27 destinations only and converts Bcf to TWh', () => {
    const rows = {
      lng_export_deu: [{ period: d(2026, 1, 1), value: 10 }],
      lng_export_gbr: [{ period: d(2026, 1, 1), value: 50 }],
      lng_export_nld: [{ period: d(2026, 1, 1), value: 5 }],
      lng_export_region_europe: [{ period: d(2026, 1, 1), value: 65 }],
    };
    const us = usLngToEu((id) => rows[id], Object.keys(rows));
    assert.equal(us.get('2026-01').bcf, 15);
    assert.ok(Math.abs(us.get('2026-01').twh - 15 * TWH_PER_BCF) < 1e-12);
    assert.ok(Math.abs(TWH_PER_BCF - 0.3039) < 0.001);
  });

  it('uses complete send-out months only and a 12-month ratio', () => {
    const us = new Map();
    for (let m = 1; m <= 12; m++) us.set(`2025-${String(m).padStart(2, '0')}`, { twh: 30, bcf: 30 / TWH_PER_BCF });
    const euDaily = days(d(2025, 1, 1), d(2025, 12, 20), () => 2000); // 2 TWh/d, December partial
    const link = transatlanticLng(us, monthlyEnergy(euDaily));
    assert.equal(link.length, 11, 'partial December is not a month');
    assert.ok(Math.abs(link[0].share - (30 / 62) * 100) < 1e-9);
  });
});

describe('Demand cover', () => {
  const cons = new Map();
  for (let y = 2019; y <= 2026; y++) {
    for (let m = 1; m <= 12; m++) {
      if (y === 2026 && m > 10) break;
      const winter = m === 12 || m <= 2;
      const dim = new Date(y, m, 0).getDate();
      cons.set(`${y}-${String(m).padStart(2, '0')}`, { twh: (winter ? 10 : 4) * dim, flag: '' });
    }
  }

  it('peak-winter rate is the Dec-Feb daily mean of recent winters', () => {
    const w = winterDemandRate(cons, d(2026, 11, 19));
    assert.equal(w.twhPerDay, 10);
    assert.deepEqual(w.winters, ['2025/26', '2024/25', '2023/24']);
  });

  it('days of cover and share of annual use', () => {
    const c = demandCover(500, cons, d(2026, 11, 19));
    assert.equal(c.winterDays, 50);
    const annual = annualDemand(cons);
    assert.equal(annual.to, '2026-10');
    assert.ok(Math.abs(c.annualSharePct - (500 / annual.twh) * 100) < 1e-9);
  });

  it('a sum over countries keeps only months all report', () => {
    const a = new Map([['2026-01', { twh: 1, flag: '' }], ['2026-02', { twh: 1, flag: '' }]]);
    const b = new Map([['2026-01', { twh: 2, flag: 'p' }]]);
    const s = sumConsumption([a, b]);
    assert.deepEqual([...s.keys()], ['2026-01']);
    assert.equal(s.get('2026-01').twh, 3);
    assert.equal(s.get('2026-01').flag, 'p');
  });

  it('reads the provisional flag from the region field', () => {
    const m = consumptionMap([{ period: d(2026, 9, 1), value: 5, region: 'FR:p' }, { period: d(2026, 8, 1), value: 4, region: 'FR' }]);
    assert.equal(m.get('2026-09').flag, 'p');
    assert.equal(m.get('2026-08').flag, '');
  });
});

const bundleUrl = new URL('../docs/data/bundle.json', import.meta.url);
const bundle = existsSync(bundleUrl) ? JSON.parse(readFileSync(bundleUrl, 'utf8')) : null;

describe('Live bundle invariants', { skip: !bundle?.sources?.entsog && 'bundle without ENTSOG' }, () => {
  it('mix shares sum to 100 and Russian share collapsed since 2021', () => {
    const en = bundle.sources.entsog.data;
    const idx = buildRouteIndex((id) => getSeries(en, `entsog_route_${id}`));
    const mix = supplyMix(idx, idx.days[idx.days.length - 1]);
    const sum = mix.rows.reduce((s, r) => s + r.share, 0);
    assert.ok(Math.abs(sum - 100) < 1e-9);
    assert.ok(mix.russianShare2021 > 25 && mix.russianShare2021 < 45, `2021 Russian share ${mix.russianShare2021}`);
    assert.ok(mix.total > 5000 && mix.total < 14000, `imports ${mix.total} GWh/d`);
    assert.equal(ROUTES.length, new Set(ROUTES.map((r) => r.id)).size);
  });

  it('US LNG never exceeds EU send-out over 12 months', () => {
    const lngRows = bundle.sources.eia_lng_exports.data;
    const us = usLngToEu((id) => getSeries(lngRows, id), [...new Set(lngRows.map((r) => r.series_id))]);
    const link = transatlanticLng(us, monthlyEnergy(getSeries(bundle.sources.entsog.data, 'entsog_route_lng')));
    for (const r of link) if (r.share12 != null) assert.ok(r.share12 > 20 && r.share12 < 100, `${r.month} ${r.share12}`);
  });
});
