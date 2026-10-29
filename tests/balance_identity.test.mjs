/**
 * Balance panel: EIA's US supply/disposition identity, printed rather than assumed.
 *
 * EIA's Natural Gas Monthly Table 1 states
 *
 *   dry production + supplemental gaseous fuels + net imports
 *       − net storage withdrawals = consumption + balancing item
 *
 * and defines the balancing item as "the difference between the sum of the
 * components of natural gas supply and the sum of the components of natural gas
 * disposition" — a reconciliation line, not demand and not an import term.
 *
 * The panel has made two successive mistakes that these tests exist to prevent:
 * it once subtracted only production and consumption and called the result an
 * implied storage flow, and it then treated the balancing item as the import leg,
 * which would have absorbed ~11 Bcf/d of net imports into a line that is near
 * zero because imports and the residual happen to share the same sign.
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

// The panel draws with d3; nothing here asserts on pixels, so an inert stub for
// the chart API keeps these assertions runnable in a fresh checkout — d3 is not
// an installed dependency. The *numeric* helpers the model uses (`max`, `median`)
// are real implementations: the panel imports d3 before it defines the logger,
// so a Proxy here would make every reported statistic NaN without failing
// anything else. `expectedD3Median` below mirrors this median for the assertion
// on that path.
const D3_STUB = `
const chain = new Proxy(function () {}, {
  get: (_, prop) => (prop === Symbol.toPrimitive ? () => 0 : chain),
  apply: () => chain,
  construct: () => chain,
});
export const select = () => chain;
export const scaleTime = () => chain;
export const scaleLinear = () => chain;
export const extent = (vals) => [Math.min(...vals), Math.max(...vals)];
export const max = (vals, fn) => vals.reduce((m, v) => Math.max(m, fn ? fn(v) : v), -Infinity);
export const median = (vals, fn) => {
  const s = vals.map(fn ?? ((v) => v)).slice().sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
};
export const area = () => chain;
export const line = () => chain;
export const curveMonotoneX = chain;
export const axisBottom = () => chain;
export const timeYear = { every: () => chain };
export const timeFormat = () => chain;
export const pointer = () => [0, 0];
`;
registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3' || specifier.startsWith('d3/')) {
      return { url: 'data:text/javascript,' + encodeURIComponent(D3_STUB), shortCircuit: true };
    }
    return next(specifier, context);
  },
});

const { buildBalanceModel, buildFeedgasByMonth, buildLngByMonth, monthBalance, monthKeyOf, daysInMonth } =
  await import('../docs/js/panels/balance.js');
const { LNG_TERMINALS } = await import('../docs/js/util/lng-terminals.js');
const { parsePeriod } = await import('../docs/js/util/period.js');

const bundle = JSON.parse(readFileSync(new URL('../docs/data/bundle.json', import.meta.url), 'utf8'));
const rows = bundle.sources.eia_supply.data;
const model = buildBalanceModel(rows);
// buildBalanceModel reduces MMCF to Bcf; the leg lookups below work in MMCF.
const rawModel = buildBalanceModel(
  rows.map((r) => ({ ...r, value: Number(r.value) * 1000 })),
);
const lng = buildLngByMonth(bundle.sources.eia_lng_exports?.data ?? []);
const expectedD3Median = (values, accessor = (v) => v) => {
  const s = values.map(accessor).sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
};
const mmcfPerMonth = (period, id) =>
  rows
    .filter((r) => r.series_id === id && r.period.startsWith(period))
    .reduce((sum, r) => sum + Number(r.value), 0);

describe('Table 1 identity', () => {
  it('imbalance is EIA\'s published function of the legs this route carries', () => {
    const closing = model.series.filter((m) => m.imbalanceBcf != null);
    assert.ok(closing.length > 12, `only ${closing.length} months carry a balancing item`);
    for (const m of closing) {
      const key = monthKeyOf(m.period);
      const expected =
        (mmcfPerMonth(key, 'supply_fpd') +
          mmcfPerMonth(key, 'supply_ovi') -
          (mmcfPerMonth(key, 'supply_saw') - mmcfPerMonth(key, 'supply_sai')) -
          mmcfPerMonth(key, 'supply_vc0') -
          mmcfPerMonth(key, 'supply_vg4')) /
        1000;
      assert.ok(
        Math.abs(m.imbalanceBcf - expected) < 1e-6,
        `${key}: panel ${m.imbalanceBcf} Bcf vs legs ${expected} Bcf`,
      );
    }
  });

  it('does not treat the balancing item as demand', () => {
    // If vg4 were being folded into consumption the two would be inseparable and
    // the panel could not print it; EIA's line is small but real in most months.
    const closing = model.series.filter((m) => m.imbalanceBcf != null);
    const nonZero = closing.filter((m) => Math.abs(m.vg4) > 1);
    assert.ok(nonZero.length > closing.length * 0.9, 'balancing item was silently zeroed');
    assert.ok(Math.max(...closing.map((m) => Math.abs(m.vg4))) > 10, 'balancing item is being discarded');
  });

  it('does not treat the balancing item as the import leg', () => {
    // EIA's net imports run in the hundreds of Bcf every month of this sample
    // (Canada in, Mexico + LNG out). The balancing item is two orders of
    // magnitude smaller, so any code path that lets vg4 stand in for imports —
    // or lets it absorb the imbalance — is identifiable arithmetically: the
    // imbalance this panel prints must remain large and positive instead of
    // collapsing to a handful of Bcf.
    const closing = model.series.filter((m) => m.imbalanceBcf != null);
    const median = model.medianAbsResidualBcf;
    assert.ok(median > 100, `median |imbalance| ${median} Bcf — the missing legs are being absorbed`);
    const vg4Median = closing
      .map((m) => Math.abs(m.vg4))
      .sort((a, b) => a - b)[Math.floor(closing.length / 2)];
    assert.ok(vg4Median < median / 4, 'balancing item is the same order as the imbalance');
  });

  it('carries the sign of omitted net imports, not of the balancing item', () => {
    // EIA's net imports are positive (Canada in, Mexico + LNG out) in almost
    // every month of this sample, so a correctly signed imbalance inherited
    // from that leg is positive in almost every month too. A sign error on the
    // storage term, or vg4 standing in for imports, both flip most of it.
    const raws = rawModel.series.filter((m) => m.imbalanceBcf != null);
    const positive = raws.filter((m) => m.imbalanceBcf > 0).length;
    assert.ok(
      positive > raws.length * 0.7,
      `only ${positive}/${raws.length} months are positive — the missing leg no longer behaves like net imports`,
    );
    const inBcf = model.series.filter((m) => m.imbalanceBcf != null);
    assert.ok(inBcf.filter((m) => m.imbalanceBcf > 0).length / inBcf.length > 0.7);
  });

  it('a month missing a leg is not reported as balanced', () => {
    const period = new Date(2026, 0, 1);
    const partial = monthBalance(
      { fpd: 3000, ovi: 5, saw: 200, sai: null, vc0: 2800, vg4: null },
      daysInMonth(period),
    );
    assert.equal(partial.imbalanceBcf, null);
    assert.equal(partial.swd, null);
    // The legs that did publish are still carried, so the bridge can name which
    // one is missing instead of rendering zero.
    assert.equal(partial.vc0, 2800);
    assert.equal(partial.sai, null);
  });

  it('a month with legs but no balancing item has no measurable residual', () => {
    // Late, not unbalanced: EIA publishes vg4 a month behind the other legs.
    const m = monthBalance({ fpd: 3000, ovi: 5, saw: 200, sai: 100, vc0: 2800, vg4: null }, 30);
    assert.equal(m.swd, 100);
    assert.equal(m.imbalanceBcf, null);
  });

  it('the former "production − consumption" figure is not a storage change', () => {
    const withGap = model.series.filter((m) => m.naiveFlowBcf != null && m.storageChangeBcf != null);
    assert.ok(withGap.length > 24);
    const worst = Math.max(...withGap.map((m) => Math.abs(m.naiveFlowBcf - m.storageChangeBcf)));
    // The panel used to present this subtraction as an implied storage flow.
    // If the gap ever collapses to nothing, this test should be revisited
    // rather than relaxed: it would mean the legs are no longer distinct.
    assert.ok(worst > 100, `worst discrepancy ${worst} Bcf — the legs may have collapsed`);
  });

  it('converts monthly MMCF to Bcf/d on the month’s own length', () => {
    assert.equal(daysInMonth(new Date(2026, 1, 1)), 28);
    assert.equal(daysInMonth(new Date(2024, 1, 1)), 29);
    assert.equal(daysInMonth(new Date(2026, 6, 1)), 31);
    const m = monthBalance({ fpd: 31000, ovi: 0, saw: 0, sai: 0, vc0: 31000, vg4: 0 }, 31);
    // monthBalance takes Bcf/month and divides by the month's own length.
    assert.equal(m.fpd, 31000);
    assert.equal(Math.round(m.fpdBcfd), 1000);
    assert.equal(m.imbalanceBcf, 0);
  });

  it('net storage withdrawals are the identity leg, not gross injections', () => {
    const m = monthBalance({ fpd: 3000, ovi: 0, saw: 400, sai: 100, vc0: 3200, vg4: 100 }, 30);
    assert.equal(m.swd, 300);
    // 3000 + 0 − 300 − 3200 − 100 = −600 Bcf of imbalance, i.e. the omitted net
    // imports; charging gross injections (100) instead would read −500.
    assert.equal(m.imbalanceBcf, -600);
  });

  it('parses periods on the calendar month, so a month never splits in two', () => {
    const a = { series_id: 'supply_fpd', period: '2025-03', value: 3_000_000 };
    const b = { series_id: 'supply_vc0', period: '2025-03', value: 2_800_000 };
    const one = buildBalanceModel([a, b]);
    assert.equal(one.series.length, 1);
    assert.equal(one.series[0].period.getMonth(), 2);
  });

  it('drops a month that has consumption but no production', () => {
    const only = buildBalanceModel([{ series_id: 'supply_vc0', period: '2025-03', value: 2_800_000 }]);
    assert.equal(only.series.length, 0, 'a half-populated month was rendered as a collapse');
    assert.equal(only.latest, null);
  });
});

describe('LNG join', () => {
  it('joins on the calendar month despite the two different period formats', () => {
    // supply writes `YYYY-MM`; LNG exports write `YYYY-MM-01`. A raw string join
    // returns nothing and the panel silently omits its only import leg.
    const key = monthKeyOf(model.latest.period);
    assert.ok(lng.has(key), `no LNG row for ${key}`);
    const lngValue = lng.get(key);
    assert.ok(lngValue > 100 && lngValue < 1500, `${key} LNG ${lngValue} Bcf is out of plausible range`);
  });

  it('does not sum overlapping region ids', () => {
    // `lng_export_total` is the only additive series; country ids overlap with
    // regional ones, so a sum over all ids is several times too large.
    const all = (bundle.sources.eia_lng_exports?.data ?? []).filter((r) => r.period.startsWith('2026-06'));
    const total = all.filter((r) => r.series_id === 'lng_export_total').reduce((s, r) => s + r.value, 0);
    const sumOfIds = all.reduce((s, r) => s + r.value, 0);
    assert.ok(total > 0);
    assert.ok(sumOfIds > total * 1.5, 'region ids appear additive — the bundle may have changed shape');
  });
});

describe('feedgas cross-check', () => {
  const feedgas = buildFeedgasByMonth(bundle.sources);
  // `buildFeedgasByMonth` returns months, not per-feed day counts, so the
  // expected meter-days are recomputed here from the same registry rules.
  const latestKey = [...feedgas.keys()].sort().pop();
  const latest = feedgas.get(latestKey);

  it('sums only measured scheduled-quantity meters', () => {
    // Cheniere terminals publish no SQ — their headline is a capacity residual —
    // and a feed marked `comparison` re-measures gas another meter already
    // counts. Either one in the sum turns the cross-check into a multiple of
    // the same molecules.
    assert.ok(latest, 'no month of feedgas was produced');
    const sqIds = Object.values(LNG_TERMINALS)
      .filter((t) => t.signal === 'sq' && t.operational !== false)
      .map((t) => t.id);
    // The panel must not count more terminals than the bundle carries data for.
    assert.ok(latest.terminals >= 1 && latest.terminals <= sqIds.length,
      `counted ${latest.terminals} terminals out of ${sqIds.length} SQ terminals`);
    const withData = new Set(measuredFeeds.map((f) => f.terminal));
    assert.ok(withData.size >= latest.terminals,
      `${latest.terminals} terminals counted but only ${withData.size} measured feeds have data`);
    // A terminal that does not post in the month must not appear in it: every
    // counted terminal has at least one positive day in `latestKey`.
    const posting = new Set(
      measuredFeeds
        .filter((f) => rowsFor(f).some((r) => r.period.startsWith(latestKey) && Number(r.value) > 0))
        .map((f) => f.terminal),
    );
    assert.equal(latest.terminals, posting.size,
      `month counted ${latest.terminals} terminals, ${posting.size} are posting`);
  });

  it('is a floor on net imports — larger than LNG exports, far smaller than consumption', () => {
    // The feedgas month runs ahead of EIA's monthly supply series, so the ratio
    // is compared against *daily* rates over the same month rather than against
    // whatever month EIA happened to publish last: a partial month of feedgas
    // against a full month of consumption is a fraction, not a ratio.
    const supplySameMonth = model.series.find((m) => monthKeyOf(m.period) === latestKey);
    assert.ok(latest.days > 0, 'feedgas month has no days');
    if (lng.has(latestKey)) {
      const lngDays = new Set(
        (bundle.sources.eia_lng_exports?.data ?? [])
          .filter((r) => r.series_id === 'lng_export_total' && r.period.startsWith(latestKey))
          .map((r) => r.period),
      ).size || 1;
      assert.ok(latest.bcf / latest.days > lng.get(latestKey) / lngDays,
        `feedgas ${latest.bcf / latest.days} Bcf/d ≤ LNG exports ${lng.get(latestKey) / lngDays} Bcf/d`);
    }
    if (supplySameMonth) {
      // Feedgas is a floor on LNG-plant intake, which is itself a fraction of
      // total US demand; it can never approach it.
      assert.ok(latest.bcf / latest.days < supplySameMonth.vc0Bcfd * 0.5,
        `feedgas ${latest.bcf / latest.days} Bcf/d swamped consumption ${supplySameMonth.vc0Bcfd} Bcf/d`);
    }
  });
});

/** Measured `sq` feeds the panel's feedgas runner is expected to find. */
const measuredFeeds = Object.values(LNG_TERMINALS).flatMap((t) => {
  if (t.signal !== 'sq' || t.operational === false) return [];
  const feeds = t.feeds?.length ? t.feeds : [{ source: t.source, seriesPrefix: t.seriesPrefix }];
  return feeds
    .filter((f) => (f.kind ?? 'measured') === 'measured' && bundle.sources[f.source]?.data)
    .map((f) => ({ terminal: t.id, ...f }));
});

/** Rows for one feed: the exact stem, its cycle tokens, or the whole prefix family. */
const rowsFor = (feed) =>
  bundle.sources[feed.source].data.filter((r) =>
    feed.series
      ? r.series_id === feed.series ||
        (r.series_id.startsWith(feed.series) &&
          /^_(timely|evening|id\d+|best|late[a-z]*)$/i.test(r.series_id.slice(feed.series.length)))
      : String(r.series_id).startsWith(feed.seriesPrefix));

describe('balance model reporting', () => {
  it('reports how many months its residual statistic rests on', () => {
    assert.equal(model.closingMonths, model.series.filter((m) => m.imbalanceBcf != null).length);
    assert.ok(model.closingMonths >= 12);
    // The logged statistics are computed with d3, which the panel imports ahead
    // of the stub; if the stub ever shadows it, these come back as a chain and
    // the panel silently prints NaN.
    const closing = model.series.filter((m) => m.imbalanceBcf != null);
    const expectedMedian = expectedD3Median(closing, (m) => Math.abs(m.imbalanceBcf));
    assert.equal(typeof model.medianAbsResidualBcf, 'number');
    assert.ok(Math.abs(model.medianAbsResidualBcf - expectedMedian) < 1e-9);
  });

  it('solves the identity for the leg it is missing, and labels it an inference', () => {
    const closing = model.series.filter((m) => m.impliedNetImportsBcf != null);
    assert.ok(closing.length > 12);
    for (const m of closing) {
      // imbalance = netImports − vg4, so netImports = imbalance + vg4 on the
      // signed legs, i.e. imbalance + (published vg4) = imbalance − (−vg4).
      assert.ok(Math.abs(m.impliedNetImportsBcf - (m.imbalanceBcf - m.vg4)) < 1e-9);
    }
    // A month whose balancing item never published must not carry an implied
    // import figure — the subtraction would still return a number, but it would
    // be the imbalance mislabelled as imports.
    const late = monthBalance({ fpd: 3000, ovi: 5, saw: 200, sai: 100, vc0: 2800, vg4: null }, 30);
    assert.equal(late.impliedNetImportsBcf, null);
  });

  it('prints an imbalance of the same magnitude in Bcf and Bcf/d', () => {
    for (const m of model.series) {
      if (m.imbalanceBcf == null) continue;
      assert.ok(Math.abs(m.imbalanceBcfd * m.days - m.imbalanceBcf) < 1e-9);
    }
  });

  it('the chart band is production + supplemental fuels vs consumption', () => {
    // The band is drawn between suppliedBcf and drainedBcf; storage is a third
    // supply leg and must not be folded into either side.
    const m = monthBalance({ fpd: 3000, ovi: 5, saw: 400, sai: 100, vc0: 3200, vg4: 10 }, 30);
    assert.equal(m.suppliedBcf, 3005);
    assert.equal(m.drainedBcf, 3200);
    assert.equal(m.imbalanceBcf, 3000 + 5 - 300 - 3200 - 10);
  });

  it('a meter posting several cycles in one day is not multiplied by them', () => {
    const built = buildFeedgasByMonth(bundle.sources);
    const key = [...built.keys()].sort().pop();
    // Every measured SQ feed contributes at most one value per day, so a feed
    // can contribute at most one meter-day per calendar day it posts in.
    let maxMeterDays = 0;
    for (const feed of measuredFeeds) {
      maxMeterDays += new Set(
        rowsFor(feed).filter((r) => r.period.startsWith(key)).map((r) => r.period),
      ).size;
    }
    assert.ok(built.get(key).days <= maxMeterDays,
      `${built.get(key).days} meter-days for ${measuredFeeds.length} meters that posted ${maxMeterDays} day-values`);
    // And each posting meter must land once, not once per cycle: the count can
    // never exceed the number of (terminal, day) pairs in the month.
    const dayCount = new Set(
      Object.values(bundle.sources).flatMap((s) => (s?.data ?? [])
        .filter((r) => r.period.startsWith(key))
        .map((r) => r.period)),
    ).size;
    assert.ok(built.get(key).days <= measuredFeeds.length * dayCount,
      `${built.get(key).days} meter-days for ${measuredFeeds.length} meters in ${dayCount} days`);
    assert.ok(built.get(key).days >= measuredFeeds.length * 0.8 * dayCount * 0.5,
      'most measured feeds stopped posting — the cross-check rests on too few days');
  });

  it('a net injection month reads as it should, not as a withdrawal', () => {
    const withdrawal = monthBalance({ fpd: 3000, ovi: 0, saw: 400, sai: 100, vc0: 2700, vg4: 0 }, 30);
    const injection = monthBalance({ fpd: 3000, ovi: 0, saw: 100, sai: 400, vc0: 2700, vg4: 0 }, 30);
    assert.equal(withdrawal.swd, 300);
    assert.equal(injection.swd, -300);
    // 300 Bcf more storage on the supply side in the withdrawal month lowers
    // the imbalance by exactly that much relative to the injection month.
    assert.equal(withdrawal.imbalanceBcf - injection.imbalanceBcf, -600);
  });

  it('latest month is the last one with both production and consumption', () => {
    const latest = model.latest;
    assert.ok(latest);
    assert.ok(Number.isFinite(latest.fpd));
    assert.ok(Number.isFinite(latest.vc0));
    assert.ok(latest.period >= model.series[model.series.length - 2].period);
  });
});
