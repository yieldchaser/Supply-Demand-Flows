/**
 * Storage panel: session-level claims that used to be missing or wrong.
 *
 *  - the regional table's fill share must be the region's *own* observed peak,
 *    not the Lower 48 4,000 Bcf convention spread over seven regions;
 *  - season-to-date must be date-matched and report its n, because "72% full"
 *    plus a weekly change still cannot say whether the season is running hot;
 *  - a "largest / smallest" badge is a claim about a distribution, so it must
 *    refuse to fire on a sample of one;
 *  - `getSeries` must not read a date-only period as UTC midnight.
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

// The panel imports `d3` for drawing; these assertions are about arithmetic and
// wording only, so a stub keeps the file runnable in a fresh checkout. The
// chainable surface is deliberately inert: the chart draws into a stub container
// and nothing here asserts on pixels.
const D3_STUB = `
const chain = new Proxy(function () {}, {
  get: (_, prop) => (prop === Symbol.toPrimitive ? () => 0 : chain),
  apply: () => chain,
  construct: () => chain,
});
export const select = () => chain;
export const scaleLinear = () => chain;
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
  regionTable,
  restOfSeason,
  seasonToDateDelta,
  seasonNormal,
  computeRegionSeasonFill,
} = await import('../docs/js/util/storage-analysis.js');
const { matchYearAgo } = await import('../docs/js/util/season.js');
const { getSeries, groupBySeriesId } = await import('../docs/js/util/series.js');
const { seasonToDateStanding, renderStoragePanel } = await import('../docs/js/panels/storage.js');
const { parsePeriod } = await import('../docs/js/util/period.js');

const bundle = JSON.parse(readFileSync(new URL('../docs/data/bundle.json', import.meta.url), 'utf8'));
const rows = bundle.sources.eia_storage.data;
const analysis = analyseStorage(rows);

/** Synthetic weekly Fridays from a start date, `n` weeks. */
function fridays(start, n) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    out.push({ series_id: 'storage_x', period: new Date(start.getTime() + i * 7 * 86400000), value: 100 + i });
  }
  return out.map((r) => ({ ...r, period: `${r.period.getFullYear()}-${String(r.period.getMonth() + 1).padStart(2, '0')}-${String(r.period.getDate()).padStart(2, '0')}` }));
}

describe('regional fill share', () => {
  it('uses each region’s own observed peak as the denominator', () => {
    const regions = regionTable(analysis.periods);
    assert.ok(regions.length >= 5);
    for (const r of regions) {
      const seriesRows = analysis.periods
        .filter((p) => Number.isFinite(p.values[r.seriesId]))
        .map((p) => p.values[r.seriesId]);
      const max = Math.max(...seriesRows);
      assert.ok(r.fillPctOfObservedMax <= 100.0001, `${r.label} fill exceeded 100%`);
      assert.ok(Math.abs(r.fillPctOfObservedMax - (r.levelBcf / max) * 100) < 1e-9, r.label);
      assert.ok(Math.abs(r.roomToObservedMaxBcf - (max - r.levelBcf)) < 1e-9, r.label);
    }
  });

  it('does not equal the share-of-L48 column (the two denominators differ)', () => {
    const regions = regionTable(analysis.periods).filter((r) => r.shareOfL48Pct != null);
    const same = regions.filter((r) => Math.abs(r.fillPctOfObservedMax - r.shareOfL48Pct) < 1e-6);
    assert.equal(same.length, 0, 'fill collapsed back onto the L48 share');
  });

  it('a small region can be fuller than a large one', () => {
    // The point of the column: comparable percentages hid the fact that 60% of
    // a 400 Bcf region and 60% of a 1,100 Bcf region are different questions.
    const regions = regionTable(analysis.periods);
    const fullest = regions.reduce((a, b) => (b.fillPctOfObservedMax > a.fillPctOfObservedMax ? b : a));
    assert.ok(fullest.fillPctOfObservedMax > 50);
  });
});

describe('season to date', () => {
  it('starts at the season opening month, not at the start of the data', () => {
    const std = analysis.seasonToDate;
    assert.ok(std.start, 'no start');
    assert.ok(std.end);
    assert.ok(std.start < std.end);
    assert.equal(std.end.getTime(), analysis.latest.period.getTime());
    // The window starts at the season's opening month, so the start is the
    // first print on or after the 1st of April or of November.
    const m = std.start.getMonth() + 1;
    assert.ok(m === 4 || m === 11, `season opened in month ${m}`);
    assert.ok(std.start.getDate() <= 7, `window opens ${std.start.getDate()} days into the month`);
  });

  it('net change equals latest minus window start', () => {
    const std = analysis.seasonToDate;
    const startRow = analysis.l48.find((r) => r.period.getTime() === std.start.getTime());
    assert.ok(startRow, 'window start is not an observation');
    assert.equal(std.netChangeBcf, analysis.latest.value - startRow.value);
  });

  it('reports the prior-year sample size and its range', () => {
    const std = analysis.seasonToDate;
    assert.ok(std.n >= 1, 'no prior years matched');
    assert.equal(std.prior.length, std.n);
    // The sample size is what the panel prints as `n=…`; a window that fell
    // through to no matches at all would print an empty claim instead.
    for (const p of std.prior) {
      assert.ok(Number.isFinite(p.netChangeBcf), `prior year ${p.year} has no net change`);
      assert.ok(p.lagDays <= 7, `prior year ${p.year} matched ${p.lagDays} days off`);
    }
    assert.equal(std.priorMinBcf, Math.min(...std.prior.map((p) => p.netChangeBcf)));
    assert.equal(std.priorMaxBcf, Math.max(...std.prior.map((p) => p.netChangeBcf)));
  });

  it('date-matches each prior year instead of taking the same table position', () => {
    // Each year's injection window opens one Friday sooner than the year before,
    // and 2025 has an extra leading week. Matching by table position would
    // compare a different number of weeks per year; matching by calendar date
    // gives every year the same window (4 Apr → 2 May here).
    const mk = (y, day, value) => ({ series_id: 'storage_x', period: `${y}-${day}`, value });
    const rowsSynth = [
      mk(2023, '03-31', 900),
      mk(2023, '04-07', 1000), mk(2023, '04-14', 1050), mk(2023, '04-21', 1100), mk(2023, '04-28', 1115), mk(2023, '05-05', 1170),
      mk(2023, '05-12', 1200), mk(2023, '05-19', 1230), mk(2023, '05-26', 1260), mk(2023, '06-02', 1290), mk(2023, '06-09', 1320),
      mk(2024, '03-29', 1150),
      mk(2024, '04-05', 1200), mk(2024, '04-12', 1220), mk(2024, '04-19', 1240), mk(2024, '04-26', 1260), mk(2024, '05-03', 1280),
      mk(2024, '05-10', 1300), mk(2024, '05-17', 1320), mk(2024, '05-24', 1340), mk(2024, '05-31', 1360), mk(2024, '06-07', 1380),
      mk(2025, '03-28', 1250),
      mk(2025, '04-04', 1300), mk(2025, '04-11', 1325), mk(2025, '04-18', 1350), mk(2025, '04-25', 1370), mk(2025, '05-02', 1390),
    ];
    const l48 = rowsSynth
      .map((r) => ({ period: parsePeriod(r.period), value: r.value }))
      .sort((a, b) => a.period - b.period);
    const std = seasonToDateDelta(l48, { lookbackYears: 2 });
    assert.equal(std.n, 2);
    // Latest is 2025-05-02 and the season opens on 1 April, so the window
    // starts at the first print on or after 1 April: 2025-04-04.
    assert.equal(std.start.getMonth(), 3);
    assert.equal(std.start.getDate(), 4);
    assert.ok(Math.abs(std.netChangeBcf - 90) < 1e-9, `net ${std.netChangeBcf}`);
    // The prior window starts at the Friday nearest 4 April and ends at the
    // first print on or after the Friday nearest 2 May — never the print before
    // it, which is the one that would add an extra week:
    //   2025:  4 Apr (1300) →  2 May (1390) = 90
    //   2024:  5 Apr (1200) →  3 May (1280) = 80
    //   2023:  7 Apr (1000) →  5 May (1170) = 170, not 28 Apr (1115) → 115
    assert.ok(Math.abs(std.priorMinBcf - 80) < 1e-9, `min ${std.priorMinBcf}`);
    assert.ok(Math.abs(std.priorMaxBcf - 170) < 1e-9, `max ${std.priorMaxBcf}`);
  });

  it('covers the same number of weeks in a 53-week year as in a 52-week one', () => {
    // 2020's Friday calendar runs 53 labelled weeks: the season's opening Friday
    // is 3 April 2020, one day later than 2019's 29 March and 2021's 2 April.
    // A prior window counted by table position would gain a week here; a
    // date-matched window must still span the same number of weekly steps.
    const mk = (y, day, value) => ({ series_id: 'storage_x', period: `${y}-${day}`, value });
    const fridays = [
      ['2019', '03-01'], ['2019', '03-08'], ['2019', '03-15'], ['2019', '03-22'], ['2019', '03-29'],
      ['2019', '04-05'], ['2019', '04-12'], ['2019', '04-19'], ['2019', '04-26'], ['2019', '05-03'], ['2019', '05-10'],
      ['2020', '02-28'], ['2020', '03-06'], ['2020', '03-13'], ['2020', '03-20'], ['2020', '03-27'], ['2020', '03-31'],
      ['2020', '04-03'], ['2020', '04-10'], ['2020', '04-17'], ['2020', '04-24'], ['2020', '05-01'], ['2020', '05-08'],
      ['2021', '02-26'], ['2021', '03-05'], ['2021', '03-12'], ['2021', '03-19'], ['2021', '03-26'],
      ['2021', '04-02'], ['2021', '04-09'], ['2021', '04-16'], ['2021', '04-23'], ['2021', '04-30'], ['2021', '05-07'],
    ];
    // Same +10 Bcf per week in every year, so any window that spans a
    // different number of weeks shows up as a different net change.
    const l48 = fridays
      .map(([y, d], i) => mk(y, d, 1000 + i * 10))
      .map((r) => ({ period: parsePeriod(r.period), value: r.value }))
      .sort((a, b) => a.period - b.period);
    const std = seasonToDateDelta(l48, { lookbackYears: 3 });
    assert.equal(std.n, 2, 'expected 2019 and 2020 to match');
    const byYear = Object.fromEntries(std.prior.map((p) => [p.year, p]));
    assert.equal(byYear[2019].weeks, byYear[2020].weeks, '53-week year got a different span');
    assert.equal(byYear[2019].netChangeBcf, byYear[2020].netChangeBcf, '53-week year counted an extra week');
  });

  it('returns an empty result rather than a confident zero on a one-week series', () => {
    const std = seasonToDateDelta([{ period: new Date(2026, 3, 3), value: 1000 }]);
    assert.equal(std.n, 0);
    assert.equal(std.netChangeBcf, null);
  });

  it('the season-end line is arithmetic on a prior path and says how far ahead', () => {
    const rest = analysis.seasonRest;
    if (!rest) return; // data window may not reach the season end
    assert.ok(rest.weeksAhead >= 0);
    assert.ok(Number.isFinite(rest.impliedSeasonEndBcf));
  });

  it('restOfSeason does not invent a path beyond the available weeks', () => {
    const rowsSynth = fridays(new Date(2024, 0, 5), 52).map((r) => ({ period: parsePeriod(r.period), value: r.value }));
    // Only one year of history: nothing to repeat, so nothing is claimed.
    const rest = restOfSeason(rowsSynth);
    assert.equal(rest, null);
  });
});

describe('seasonToDateStanding', () => {
  const std = (netChangeBcf, prior) => ({ netChangeBcf, prior, n: prior.length });

  it('refuses to crown a sample of one', () => {
    assert.equal(seasonToDateStanding(std(500, [{ netChangeBcf: 300 }])), 'mid-range');
  });

  it('calls it largest when it exceeds matched prior years in the same direction', () => {
    assert.equal(
      seasonToDateStanding(std(500, [{ netChangeBcf: 300 }, { netChangeBcf: 450 }])),
      'largest',
    );
  });

  it('is mid-range when a prior year moved the other way, not smallest', () => {
    assert.equal(
      seasonToDateStanding(std(-40, [{ netChangeBcf: 30 }, { netChangeBcf: 10 }])),
      'mid-range',
    );
  });
});

describe('matchYearAgo', () => {
  it('matches within the same year when yearDelta is 0', () => {
    // The storage panel calls this with yearDelta = 0 to find the same calendar
    // window inside one year's own rows. `year - 0` is the same year, so the
    // lookup only works if the distance is measured in whole days.
    const rows = [
      { period: new Date(2025, 3, 4), value: 1300 },
      { period: new Date(2025, 3, 11), value: 1325 },
    ];
    // 10 April sits 6 days after the 4th and 1 day before the 11th, so the
    // nearest row wins: a tolerance is not "the first row inside it".
    const hit = matchYearAgo(rows, new Date(2025, 3, 10), 0, 7);
    assert.ok(hit, 'no same-year match');
    assert.equal(hit.value, 1325);
    assert.equal(hit.lagDays, 1);
  });

  it('picks the nearest Friday, not the first one inside the tolerance', () => {
    const rows = [
      { period: new Date(2025, 3, 18), value: 1 },
      { period: new Date(2025, 3, 25), value: 2 },
    ];
    // Target is 20 April 2025: the 18th is 2 days back, the 25th is 5 days on.
    const hit = matchYearAgo(rows, new Date(2025, 3, 20), 0, 7);
    assert.equal(hit.value, 1);
    assert.equal(hit.lagDays, 2);
  });

  it('reads a timestamped period as the calendar day it names', () => {
    // A row stored as ``2026-09-11T00:00Z`` is the 11th; built as local midnight
    // in a timezone ahead of UTC it sits at 05:30 on the same day. Both must
    // land on lag 0, and a tolerance in *days* must not be compared in ms.
    // In a timezone ahead of UTC (IST here) the UTC midnight of the 11th is
    // 05:30 on the 11th, so both spellings are the same storage day.
    const rows = [{ period: new Date('2026-09-11T00:00:00.000Z'), value: 7 }];
    const hit = matchYearAgo(rows, new Date(2026, 8, 11), 0, 0);
    assert.ok(hit, 'same-day timestamp was rejected');
    assert.equal(hit.lagDays, 0);
  });

  it('never matches two days apart when the tolerance is one day', () => {
    const rows = [{ period: new Date(2025, 3, 4), value: 1 }];
    assert.equal(matchYearAgo(rows, new Date(2025, 3, 6), 0, 1), null);
  });
});

describe('seasonNormal', () => {
  it('counts the years that contributed and never promises the lookback', () => {
    const l48 = analysis.l48;
    const normal = seasonNormal(l48, l48[l48.length - 1].period);
    assert.ok(normal.n >= 1);
    assert.equal(normal.years.length, normal.n);
    assert.ok(normal.mean !== null);
  });
});

describe('computeRegionSeasonFill', () => {
  it('measures each region against its own observed peak', () => {
    for (const r of computeRegionSeasonFill(analysis.periods)) {
      if (r.observedMaxBcf <= 0) continue;
      assert.ok(Math.abs(r.pctOfObservedMax - (r.levelBcf / r.observedMaxBcf) * 100) < 1e-9, r.key);
      assert.ok(r.pctOfObservedMax <= 100.0001, `${r.key} exceeds its own peak`);
    }
  });
});

describe('season-to-date render', () => {
  /**
   * Just enough DOM for `renderStoragePanel`: html assignment, the three
   * containers the panel walks to, and a permissive querySelector. The chart
   * itself is skipped (the d3 stub throws), so this only asserts that the
   * season block actually reaches the page with its numbers.
   */
  // `appendSeasonToDate` builds its wrapper with `document.createElement`.
  globalThis.document = {
    createElement: () => ({
      className: '',
      innerHTML: '',
      children: [],
      appendChild(c) { this.children.push(c); return c; },
      insertBefore(c) { this.children.push(c); return c; },
    }),
  };

  function stubPanel() {
    const make = () => {
      const el = {
        innerHTML: '',
        textContent: '',
        children: [],
        querySelector: () => null,
        querySelectorAll: () => [],
        appendChild(c) { this.children.push(c); return c; },
        insertBefore(c) { this.children.push(c); return c; },
        addEventListener() {},
        removeEventListener() {},
        getBoundingClientRect: () => ({ width: 800, height: 400, top: 0, left: 0 }),
        setAttribute() {},
        remove() {},
      };
      return el;
    };
    const chart = make();
    const sidebar = make();
    const body = make();
    body.querySelector = (sel) => (sel === '.panel-sidebar' ? sidebar : null);
    chart.querySelector = () => null;
    sidebar.querySelector = () => null;
    const panel = make();
    panel.querySelector = (sel) => {
      if (sel === '.panel-chart') return chart;
      if (sel === '.panel-sidebar') return sidebar;
      if (sel === '.panel-body') return body;
      return null;
    };
    return { panel, body };
  }

  it('mounts the season-to-date block with its n and prior range', () => {
    const { panel, body } = stubPanel();
    assert.doesNotThrow(
      () => renderStoragePanel(panel, { sources: { eia_storage: { data: rows, latest_period: bundle.sources.eia_storage.latest_period } } }),
      'renderStoragePanel threw',
    );
    const block = body.children.find((c) => String(c.innerHTML).includes('Season to date'));
    assert.ok(block, 'no season-to-date block was inserted');
    const html = block.innerHTML;
    assert.ok(html.includes(`n=${analysis.seasonToDate.n}`), 'sample size not printed');
    assert.ok(html.includes('Bcf'), 'net change not printed');
    assert.ok(html.includes('not a forecast') || !analysis.seasonRest, 'arithmetic repeat not labelled');
  });
});

describe('getSeries period parsing', () => {
  it('keeps a date-only period on its own calendar day', () => {
    const out = getSeries([{ series_id: 's', period: '2026-09-11', value: '100' }], 's');
    assert.equal(out.length, 1);
    assert.equal(out[0].period.getFullYear(), 2026);
    assert.equal(out[0].period.getMonth(), 8);
    assert.equal(out[0].period.getDate(), 11);
    assert.equal(out[0].value, 100);
  });

  it('drops an unparseable period instead of sorting an Invalid Date', () => {
    const out = getSeries([
      { series_id: 's', period: '2026-09-11', value: 100 },
      { series_id: 's', period: 'not-a-date', value: 200 },
    ], 's');
    assert.equal(out.length, 1);
    assert.ok(out.every((r) => Number.isFinite(r.period.getTime())));
  });

  it('groupBySeriesId does not mutate the caller’s rows', () => {
    const raw = [{ series_id: 's', period: '2026-09-11', value: '100' }];
    const grouped = groupBySeriesId(raw);
    assert.equal(raw[0].period, '2026-09-11');
    assert.ok(grouped.get('s')[0].period instanceof Date);
  });
});
