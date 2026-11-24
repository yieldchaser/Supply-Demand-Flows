/**
 * EU supply and demand analysis: imports by route (ENTSOG), consumption
 * (Eurostat) and the US LNG link (EIA).
 *
 * Pure and synchronous, like eu-storage-analysis.js: the panels format what
 * this returns and the Node tests assert it.
 *
 * Units
 *   entsog_route_*         GWh/d (energy, gross calorific value), daily
 *   eurostat_gas_*         TWh per calendar month (GCV)
 *   lng_export_*           Bcf per calendar month (EIA)
 * Bcf is converted to TWh with EIA's average heat content of LNG-quality gas,
 * ~1,037 Btu/cf: 1 Bcf = 1.037 TBtu = 0.3039 TWh. The factor is exported so the
 * panel prints it.
 */

import { dayOnly } from './season.js';

export const TWH_PER_BCF = 1.037 * 0.293071;

/** Import routes in stacking order (largest, most stable first). */
export const ROUTES = [
  { id: 'norway', label: 'Norway', color: '#7dd3fc' },
  { id: 'lng', label: 'LNG', color: '#fbbf24' },
  { id: 'north_africa', label: 'North Africa', color: '#34d399' },
  { id: 'tap', label: 'Azerbaijan (TAP)', color: '#a78bfa' },
  { id: 'uk', label: 'UK interconnectors', color: '#94a3b8' },
  { id: 'russia_turkstream', label: 'Russia: TurkStream', color: '#f87171' },
  { id: 'ukraine', label: 'Ukraine border (Russian transit until 2025)', color: '#fb923c' },
  { id: 'russia_nordstream', label: 'Russia: Nord Stream', color: '#dc2626' },
  { id: 'russia_other', label: 'Russia: Baltic & Finland', color: '#9f1239' },
];

/** Routes whose gas is Russian-origin at all times. */
export const RUSSIAN_ROUTES = new Set(['russia_turkstream', 'russia_nordstream', 'russia_other']);

/** Russian transit through Ukraine ended when the transit contract expired. */
export const UKRAINE_TRANSIT_END = new Date(2025, 0, 1);

/**
 * Whether a route carried Russian-origin gas on a date. The Ukraine border
 * did until 1 January 2025; flows across it since are not Russian transit.
 */
export function isRussianOn(routeId, date) {
  if (RUSSIAN_ROUTES.has(routeId)) return true;
  return routeId === 'ukraine' && date < UKRAINE_TRANSIT_END;
}

/** EU-27 members as EIA destination codes (ISO 3166-1 alpha-3). */
export const EU27_ISO3 = new Set([
  'aut', 'bel', 'bgr', 'hrv', 'cyp', 'cze', 'dnk', 'est', 'fin', 'fra', 'deu', 'grc', 'hun', 'irl',
  'ita', 'lva', 'ltu', 'lux', 'mlt', 'nld', 'pol', 'prt', 'rou', 'svk', 'svn', 'esp', 'swe',
]);

const DAY_MS = 86400000;

function monthKey(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}
function daysInMonthKey(key) {
  const [y, m] = key.split('-').map(Number);
  return new Date(y, m, 0).getDate();
}
function median(xs) {
  const s = xs.filter(Number.isFinite).sort((a, b) => a - b);
  if (!s.length) return null;
  const n = s.length;
  return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

/* ------------------------------------------------------------------ */
/*  Imports by route                                                   */
/* ------------------------------------------------------------------ */

/**
 * Index each route's daily rows by local-midnight time.
 * @param {(id: string) => Array<{period: Date, value: number}>} getRoute
 * @returns {{byRoute: Map<string, Map<number, number>>, days: Date[]}}
 */
export function buildRouteIndex(getRoute, routes = ROUTES) {
  const byRoute = new Map();
  const all = new Set();
  for (const r of routes) {
    const m = new Map();
    for (const row of getRoute(r.id) || []) {
      if (!(row.period instanceof Date) || !Number.isFinite(row.value)) continue;
      const t = dayOnly(row.period).getTime();
      m.set(t, row.value);
      all.add(t);
    }
    byRoute.set(r.id, m);
  }
  // A day counts only when every route reported it (the transformer already
  // blanks incomplete route-days; this keeps the stack honest at the edges).
  const days = [...all].filter((t) => routes.every((r) => byRoute.get(r.id).has(t))).sort((a, b) => a - b).map((t) => new Date(t));
  return { byRoute, days };
}

/** Mean of each route over the `window` complete days ending at `end`. */
export function routeWindowMean(index, end, window = 7, routes = ROUTES) {
  const e = dayOnly(end).getTime();
  const days = index.days.filter((d) => d.getTime() <= e && d.getTime() > e - window * DAY_MS);
  if (days.length < Math.ceil(window / 2)) return null;
  const out = { n: days.length, total: 0, byRoute: {} };
  for (const r of routes) {
    const m = index.byRoute.get(r.id);
    const v = days.reduce((s, d) => s + m.get(d.getTime()), 0) / days.length;
    out.byRoute[r.id] = v;
    out.total += v;
  }
  return out;
}

/**
 * Supply mix now vs the same window a year earlier and the 2021 average.
 * Shares are of total measured imports (domestic production is not in ENTSOG
 * import points and is not part of the total).
 */
export function supplyMix(index, asOf, { window = 7, routes = ROUTES } = {}) {
  const now = routeWindowMean(index, asOf, window, routes);
  if (!now) return null;
  const ya = new Date(asOf.getFullYear() - 1, asOf.getMonth(), asOf.getDate());
  const prior = routeWindowMean(index, ya, window, routes);
  const base = baselineYearMean(index, 2021, routes);
  const rows = routes.map((r) => {
    const v = now.byRoute[r.id];
    const p = prior?.byRoute[r.id] ?? null;
    return {
      id: r.id,
      label: r.label,
      color: r.color,
      gwh: v,
      share: now.total > 0 ? (v / now.total) * 100 : null,
      priorGwh: p,
      yoyGwh: p == null ? null : v - p,
      share2021: base && base.total > 0 ? (base.byRoute[r.id] / base.total) * 100 : null,
      gwh2021: base ? base.byRoute[r.id] : null,
    };
  });
  const russian = rows.filter((r) => isRussianOn(r.id, asOf)).reduce((s, r) => s + r.gwh, 0);
  return {
    asOf: now,
    window,
    total: now.total,
    priorTotal: prior?.total ?? null,
    rows,
    russianShare: now.total > 0 ? (russian / now.total) * 100 : null,
    russianShare2021: base ? (routes.filter((r) => isRussianOn(r.id, new Date(2021, 6, 1))).reduce((s, r) => s + base.byRoute[r.id], 0) / base.total) * 100 : null,
  };
}

function baselineYearMean(index, year, routes) {
  const days = index.days.filter((d) => d.getFullYear() === year);
  if (days.length < 300) return null;
  const out = { total: 0, byRoute: {} };
  for (const r of routes) {
    const m = index.byRoute.get(r.id);
    out.byRoute[r.id] = days.reduce((s, d) => s + m.get(d.getTime()), 0) / days.length;
    out.total += out.byRoute[r.id];
  }
  return out;
}

/** Trailing `window`-day mean per route for every complete day (for the stack). */
export function rollingRouteSeries(index, { window = 7, from = null, routes = ROUTES } = {}) {
  const out = [];
  const { days } = index;
  for (let i = 0; i < days.length; i++) {
    const d = days[i];
    if (from && d < from) continue;
    const lo = d.getTime() - window * DAY_MS;
    let j = i;
    const acc = Object.fromEntries(routes.map((r) => [r.id, 0]));
    let n = 0;
    while (j >= 0 && days[j].getTime() > lo) {
      for (const r of routes) acc[r.id] += index.byRoute.get(r.id).get(days[j].getTime());
      n += 1;
      j -= 1;
    }
    if (n < Math.ceil(window / 2)) continue;
    const row = { period: d };
    let total = 0;
    for (const r of routes) {
      row[r.id] = acc[r.id] / n;
      total += row[r.id];
    }
    row.total = total;
    out.push(row);
  }
  return out;
}

/**
 * Monthly totals (TWh) of a daily GWh/d series, with the number of days
 * observed so a partial month is never shown as a full one.
 */
export function monthlyEnergy(rows) {
  const by = new Map();
  for (const r of rows || []) {
    if (!(r.period instanceof Date) || !Number.isFinite(r.value)) continue;
    const k = monthKey(r.period);
    const s = by.get(k) ?? { twh: 0, days: 0 };
    s.twh += r.value / 1000;
    s.days += 1;
    by.set(k, s);
  }
  for (const [k, s] of by) s.complete = s.days >= daysInMonthKey(k) - 1;
  return by;
}

/* ------------------------------------------------------------------ */
/*  US LNG to the EU                                                   */
/* ------------------------------------------------------------------ */

/**
 * Monthly US LNG exports to EU-27 destinations, from EIA's per-country series.
 * @param {(id: string) => Array<{period: Date, value: number}>} getSeriesById
 * @param {string[]} seriesIds  all lng_export_* ids present
 * @returns {Map<string, {bcf: number, twh: number, countries: number}>}
 */
export function usLngToEu(getSeriesById, seriesIds) {
  const by = new Map();
  for (const id of seriesIds) {
    const iso = id.replace('lng_export_', '');
    if (!EU27_ISO3.has(iso)) continue;
    for (const r of getSeriesById(id)) {
      if (!(r.period instanceof Date) || !Number.isFinite(r.value)) continue;
      const k = monthKey(r.period);
      const s = by.get(k) ?? { bcf: 0, twh: 0, countries: 0 };
      s.bcf += r.value;
      s.twh += r.value * TWH_PER_BCF;
      if (r.value > 0) s.countries += 1;
      by.set(k, s);
    }
  }
  return by;
}

/**
 * Join US LNG to the EU with EU LNG send-out per month.
 * Share is a ratio of two independent measurements (EIA cargo exports by
 * destination vs ENTSOG regasification send-out); cargoes in transit and
 * tank stock changes make single months noisy, so the panel also shows the
 * trailing 12-month ratio.
 */
export function transatlanticLng(usByMonth, euLngByMonth) {
  const keys = [...usByMonth.keys()].filter((k) => euLngByMonth.get(k)?.complete).sort();
  const rows = keys.map((k) => {
    const us = usByMonth.get(k);
    const eu = euLngByMonth.get(k);
    return { month: k, usTwh: us.twh, usBcf: us.bcf, euTwh: eu.twh, share: eu.twh > 0 ? (us.twh / eu.twh) * 100 : null };
  });
  for (let i = 0; i < rows.length; i++) {
    if (i < 11) { rows[i].share12 = null; continue; }
    const win = rows.slice(i - 11, i + 1);
    const us = win.reduce((s, r) => s + r.usTwh, 0);
    const eu = win.reduce((s, r) => s + r.euTwh, 0);
    rows[i].share12 = eu > 0 ? (us / eu) * 100 : null;
  }
  return rows;
}

/* ------------------------------------------------------------------ */
/*  Demand                                                             */
/* ------------------------------------------------------------------ */

/**
 * Monthly consumption map from Eurostat rows (`region` carries a flag after
 * ':' for provisional/estimated months).
 * @returns {Map<string, {twh: number, flag: string}>}
 */
export function consumptionMap(rows) {
  const m = new Map();
  for (const r of rows || []) {
    if (!(r.period instanceof Date) || !Number.isFinite(r.value)) continue;
    const flag = String(r.region ?? '').split(':')[1] ?? '';
    m.set(monthKey(r.period), { twh: r.value, flag });
  }
  return m;
}

/** Sum several countries' maps, keeping only months every one of them reports. */
export function sumConsumption(maps) {
  if (!maps.length) return new Map();
  const keys = [...maps[0].keys()].filter((k) => maps.every((m) => m.has(k)));
  const out = new Map();
  for (const k of keys) {
    out.set(k, {
      twh: maps.reduce((s, m) => s + m.get(k).twh, 0),
      flag: maps.some((m) => m.get(k).flag) ? 'p' : '',
    });
  }
  return out;
}

/**
 * Typical peak-winter demand: mean daily consumption over December-February,
 * median across the last `winters` complete winters before `asOf`.
 * @returns {{twhPerDay: number, winters: string[]} | null}
 */
export function winterDemandRate(cons, asOf, { winters = 3 } = {}) {
  const vals = [];
  const labels = [];
  let y = asOf.getMonth() >= 2 ? asOf.getFullYear() : asOf.getFullYear() - 1; // last Feb fully past
  for (; vals.length < winters && y > 2015; y--) {
    const keys = [`${y - 1}-12`, `${y}-01`, `${y}-02`];
    if (!keys.every((k) => cons.has(k))) continue;
    const twh = keys.reduce((s, k) => s + cons.get(k).twh, 0);
    const days = keys.reduce((s, k) => s + daysInMonthKey(k), 0);
    vals.push(twh / days);
    labels.push(`${y - 1}/${String(y % 100).padStart(2, '0')}`);
  }
  if (!vals.length) return null;
  return { twhPerDay: median(vals), winters: labels };
}

/** Last 12 reported months of consumption, TWh. */
export function annualDemand(cons) {
  const keys = [...cons.keys()].sort();
  if (keys.length < 12) return null;
  const last = keys.slice(-12);
  return { twh: last.reduce((s, k) => s + cons.get(k).twh, 0), from: last[0], to: last[last.length - 1], provisional: last.some((k) => cons.get(k).flag) };
}

/**
 * Storage measured against demand: days of typical peak-winter demand the
 * stock represents, and the stock as a share of a year's consumption.
 * Storage is one supply among several (imports, production), so "days of
 * cover" is a yardstick of size, not a run-out date; the panel says so.
 */
export function demandCover(stockTwh, cons, asOf) {
  if (!Number.isFinite(stockTwh) || !cons?.size) return null;
  const winter = winterDemandRate(cons, asOf);
  const annual = annualDemand(cons);
  return {
    winterDays: winter ? stockTwh / winter.twhPerDay : null,
    winterTwhPerDay: winter?.twhPerDay ?? null,
    winters: winter?.winters ?? [],
    annualSharePct: annual ? (stockTwh / annual.twh) * 100 : null,
    annualTwh: annual?.twh ?? null,
    annualTo: annual?.to ?? null,
  };
}
