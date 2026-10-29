/**
 * Panel windows must be anchored on the newest observation in the data, never
 * on the wall clock. A bundle read long after it was built would otherwise
 * slide its window forward, drop real history, and change chart shape on
 * every redraw (balance, rigs, basin-share all filter by "last N years").
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { referenceDate, referenceYear } from '../docs/js/util/series.js';
import { formatPeriodKey } from '../docs/js/util/period.js';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');

const row = (iso, value = 1) => ({ period: new Date(iso), value });

test('referenceDate returns the newest period as a Date', () => {
  const got = referenceDate([row('2025-01-10'), row('2026-03-06'), row('2025-12-05')]);
  assert.ok(got instanceof Date);
  assert.equal(got.toISOString(), new Date('2026-03-06').toISOString());
});

test('referenceDate accepts Date and string periods alike', () => {
  const got = referenceDate([{ period: '2026-02-06', value: 2 }, row('2026-01-02')]);
  // A date-only string is a calendar day in local time (see parsePeriod), so
  // compare the day rather than an instant: `toISOString` of a local midnight
  // is the previous day in any timezone east of Greenwich.
  assert.equal(formatPeriodKey(got), '2026-02-06');
});

test('referenceDate spans several series and ignores junk', () => {
  const got = referenceDate(
    [row('2025-05-01'), null, { period: 'not-a-date', value: 1 }],
    [],
    [row('2026-07-31')],
  );
  assert.equal(got.getFullYear(), 2026);
});

test('referenceDate is null for empty or missing input', () => {
  assert.equal(referenceDate(), null);
  assert.equal(referenceDate([], []), null);
  assert.equal(referenceDate([{ period: 'garbage' }]), null);
});

test('referenceDate does not read the wall clock', () => {
  // A synthetic series in the past must still anchor on itself.
  const got = referenceDate([row('2019-06-07')]);
  assert.equal(got.getFullYear(), 2019);
  assert.ok(got < new Date());
});

test('referenceYear still agrees with referenceDate', () => {
  const rows = [row('2024-11-01'), row('2026-01-09')];
  assert.equal(referenceYear(rows), referenceDate(rows).getFullYear());
  assert.equal(referenceYear([]), null);
});

test('panel windows are not anchored on the wall clock', () => {
  for (const rel of ['docs/js/panels/balance.js', 'docs/js/panels/rigs.js', 'docs/js/panels/basin-deep.js']) {
    const src = readFileSync(join(root, rel), 'utf8');
    assert.match(src, /referenceDate/, `${rel} should anchor its window on referenceDate`);
    // The old pattern: build a cutoff from `new Date()` and subtract years.
    assert.doesNotMatch(
      src,
      /const\s+cutoff\s*=\s*new Date\(\)\s*;/,
      `${rel} still builds its cutoff from the wall clock`,
    );
  }
});
