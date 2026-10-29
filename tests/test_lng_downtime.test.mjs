/**
 * Node.js test suite for LNG terminal downtime detector.
 *
 * Runs via built-in `node --test` with ZERO dependencies.
 * Asserts:
 * 1. Golden fixture agreement with expected outputs
 * 2. Multi-feed routing suppression (one feed at zero is not an outage)
 * 3. Posting gap discipline (unposted day is never counted as zero)
 * 4. Both feeds posting zero triggers real OFFLINE outage
 * 5. Pre-operational commissioning zeros produce single NOT_YET_OPERATIONAL span
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  detectDowntime,
  buildDailyTotal,
  cyclePriority,
  DOWNTIME_CONF,
} from '../docs/js/util/lng-downtime.js';
import { LNG_TERMINALS } from '../docs/js/util/lng-terminals.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test('Golden fixture agreement', () => {
  const fixturePath = path.join(__dirname, 'fixtures', 'downtime_golden_fixture.json');
  const raw = fs.readFileSync(fixturePath, 'utf8');
  const fixture = JSON.parse(raw);

  const events = detectDowntime(fixture.daily, fixture.config);
  assert.strictEqual(
    events.length,
    fixture.expected_events.length,
    `Expected ${fixture.expected_events.length} events, got ${events.length}: ${JSON.stringify(events)}`
  );

  for (let i = 0; i < fixture.expected_events.length; i++) {
    const expected = fixture.expected_events[i];
    const actual = events[i];
    assert.strictEqual(actual.type, expected.type, `Event ${i} type mismatch`);
    assert.strictEqual(actual.date, expected.date, `Event ${i} date mismatch`);
    assert.strictEqual(actual.duration, expected.duration, `Event ${i} duration mismatch`);
  }
});

test('Multi-feed routing: feed at zero while sibling covers is NOT an outage', () => {
  const conf = DOWNTIME_CONF.freeport;

  // 40 normal baseline days
  const daily = [];
  for (let i = 1; i <= 35; i++) {
    const dStr = i < 10 ? `2026-06-0${i}` : (i <= 30 ? `2026-06-${i}` : `2026-07-0${i - 30}`);
    daily.push({
      dateStr: dStr,
      value: 1500,
      posted: true,
      postedZero: false,
      feedsPosted: 2,
      feedValues: { 'Gulf South': 1000, TETCO: 500 },
    });
  }

  // 3 routing days: TETCO drops to 0, but Gulf South swings to 1,500
  for (let i = 6; i <= 8; i++) {
    daily.push({
      dateStr: `2026-07-0${i}`,
      value: 1500,
      posted: true,
      postedZero: false,
      feedsPosted: 2,
      feedValues: { 'Gulf South': 1500, TETCO: 0 },
    });
  }

  const events = detectDowntime(daily, conf);
  const offline = events.filter((e) => e.type === 'OFFLINE');
  assert.strictEqual(offline.length, 0, `Routing falsely flagged as OFFLINE: ${JSON.stringify(offline)}`);
});

test('Posting gap (did-not-post) is NEVER counted as zero', () => {
  const conf = DOWNTIME_CONF.freeport;
  const daily = [];

  for (let i = 1; i <= 30; i++) {
    const dStr = i < 10 ? `2026-06-0${i}` : `2026-06-${i}`;
    daily.push({
      dateStr: dStr,
      value: 1500,
      posted: true,
      postedZero: false,
      feedsPosted: 2,
    });
  }

  // Days 2026-07-01 and 2026-07-02 did not post at all (posting gap, omitted from daily)
  // Day 2026-07-03 posts normally
  daily.push({
    dateStr: '2026-07-03',
    value: 1500,
    posted: true,
    postedZero: false,
    feedsPosted: 2,
  });

  const events = detectDowntime(daily, conf);
  assert.strictEqual(events.length, 0, `Posting gap caused false event: ${JSON.stringify(events)}`);
});

test('Both feeds posting zero IS a real OFFLINE outage', () => {
  const conf = DOWNTIME_CONF.freeport;
  const daily = [];

  for (let i = 1; i <= 30; i++) {
    const dStr = i < 10 ? `2026-06-0${i}` : `2026-06-${i}`;
    daily.push({
      dateStr: dStr,
      value: 1500,
      posted: true,
      postedZero: false,
      feedsPosted: 2,
    });
  }

  // 3 consecutive days where both feeds post zero
  for (let i = 1; i <= 3; i++) {
    daily.push({
      dateStr: `2026-07-0${i}`,
      value: 0,
      posted: true,
      postedZero: true,
      feedsPosted: 2,
      feedValues: { 'Gulf South': 0, TETCO: 0 },
    });
  }

  const events = detectDowntime(daily, conf);
  const offline = events.filter((e) => e.type === 'OFFLINE');
  assert.strictEqual(offline.length, 1, `Expected 1 OFFLINE event, got ${offline.length}`);
  assert.strictEqual(offline[0].duration, 3);
  assert.strictEqual(offline[0].date, '2026-07-03');
});

test('Pre-operational commissioning zeros produce single NOT_YET_OPERATIONAL span', () => {
  const conf = DOWNTIME_CONF.plaquemines;
  const daily = [];

  // 25 days of zero flow during pipeline commissioning
  for (let i = 1; i <= 25; i++) {
    const dStr = i < 10 ? `2024-05-0${i}` : `2024-05-${i}`;
    daily.push({
      dateStr: dStr,
      value: 0,
      posted: true,
      postedZero: true,
      feedsPosted: 1,
    });
  }

  // Commercial operation begins on 2024-05-26
  for (let i = 26; i <= 30; i++) {
    daily.push({
      dateStr: `2024-05-${i}`,
      value: 300,
      posted: true,
      postedZero: false,
      feedsPosted: 1,
    });
  }

  const events = detectDowntime(daily, conf);
  const offline = events.filter((e) => e.type === 'OFFLINE');
  const preOp = events.filter((e) => e.type === 'NOT_YET_OPERATIONAL');

  assert.strictEqual(offline.length, 0, `Pre-operational zeros emitted OFFLINE: ${JSON.stringify(offline)}`);
  assert.strictEqual(preOp.length, 1, `Expected exactly 1 NOT_YET_OPERATIONAL span, got ${preOp.length}`);
  assert.strictEqual(preOp[0].duration, 25);
  assert.strictEqual(preOp[0].date, '2024-05-25');
});

test('Cycle priority order: later cycle supersedes earlier cycle', () => {
  assert.strictEqual(cyclePriority('best'), 1);
  assert.strictEqual(cyclePriority('timely'), 2);
  assert.strictEqual(cyclePriority('evening'), 3);
  assert.strictEqual(cyclePriority('evng'), 3);
  assert.strictEqual(cyclePriority('late'), 4);
  assert.strictEqual(cyclePriority('latec'), 5);
  assert.strictEqual(cyclePriority('id1'), 6);
  assert.strictEqual(cyclePriority('itrd1'), 6);
  assert.strictEqual(cyclePriority('id2'), 7);
  assert.strictEqual(cyclePriority('itrd2'), 7);
  assert.strictEqual(cyclePriority('id3'), 8);
  assert.strictEqual(cyclePriority('itrd3'), 8);

  // Hourly operational snapshots (id{HH}00) are placeholders and return 0
  assert.strictEqual(cyclePriority('id0900'), 0);
  assert.strictEqual(cyclePriority('id2300'), 0);

  // id3 > timely
  assert.ok(cyclePriority('id3') > cyclePriority('timely'));
  assert.ok(cyclePriority('best') < cyclePriority('timely'));
  // Genuinely nominated cycles always beat placeholder snapshots
  assert.ok(cyclePriority('timely') > cyclePriority('id2300'));
  assert.ok(cyclePriority('id3') > cyclePriority('id2300'));
});

test('Nameplate parity: DOWNTIME_CONF matches LNG_TERMINALS registry (Freeport red pending user decision)', () => {
  for (const [key, conf] of Object.entries(DOWNTIME_CONF)) {
    const reg = LNG_TERMINALS[key];
    if (!reg) continue;
    assert.strictEqual(
      conf.nameplate,
      reg.nameplate,
      `Nameplate mismatch for ${key}: DOWNTIME_CONF has ${conf.nameplate}, registry has ${reg.nameplate}`
    );
  }
});


test('Meter pinned at its floor is a real OFFLINE outage, not a DEPRESSED blip', () => {
  // Production case: Cove Point plant intake (10001-D) sat frozen at 15,000 Dth/d
  // (14.6 MMcf/d ≈ 2% of a 784 MMcf/d operating level) for 23 consecutive days
  // from 2026-09-19. Because the value is not exactly zero it escaped the
  // posted-zero branch and classified as a 16-day DEPRESSED event, and the
  // collapse dragged the 30-day baseline onto the floor so the recovery then
  // registered as a phantom ~50x RAMPING event. Verified against the live bundle
  // at docs/data/src.bhe.<hash>.json on 2026-10-28.
  const conf = DOWNTIME_CONF.cove_point;
  const daily = [];
  const day = (offset) => {
    const dt = new Date(Date.UTC(2026, 5, 1));
    dt.setUTCDate(dt.getUTCDate() + offset);
    return dt.toISOString().slice(0, 10);
  };

  // 40 healthy days at ~780 MMcf/d
  for (let i = 0; i < 40; i++) {
    daily.push({ dateStr: day(i), value: 780, posted: true, postedZero: false, feedsPosted: 1 });
  }
  // 23 days pinned at the 14.6 MMcf/d floor — posted, non-zero
  for (let i = 40; i < 63; i++) {
    daily.push({ dateStr: day(i), value: 14.6, posted: true, postedZero: false, feedsPosted: 1 });
  }
  // recovery to the prior operating level
  for (let i = 63; i < 80; i++) {
    daily.push({ dateStr: day(i), value: 780, posted: true, postedZero: false, feedsPosted: 1 });
  }

  const events = detectDowntime(daily, conf);
  const offline = events.filter((e) => e.type === 'OFFLINE');
  const depressed = events.filter((e) => e.type === 'DEPRESSED');
  const ramping = events.filter((e) => e.type === 'RAMPING');

  assert.strictEqual(offline.length, 1, `expected 1 OFFLINE, got ${offline.length}: ${JSON.stringify(events)}`);
  assert.strictEqual(offline[0].duration, 23, 'floor run must be reported at its true 23-day length');
  assert.strictEqual(offline[0].date, day(62));
  assert.match(offline[0].detail, /meter floor/);
  assert.strictEqual(depressed.length, 0, 'a floor-pinned meter must not be reported as DEPRESSED');
  assert.strictEqual(ramping.length, 0, 'recovery from an outage is not a ramp');
});

test('Fleet card surfaces a live outage from the data, and clears on recovery', async () => {
  // A low utilization bar cannot distinguish "running soft" from "shut in".
  // Cove Point at 2% of nameplate for 23 days must announce itself on the card.
  const { summarizeDaily } = await import('../docs/js/util/lng-fleet-data.js');
  const mk = (dateStr, value, postedZero = false) => ({
    dateStr,
    date: new Date(dateStr),
    value,
    posted: true,
    postedZero,
  });

  const healthy = [];
  for (let i = 0; i < 30; i++) healthy.push(mk(`2026-08-${String(i + 1).padStart(2, '0')}`, 780));

  const down = [...healthy, ...Array.from({ length: 5 }, (_, i) => mk(`2026-09-0${i + 1}`, 14.6))];
  const live = summarizeDaily(down, 750);
  assert.ok(live.outage, 'mid-outage day must report an outage');
  assert.strictEqual(live.outage.type, 'AT_FLOOR');
  assert.strictEqual(live.outage.duration, 5);

  const recovered = [...down, mk('2026-09-06', 784), mk('2026-09-07', 780)];
  assert.strictEqual(summarizeDaily(recovered, 750).outage, null, 'recovery must clear the outage');

  const trueZero = [...healthy, mk('2026-09-01', 0, true)];
  assert.strictEqual(summarizeDaily(trueZero, 750).outage.type, 'OFFLINE');
});
