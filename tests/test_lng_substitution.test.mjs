/**
 * Feed substitution: a share flip at a stable total is routing; a day when
 * every feed falls and the total collapses is a supply event. Before, the
 * supply branch sat behind the stable-total filter and could never fire, and
 * the routing/supply label only read the sign of the mover.
 */
import assert from 'node:assert/strict';
import { it } from 'node:test';
import { detectSubstitutionEvents } from '../docs/js/util/lng-substitution-data.js';

const feeds = [{ label: 'A' }, { label: 'B' }];
const rec = (i, a, b) => {
  const total = a + b;
  return { date: `2026-06-${String(i + 1).padStart(2, '0')}`, total, A: (a / total) * 100, B: (b / total) * 100, A__val: a, B__val: b };
};

it('a share flip at constant total is routing, whichever feed gains', () => {
  const rs = [rec(0, 900, 100), rec(1, 700, 300), rec(2, 900, 100)];
  const ev = detectSubstitutionEvents(rs, feeds);
  assert.equal(ev.length, 2);
  assert.ok(ev.every((e) => e.kind === 'routing'));
});

it('every feed down and the total collapsed is one supply episode', () => {
  const rs = [];
  for (let i = 0; i < 8; i++) rs.push(rec(i, 900, 100));
  rs.push(rec(8, 300, 40), rec(9, 250, 30), rec(10, 900, 100));
  const ev = detectSubstitutionEvents(rs, feeds).filter((e) => e.kind === 'supply');
  assert.equal(ev.length, 1, 'two consecutive days are one episode');
  assert.equal(ev[0].date, '2026-06-09');
  assert.ok(ev[0].totalChg < -60);
});

it('one feed down while the other holds is not a supply event', () => {
  const rs = [];
  for (let i = 0; i < 8; i++) rs.push(rec(i, 500, 500));
  rs.push(rec(8, 100, 520));
  assert.equal(detectSubstitutionEvents(rs, feeds).filter((e) => e.kind === 'supply').length, 0);
});
