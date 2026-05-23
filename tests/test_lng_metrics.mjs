import assert from 'assert';
import { 
  dth_to_mmcf, 
  get_utilization_level, 
  calculate_wow_delta, 
  should_show_envelope 
} from '../docs/js/util/lng-metrics.js';

console.log('Running LNG metrics JS tests...');

// 1. test_dth_to_mmcf (920149 → 897.7)
const mmcf = dth_to_mmcf(920149);
console.log(`920149 Dth to MMcf = ${mmcf}`);
// 920149 / 1.025 / 1000 = 897.70634
assert.ok(Math.abs(mmcf - 897.7) < 0.1, `Expected ~897.7, got ${mmcf}`);

// 2. test_utilization_buckets (43% → normal/green; 95% → saturation/red)
const lowUtil = get_utilization_level(30);
assert.strictEqual(lowUtil.level, 'low');
assert.strictEqual(lowUtil.colorClass, 'utilization-gray');

const normalUtil = get_utilization_level(43);
assert.strictEqual(normalUtil.level, 'normal');
assert.strictEqual(normalUtil.colorClass, 'utilization-green');

const highUtil = get_utilization_level(80);
assert.strictEqual(highUtil.level, 'high');
assert.strictEqual(highUtil.colorClass, 'utilization-amber');

const satUtil = get_utilization_level(95);
assert.strictEqual(satUtil.level, 'saturation');
assert.strictEqual(satUtil.colorClass, 'utilization-red');

// 3. test_wow_delta_missing_returns_dash
const rowsByDateEmpty = {};
const resultEmpty = calculate_wow_delta(898, 'id3', '2026-05-22', rowsByDateEmpty);
assert.strictEqual(resultEmpty.valueText, '—');
assert.strictEqual(resultEmpty.deltaProps, null);

// Also test when same cycle exists
const rowsByDateWithHistory = {
  '2026-05-15': { id3: 800 }
};
const resultHistory = calculate_wow_delta(880, 'id3', '2026-05-22', rowsByDateWithHistory);
assert.strictEqual(resultHistory.valueText, '+80 MMcf/d');
assert.deepStrictEqual(resultHistory.deltaProps, { value: '+10.0%', kind: 'bullish' });

// 4. test_envelope_skipped_when_insufficient_history
assert.strictEqual(should_show_envelope(29), false);
assert.strictEqual(should_show_envelope(30), true);
assert.strictEqual(should_show_envelope(10), false);
assert.strictEqual(should_show_envelope(365), true);

console.log('All LNG metrics JS tests passed successfully!');
