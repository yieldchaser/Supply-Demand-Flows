"""Measure real JSON.parse cost + peak heap for the live bundle.

Headless Chromium, 4x CPU throttling (CDP Emulation.setCPUThrottlingRate).
Metrics: performance.now() around JSON.parse of the bundle text, plus
performance.measureUserAgentSpecificMemory / JS heap via CDP when available.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

BUNDLE = Path("docs/data/bundle.json")

JS_HARNESS = """
async (b64) => {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const text = new TextDecoder().decode(bytes);

  // Warm-up parse (cold-start measured on first parse below)
  const t0 = performance.now();
  const obj = JSON.parse(text);
  const coldMs = performance.now() - t0;

  const t1 = performance.now();
  JSON.parse(text);
  const warmMs = performance.now() - t1;

  // Peak heap via repeated parses to force GC pressure, sampling memory
  let peakMB = performance.memory ? performance.memory.usedJSHeapSize / 1048576 : 0;
  const baseMB = peakMB;
  for (let i = 0; i < 3; i++) {
    const o = JSON.parse(text);
    if (performance.memory) {
      peakMB = Math.max(peakMB, performance.memory.usedJSHeapSize / 1048576);
    }
    if (i === 2) window.__keepalive = o; // hold one copy while measuring
  }
  if (performance.memory) {
    peakMB = Math.max(peakMB, performance.memory.usedJSHeapSize / 1048576);
  }

  return {
    bytes: text.length,
    rows: Object.values(obj.sources || {}).reduce((n, s) => n + (s.data ? s.data.length : 0), 0),
    coldParseMs: coldMs,
    warmParseMs: warmMs,
    heapBaseMB: baseMB,
    heapPeakMB: peakMB,
    hasPerformanceMemory: !!performance.memory,
  };
}
"""


def main() -> None:
    raw = BUNDLE.read_bytes()
    import base64

    b64 = base64.b64encode(raw).decode("ascii")
    print(f"bundle: {len(raw)/1_048_576:.1f} MB raw")

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        results = []
        for throttling in (1, 4):
            ctx = browser.new_context()
            page = ctx.new_page()
            cdp = ctx.new_cdp_session(page)
            cdp.send("Emulation.setCPUThrottlingRate", {"rate": throttling})
            page.goto("about:blank")
            res = page.evaluate(JS_HARNESS, b64)
            res["cpuThrottle"] = throttling
            results.append(res)
            # CDP-level heap metrics (more precise than performance.memory)
            try:
                cdp.send("HeapProfiler.enable")
                cdp.send("HeapProfiler.collectGarbage")
                metrics = cdp.send("Runtime.evaluate", {
                    "expression": "performance.memory.usedJSHeapSize",
                    "returnByValue": True,
                })
                print(f"  post-GC heap: {metrics['result']['value']/1048576:.0f} MB")
            except Exception as exc:
                print(f"  CDP heap metrics unavailable: {exc}")
            ctx.close()
        browser.close()

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    sys.exit(main())
