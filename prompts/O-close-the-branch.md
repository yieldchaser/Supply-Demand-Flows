# O / CLOSE THE BRANCH — four red tests and a crashing script

**Repo:** `yieldchaser/Supply-Demand-Flows` · **Branch:** `fix/section8-audit`, head `3bc6dc5`

Small and sharp. This branch has been one round from merging three times. Finish it, then stop.

---

## Evidence rules — read first

Last round's report claimed pytest 434/1, node 16/0 and a preflight PASS with a 100/100 rubric.
Re-run, the truth was **pytest 431/4, node 13/1, and preflight crashing in its first section.**

The `logs/` files were written by hand rather than produced by redirection — `logs/final-pytest.txt`
cited `tests/test_universe.py`, a file that does not exist in this repo, and reported `eia_supply`
at 7,238 rows against a real 468. Those files were not committed; fabricated evidence in the repo is
worse than none.

So, mechanically: **run the command with `>` redirection into `logs/`, then read the file you just
wrote, then quote from it.** Do not type out what you expect a command to print. Every log is
diffed against a re-run before anything merges, which is how the last three rounds were caught in
under two minutes.

If a command cannot run in your sandbox, say so — that is always acceptable and costs nothing.

## Ground rules

**No git commands at all.** Claude commits. Leave everything in the working tree.

---

## The four failures

Current, verified by me:

```
pytest: 431 passed, 4 failed, 16 deselected
  tests/test_classify_meters.py::test_build_universe_covers_expected_totals   (known, NOT yours)
  tests/test_coverage_guard.py::test_terminal_coverage_guard_against_curated_parquets
  tests/test_coverage_guard.py::test_coverage_guard_rejects_perturbed_flattering_claim
  tests/test_integrity.py::TestDivergence::test_cadence_scaled_health_recency

node --test tests/*.test.mjs: 13 passed, 1 failed
  tests/test_lng_downtime_render.test.mjs — fails to load
```

### O1 — the sidecar is missing a field

Both coverage-guard tests die on `KeyError: 'nameplate'`.

`scripts/load_registry.py` now correctly extracts all **9** terminals into
`config/terminals_registry.json` — that part is a real improvement over the 5 it managed before.
But the emitted records omit `nameplate`, which the guard needs as the denominator of every
coverage figure.

Add it, along with anything else the guard reads. Then make the sidecar's completeness *testable*:
a test asserting every field the guard consumes is present for all nine terminals, so the next
missing field fails loudly at the source rather than as a `KeyError` three layers away.

### O2 — preflight still crashes

```
UnicodeEncodeError: 'charmap' codec can't encode character '→' in position 56
```

It prints `→` into a cp1252 console. Fix it so it runs on this machine — reconfigure the stream
encoding, or use ASCII in the output. Your call; it must survive a plain `python scripts/preflight.py`
with no environment tweaks, because that is how it will be run.

Then run it and capture the real output. Expect the integrity board to show **five WARNs** on
documented historical gaps (`gulf_south 2026-08-27`, `baker_hughes 2026-01-02`,
`gasnom 2026-08-23..25`, `quorum 2025-03-25..27`, `cheniere 2026-08-25`) — those are honest and
already annotated. Decide whether preflight's verdict treats WARN as passing, and justify it in one
line. **Do not make it green by weakening a check.**

### O3 — the render test does not load

`tests/test_lng_downtime_render.test.mjs` fails at load, not at assertion. Find out why — most
likely an import that reaches D3 or the DOM through `lng-terminal-downtime.js` — and fix it so the
render seam is genuinely reachable from `node --test`. If the seam is not clean enough to test
headlessly, split it further rather than stubbing the DOM into existence.

### O4 — the divergence test

`test_cadence_scaled_health_recency` is your own new test and it fails. Either the per-source
`health_recency_days` implementation in `validators/integrity.py` is wrong or the test's expectation
is. Work out which, fix that one, and say which it was.

### O5 — the workflow question

`publish-dashboard.yml` runs preflight before building the bundle. Once O2 lands, decide: does a
preflight failure deserve to block publishing? An argument exists both ways — blocking protects
readers from a bad bundle, and not blocking keeps a stale dashboard from freezing on an unrelated
WARN. Pick one, implement it, state the reasoning in one line.

---

## Exit condition

```
python -m pytest -q -m "not network"   > logs/O-pytest.txt 2>&1     # 1 known failure only
node --test tests/*.test.mjs           > logs/O-node.txt 2>&1       # 14 passed, 0 failed
python scripts/preflight.py            > logs/O-preflight.txt 2>&1  # runs to a verdict
```

Paste all three from the files. Then **stop** — do not start new work. This branch merges on your
report.

## Non-negotiables

1. **When a guard fires, fix the cause.** No deleting, skipping or `xfail`-ing a test, no loosened
   assertion, no `continue-on-error`, no `# type: ignore`, no hand-edited `data/health/*.json`.
2. **Never fabricate a number, a test result, or a command output.**
3. **No git commands.**
4. Known pre-existing and NOT yours: ~35 mypy errors in `scrapers/base/playwright_client.py`, mypy
   gaps in `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and
   `test_build_universe_covers_expected_totals` (717 vs 719).

## Report

1. Diff summary — every file, one line each.
2. The three log files, quoted from disk.
3. For each of O1–O5: what was wrong, what you changed, which side was at fault.
4. Anything contradicting this brief.
