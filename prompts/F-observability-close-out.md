# Implementation task: close out observability — fix the defects, then finish the job (P0, six parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/freshness-observability` holds the previous round's work (commit `27955b9`, tagged
`[NOT MERGEABLE]`). **Build on it. Do not start over and do not revert it.** The workflow and
config edits in it are sound and verified:

- `publish-dashboard.yml` triggers on all twelve sources, every name matching the target
  workflow's real `name:` field.
- `kinder-morgan.yml` commits its health file.
- `quorum.yml` carries per-tenant health across the job boundary and merges before committing.
- `integrity.py` gained weekly/monthly gap cadences; `integrity_rules.yaml` gained
  `mode: accumulation` on four sources and `gap_rule` on twelve.

What was **not** sound was the verification. Two tests were failing while the report said the
suite was green, and the Part 3 gap table was written from stale context rather than from running
the checks. Parts 1–3 below fix that. Parts 4–6 are new work.

---

## Ground rules

### You may not run git — this is not about your capability

**No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`.

To be explicit, since this restriction has now survived several rounds: it is not a judgement
about the model doing the work. This *sandbox* has destroyed this repository's `.git` metadata
twice — `refs/heads` deleted, `main` force-reset to the root commit, 965 commits of local history
pruned — during ordinary `commit`/`checkout` operations, once while the agent was correctly
staying inside a documented allowlist. A faster or smarter model does not make the sandbox safe.
Claude does every commit, and if the working tree is intact that has always been enough to
recover. If you need a file's history or a prior version, ask for it in your report.

### Where you have more room than before

Previous briefs pinned you to a named file list. This one does not. Within the six parts below:

- **Decide and proceed.** Where a design choice appears, pick the one you can defend and record
  the reasoning in the report. Do not stop to ask.
- **Edit whatever those parts require** — `validators/`, `scrapers/`, `config/`,
  `.github/workflows/`, `tests/`. New modules and new test files are fine.
- **Extend the validator** rather than forcing a bad fit into what exists.
- **Fix adjacent bugs you find inside these areas**, provided you report each one separately with
  its own evidence. You no longer need to leave a real defect in place because it was unnamed.

Still off limits: `docs/`, any backfill or synthesis of data, hand-editing `data/health/*.json`,
and loosening any threshold without a cited external cadence.

### Evidence

The previous round reported a passing suite while two tests failed, and reported a gap table that
disagreed with the checker on four of five sources. So for this brief:

**Paste raw terminal output for every claim — the actual command and its actual output.** Not a
summary, not a reconstructed table, not a formatted rendering of what the output would say. If a
command was not run, say it was not run. A missing result is fine; an invented one is not.

---

# PART 1 — the two failing tests

Current state, which I ran:

```
FAILED tests/test_freshness_observability.py::test_publish_dashboard_trigger_graph
FAILED tests/test_integrity.py::TestShippedRules::test_ebb_sources_use_daily_thresholds
2 failed, 69 passed in 8.69s
```

### 1a. The `on:` / `True` YAML gotcha

`test_publish_dashboard_trigger_graph` does `content.get("on", {})`. YAML 1.1 parses the bare key
`on` as the boolean `True`, so that lookup misses and the trigger list reads as empty:

```
AssertionError: publish-dashboard.yml missing trigger for gulf-south-sq.yml ('Gulf South SQ (4× daily)')
assert 'Gulf South SQ (4× daily)' in []
```

**The workflow file is correct — the test is wrong.** Fix the test so it reads the trigger list
whichever way the loader represents that key, and make it fail loudly if the key cannot be found
at all, so this cannot silently degrade into an assertion over an empty list again. Check whether
any other test in the repo reads a workflow's `on:` block and has the same latent bug.

### 1b. The quorum `gap_rule` decision

`tests/test_integrity.py::TestShippedRules::test_ebb_sources_use_daily_thresholds` asserts:

```python
# quorum must NOT enforce calendar gaps: the tenant has retention holes.
assert "gap_rule" not in rules["sources"]["quorum"]
```

The previous round added `gap_rule: calendar_daily` to quorum anyway, which broke that test.
That assertion encodes a deliberate prior decision, so this needs resolving on the merits rather
than by deleting whichever side is inconvenient.

The evidence: with the rule on, quorum reports `3 missing calendar day(s) between 2021-03-15 and
2026-09-02: 2025-03-25, 2025-03-26, 2025-03-27` — three consecutive days in five and a half years
of history.

Decide: either quorum keeps the rule (and the test's assertion is updated, with the comment
rewritten to say why the earlier reasoning no longer holds), or the rule comes off (and the config
change is reverted). **Justify whichever you choose against the actual gap data**, and say
explicitly what those three days were — a real retention hole, an outage, or something else.

---

# PART 2 — re-run the gap report for real

The previous report's gap table disagreed with the checker on four of five sources. Ground truth,
from `python -m validators.run_integrity` on the current tree:

```
gulf_south       gaps  WARN  1 missing calendar day(s) ...: 2026-08-27
baker_hughes     gaps  WARN  1 missing weekly release(s) ...: 2026-01-02
gasnom           gaps  WARN  3 missing calendar day(s) ...: 2026-08-23, 2026-08-24, 2026-08-25
quorum           gaps  WARN  3 missing calendar day(s) ...: 2025-03-25, 2025-03-26, 2025-03-27
cheniere         gaps  WARN  1 missing calendar day(s) ...: 2026-08-25
```

Run it yourself, **paste the raw output**, and then produce the real deliverable: for **every**
gap listed, an explanation of what it is. Cross-reference `data/health/*.json`, the CI run
history where you can see it, and the source's own retention behaviour. Each gap gets one of:

- a genuine upstream posting hole (the source never published),
- a scraper outage on our side (we failed to collect something that existed),
- a legitimate non-posting day (holiday, terminal idle, source cadence),
- unknown, with what you would need to find out.

`2026-01-02` on `baker_hughes` and `2026-08-23..25` on `gasnom` are the interesting ones —
consecutive days suggest an outage rather than a cadence quirk.

**Backfill nothing.** The deliverable is an honest annotated list. Triage is a separate decision.

---

# PART 3 — the alarms the last round moved or lit without saying so

### 3a. `eia_supply` is now FAIL, and the whole board is FAIL

```
eia_supply  divergence  FAIL  DIVERGENCE: scraper 'ok' 22h ago but dataset degraded —
                              accumulation row count flat 4 consecutive runs at 468 rows
                              while 64d stale (warn 45d)
OVERALL: FAIL
```

This is a **new and probably correct** detection, produced by adding `mode: accumulation` to
`eia_supply` — the scraper reports success while the dataset has not moved in 64 days. It will
fail the integrity workflow on the next run.

Find out which it is: has EIA genuinely stopped publishing that series, is our scraper silently
fetching nothing, or is 64 days normal lag for it? Check EIA's actual publication calendar for
the series the scraper pulls. Then act on the cause — fix the scraper if it is ours, or adjust
the threshold **with the real cadence cited in a comment** if the lag is normal. Do not silence
it by removing `mode: accumulation`.

### 3b. `normalize_period` now moves a staleness alarm

The change to apply `month_end_normalize` to full ISO dates is defensible, and I want it kept:
`eia_lng_exports` sets `month_end_normalize: true` while its periods are full ISO (`2026-05-01`),
so the flag silently did nothing — config and code disagreeing quietly, the same family as every
other bug in the catalogue.

But it moves `eia_lng_exports` staleness from 124d to 94d, i.e. away from its FAIL threshold, and
it shipped with no test.

- Add tests covering both paths: a bare `YYYY-MM` period and a full ISO period, each with and
  without `month_end_normalize`.
- **Prove no other source's staleness changed.** Only `eia_lng_exports` and `eia_supply` set the
  flag today; show the before/after staleness for every source, not just the two.
- Note the alarm movement explicitly in the report.

### 3c. `merge_health` downgrades terminal-bad statuses

`scrapers/quorum/merge_health.py` does `_STATUS_RANK.get(status, 2)`, so any status not in the
map falls through to rank 2 (`warn`). `HealthWriter` really emits `fail` and `guard_failure`, and
both would be reported as a warning — a guard failure on one tenant quietly demoted.

Fix the ranking to cover HealthWriter's full vocabulary. Read `scrapers/base/health_writer.py`
for the authoritative list rather than guessing it. An unknown status must escalate, never
default to something milder. Test it.

---

# PART 4 — `gasnom`: three of four pipelines are invisible to the monitor

The previous round spotted this and left it; it is real and it is the same bug it had just fixed
for quorum.

`config/integrity_rules.yaml` points `gasnom` at a single slug's stamp:

```
gasnom    health_file: data/health/gasnom_goldenpass.json
```

but `.github/workflows/gasnom.yml` runs a matrix over four:

```yaml
slug: [goldenpass, cameron, SABINE, portarthurpipeline]
```

and all four write their own file (`data/health/gasnom_{slug}.json`). If Golden Pass succeeds
while Cameron, Sabine and Port Arthur all fail, `gasnom` reads healthy. Three quarters of the
source's health is invisible.

Fix it the way quorum was fixed — merge the per-slug stamps into one canonical
`data/health/gasnom.json` that the monitor reads, with the same escalation discipline (worst
status wins, unknown statuses escalate). The quorum merge and this one are now the same problem
twice; **factor out the shared logic** rather than copying it, and put it somewhere both can use.

Update `integrity_rules.yaml` to point at the merged file, and make sure the workflow commits it.

---

# PART 5 — sweep the health-file wiring

Two of twelve sources have now been found pointing the monitor at the wrong or an incomplete
stamp. Check all twelve rather than waiting for the third to surface.

For each source in `config/integrity_rules.yaml`, verify:

1. The configured `health_file` exists and is the file the scraper actually writes.
2. If the scraper runs as a matrix, the configured file represents **all** legs, not one.
3. The workflow commits that file.
4. The stamp is refreshed on successful runs, not only on failure.

Current `health_file` mappings, for reference — note `baker_hughes` → `baker_hughes_rigs.json` and
`eia_lng_exports` → `eia_lng.json`, where the config key and filename differ; confirm those are
correct rather than assuming:

```
gulf_south → gulf_south.json          gie_agsi → gie_agsi.json
baker_hughes → baker_hughes_rigs.json eia_storage → eia_storage.json
eia_lng_exports → eia_lng.json        eia_supply → eia_supply.json
gasnom → gasnom_goldenpass.json       quorum → quorum.json
bhe → bhe.json                        cheniere → cheniere.json
enbridge → enbridge.json              kinder_morgan → kinder_morgan.json
```

Report a table of all twelve with a verdict each, and fix what is broken.

---

# PART 6 — prove the publish path is safe under cancellation

The previous round chose `cancel-in-progress: true` on `publish-dashboard.yml`, on the reasoning
that a cancelled run cannot commit a partial bundle. That reasoning was argued rather than
demonstrated, and it is now load-bearing: with twelve triggers, cancellation will happen daily.

Establish it properly by reading the workflow:

- Can any step commit or push after a cancellation signal — is there an `if: always()` or
  `continue-on-error` anywhere on the commit path?
- Does the publisher write bundle files into the working tree **before** the commit step, such
  that a later run's `git add` could pick up a half-written bundle from an earlier cancelled run?
  (CI runners start clean, so reason about a same-runner sequence only if one is possible.)
- Is `export_dashboard_json` atomic in its own writes — does it write-then-rename, or write in
  place? Read `publishers/export_dashboard_json.py` and say which.

If anything on that path can commit partial output, fix it. If the path is genuinely safe,
say what you read that proves it, quoting the relevant lines.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing
   a check to stop an alarm. A threshold may change only with a cited external cadence.
2. **Never fabricate a number, a test result, or a command output.** This brief exists partly
   because a gap table was written from memory instead of from the checker.
3. **Never hand-edit `data/health/*.json`.** They are outputs.
4. **`merge_into_curated` always** for curated writes.
5. **RAW `Dth/d` in Python; convert only in frontend JS.**
6. **No git commands at all.**
7. **Gates:** `pytest`, `ruff check` on Python files only (it cannot parse YAML), `mypy --strict`
   on new files. Known pre-existing and NOT yours to fix: ~35 mypy errors in
   `scrapers/base/playwright_client.py`, pre-existing mypy gaps in untouched
   `transformers/baker_hughes.py` functions, some ruff in `tests/test_gie_agsi_scraper.py`, and a
   failing `test_build_universe_covers_expected_totals` (717 vs 719).
8. **Do not touch `docs/`.**

## What you must report back

With raw pasted output for each.

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — the two tests passing, with the actual pytest output; your quorum `gap_rule`
   decision and the evidence behind it; whether other tests share the `on:`/`True` bug.
3. **Part 2** — raw `run_integrity` output, then the annotated gap list, every gap classified.
4. **Part 3** — the `eia_supply` verdict with citation; before/after staleness for all twelve
   sources; the `merge_health` ranking fix and its test.
5. **Part 4** — the gasnom merge, where you put the shared logic, and how you tested it.
6. **Part 5** — the twelve-source health-wiring table with a verdict each.
7. **Part 6** — what you read on the publish path and what it proves, with quoted lines.
8. **Test output** — real `pytest`, `ruff`, `mypy` output. **Full-suite counts must come from an
   actual run**; derive any difference from the previous count rather than attributing it to
   environment variance. The last two rounds both got this wrong.
9. **Anything contradicting this brief** — the tables and command outputs above are measurements
   from the current tree. If the repo disagrees, the repo is right and I want to know.
10. **Anything noticed but not fixed**, and anything you fixed under the wider remit above.

Leave everything uncommitted. Claude reviews the working tree and commits.
