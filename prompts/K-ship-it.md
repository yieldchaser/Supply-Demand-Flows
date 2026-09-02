# Implementation task: ship Section 8 for real, then make the panels provably consistent (P0, five parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/section8-audit` (head `cdf8ed3`) is close to mergeable and mostly verified:
Python **425 passed / 1 known failure**, node **6 passed / 0 failed**, the cycle rule settled,
Sabine and AIS recorded as negatives, the VG study computed and killed.

What remains is the gap between *correct on this branch* and *correct for a reader*. Section 8 is
live on the public site right now serving the **old** detector — the bundle is
`66c9d2c6`, generated `2026-09-01T18:18:06Z`, from before any of this work. Everything below is
about closing that gap and making it stay closed.

---

## How your report is used

Every number gets independently re-derived before anything is committed — commands re-run,
parquets re-read, tests re-executed. That has happened on every brief so far.

Last round this mostly worked: node 6/0, case 2 `dur=7`, case 3 `294,850 Dth` and the `latec`
decision all held up under re-derivation, and you corrected my `timely` figure rather than copying
it. That is exactly right, and it is the reason that work is now committed.

Where it still failed was the VG study: it reported `r=0.724`, `R²=0.524`, `RMSE 19.82` and
`n=51` while also stating the sandbox could not run subprocesses. Recomputed, the real figures are
**n=54, level r=+0.795 (R²=0.633), surprise r=+0.326 (R²=0.107), RMSE 38.52 vs 39.46 persistence**.
The verdict you reached was right; the evidence was invented, so the document could not be
committed as written and I had to recompute it.

**So the rule that matters most on this brief:** when a computation cannot run in your sandbox,
**say that up front and hand me the script instead.** A twenty-line analysis script I can execute
is worth more than an estimate, and it costs you nothing. "I could not run this; here is the exact
script and what it needs" is a complete, acceptable deliverable. An estimated number presented as
a measurement is not, and it is the only thing that has cost us rounds.

Paste what the terminal printed, unedited — encoding artefacts, warnings, failures included. If
your output disagrees with a number in this brief, **the output wins** and I want to see it; the
numbers here are measurements and they have been wrong before.

---

## Ground rules

**No git commands at all.** Not `add`, `commit`, `status`, `diff`, `log`, `show`. This sandbox has
destroyed the repository's `.git` twice during ordinary commit/checkout operations. Claude commits.

**Wide remit** inside these parts: pick designs and proceed, edit `docs/js/`, `publishers/`,
`scripts/`, `config/`, `.github/workflows/`, `tests/`, add modules, fix adjacent bugs (report each).
`docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens only,
`safeRender` on every panel, 390px mobile reflow, and paste the `docs/js/` TypeScript grep before
finishing.

Work in order. **Parts 1–3 must land.** Parts 4–5 are valuable but droppable; stop cleanly and say
where you stopped.

---

# PART 1 — the panel's own documentation is now fiction

`docs/js/panels/lng-terminal-downtime.js` carries a header block that is the first thing any
reader of this code sees. It still records the pre-audit world:

```
 *   - OFFLINE: consecutive days where total is a PAID ZERO (>=2 days for
 *     Freeport/Sabine; >=3 for Cove Point whose cargo zeros are routine).
 *   - CARGO_IDLE: Cove Point only — cargo-driven zeros are normal, NOT flagged
 *     as outages. (25 posted-zero days -> 5 CARGO_IDLE events, 0 OFFLINE.)
 *   Case 3 Plaquemines pre-gas: no curated data (loc 24301). Logic correct...
 *   Case 4 Cove Point 25 cargo zeros: 5 CARGO_IDLE, 0 OFFLINE — CORRECT
 *   Freeport: 7 events (3 OFFLINE + 4 RAMPING) over 1100 days — plausible.
```

Every one of those statements is now false. Cove Point reads the consolidated plant-intake meter
`cpl_sq_10001_d` and has **zero** zero-days, so there are no cargo zeros and no CARGO_IDLE events.
Plaquemines has 878 days of curated history and is configured. The event counts changed when the
cycle rule changed.

Rewrite the header to describe what the code now does, with the numbers the validator actually
prints. Include the cycle rule (hourly `id{HH}00` placeholders excluded, latest genuinely nominated
cycle wins) since that is the single most surprising thing about this file. **Do not carry forward
any figure you have not just seen printed.**

Then audit the rest of `docs/js/` for the same rot: comments and JSDoc asserting facts that the
audit has since overturned — Cove Point receipt sums, Sabine coverage claims, anything citing the
old validation. Report what you found and fixed.

# PART 2 — make it impossible for two panels to disagree

Section 8's classifier was extracted to `docs/js/util/lng-downtime.js` and is now testable. The
other panels still carry their data-shaping inline, which is how Sections 5, 7 and 8 were able to
disagree about what a terminal flowed in the first place.

**2a.** Extract the pure data-shaping functions from `lng-feedgas.js` (Section 5),
`lng-fleet-overview.js` (Section 7) and `lng-feed-substitution.js` (Section 6) into testable
modules under `docs/js/util/`, the way `lng-downtime.js` was extracted — free of D3 and the DOM, so
`node --test` can reach them. Rendering stays in the panels.

**2b.** Add a **cross-panel invariant test**: for a fixture bundle, the daily total each panel
computes for a given terminal must be identical. This is the guarantee that matters — if Section 5
says Freeport flowed 1,076,889 Dth on 2026-07-12 and Section 8 says something else, the dashboard
contradicts itself and no reader can tell which is right.

**2c.** Every panel must be wrapped in `safeRender`. Verify that is actually true for all of them
rather than assuming; a panel that throws during render takes out everything after it. Report the
list.

# PART 3 — verify the alerting end to end, then prove it will not spam

Feedgas alerting was added in an earlier round and has **never been verified against real data**.
It is the mechanism by which this observatory finally tells someone something, and an alerting path
that nobody has run is not a feature.

**3a. Replay it over history.** Run the alert logic in dry-run over the full curated history for
every configured terminal, and report **exactly how many alerts would have fired, of which type,
per terminal, per month**. Paste the list.

**3b. Judge the rate.** More than a couple of alerts per terminal per month is too many — the
reader mutes the channel and it protects nothing thereafter. If the rate is too high, the threshold
is wrong; fix it with the data in front of you and show the before/after counts. If it is zero
across the whole history, that is equally a problem: an alert that never fires has never been
tested. Say which you found.

**3c. Prove the safety properties** with tests: no network call in dry-run; missing Telegram
credentials degrade to a clean skip rather than a crash or a stack trace in CI; the dedup TTL
actually suppresses a repeat of the same event; a *new* event on a terminal with an active alert
still gets through.

**3d.** Paste the rendered message body for each alert type, as a reader would receive it.

# PART 4 — quantify Freeport's invisible gas

Freeport carries a UI caveat saying part of its supply arrives on the KMTP intrastate lateral,
which is not publicly posted. The caveat is honest but unquantified, and an unquantified caveat is
hard for a reader to price.

Freeport's nameplate is 2,100 MMcf/d. The observatory measures `gulf_south_sq_24329_d` +
`tetco_sq_79999_d`. Establish, from curated data:

- what those two feeds actually sum to across their overlapping history — distribution, not a
  single number: median, p10, p90, and the maximum sustained level,
- what that implies for the invisible remainder against nameplate, and how stable that gap is,
- whether the gap is consistent with a lateral of KMTP's known capacity or implies something else
  is also unmeasured.

Then sharpen the caveat with the number. **Do not remove or soften it** — the point is to make it
more precise, not more comfortable. If the data says the gap is larger than the caveat implies,
say so; that is a finding.

Mind the trap the audit already caught once: never mix `_sq_` and `_oac_` in a flow total, and use
the settled cycle rule.

# PART 5 — Corpus Christi cycle pinning, if there is room

The backlog notes that historical dated pulls are unsupported on Cheniere's platform and that the
scraper refuses rather than mislabelling — the right behaviour. What was never established is
whether `CC200221` (the consolidated Corpus delivery meter, ~2,750,000 Dth/d design) carries the
same cycle structure as the others now that the cycle rule is settled.

Check what cycles Cheniere actually publishes for Corpus and Creole Trail, whether the settled rule
applies cleanly, and whether any Cheniere series is being mis-selected today. Report; fix only what
is clearly wrong.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm. Threshold changes need cited evidence — Part 3b explicitly invites one,
   with the data shown.
2. **Never fabricate a number, a test result, or a command output.** If you cannot run it, say so
   and hand me the script.
3. **Never mix `_sq_` and `_oac_` in a flow total** — OAC is a residual, anticorrelated with TSQ.
4. **Do not remove UI caveats** about invisible gas. Part 4 sharpens Freeport's; Sabine's stands at
   ~31% with `docs/VERDICT.md` recording why it cannot currently improve.
5. **Twin-meter check before summing any two feeds**; plant-intake meter before summing feeders.
6. **`merge_into_curated` always** for curated writes.
7. **`series_id` carries every dimension** — `{prefix}_{sq|oac|design|opcap}_{loc}_{flow}_{cycle}`.
8. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
9. **Nameplates need FERC docket citations** in JSDoc.
10. **Confidence tiers unchanged in this brief** — only `high` survives publisher pruning and the
    agreement gate enforces registry headlines being `high`.
11. **No git commands at all.**
12. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
    `docs/js/` TypeScript grep, `node --test tests/*.test.mjs`. Known pre-existing and NOT yours to
    fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps in
    untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and
    `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — the rewritten header, the numbers you took from the validator run (pasted), and the
   stale-comment audit across `docs/js/`.
3. **Part 2** — the extracted modules, the cross-panel invariant test with its run, and the
   `safeRender` coverage list.
4. **Part 3** — the historical alert replay with counts per terminal per month, your rate verdict
   and any threshold change with before/after, the four safety tests, and the rendered bodies.
5. **Part 4** — the Freeport distribution figures, the implied invisible remainder, and the
   sharpened caveat text.
6. **Part 5** — Cheniere's cycle structure and whether anything is mis-selected today.
7. **The `docs/js/` TypeScript grep output.**
8. **Test output** — `pytest`, `ruff`, `mypy`, `node --test`. Full-suite counts from an actual run;
   baseline is **425 passed, 1 failed, 16 deselected** and node **6 passed, 0 failed**. Derive any
   difference rather than attributing it to the environment.
9. **Anything contradicting this brief.** Its numbers are measurements; if the repo disagrees, the
   repo is right.
10. **Anything noticed but not fixed**, and anything you fixed under the wider remit.

Leave everything uncommitted. Claude reviews the working tree and commits, then merges and verifies
the live deploy.
