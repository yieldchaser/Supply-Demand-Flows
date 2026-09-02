# Implementation task: coverage honesty, and the partial-day trap (P0, five parts)

You already know this repo — **Blue Tide**, `yieldchaser/Supply-Demand-Flows`.

Branch `fix/section8-audit` (head `09692b7`, tagged `[CAVEAT WRONG]`). Everything on it is verified
and ready except one thing, and that thing is why it is not merged: **a coverage claim that went
into user-facing text and is wrong in the flattering direction.**

Current state, re-run: **node 7 passed / 0 failed**, **python 428 passed / 1 known failure**. The
cross-panel invariant test, the extracted shaper modules, the rewritten Section 8 header and the
alert dedup fix are all good work and stay.

This brief is about the numbers this observatory *asserts to readers*. Its entire value proposition
is that it measures physical flow honestly and says plainly what it cannot see. A coverage figure
that is too generous is worse than no figure at all.

---

## How your report is used

Every number is independently re-derived before commit. Last round node and pytest counts matched
exactly, the alert-dedup bug you found was real, and you wrote the analysis as handoverable scripts
rather than estimating — all of that is why the work is committed.

The Freeport figures did not survive. Reported: median **1,682.5 MMcf/d**, **80.1% of nameplate**,
over **1,105 baseload days**, with a ~420 MMcf/d invisible remainder "matching KMTP almost exactly".
Recomputed under the settled cycle rule, SQ only:

```
days where both feeds post   100      (gulf_south starts 2026-05-25; tetco 2023-08-24)
baseload days (>=500)         99
p10   998.1   p25 1053.0   median 1111.5   p75 1586.4   p90 1646.2
max 1d 1674.3   max 30d rolling 1538.0
```

**Median coverage 52.9%, not 80%. Invisible remainder ~988 MMcf/d, not ~420.** The 1,105-day figure
was the tell — that is TETCO's history alone, and Gulf South only overlaps for 100 days of it.

The conclusion drawn from it breaks too: a ~990 MMcf/d gap cannot be explained by a 400–450 MMcf/d
KMTP lateral. That is Part 1's real question.

**When a computation cannot run in your sandbox, say so and hand over the script.** That worked
last round. What did not work was computing over a window where one feed does not exist and
reporting the result as a coverage rate.

Paste unedited terminal output. If your output disagrees with a number here, **the output wins** —
these are measurements and they have been wrong before.

---

## Ground rules

**No git commands at all.** Claude commits. This sandbox has destroyed the repository's `.git`
twice during ordinary commit/checkout operations.

**Wide remit**: `docs/js/`, `publishers/`, `scripts/`, `config/`, `tests/`. `docs/` rules apply —
vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`, 390px reflow, and
paste the `docs/js/` TypeScript grep before finishing.

Parts 1–3 must land. Parts 4–5 are droppable; stop cleanly and say where.

---

# PART 1 — Freeport: correct the caveat, then answer what the gap actually is

**1a.** Three caveat strings in `lng-feedgas.js`, `lng-fleet-overview.js` and
`lng-feed-substitution.js` now claim "~80% coverage" and a "~420 MMcf/d (~20%) invisible
remainder". Replace them with what the data says. The previous wording was unquantified but true;
do not regress to vagueness either — quantify it correctly.

**1b.** Then answer the question the real number raises. Against a 2,100 MMcf/d nameplate the
measured median implies ~988 MMcf/d unseen — far more than KMTP's ~400–450 MMcf/d lateral can
explain. Work out which of these it is, with evidence:

- **The denominator is wrong.** Nameplate is not what a terminal actually runs. Compare against
  realistic sustained utilisation instead — what do the visible feeds do on their best sustained
  30-day stretch, and what does that imply?
- **The window is wrong.** Only 100 days have both feeds. What does coverage look like restricted
  to days where *both* feeds posted, versus the full span?
- **Something else really is unmeasured.** If so, name what and say how you would test for it.

Whatever you conclude, the caveat must state the coverage figure, the basis it is computed on, and
the window. A reader must be able to tell whether "52.9%" means "of nameplate" or "of what the
terminal actually runs", because those are very different claims.

# PART 2 — every terminal's coverage claim, checked

If Freeport's was wrong, assume the others are until proven. The registry
(`docs/js/util/lng-terminals.js`) is full of hard-coded assertions — "102% of nameplate typical",
"~31% of the 4,500 MMcf/d nameplate", "1,408 MMcf/d", "~57% of CPL throughput" — none of which are
tested against the data they describe.

Here is what I measured, headline meter only, median of the last 60 posted days, under the settled
cycle rule (SQ only, hourly `id{HH}00` placeholders excluded):

| terminal | days | median MMcf/d | % nameplate | last day | % np |
|---|---|---|---|---|---|
| Plaquemines | 878 | 3820.9 | 112.4% | 3913.9 | 115.1% |
| Calcasieu | 1995 | 1605.8 | 123.5% | 1548.3 | 119.1% |
| Corpus | 101 | 2384.7 | 99.4% | 2644.3 | 110.2% |
| Sabine | 101 | 1365.2 | 30.3% | 1231.6 | 27.4% |
| Cameron | 97 | 1458.6 | 72.9% | 1426.2 | 71.3% |
| Golden Pass | 97 | 330.4 | 12.7% | 356.5 | 13.7% |
| Cove Point | 100 | 728.5 | 97.1% | 842.4 | 112.3% |
| Freeport | 1106 | 1061.7 | 50.6% | **209.2** | **10.0%** |

Reconcile every registry claim against this. Some hold — Plaquemines ~113% and Calcasieu ~122% are
close to the registry's figures, and Sabine's ~31% is right. Others have drifted: Cove Point is
documented as "102% of nameplate typical" and measures 97.1%; Sabine's headline is documented as
1,408 MMcf/d and measures 1,365.2.

For each of the nine terminals report: the registry claim, the measured value, whether they agree,
and what you changed. Where a claim is a *range* or a *typical*, say what tolerance you applied.
**Do not adjust a nameplate** — those carry FERC docket citations and are the denominator of every
utilisation figure in the project.

# PART 3 — the partial-day trap (this is the P0)

Look at Freeport's last day in that table: **209.2 MMcf/d, 10.0% of nameplate**, against a median of
1,061.7.

Freeport did not fall over. `gulf_south` curated ends 2026-09-01 while `enbridge` ends 2026-09-02,
so on the newest gas day **only one of the two feeds has posted**, and the total collapses to
whatever that one feed carries. Every multi-feed terminal has this on its newest day, every day,
until the slower source lands.

The consequences are live and they are bad:

- **Section 8** sees an enormous drop against baseline on the newest day, every day.
- **Feedgas alerting** would fire an `ACUTE_DROP` daily — an 87% drop against baseline on a
  terminal that is running normally. That is exactly the alert-fatigue failure the dedup work was
  meant to prevent, arriving through a different door.
- **Sections 5 and 7** render a cliff at the right-hand edge of every multi-feed chart.

The design already distinguishes *posted zero* from *did not post* for the outage logic. **The same
discipline has to apply to totals and to alerting.** A day where not all of a terminal's feeds have
reported is not a low-flow day; it is an incomplete day.

Fix it across the shared shapers so all four panels and the alert path inherit it. Decide and
justify the rule — candidates include suppressing incomplete days from the total series, marking
them and excluding them from baselines and alerting, or waiting for feed parity before the newest
day counts. State what you chose, what it does to each panel's right-hand edge, and what happens on
a day when a feed is genuinely down versus merely late — those must remain distinguishable, because
one is an outage and the other is not.

Then prove it: tests covering an incomplete newest day, a genuinely-zero feed, and a real single-feed
outage, in both the JS shapers and the Python alert path. Re-run the alert replay from the previous
round and report how many alerts this removes.

# PART 4 — an anti-rot guard for coverage claims

The reason a wrong figure reached user-facing text is that nothing tests the registry's assertions
against the data. Build that guard.

Add a test that recomputes each terminal's headline coverage from curated and fails when the
registry's documented figure drifts beyond a stated tolerance. Put the machine-checkable numbers
somewhere structured — a field on the registry entry rather than prose in a comment — so the test
has something to read, and keep the prose for the parts a number cannot express.

Choose the tolerance deliberately and say why: too tight and it fails on ordinary commissioning
movement (Golden Pass is at 12.7% and climbing), too loose and it would not have caught 80% versus
52.9%.

# PART 5 — the fleet aggregate

The project's headline claim is that it measures roughly **12,300 MMcf/d** of US LNG feedgas, about
64% of 19,050 MMcf/d of fleet nameplate. Recompute both from curated: sum the headline meters
across the nine terminals for a recent complete day (mind Part 3 — use a day where every feed has
posted), and compare.

Report the real figures. If the aggregate has moved materially — Plaquemines has been ramping
throughout — update the claim wherever it appears, with the date and the basis. If it holds, say so.

---

## Non-negotiables

1. **When a guard fires, fix the cause.** No demoting a meter, loosening a threshold, or removing a
   check to stop an alarm.
2. **Never fabricate a number, a test result, or a command output.** If you cannot run it, hand over
   the script.
3. **Never mix `_sq_` and `_oac_` in a flow total.**
4. **Never compute a coverage rate over a window where a feed does not exist.** That is the specific
   error this brief corrects.
5. **Do not remove UI caveats** — Part 1 sharpens Freeport's, Part 2 corrects the rest. Sabine stays
   at its measured share with `docs/VERDICT.md` recording why it cannot improve.
6. **Do not change any nameplate** — FERC docket citations, denominator of every utilisation figure.
7. **Confidence tiers unchanged**; only `high` survives publisher pruning and the agreement gate
   enforces registry headlines being `high`.
8. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
9. **No git commands at all.**
10. **Gates:** `pytest`, `ruff check` on Python files only, `mypy --strict` on new files, the
    `docs/js/` TypeScript grep, `node --test tests/*.test.mjs`. Known pre-existing and NOT yours to
    fix: ~35 mypy errors in `scrapers/base/playwright_client.py`, pre-existing mypy gaps in
    untouched `transformers/baker_hughes.py`, some ruff in `tests/test_gie_agsi_scraper.py`, and
    `test_build_universe_covers_expected_totals` (717 vs 719).

## What you must report back — unedited pasted output for every claim

1. **Diff summary** — every file changed, one line of reasoning each.
2. **Part 1** — the corrected caveat text, the distribution behind it, and your verdict on what the
   ~988 MMcf/d gap actually is: wrong denominator, wrong window, or genuinely unmeasured.
3. **Part 2** — the nine-terminal table: registry claim, measured value, agree or not, action taken,
   tolerance applied.
4. **Part 3** — the rule you chose for incomplete days and why; what it does to each panel's newest
   day; how a late feed stays distinguishable from a down feed; the tests; and the alert replay
   before/after count.
5. **Part 4** — the guard, where the machine-checkable numbers live, the tolerance and its
   justification, and proof the guard fails on a deliberately wrong claim.
6. **Part 5** — measured fleet aggregate and nameplate total, against the ~12,300 / 19,050 / 64%
   claim, with the date and basis.
7. **The `docs/js/` TypeScript grep output.**
8. **Test output** — `pytest`, `ruff`, `mypy`, `node --test`. Baseline is **428 passed, 1 failed,
   16 deselected** and node **7 passed, 0 failed**. Derive any difference.
9. **Anything contradicting this brief.** Its numbers are measurements; if the repo disagrees, the
   repo is right.
10. **Anything noticed but not fixed.**

Leave everything uncommitted. Claude reviews, commits, merges the stack, and verifies the live
deploy picks up the corrected detector.
