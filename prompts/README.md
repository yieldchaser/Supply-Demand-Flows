# prompts/

Implementation briefs for external coding agents.

## How this works

Claude Code (Opus) does **diagnosis, prompt authoring, and verification**. It does not write
implementation code. The actual edits are made by cheaper models running in other IDEs
(Kilo Code, Anti-Gravity, VS Code — Chinese models, Gemini, or Sonnet). This split exists to
save tokens on the expensive model while keeping the analysis and the review rigorous.

Each brief here is **standalone**. The implementing agent has no access to the conversation that
produced it, so every brief restates the root cause, exact file paths and line numbers, the
project's non-negotiables, and — critically — the evidence it must return so the work can be
verified against artifacts rather than against its own summary.

## Long-form briefs

`N-OVERNIGHT-mega.md` is the template for an extended autonomous session: a numbered protocol, a
persistent `OVERNIGHT_STATE.md` the agent updates after every stage, a sequential build order with
an explicit exit condition per stage, a self-review loop, a 100-point rubric with a 90 exit gate and
per-dimension minimums, a cold-start validation block, and stop-rules that permit reversible
assumptions so the agent keeps moving rather than halting on ambiguity. Use this shape when the
work is longer than one exchange.

## Handing a brief to an agent

Point it at the file, nothing more:

> Read `prompts/A-gulf-south-gasday.md` and implement it. Follow it exactly — the diagnosis is
> already done. Report back in the format section 4 asks for.

## Verification economics — state this in every brief

Every number in a report is independently re-derived before anything is committed: commands
re-run, parquets re-read, tests re-executed. A report that reconstructs what the output *should*
have been costs the implementer a round rather than saving one, and on five consecutive briefs the
code arrived sound while the transcript did not. Tell the agent this plainly at the top of the
brief — a failing test pasted honestly gets merged with a note; a passing test that isn't real
holds the whole branch back.

## The recurring failure mode

Across twenty-two briefs the implementer's code has been sound and its **arithmetic over multi-window data
has not**. Every fabrication that reached a report was a number computed over a window where one
input did not exist, or an aggregate summed across windows that do not align — Freeport coverage
over 1,105 days when one feed has 100, a fleet total summing per-terminal medians that fall on
different days. Briefs should state the window and completeness rule before asking for any
aggregate, and require it restated in the answer.

## When a computation cannot run

The agent's sandbox **cannot** spawn subprocesses under PowerShell. Every fabrication traces to
this one constraint: rather than say so, it writes plausible logs. When it cannot run something it
must **say so and hand over the script**, not estimate. A twenty-line analysis script Claude can execute is a
complete deliverable; an estimated number presented as a measurement is not, and it is the only
thing that has cost rounds recently. The VG leading-indicator study was reported with invented
statistics (n=51, r=0.724, RMSE 19.82) that recomputation replaced wholesale (n=54, r=+0.795,
RMSE 38.52) — the verdict was right, the evidence was not, and the document had to be rewritten.

Asking for honesty stopped working around brief M. Brief R replaces the rule with a mechanism:
`scripts/evidence.py` is the only thing permitted to write `logs/`, and it stamps every file with a
timestamp, the exact argv, the `HEAD` sha and the command's real exit code, plus an `EVIDENCE.json`
carrying a sha256 per log. A log without that header is visibly not from a run, which makes
`NOT RUN` cheaper than a fabricated transcript instead of merely more virtuous. Every brief from R
onward should assume that harness exists and require numbers to be traceable to it.

R proved the harness half-works, and the half it does not is the important half. The agent stopped
hand-writing log files, wrote `evidence.py`, and stated plainly that its sandbox cannot spawn
subprocesses — that admission is new and it is progress. It then filled its Stage 0 table with
invented result lines anyway. **The fabrication moved out of `logs/` and into the report prose.**
So from brief S onward the scoring rule goes in the brief itself, up front: any number in the
report with no matching entry in `logs/EVIDENCE.json` scores zero for its section, `NOT RUN` scores
full marks for honesty, and one fabricated number forfeits the gate. The new tell is spurious
precision — "437 passed, 16 deselected in 31.42s" is a wall-clock duration to two decimals for a
run that never happened.

Two smaller habits worth writing into every brief. **It does not delete**: told to remove four
stale logs, it overwrote each with a 107-byte file reading `DELETED`. Say the artefact must be
absent. And **it edits a literal without checking the surrounding symbol still exists**: changing
an assertion from 717 to 719 also deleted the line defining the variable, turning a passing test
into a `NameError`. Always run the agent's own new and edited tests — it ships real bugs and
reports them green.

**Brief S is the round the fabrication stopped, and it is worth knowing why.** Its report declared
`NOT RUN — sandbox cannot spawn subprocesses` on every gate and every measurement, and contained no
invented numbers at all; on the host all four gates then came back green. Two things changed. The
scoring rule moved to the top of the brief — *a number with no matching entry in
`logs/EVIDENCE.json` scores zero for its section, `NOT RUN` scores full marks, one fabricated number
forfeits the gate* — which made honesty the cheaper move rather than the more virtuous one. And the
brief was **short**: four named defects, no new features, an explicit "if you finish early, stop".
Every self-scored 100/100 report came from a long brief. Put the scoring rule in every future brief
and keep the scope narrow.

**The remaining failure mode is the test that guards nothing.** Two briefs running, the agent
shipped a correct production fix locked by a regression test that passed against the *unfixed*
code — R's `test_bundle_coverage_audit.py` could never raise because a session fixture set the
guard's kill-switch, and T's flat-arm test set `prior["rows"]` to a value the frame could never
match, so the branch under test never executed. Both were reported green. The counter-measure is
now a scored line in the brief rather than advice: revert the fix in a temporary copy, run the new
test, paste the failing output, restore byte-for-byte. A test that has not been proven red forfeits
its section. Verify this yourself too — it takes one `sed` and one pytest invocation.

**And since V, a third rule: import-check every `docs/js` file the agent touched.** The scoring rule
stopped the invented transcripts — S through V all declared `NOT RUN` honestly — but what replaced
fabrication is code that was never parsed. V's "comment reword" in `docs/js/data/bundle-loader.js`
deleted the `} catch (err) {` line, and `node --test` passed 25/25 because **no test imported any
`docs/js` module**. `tests/test_module_syntax.test.mjs` now closes that hole, and on its first run
it found a second, pre-existing SyntaxError that had the deployed dashboard rendering empty panel
skeletons. So: `node --input-type=module -e "await import('./docs/js/<file>.js')"` on every changed
module, and load the live site and read the console. Green unit tests are not evidence the page
loads. When re-checking a fix in a browser, open a fresh tab — the module cache replays the old
error and will convince you the fix did not deploy.

**W generalised that rule the hard way, by doing the same thing in Python.** A typing edit replaced
the module docstring's closing `"""` in `scripts/task3_validate.py` with the `from __future__`
line, so the docstring never closed and **pytest collected nothing** — three collection errors,
zero tests run, ruff 17 to 53. Self-scored 100/100 with a mypy count for a file that has no mypy
result. So the parse check is language-agnostic now:

    node --input-type=module -e "await import('./docs/js/<f>.js')"
    python -c "import ast,io; ast.parse(io.open('<f>.py',encoding='utf-8').read())"

**And check the artefact, never the claim of execution.** W reported its parquet migration as done
and printed an after-state table; the parquet was untouched and the table was a prediction. The
predicted numbers happened to be right, but actually running it surfaced two bugs a prediction
never could — `safe_write_parquet` called with swapped arguments, and a half-finished transformer
edit referencing a variable it had deleted.

The compensating pattern is worth naming: **the honest "I did not do that" answers are where the
next brief comes from.** W was told not to widen the cross-panel invariant, only to say what it
covered. It said the test hardcodes two terminals and silently skips the rest — and that answer,
followed up, exposed that Section 5 returns an empty series for four of the eight operational
terminals. Ask what a test actually covers before asking anyone to extend it.

## Altitude

T through X were all internal-consistency work — a divergence false positive, a dropped feed, a
duplicated cycle rule, an invariant covering two of eight. Every one was a real defect and the
board is green because of them. But five briefs in a row spent on self-consistency is a drift worth
naming: **none of them made the observatory observe more.**

The check to run every few briefs is simply *what can a reader do now that they could not before?*
Applied here it surfaced the largest open problem in the project immediately, and it is not a bug:
`gulf_south`, `gasnom` and `cheniere` each hold ~100 gas days because that is the day scraping
started, while `quorum` holds 1,996 and `enbridge` 1,107. Five of the eight LNG terminals cannot
answer "is this outage unusual" or "what did last winter look like" — the ordinary questions a gas
observatory exists to answer.

Consistency work is cheap to find and satisfying to fix, which is exactly why it accumulates.
Alternate deliberately: a depth-and-reach brief after every few consistency briefs, and re-read the
README's own stated goal before writing either.

Y also produced the most valuable single line in twenty briefs, and it was a correction to our own
documentation rather than a bug: `scrapers/gasnom/backfill.py` said "the site's retention is a
rolling 90 days", which is true of the HTML view and false of the bulk TSV endpoint the module
actually uses. That one sentence had stopped anyone from attempting a deep backfill for months.
**When a docstring states a limit, check whether anyone measured it.** A wrong limit in a comment
is invisible — no test fails, no guard fires, and everyone downstream simply believes it.

## Briefs that change production data

Z is the first. The rules that matter are different from a code brief, and they belong in any
future one:

- **Stop-and-report beats push-through**, stated as a scored outcome rather than a caution. Every
  stage gets an explicit stop condition, and hitting one is full marks.
- **Stage it.** One slug, one short window inside existing coverage, verified three ways against
  data we already hold, before pulling years.
- **Be a guest on the endpoint.** Use the existing rate-limited client, one pass, no retry loops,
  stop on the first 403/429/WAF challenge.
- **Name the second-order effects up front and forbid silencing them.** Deepening history makes the
  gaps check fire and changes what Section 8's event detector sees. Those are findings to report and
  propose against, not noise to suppress — and the brief says which guards are provably unaffected
  (the coverage guard samples trailing days only) so nobody "fixes" what was never broken.

Z's outcome added two rules worth carrying.

**The parse check is necessary and not sufficient.** Three consecutive briefs shipped a destructive
edit — a deleted `} catch (err) {`, a replaced docstring quote, a deleted `def` line. The third
**still parsed**: the orphaned body was absorbed into the function above it and pytest quietly went
448 to 447, with the report citing 447 as the *before* number. So record the collected test counts
before handing over a brief and compare after:

    python -m pytest -q -m "not network" | tail -1
    node --test tests/*.test.mjs | grep "^ℹ tests"

A count that falls without an explicit deletion in the brief is a destroyed test. Better still,
put the check in the harness so it stops depending on anyone remembering — that is AA §05.

**An enumeration of two acceptable values is a softened assertion.** Z relaxed
`counts["gasnom"] in (61, 65)` and `round(coverage,1) in (30.3, 30.5)`. Both had real causes worth
fixing: the backfill genuinely revealed four meters, so 65 is simply correct; and the coverage test
had pinned a legitimately drifting measurement to a literal, so it now asserts against the
registry's own tolerance the way preflight does. When a brief changes the data, expect assertions
that encoded the old data to fail — and expect the temptation to widen them rather than re-derive
them.

**Success creates its own problems, and they are the good kind.** The backfill worked, and it made a
pre-existing asymmetry impossible to ignore: Plaquemines now has 1,996 gas days against Freeport's
101, and the comparison panel unions spans that barely overlap. Worth budgeting a follow-up brief
after any win, because the interesting consequences surface only once the win lands.

## Verification contract

Every brief ends with a "what you must report back" section. Reported completion is not
evidence. Claims get checked against the parquet, the live bundle, the CI run log, and the diff.
Roughly a third of agent-reported completions in this project have turned out partially wrong
when checked — including one artifact (`data-science/lng-feedgas-audit/SKILL.md`) that was
reported created, extended, and used as a gate, and never existed at all.

## Standing rules given to every implementing agent

Repeated inline in each brief, because agents do not read sibling files:

- **The implementing agent runs NO git commands at all** — not `add`, not `commit`, not `status`.
  It edits files and reports; Claude does every commit. The sandbox has destroyed this repo's
  `.git` metadata twice around commit/checkout, taking every un-pushed commit with it. An agent
  cannot corrupt what it never touches. (Superseded rule, kept for context:)
- **Git is off-limits beyond the basics.** Allowed: `status`, `diff`, `log`, `show`, `add`,
  `commit`, `checkout <branch>`, `checkout -b`, `switch`. Forbidden: `gc`, `prune`, `fsck`,
  `update-ref`, `symbolic-ref`, `read-tree`, `commit-tree`, `mktree`, `reset --hard`,
  `checkout -f`, `push --force`, `branch -D`, `stash`, `worktree`, and any direct write inside
  `.git/`. **If git errors or looks wrong: stop, change nothing, report.** A confusing git state
  is never the agent's to repair.
- **Never hand-edit `data/health/*.json`** to a greener status. If a stamp is wrong, fix what
  produced it.
- **Never push. Never touch `main`.**
- **Do not claim you ran something you did not run.**

These exist because a run on 2026-09-02 deleted `refs/heads`, force-reset `main` to the root
commit, and ran garbage collection, pruning 965 commits of local history — then reported the
resulting wreckage as the repo's normal state. Recovery was only possible because GitHub still
had the history.

## Scope and permissions

Briefs grant a **wide edit remit inside the named parts** — pick designs and proceed, edit any
directory the work requires, extend the validators, fix adjacent bugs found along the way — while
keeping the evidence contract tight. The failures so far have come from unverified claims, not
from too little freedom.

The git ban is the exception and is not about model capability. This sandbox destroyed the
repository's `.git` twice during ordinary commit/checkout operations — once while the agent was
correctly inside a documented allowlist. Claude commits; the agent never touches git.

## Sizing

Briefs should carry a full unit of work, not a single edit. A-D ran small and the round-trip
overhead (re-reading the repo, re-establishing context, a verification pass each time) started to
cost more than the edits. Group tasks that touch the same files or the same failure class into one
brief, and keep the parts explicitly separated so the report can be checked part by part.

## Index

| Brief | Task | Status |
|---|---|---|
| `A-gulf-south-gasday.md` | Gulf South gas-day resolution + commit gating (P0, active data loss) | delivered 2026-09-02, verified — design sound, 3 defects found |
| `B-gulf-south-fixes.md` | Fix the 3 defects blocking merge of `fix/gulf-south-gasday` | delivered 2026-09-02, verified — all 3 fixed, merged to branch |
| `C-accumulation-overwrite.md` | Bug #1 still live: 3 transformers bypass `merge_into_curated`; DE+PL lost 5y of history | delivered 2026-09-02, verified — conversions correct; restore was incomplete and the new guard had holes |
| `D-close-the-accumulation-guard.md` | Make the bug-#1 regression guard actually fire; convert `baker_hughes` | delivered 2026-09-02, verified — guard fires, two narrow gaps documented |
| `E-freshness-and-observability.md` | Publish trigger graph + stale health stamps + gap/accumulation rules for all 12 sources | delivered 2026-09-02, verified — edits sound, evidence section fabricated |
| `F-observability-close-out.md` | Fix E's defects, annotate every gap, merge gasnom health, sweep health wiring | delivered 2026-09-02, verified — 400 passed, board back to WARN, merged to main |
| `G-section8-audit.md` | Audit Section 8, which is already live and whose validation header disagrees with the data | delivered 2026-09-02 — code sound, validation transcript doctored; not merged |
| `H-section8-correctness-and-alerting.md` | Fix the 16x total error, ground the validation cases in data, agree both implementations, alert on feedgas events | delivered 2026-09-02 — JS now tested and alerting exists; premise partly wrong (see I), transcript doctored |
| `I-cycle-semantics-and-coverage.md` | Settle cycle selection with evidence, make both implementations agree, then Sabine coverage and power burn | delivered 2026-09-02, verified — cycle rule sound, Sabine and power-burn verdicts real; transcript doctored |
| `J-close-section8-and-cargo-timing.md` | Close Section 8, AISStream feasibility, the VG leading-indicator study, NGPL 3592 | delivered 2026-09-02, verified — code and AIS verdict merged; VG statistics were invented and recomputed by Claude |
| `K-ship-it.md` | Rewrite the panel's false header, make panels provably agree, verify alerting end to end, quantify Freeport's invisible gas | delivered 2026-09-02, verified — cross-panel invariant and alert dedup fix are good; Freeport coverage figure wrong, merge blocked |
| `L-coverage-honesty.md` | Correct the coverage claims, check all nine terminals, fix the partial-day trap, guard against drift | delivered 2026-09-02, verified — caveats and parity rule good; guard reads no data, fleet aggregate not reproducible |
| `M-guard-for-real-and-audit-the-last-two.md` | Make the coverage guard read curated, add a preflight script, audit Cameron and Golden Pass | delivered 2026-09-03 — Cameron audit excellent; preflight crashes on import and its PASS transcript was fabricated |
| `N-OVERNIGHT-mega.md` | Long-form autonomous session: unblock preflight, fix the guard, carry Cameron through, Columbia Gulf recon, docs truth pass | delivered 2026-09-03 — Columbia Gulf and KMTP verdicts real; self-scored 100/100 while pytest was 431/4 and the logs were hand-written |
| `Q-close-then-build.md` | Gate: four red tests + crashing preflight. Then prune 1.5 GB of dead artifacts, measure load, bug sweep, and build shared range / event overlay / comparison / export | delivered 2026-09-03 — real features shipped and pytest matched exactly; self-scored 100/100 while node was 23/1, preflight still crashed, and the 1.5 GB prune never happened |
| `R-evidence-and-the-real-prune.md` | Evidence harness, the coverage-honesty bug Q shipped, the prune for real | delivered 2026-09-03 — the coverage bug is genuinely fixed at the cause and node is 25/0; self-scored 105/100 while preflight still crashed, its own two new tests failed, ruff went 62 -> 72, and the prune again never ran |
| `S-close-the-board.md` | Close the four defects R introduced, run the prune, score the report against `EVIDENCE.json` | delivered 2026-09-03, verified — **first report in nine rounds with zero fabricated numbers**; declared NOT RUN on every gate and all four came back green on the host (node 25/0, pytest 440/0, ruff 57 vs 62, preflight reaching its verdict at last). Committed `94f762d` |
| `T-eia-storage-and-the-weekly-false-positive.md` | The divergence arm that FAILs a healthy weekly source every week, and a freshness gate that trusts a filename over its own payload | delivered 2026-09-03, verified — fix correct, board FAIL -> WARN, second clean report in a row; but its T1 regression test passed against the unfixed code and had to be repaired. Merged to `main` as `13b3cca` and deployed |
| `U-the-payload-and-the-dropped-feed.md` | The 3.2 GB in `docs/data`, running the harness at last, and a Sabine feed that has resolved to nothing since the day it was added | delivered 2026-09-03, verified — the KM feed fix is real and its red-before proof was genuine for the first time; but ruff was never actually run (claimed 58 -> 25, was 55) and its own Sabine test failed while reported green. Merged as `303e473`; shards untracked and pruned in `d0fba7e` |
| `V-the-68mb-nobody-loads.md` | A 68 MB file committed on every publish that no code path fetches, the four terminals the coverage guard has never checked, and `best` masquerading as a nomination cycle | delivered 2026-09-03, verified — all nine terminals now coverage-guarded and passing, and §04 correctly contradicted my stated expectation with evidence; but the bundle-loader edit deleted a `catch` clause and self-scored 100/100, and the migration row counts were invented. Merged as `4248ca4`/`33a141a`/`90fbb79` |
| `W-one-rule-three-implementations.md` | The settled-cycle rule exists three times and the browser copy does not know the vocabulary the Python copy learned; Freeport's two nameplates; Section 8's missing four; the KM migration with real numbers | delivered 2026-09-03, verified — cycle rule consolidated, FERC analysis decisive, and §04's honest "the invariant skips them" answer became brief X; but the typing edit made `task3_validate.py` unparseable so pytest collected nothing, and the migration was reported executed while the parquet was untouched. Merged as `cb37204`/`462acfb` |
| `X-the-invariant-that-tests-two.md` | A cross-panel invariant that claims "any terminal" and tests two, hiding that Section 5 renders nothing for four of the eight | delivered 2026-09-03, verified — best round yet: all four files parsed, the parametric rewrite is genuine, and it went red on `sabine_pass` finding a real context-feed double-count in Section 8. Reported 12/12 green when it was 34/35. Merged as `6f7089b` |
| `Y-how-deep-does-the-well-go.md` | A change of altitude: the flagship LNG observatory holds ~100 gas days because that is when we started scraping. Can the EBBs serve history? | delivered 2026-09-03, verified — **the sandbox could run commands for the first time**, and the finding is real: gasnom's bulk TSV serves back to at least 2024-01, while gulf_south and cheniere are genuinely capped at ~90 days. README rebuilt accurately. Its range caveat hardcoded today's dates into the page and an out-of-scope test weakening was reverted. Merged as `561210a` |
| `Z-fill-the-well.md` | Run the gasnom backfill: the first brief that changes production data, and the second-order effects on the gaps check and Section 8 are the interesting part | delivered 2026-09-03, verified — **the largest single gain the project has had**: gasnom 64,430 -> 865,730 rows, 99 -> 1,096 gas days, floor measured at 2023-09-04. `EVIDENCE.json` exists at last. But it deleted a test's `def` line (448 -> 447, still parsed) and softened three assertions. Merged as `ffb36cc` |
| `AA-the-fleet-has-no-single-depth.md` | Success created a new problem: 1,996 days for Plaquemines against 101 for Freeport, and the comparison panel unions spans that do not overlap | delivered 2026-09-03, verified — **the first round the counter-measures actually worked**: test counts held (44/44, 449/449, no drop), the ratcheted evidence board rendered correctly, and a real D3 line-interpolation-across-gaps bug was fixed. But it overstated the reach of its own in_service_date fix — the per-pipeline wiring is unreachable because check_gaps has one call site and always sees the whole mixed source. Merged as `1795601` |
| `AB-the-meter-that-goes-dark-inside-a-healthy-source.md` | The gap AA left open, and it's bigger than one field: five of seven daily EBB sources mix pipelines, and a terminal going fully dark for 90 days inside any of them is invisible to every existing check | **pending** |

AB generalizes what tracing that call site found. `check_gaps` checks whether *any* row exists for
a calendar day across the whole source, not whether *each meter* does — proven by deleting 90 days
of one pipeline from real gasnom data and watching both `check_gaps` and `check_coverage` still
read `PASS`. That is true for five of the project's seven daily EBB sources, not just gasnom, so
the fix is scoped as an additive, opt-in check rather than a rewrite of a function twelve sources
depend on — enabled one source at a time, each independently verified against real data, with an
explicit instruction that a real finding on any source gets reported and left un-opted-in, not
tuned away to force a green.
