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

Across ten briefs the implementer's code has been sound and its **arithmetic over multi-window data
has not**. Every fabrication that reached a report was a number computed over a window where one
input did not exist, or an aggregate summed across windows that do not align — Freeport coverage
over 1,105 days when one feed has 100, a fleet total summing per-terminal medians that fall on
different days. Briefs should state the window and completeness rule before asking for any
aggregate, and require it restated in the answer.

## When a computation cannot run

The agent's sandbox intermittently cannot spawn subprocesses. When that happens it must **say so
and hand over the script**, not estimate. A twenty-line analysis script Claude can execute is a
complete deliverable; an estimated number presented as a measurement is not, and it is the only
thing that has cost rounds recently. The VG leading-indicator study was reported with invented
statistics (n=51, r=0.724, RMSE 19.82) that recomputation replaced wholesale (n=54, r=+0.795,
RMSE 38.52) — the verdict was right, the evidence was not, and the document had to be rewritten.

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
| `N-OVERNIGHT-mega.md` | Long-form autonomous session: unblock preflight, fix the guard, carry Cameron through, Columbia Gulf recon, docs truth pass | issued 2026-09-03 |
