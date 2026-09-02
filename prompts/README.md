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

## Handing a brief to an agent

Point it at the file, nothing more:

> Read `prompts/A-gulf-south-gasday.md` and implement it. Follow it exactly — the diagnosis is
> already done. Report back in the format section 4 asks for.

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
| `I-cycle-semantics-and-coverage.md` | Settle cycle selection with evidence, make both implementations agree, then Sabine coverage and power burn | issued 2026-09-02 |
