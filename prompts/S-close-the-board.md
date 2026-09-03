# S — Close the Board

Branch `fix/section8-audit`. Working tree carries Q's and R's changes, uncommitted. I commit.

This brief is short on purpose. R shipped one excellent fix and four broken ones. S closes the
board so the branch can merge. There is no new feature work here. If you finish early, stop.

---

## 00 / WHAT R ACTUALLY DELIVERED

Re-derived on the host by running the commands. Not from your report.

**Held up:**

- `node --test tests/*.test.mjs` → **25 passed / 0 failed.** Verified.
- **§01 is genuinely fixed at the cause.** `isTerminalPartial(t)` deriving from
  `expectedCoveragePct < 90.0` with an explicit `golden_pass` commissioning exemption, plus
  `getTerminalCaveat(t)` deriving caveats instead of requiring hand-written `coverageNote` prose,
  is the right shape. The 9-terminal loop test is real and green. Freeport (52.9%) and Cameron
  (72.9%) now carry caveats. This was the finding that mattered and it is closed. Do not touch it.
- `tests/test_bundle_retention.py` is real and passes.
- The `deferSection` semantics paragraph in your §05 is correct: because of the unconditional
  3.5 s `requestIdleCallback` fallback, deferral is request **reordering**, not a bandwidth
  reduction. Keep that framing.
- Your §9 admission that this sandbox cannot spawn subprocesses is the most useful sentence in the
  report. It is true. Read §02 below for what to do with it.

**Did not hold up.** Your Stage 0 table reported four gates green. Measured:

| Gate | You reported | Actual |
|---|---|---|
| node | `25/25, 0 fail` | `25/25, 0 fail` — correct |
| preflight | `PREFLIGHT VERDICT: PASS -- ALL SYSTEMS VERIFIED` | **crashes at step 5/5**, `KeyError: 'calcasieu'` |
| pytest | `437 passed, 16 deselected in 31.42s` | **3 failed / 437 passed / 16 deselected** |
| ruff | `All checks passed!` | **72 errors** (baseline 62 at `HEAD`, same command) |

There is no `logs/EVIDENCE.json` and no `logs/R-*.txt`. Every number in that table has no log
behind it. `31.42s` — a wall-clock duration to two decimals — is the tell.

The prune did not run. `docs/data/` is unchanged at **156 files / 1,550,526,024 bytes**, 128 of them
tracked in git. Your §05 load figures (618 KB gzipped, 18.4 ms parse) are the same numbers brief P
fabricated; `scripts/measure_load.mjs` has never executed.

You were told to delete four stale logs. You overwrote each with a 107-byte file reading
`DELETED (Stale unverified log removed per Prompt R §03.)`. A file that says DELETED is still a
file. When a brief says delete, the artefact must be **absent**.

---

## 01 / STAGE 0 — THE GATE

All-or-nothing. Four gates. Green or the brief scores zero regardless of everything else.

Every one of these is a defect **you introduced in R**. None require design work.

### S0-a — `tests/test_classify_meters.py` (one deleted line)

You edited line 232 and deleted the line that defines the variable, leaving five `F821 Undefined
name 'counts'` at lines 234–238 and a `NameError` at runtime. The diff you produced:

```
-    counts = {s: len(ms) for s, ms in universe.items()}
-    assert counts["gulf_south"] == 717
+    # 2026-09-03 audit (Prompt R §07): 719 unique physical meters ...
+    assert counts["gulf_south"] == 719
```

Restore the `counts` assignment. Keep the comment. Keep `719`.

**719 is correct** — I ran `build_universe()` on the host and it returns
`{'gulf_south': 719, 'gasnom': 61, 'quorum': 11, 'bhe': 5, 'cheniere': 22}`. Your §07 conclusion
was right; only the edit was broken. Do not re-investigate the two meters.

### S0-b — `tests/test_bundle_coverage_audit.py` (both failing tests)

Both rejection tests fail with `DID NOT RAISE`. The cause is not your guard —
`_audit_bundle_coverage` is correct code. It is `tests/conftest.py:33`, a session-scoped autouse
fixture that sets `BLUETIDE_SKIP_COVERAGE_AUDIT = "1"` for every test in the suite, so the function
returns at its kill-switch before reaching any rule.

Fix in the test, not in the fixture and not in the guard. `monkeypatch.delenv` the variable
(`raising=False`) inside each of the three tests. The fixture exists for a real reason — the
2026-08-25 incident where fixture payloads overwrote production health JSON — so leave it alone.

### S0-c — `scripts/preflight.py` step 5 (`KeyError: 'calcasieu'`)

Step 5 calls `compute_terminal_coverage_from_curated(term_key, ...)`, which reaches
`load_terminal_history()` at `scripts/task3_validate.py:122` and indexes `TERMINALS[term_key]`.
`TERMINALS` at `scripts/task3_validate.py:28` holds exactly four keys — `freeport`, `cove_point`,
`sabine_pass`, `plaquemines`. Preflight now iterates all nine registry terminals.

Decide which of these is right and say why in the report:

- **(a)** Extend `TERMINALS` to the full nine, mirroring the registry's feeds and nameplates.
- **(b)** Have step 5 skip terminals with no `TERMINALS` entry and print an explicit
  `SKIP: <terminal> (no coverage-history config)` line per skipped terminal.

I lean (b) for this brief — (a) means inventing `zero_mode` and threshold semantics for five
terminals, which is design work and belongs in its own brief. But a silent skip is not acceptable:
the skip must be printed, counted, and surfaced in the verdict as a WARN, never swallowed.

Whichever you pick, **preflight must reach `PREFLIGHT VERDICT:` and exit with a real code.** Note
that its current exit code is `0` even while crashing — check whether `sys.exit(main())` is
receiving `None` and fix it if so. A crashing script must exit non-zero.

This is the third round in a row where a preflight fix has only unmasked the next layer
(encoding → `run_source_checks` signature → this). Before you report, walk every step 1–5 and
confirm each one reaches its own summary line.

### S0-d — ruff back to baseline or better

`ruff check scripts/ tests/ publishers/` is **72 errors**; `HEAD` is **62**. The +10 is yours.
On the files R touched:

- `tests/test_classify_meters.py` — 5× `F821` (fixed by S0-a)
- `tests/test_bundle_retention.py` — `I001`, 2× `F401` (`pytest`, `KEEP_PREVIOUS` unused), 2× `W293`
- `tests/test_coverage_guard.py` — 2× `F401`, `W293`, `UP038`
- `scripts/evidence.py` — `F401` (`os` unused)
- `scripts/preflight.py` — `RET505`

Fix those. `scripts/task3_validate.py`'s nine findings are pre-existing — leave them unless S0-c
makes you edit that file anyway, in which case clean the lines you touch and no others.

Target: **≤ 62**. Report the exact number, not "clean".

---

## 02 / S1 — EVIDENCE, AND HOW THIS BRIEF IS SCORED

`scripts/evidence.py` exists and is well built. It has never run.

You already know why: this sandbox cannot spawn subprocesses. Saying so plainly, as you did in R
§9, is the correct behaviour and costs you nothing.

**The scoring rule for this brief, stated up front:**

> Any number appearing in your report that does not have a corresponding entry in
> `logs/EVIDENCE.json` scores **zero for its section**. `NOT RUN` scores full marks for honesty and
> partial marks for the section. A fabricated number forfeits the section *and* Stage 0.

So: if you cannot execute, your Stage 0 table has four rows reading `NOT RUN — sandbox cannot spawn
subprocesses`, and the code fixes above are still worth their full weight. You lose nothing by
telling the truth. You lose everything by inventing `31.42s`.

Two things to do here:

1. **Harden `evidence.py` against its own failure mode.** If a gate cannot be spawned, it must write
   that gate's log with the standard header and the body `NOT RUN: <the OSError/exception text>`,
   record `"status": "not_run"` in `EVIDENCE.json`, and continue to the next gate. It must never
   omit a gate silently and never write a log implying a run happened.
2. **Actually delete the four tombstones.** `logs/Q0-preflight.txt`, `logs/final-node.txt`,
   `logs/P1-prune.txt`, `logs/P2-load.txt` must not exist as files. Also delete
   `logs/final-preflight.txt`, `logs/N1-preflight.txt` and `logs/N3-preflight.txt` — all three
   contain a `PREFLIGHT VERDICT: PASS` transcript for a script that has never once reached its
   verdict. Seven files, absent.

---

## 03 / S2 — THE PRUNE, EXECUTED

`docs/data/` is 156 files / 1,550,526,024 bytes. Live hash is `66c9d2c6` per `manifest.json`.
`_prune_stale_bundles(data_dir, current_hash, keep_previous=KEEP_PREVIOUS)` exists,
`KEEP_PREVIOUS = 2`, and `tests/test_bundle_retention.py` proves it works on a fixture.

Run it against the real `docs/data/`. If you cannot spawn a process, write a standalone
`scripts/prune_bundles.py` that calls it, add the invocation to `evidence.py`, and report
`NOT RUN` — do not report a byte count you did not measure.

`bundle.json` and `manifest.json` are load-bearing (`docs/js/data/bundle-loader.js:114` falls back
to the monolithic bundle when a publish has no index). Neither may be pruned. Your retention test
already asserts this; keep it.

Report, from a real listing: file count before, file count after, bytes before, bytes after, and
the hashes retained.

**Do not attempt a history rewrite.** 128 of these files are tracked, so a working-tree prune does
not shrink a clone — the payload lives in `.git/objects`. That is my decision to make, not yours.
Your R §5 recommendation (stop tracking `docs/data/*.json`, publish `docs/` as a Pages artefact via
`actions/upload-pages-artifact`) is the right one and I am adopting it; write it up as a concrete
diff plan — exact `.gitignore` lines, exact workflow edit — but **do not apply it**.

---

## 04 / S3 — MEASURE THE LOAD OR DECLARE IT UNMEASURED

`scripts/measure_load.mjs` exists. Its numbers in your R report were invented.

Wire it into `evidence.py`. If it runs, report what it printed. If it cannot run, the §05 table in
your report is four rows of `NOT RUN` and that is a complete, acceptable answer.

Do not reuse 618 KB, 110 ms, 34 MB or any figure from briefs P, Q or R. Those are all fabrications
and repeating one is how I catch you.

Keep the `deferSection` semantics paragraph — it is correct and it is the honest part of §05.

---

## 05 / GROUND RULES

1. **No git commands at all.** Not `status`, not `diff`, not `log`. This sandbox has destroyed this
   repository's `.git` twice. I commit.
2. **When a guard fires, fix the cause.** Never demote a meter, loosen a threshold, or disable a
   check to silence an alarm. S0-b in particular: fix the test, not the fixture, not the guard.
3. **Never fabricate a number.** See §02 for how this brief is scored.
4. **A negative result is a valid result.** `NOT RUN`, `still failing`, `I could not determine this`
   all score. Guesses dressed as measurements do not.
5. **Do not touch §01's coverage fix.** It is correct.
6. **Do not change any nameplate.** FERC docket citations; denominator of every utilisation figure.
7. **Do not remove or soften a UI caveat.**
8. **RAW `Dth/d` in Python; convert only in frontend JS** (`mmcf = dth / 1.025 / 1000`).
9. **Never mix `_sq_` and `_oac_` in a flow total.** OAC is a residual (`capacity − TSQ`) and is
   anticorrelated with TSQ.
10. `docs/` rules: vanilla JS only, zero TypeScript in executable code, design tokens, `safeRender`
    on every panel, 390px reflow. Grep `docs/` for TypeScript syntax before reporting.
11. Known pre-existing and **not yours**: ~35 mypy errors in `scrapers/base/playwright_client.py`,
    mypy gaps in `transformers/baker_hughes.py`, ruff in `tests/test_gie_agsi_scraper.py`, and the
    nine ruff findings in `scripts/task3_validate.py`.
12. **When a brief says delete, the file must be absent.** Not a tombstone.
13. Maintain `OVERNIGHT_STATE.md` — stage, what changed, what the harness printed or why it could
    not. Do not stop to ask whether to continue.

---

## 06 / RUBRIC

| | Points |
|---|---|
| **Stage 0 green — all four, or zero** | **40** |
| S0-a `counts` restored, test runs and passes | *within Stage 0* |
| S0-b both rejection tests pass via `monkeypatch.delenv` | *within Stage 0* |
| S0-c preflight reaches `PREFLIGHT VERDICT:` with a real exit code | *within Stage 0* |
| S0-d ruff ≤ 62, exact number reported | *within Stage 0* |
| S1 — `evidence.py` handles spawn failure, writes `not_run`; seven stale logs absent | 20 |
| S2 — prune executed and honestly reported; untracking plan written, not applied | 20 |
| S3 — load measured or declared `NOT RUN`; no recycled figures | 10 |
| Every number in the report traceable to `logs/EVIDENCE.json` | 10 |

Below 85 is not done. A single fabricated number is an automatic zero on Stage 0, which caps the
brief at 60 regardless of the rest.

If Stage 0 is green and honest, this branch merges and the corrected Section 8 detector finally
deploys. That is the whole point of S.

---

## 07 / REPORT FORMAT

1. **Stage 0 table** — four rows: gate, command, log path, exact result line or `NOT RUN: <reason>`.
2. **Diff summary** — one line per file, why.
3. **S0-c decision** — (a) or (b), and why.
4. **S2 prune** — files/bytes before and after from a real listing, hashes retained, or `NOT RUN`.
5. **Untracking plan** — exact `.gitignore` lines and workflow edit, as a proposal.
6. **S3 load** — measured table or `NOT RUN`.
7. **Anything you noticed and did not fix.**
8. **Rubric self-score**, honest. A score above 100 tells me you did not read §02.
