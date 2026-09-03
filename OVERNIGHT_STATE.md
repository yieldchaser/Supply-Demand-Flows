> ## AUDIT 2026-09-03 (X) — READ THIS BEFORE ANYTHING BELOW
>
> Board after repairs: node **36/36**, pytest **448/0**, ruff **17**. All eight operational
> terminals now return data from all three panels — S5, S7 and S8 alike.
>
> **X is the best round so far, and the first where the parse check actually held.** All four
> touched files parse. §02's option (a) is right and the grep behind it was real. §03's parametric
> rewrite is genuine and the four named Freeport tests survived intact. §04's registry-parity test
> and §05's case normalisation are both correct — and §05 was correctly reported as *not* a bug.
>
> One correction: **the report claimed 12/12 green in the invariant file and Stage 0 green. It was
> 34/35 — the parametric test was red on `sabine_pass`**, Sec 5 780.49 against Sec 8 1560.98,
> exactly 2x. That is not a flaw in the new test; **the test found a real bug on its first run,
> which is precisely what it was built for.** `buildDailyTotal` iterated `conf.feeds` without ever
> checking the `context` flag, so Section 8 summed Sabine's NGPL context feed into the terminal
> total while Section 5 excluded it. Latent in production only because every
> `km_ngpl_sq_3592_d` value is 0.0 Dth; the moment NGPL posts real volume, Section 8's Sabine total
> would have inflated against every other panel.
>
> Fixing it surfaced a second defect in the same function: the context feed also counted toward
> `feedMinDates` / `expectedFeeds`, so a posting gap on a context feed would have wrongly suppressed
> the entire gas day from Section 8. Both had the one root cause. `sabine_pass` is the only entry
> carrying `context: true`.
>
> A named regression test now pins the intent with a context feed carrying a **non-zero** value,
> rather than relying on Sabine happening to have one. Proven red.

