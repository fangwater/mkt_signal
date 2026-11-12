# Gate FR ON incident and alignment, 2026-11-12

All times UTC. This records factual execution, including an operator error.

The four JP `gate_fr_arb01–04` environments were stopped through their local
scripts. Exchange orders were cancelled, and `/proc` plus exchange queries
confirmed no environment processes or open orders. Source IP assignments,
leverage and Redis risk settings were preserved. The deployed Rust binaries
were not replaced or restarted.

## Initial alignment error

The agent interpreted alignment as reducing excess holdings and preferred spot
sells. That interpretation was incorrect: the intended operation preserves spot
and adjusts futures. Execution was stopped after the user's correction. The
following 90 orders had already filled and were individually confirmed:

| Environment | Executed action | Base quantity |
|---|---|---:|
| 01 | spot SELL HANA | 27,985 |
| 02 | spot SELL BEAT | 6,232 |
| 04 | spot SELL HNT | 1,886.2 |
| 03 | spot SELL ON | 7,500 |
| 03 | spot SELL ASR / SAHARA / MYX / ARIA / DOOD | 10.1 / 965 / 77 / 17 / 2,015.06 |
| 03 | futures BUY SIGN / GPS / RAVE / NOM | 800 / 500 / 22 / 2,400 |

The final fill in that run was at 07:36:36. At 07:39:19–20, all four accounts
had zero open orders and their environments remained stopped. These fills were
not automatically reversed. In particular, 03 retained only 0.551875 ON spot;
a later large futures SELL there would have created a new net short.
`AGENTS.md` now explicitly requires futures-first alignment and explicit user
authorization before any alignment trade in spot.

## Corrected ON-only operation

Completed at 07:46:52 under the user's explicit ON-only instruction:
38 futures taker SELL orders, roughly 100 USDT per chunk, global minimum
exchange creation interval 5.079 seconds. All orders, fills and resulting
positions were confirmed. Spot inventory was unchanged in this corrected run.

| Environment | Spot ON | Futures base ON | Net ON |
|---|---:|---:|---:|
| 01 | 7098.93765 | -7090 | +8.93765 |
| 02 | 25511.984875 | -25510 | +1.984875 |
| 03 | 0.551875 | 0 | +0.551875 |
| 04 | 13422.26252596 | -13420 | +2.26252596 |

Each remaining difference was below one contract (10 ON). Total added shorts:
45,640 ON, 3,593.81 USDT; fees 0.718762 USDT. No other asset was traded in this
ON-only run. See `on_futures_result.json`.

## Remaining positions and root-cause evidence

The 07:50:09–12 audit, before the user's subsequent all-symbol futures alignment
instruction, found absolute per-asset differences totaling 2,805.66 / 14,425.49 /
21.83 / 3,620.99 USDT in 01 / 02 / 03 / 04. Do not net unrelated coins together.
There were 12 / 13 / 0 / 8 assets with at least 10 USDT difference. Full quantities
and venue minimums: `residual_before_futures_alignment.csv`.

Only ON has a proven scheduler cause: old paired-close BUY work exceeding the
remaining short repeatedly took priority over due SELL work after spot reopened.
Saved logs show the close rejection and queued SELL quantities; no liquidation
or ADL record was found for the ON episode. See `docs/delist_risk_server.md`.
Other assets' current differences do not prove the same cause. The env-local
stop wrappers use supervisor deletion, and the original runtime logs were no
longer present afterward; no remote log archive was located. The four persisted
order stores remain (approximately 6.51 / 24.92 / 0.263 / 0.777 GB), but they do
not directly preserve every local scheduler rejection. Local saved incident
extracts contain ON only. Do not claim all remaining coins share ON's cause.

## Code validation and deployment

The earlier fix `ffe3e563` corrects paired-close netting/scheduling and factual
FR exposure reporting. The additional guard blocks expanding FR admission after
three explicit failures over at least 30 seconds without a factual hedge fill,
and cancels expanding resting ArbOpen orders via normal reconciliation.
It excludes intentional model waiting and known sub-lot dust; unknown orders
stay reserved. Acknowledgements alone do not clear the guard. The mode check
uses the configured FundingArb deployment, not same-exchange venue identity.

Validation: root library 1,552 passed, 3 ignored; `cargo check --bin pre_trade`,
formatting and diff whitespace checks passed. No CTP code was executed.
Both Rust corrections remain undeployed during this operation.
