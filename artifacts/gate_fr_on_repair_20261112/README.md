# Gate FR ON incident and alignment, 2026-11-12

All times UTC. This records factual execution, including an operator error.

The four JP `gate_fr_arb01–04` environments were stopped through their local
scripts. Exchange orders were cancelled, and `/proc` plus exchange queries
confirmed no environment processes or open orders. Source IP assignments,
leverage and Redis risk settings were preserved. The deployed Rust binaries
were not replaced or restarted during alignment. The subsequently authorized
publication and recovery are recorded below.

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
Saved logs show the close rejection and queued SELL quantities
(`on_scheduler_evidence.log`); no liquidation
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
Both Rust corrections were subsequently published in the recovery below.

## Remaining-symbol futures alignment and recovery

The user then authorized all remaining symbols, futures only. Completed at
08:25:38: 301 taker orders across 109 environment/symbol pairs, notional
20,778.840933 USDT and fees 4.1557681866 USDT. Maximum order 99.700080 USDT;
minimum exchange creation interval 5.136 seconds. These totals exclude the
preceding 38 ON orders and the erroneous initial spot-first run.

Order 36 (02 BEAT) initially returned an exchange ID but its immediate GET
returned 404. Execution stopped globally. Subsequent order, two taker fills and
actual position agreed on the full 130-contract fill; the order was never
resubmitted. Execution resumed from fresh account state after reconciliation.

At 08:26:07–10, all four accounts had zero open orders, and every non-settlement
spot balance exactly matched the baseline for the corrected futures-only run.
Remaining absolute per-asset exposure was 39.45 / 41.08 / 12.09 / 13.43 USDT.
Five otherwise quantity-eligible differences were retained: 01 BGSC -1.846 USDT,
02 BGSC +1.269 USDT (about 3.12% futures spread); 02 HANA +9.003, IDOL +5.720,
NAORIS +5.455 USDT (adding shorts would exceed current exchange risk limits).
Other residuals were below contract minimums or tiny assets without a contract.
01 AIN retained 91.0555475 units, about 10.05 USDT, below its 100-unit minimum.
Leverage/risk limits were not changed to force execution. See
`all_futures_result.json`, `all_futures_fills.csv`, and `final_positions.csv`.

After explicit user authorization, committed `arbmm` source `c2419da3` was
archived inside the active local worktree and all FR runtime binaries built
locally in release mode. Eight uploaded binary artifacts and the Delist page
were SHA-256 verified. Seven differing binaries per Gate environment were
atomically installed with remote backups; `trade_signal` was updated on disk
but never started. The restored services in each account are account monitor,
trade engine, pre-trade, persistence, Viz, Config and dashboard. Delist was
updated/restarted, retaining the live CTA configuration links. No environment
private configuration, CPU pin, source IP, ENI/EIP or route was changed.

All runtime process hashes matched the publication manifest. Persistence stays
on CPU15, other Gate services on housekeeping 0-5,48-95, and Delist on CPU28.
01's persistence sync listener initially encountered a transient occupied port;
after confirming 50040 was free, its env-local stop/start wrappers restored the
listener. Logs were preserved before this retry. The configured 50040/50050/50080
listeners were verified; 04 has no configured sync listener. No new listener
configuration was introduced.

At 08:30:58–08:31:03, an independent exchange audit confirmed all spot inventory
unchanged, all futures positions identical to the completed alignment snapshot,
and zero orders after recovery. Runtime verification at 08:33 confirmed seven
services per environment and no `trade_signal`. See `restored_exchange_audit.jsonl`,
`deployment_manifest.json`, and `deployment_verification.json`.

Delist's factual exposure and opening-state browser regressions passed using
the release candidate. The live exposure snapshots are fresh; 01/02 still flag
BICO/PUNDIX missing prices, and 04 flags BICO (EIGEN was also briefly unpriced
during startup). Their completeness remains unknown, not falsely healthy.
03 is complete. These pricing limitations are separate from alignment.

Remote rollback binaries and runtime logs are retained in each environment's
`backups/gate_fr_recovery_c2419da3_20261112/`. Local build and operation receipts
are under `data/ops/gate_fr_recovery_20261112/` (Git-ignored). Unrelated local
worktree changes were not included in the Rust build or this task's commits.

## Scheduler simplification after recovery

The separate paired-close queue was introduced by `bf3f5698` on
2026-11-09 05:29:24 UTC to allow paired exits when existing account exposure was
already over its opening limit. The separate queue and unconditional priority
were unnecessary for that fix. Saved ON logs first directly demonstrate the
blocking pattern at 2026-11-10 11:55:24 UTC; this is the earliest retained proof,
not a proven time of first occurrence.

The replacement keeps one pending-work collection. Only factual first-leg fill
increments enter it. Opposite unsubmitted quantities offset each other and
release their paired-close work credit without inventing exchange fills.
Submitted and unknown orders retain their borrowed quantities and reservations
until reconciliation. The fill-history ledger and trailing-stop metadata do not
schedule a second copy of pending work.

Scheduling selects executable work rather than enforcing strict FIFO. A close
with no remaining reducing inventory, insufficient lot size, or unavailable
rules cannot monopolize the path ahead of ordinary work. Force exits borrow
their own work and cap quantities to factual reducing inventory minus active
same-side orders. Validated Force/paired reductions bypass opening-capacity and
hedge-stall expansion checks; normal expansion remains checked. Failed Force
submissions preserve any ordinary query already awaiting a reply. Replies for
targets fully resolved by netting are discarded before borrowing other work.
Exchange minimums, unavailable prices, query deadlines and unknown-order
reservations still apply; this is not a promise of execution without market data
or exchange cooperation.

Regression coverage reconstructs old BUY 18 plus new SELL 845 with 10, zero or
fractional remaining shorts, leaving only SELL 827. Additional cases cover both
netting arrival orders, future work, same-direction work behind an unexecutable
close, aggregated close dust, partial fills, unknown reservations, trailing-stop
metadata, stale replies, and failed Force submissions. The fixture uses synthetic
FIL prices and rules to test the shared scheduler, not an exchange replay.

Force second-leg expansion-guard risk was introduced later by `030c37fa` on
2026-11-12 07:55:39 UTC, so it cannot explain the earlier ON incident. The Force
first-leg symbol-exposure gate predates this incident. Neither review finding
proves an actual liquidation or a blocked emergency close occurred online.

The user initially paused publication pending review of emergency closes. At
the end of that review, this scheduler change and the separately committed
missing-mark REST supplement `caac540d` were still unpublished. Subsequent
authorization and publication are recorded below.

Validation of the simplified scheduler: root library 1,570 passed, 3 ignored;
`cargo check --bin pre_trade`, `cargo fmt --check` and `git diff --check` passed.
The Sol review also identified a multi-lot unsubmitted-return edge case; its
fix and regression preserve all trailing metadata through intermediate queue
syncs and leave no temporary reservations behind. No CTP code was executed.

## Authorized publication of the simplified scheduler

On 2026-11-12, after explicit authorization, synchronized `arbmm` commit
`293f63523f551984ac0f356c844f1f4b52acbe2e` was exported inside the active local
worktree and all seven Gate FR binaries built locally in release mode. Uploaded
artifacts were SHA-256 verified, then atomically installed in `gate_fr_arb01–04`
(28 binary replacements). All seven non-signal services per environment were
restored through the env-local wrappers. `trade_signal` was updated only on disk
and remains stopped. No remote Cargo build or manual alignment trade was run.

Before stopping, 25 runtime log files per environment were preserved under
`~/gate_fr_arb0N/backups/gate_fr_publish_293f6352_20261112/`, together with rollback
binaries and wrapper execution logs. Supervisor deletion left old pre-trade
processes alive; the env-local wrappers detected and terminated those processes
before installation. Script differences were inspected and existing deployed
wrappers retained, including their listener settings. `env.sh`, trading/Viz
configuration, source IPs, CPU masks and persistence sync ports were preserved.

At 10:13 UTC, all 28 running service identities and binary hashes matched the
manifest. Persistence remains on CPU15 and the other Gate services on
`0-5,48-95`; existing 50040/50050/50080 sync listeners were present. All four
accounts had zero spot/futures open orders. Non-settlement spot equity and every
futures contract quantity exactly matched the pre-publication exchange snapshot.
Startup logs showed no panic, port collision or authentication failure pattern.

The missing-mark REST worker started in all four pre-trades with the default
30-second threshold. Logs confirm actual public-contract REST supplements,
including PUNDIX in 01/02. At 10:14 UTC, Viz exposure/risk snapshots were fresh
and Delist reported complete data for 01/02/03. Environment 04 retained one
incomplete-price warning: QTUM spot liability of `0.00000004` units, no futures
position. This is below the existing `POSITION_MARK_QTY_EPSILON = 1e-6`, so the
worker does not request its price. The warning was preserved and no dust trade
was submitted. At the 10:16 UTC follow-up, the QTUM warning had cleared with the
new snapshot and all four Delist accounts reported complete/fresh/ok data.
All 28 service PIDs remained unchanged throughout observation.

Receipts: `scheduler_deployment_manifest.json` and
`scheduler_deployment_verification.json`. Local build/source, wrapper comparisons
and full operation receipts are under
`data/ops/gate_fr_publish_293f6352_20261112/` (Git-ignored). Unrelated local changes
were excluded from the build and this publication.
