# COIN-M account monitoring without order execution

Last updated: 2026-10-09 UTC.

The standard Binance COIN-M account monitor publishes a complete, sanitized
`CoinAccountSnapshot` on `<namespace>/viz_pubs/coin_account_snapshot` after each
successful `GET /dapi/v1/account`. This uses its existing CM wallet poll cycle
(default 5 seconds), account credentials, and configured account egress.
It does not poll public order rules or send trading instructions.

Viz subscribes directly in coin Exec environments. The separately maintained
coin frontend shows native-coin wallet balance, unrealized PnL, margin equity,
available balance, initial/maintenance margin, and actual positions in exchange
contracts. `notionalValue` is displayed in settlement coins. Asset amounts from
different settlement currencies are never summed. Active delivery positions
are visible as factual account exposure even though Exec supports perpetuals.
Strategy execution state remains a separate section from actual account data.

Each observation replaces the entire asset/position set, so closing or removing
a position clears it. Missing fields, invalid decimals, nonfinite values, and
fractional contract counts reject the snapshot; failed requests retain the last
good observation with its original timestamp. The browser warns after 30 seconds
without a fresh account snapshot. A missing snapshot is distinct from a valid
empty account. No account equity history or unitized NAV is persisted here.
The new observation channel supports STANDARD CM accounts; unified-account
observation is not synthesized from the standard snapshot schema.

This mode requires only `account_monitor`, Viz and Config. Keep `exec-pre-trade`,
`trade_engine`, and `trade_signal` stopped when the operator requires no orders.
Use only environment-local component wrappers, never `start-exec.sh` for this
monitoring-only setup. The monitor's existing user-stream session creation and
renewal do not submit orders. This configuration does not cancel independently
existing exchange orders.

## Position and NAV units

A contract has fixed USD face value C. N contracts conserve face F=N*C; at mark
P their coin exposure is F/P. Manager's order rules supply C (currently BTCUSD
100 USD and ETHUSD 10 USD). Exec targets use base-coin quantity, and conversion
to exchange contracts uses the same valuation reference as the position ledger,
then quantity-step/minimum filters. Consequently a fixed coin target is not a
fixed-contract target as prices move. The strategy ledger stores inverse USD
face and revalues base exposure, outstanding children and partial-fill progress
at a common reference. Missing marks or order rules block execution.

For a long, settled coin PnL is F*(1/entry - 1/exit); a short reverses the sign.
For example, 10 BTC contracts (F=1000 USD), bought at 50,000 and sold at 60,000,
yield 0.0033333333 BTC before fees, worth 200 USD at exit. FIFO matches face,
not the differing entry/exit coin quantities. Manager's existing factual NAV
reports USD-equivalent execution PnL, keeps realized amounts translated at each
close, and uses the latest fill for remaining-position marks. This does not
revalue retained realized coins or collateral as a complete account balance.
Estimated fees use each fill's face times Maker/Taker rate.

Complete account NAV additionally requires native-coin ledgers, mark prices,
funding/fees/transfers and flow-adjusted share accounting. It must not be inferred
from trade-only PnL. Manager's theoretical Kline model currently supports USD-M,
not COIN-M. These monitoring changes do not alter either NAV model.

## Validation

Account-monitor unit tests cover signed contract/native-coin units, full empty
snapshots, invalid data rejection, and separate delivery/hedged positions.
Browser fixtures exercise both native currencies, full snapshot clearing,
venue isolation and stale warnings at desktop/mobile widths.

## Current el01 deployment scope

On 2026-10-09 the operator corrected zy_group26 (`binance_exec_trade10`) and
zy_group29 (`binance_exec_trade11`) to Binance USD-M futures. Their earlier
COIN-M observations described the queried market only; subsequent USD-M checks
had already found nonzero positions. Both deployments now declare
`binance-futures` consistently in their private env's market fields, explicit
Viz config, Config server and Manager catalog. Source IDs, namespaces, listener
ports, credentials, account modes and trading IP bindings are retained.

Both accounts and bahll202210 (`binance_exec_trade01`) received the six current
Exec binaries and scripts, built locally from synchronized `arbmm` source
`8a263e04ed3190636ba1787ea65e9a9e69c45998`. The publish wrapper verified the
targets were stopped, verified SHA-256 and installed each file atomically.
Only account-monitor, Viz and Config component wrappers were started afterward;
all three accounts keep pre-trade, trade engine, signal and persistence stopped.
Trade01 was stopped before publication. No trading startup, order submission,
target publish, cancellation, leverage or exchange account-mode change was run.

The three authenticated gateways return the USD-M frontend and Config bootstrap,
HTTP 200 snapshots and WebSocket 101. A stopped pre-trade producer supplies no
fresh execution-state snapshot; this is not proof of empty account holdings.
Manager release `20261009T080509Z` reports all three accounts as USD-M.
52 other protected processes and all trading TOML hashes remained unchanged.
All 11 account-monitor and 6 Viz unit tests passed.

Each environment's `EXEC-RELEASE.json` records the current complete release and
supersedes earlier component-only manifests. The build's local Cargo.lock SHA-256
is `121321c8f88715f8189a56516d3915ef5e763d9aac3c067c87f31d636cfc00a5`;
concurrent CME/FR changes were retained and excluded from deployment commits.
Rollback binaries, scripts and private config are retained in each environment's
`backups/usdm_exec_manager_20261009T075516Z`. Manager's same-named backup holds
the PostgreSQL dump and process/config verification. Recoveries must honor the
operator's latest account-specific startup instructions below.

## Subsequent trade01 restart and xy_lxy21 preparation

On the operator's subsequent October 9 instruction, bahll202210
(`binance_exec_trade01`) resumed its USD-M execution using the existing six
`8a263e04` binaries. `scripts/start-exec.sh` passed each component's health check;
pre-trade, trade engine, persistence, account monitor, Viz and Config are running.
The signal generator remains stopped. Positions are ready and the Manager
timeline observed 40 post-restart fill records. Trade10/11 retain observation
services only. No trading TOML, target, leverage or account mode was changed.

The retired prc slot had historical orders, bindings and position snapshots.
Its order store is archived under
`/home/el01/binance_exec_trade05/archive/prc/persist_manager`, with the original
source ID retained in Manager history. Its old env.sh now blocks accidental
starts; protected old credentials/config remain in its replacement backup.
A fresh `/home/el01/binance_exec_xy_lxy21_05` deployment supplies xy_lxy21 at the
same trade05 gateway/ports, with its own source ID, namespace and Redis prefix.
All six `8a263e04` binaries and current scripts were published and hash-checked.
New credentials passed a read-only USD-M check. It has no inherited order data,
bindings or snapshots. At preparation, **all xy_lxy21 processes stayed stopped**,
as then required by the operator. No cancellation or account mutation was
submitted to the new account during preparation. The existing gateway checks
its new source ID. The subsequent activation below supersedes that stopped state.

Manager handles bahll202210 Earn through the explicitly chosen `.6` public
egress, retaining 8,000 USDT with a 2,000-USDT trigger. The first automatic
5,000-USDT round and the requested manual 34,879.04-USDT subscription both
completed at par with zero purchase fee. Automatic settings were restored after
the manual round. Kline remains on `.10`; normal trading bindings were retained.

## xy_lxy21 activated with c40 Follow

The operator subsequently authorized starting xy_lxy21, following `virtual01`
(`c40t12_group1`) at multiplier 116. Manager's Nginx API saved that configuration
before startup: its two 0.5-share bindings each became 58 shares, both complete
40-symbol target vectors reached the new source's Redis namespace, and the
durable publish queue drained. Account configure grants were explicitly given
to shaokai and dzy; their ordinary sessions verified both Viz snapshots and
Config bootstrap through `/exec_trade05/`.

The first start stopped at the missing new-source `pre_trade_risk_params` hash.
The maintained `scripts/sync_exec_risk_params.py` was published to the new env
and initialized only its empty risk hash with the standard five parameters:
10 live limit orders, 10 on each side, 400 orders/minute and 200 orders/10s.
Then synchronized `arbmm` `scripts/start-exec.sh` passed all six startup checks.
The existing `8a263e04` runtime binaries are unchanged; persistence, trade engine,
account monitor, pre-trade, Viz and Config are running. The signal generator
stays stopped. Startup's normal USD-M account-mode check and open-order
cancellation completed successfully.

Viz reported positions ready, both named strategies allocated and 12 displayed
position rows each. All 24 named-strategy rows and three system residual rows
completed the current target execution. A source-scoped Manager timeline observed
94 Maker fills after the successful start. All 62 other protected processes
remained unchanged. Trading IPs, leverage and account mode were not modified,
and retired prc history was preserved. The new env's `EXEC-RELEASE.json` records
the authorized running state and its corrected environment identity. Catalog,
risk and runtime recovery evidence is retained under
`backups/follow_c40_start_20261009T104355Z` in the new environment.
