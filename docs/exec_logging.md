# Exec runtime logging

最后更新：2026-10-18 UTC

## el01 direct Binance market data

The `cta_exec` host now runs native `spread_pbs` in
`/home/el01/spread_pbs/binance-futures`; its former `sbbzs_bn_fu` ZMQ receiver
is stopped and removed from the supervisor. The environment-local
`scripts/stop_spread_bbo_zmq_sub.sh` retires that receiver explicitly.

`spp_bn_fu_bookticker` uses CPU1 and `spp_bn_fu_market` uses CPU2. The latter
has only derivatives enabled: trades and incremental books are disabled.
`SPREAD_PBS_SYMBOLS` is empty. USD-M symbol discovery includes every `TRADING`
contract returned by exchangeInfo, regardless of quote asset or contract type,
including USDC and dated USDT contracts. At cutover it found 793 USD-M
contracts. The standard Binance publisher also receives 20 COIN-M perpetuals
and shares the same venue IPC paths through its publisher pool; this changes
market data only, not any Exec order-routing market.

The existing consumers keep using
`spread_pbs/binance-futures/ask_bid_spread` and
`dat_pbs/binance-futures/derivatives`. Logs are bounded tmpfs files:
`/dev/shm/spread_pbs-1000/binance-futures-bookticker.log` and
`/dev/shm/spread_pbs-1000/binance-futures-market.log`, each retaining the current
2 MiB file and one previous file. These contain operational summaries, not a
durable record of every market-data payload.

Manage this deployment with its environment-local
`scripts/start_spread_pbs.sh` and `scripts/stop_spread_pbs.sh`; when invoking
through a non-login SSH shell, prepend `PATH=/home/el01/.local/bin:$PATH`.
The pre-cutover binary, scripts, configuration and private env file are backed
up remotely under `.deploy-backup/native-direct-20261018T080109Z/`.
The locally built release binary SHA-256 is
`46bf3402099216984794341431b903bd65a929a7abb57b254d6f4f95dfe2714a`.

The old receiver left orphan publisher slots despite having no discoverable
dead nodes. They were identified by publisher ownership PID `405428`, then
removed from these two services by exact node ID using
`iceoryx_remove_node_from_service`, preserving the live subscriber nodes.
Cutover artifacts and the read-only IPC probe are retained locally in
`artifacts/el01_native_spread_cutover/`. No Exec process was restarted.
The subsequent 30-second IPC observation received 156,443 BBO messages across
772 symbols (including all 38 active USDC contracts) and 173,752 derivatives
messages across 965 symbols. Derivatives covered mark price, index price,
funding rate and liquidation messages; their exchange-wide stream also emits
symbols outside the active BBO discovery set. All 45 monitored pre-existing
non-relay processes retained their PID and process start time.

## el01 Exec recovery

On 2026-10-18 UTC, the 14 enabled Binance Exec environments were recovered
sequentially through their environment-local scripts: trade01–04, trade06–14,
and `binance_exec_xy_lxy21_05`. Each restart covered trade engine, account
monitor and exec pre-trade; pre-trade startup canceled existing account orders.
Each environment had to publish a fresh ready snapshot before proceeding to
the next. Persist and Viz processes remained running. The retired trade05
environment was excluded.

Final verification found all 14 environments ready with all five runtime
components present and snapshots approximately one second old or fresher.
Read-only persistence inspection confirmed new exchange order updates and
fills in 11 environments; trade01, trade04 and trade13 were ready without new
orders during the observation window. No POV configuration was changed.

The separate Manager `cta_monitor` had crashed while truncating a long UTF-8
alert. Its character-boundary truncation fix passed all 29 monitor tests and
was built locally, SHA-256 checked, atomically installed and started through
`start_monitor.sh`. The deployed SHA-256 is
`c7b69295a3385e434431bb49d02930f4ce71a675e4daef1c3106a9326b936315`.
The final read-only monitor check retained 18 stale-order alerts from trade08
and no position alerts. Direct signed Binance order queries confirmed every
one of those 18 orders was already `CANCELED`; their historical persisted NEW
state was not rewritten. Recovery, fleet snapshots and exchange-status evidence
are retained in `artifacts/el01_exec_recovery/`.

## Logging levels

Default `RUST_LOG=info` keeps startup/shutdown, order failures, reconciliation,
strategy allocation and configuration changes visible. Frequent routine details
use DEBUG:

| Component | Routine details at DEBUG |
| --- | --- |
| BatchExec / ChaseExec | Target activation and sizing, target-only Redis updates, reload notifications; BatchExec residual coalescing and post-only retry details |
| Pre-trade | Successful balance checks, Binance typed submission parameters, snapshot requests, successful periodic risk refresh, maintenance and latency statistics |
| Account monitor | Normal Binance order/trade/balance/position/PnL/risk event details and USD-M account/wallet snapshot rows |
| Persist manager | Individual uniform-order/order-update/trade-update records, including unmatched records; successful Redis connections |
| Trade engine | Normal connection establishment and planned reconnects; healthy TCP summaries |
| Viz / PM forwarder | Healthy receive/forwarding statistics |

The standalone `viz_server` uses `crates/viz_server/src/subscribers.rs`; its
statistics follow the same levels as the library's `src/viz/subscribers.rs`.

BatchExec and ChaseExec parameter changes remain INFO. Viz and PM forwarding
windows with dropped messages are WARN. TCP summaries with disconnected,
paused or protected connections retain INFO, and existing TCP anomalies and
connection/order/persistence errors retain their severity. Log reduction does
not change account-event forwarding, order execution or durable order storage.

POV feed subscription/receive failures log the first warning immediately, then
at most once per minute per feed and failure kind with the number of suppressed
repetitions. Subscription retries still run at their existing five-second
interval; receive retries and trade-volume processing are unchanged.

For a focused diagnostic session, enable only the required module, for example:

```text
RUST_LOG=info,mkt_signal::strategy::batch_exec_strategy=debug
```

Avoid a global DEBUG filter during normal operation. These source changes take
effect when the corresponding newly built binaries are deployed and started;
editing source or process environment files does not change a running logger.

`scripts/start-exec.sh` masks authentication query parameters (`listenKey`,
`signature`, `apiKey`, `access_token` and `token`) before echoing startup logs,
alongside the existing JSON credential, key-preview and authorization masks.

## el01 rollout verification

On 2026-10-08 UTC, `stable_happiness` (`binance_exec_trade06`) was republished
with the `226f4e79` Exec runtime and the `eff3abd5` standalone Viz logging fix.
The existing Viz configuration received only the required
`servers.exec_pre_trade.venue = "binance-futures"` field. The previous runtime,
scripts and Viz configuration are retained in a private account-local release
backup. Publishing used locally built release binaries, checked SHA-256 and
atomic replacement; component start/stop used the environment's scripts.

A 60-second observation at 08:21–08:22 UTC recorded 37,349 new log bytes, all
from pre-trade: one INFO, 40 exchange-minimum warnings, 120 inactive-symbol
warnings and one rate-limited POV feed warning. Target activation, individual
persist records and healthy Viz statistics produced no log lines. Account
monitor, persist manager, trade engine and Viz produced no additional log
bytes in this window. No ERROR was observed. Viz, snapshot and Config returned
HTTP 200; WebSocket upgraded with 101. The account state was ready, with 165
rows and a 524 ms-old snapshot. The final Viz-only restart kept the five other
account process identities unchanged.

The host-level `cta_monitor` was still running an older deleted executable and
reported increasing decode failures after the Exec update. The installed newer
monitor returned `CTA monitor: OK` in read-only dry runs against the live stores.
Restarting only that monitor at 08:06:57 UTC made its running SHA-256 match the
installed binary; subsequent checks through 08:26 UTC had no decode alerts or
monitor errors. No Exec RocksDB records were repaired or backfilled.

The original host audit also saw concurrent changes to trade10/trade11's
Viz/Config processes and configuration; those changes were outside this rollout.
The remaining 48 protected process identities stayed unchanged in that audit.
