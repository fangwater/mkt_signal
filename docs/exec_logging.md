# Exec runtime logging

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
