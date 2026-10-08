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
