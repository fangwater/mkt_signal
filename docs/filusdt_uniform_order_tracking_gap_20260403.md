# FILUSDT Uniform Order Tracking Gap Investigation (2026-04-03)

## Scope

This note records the current query findings for the `FILUSDT` order-tracking gap seen in the `binance_mm_alpha` export for trade date `2026-04-03`.

Primary question:

1. Were there `FILUSDT` orders in `uniform_orders.parquet` that had `NEW` but no terminal status?
2. Did the suspected issue happen near the last `open` batch around `2026-04-03 13:54:07 UTC`?

Data source used:

- `/home/ubuntu/binance_mm_alpha/20260403/uniform_orders.parquet`
- `/home/ubuntu/binance_mm_alpha/20260403/order_updates_unmatched.parquet`
- `/home/ubuntu/binance_mm_alpha/20260403/trade_updates_unmatched.parquet`

Relevant code references:

- `uniform_orders.parquet` contains `client_order_id`, not exchange `order_id`: [src/persist_manager/parquet.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/persist_manager/parquet.rs#L216)
- terminal order states are `FILLED`, `CANCELED`, `EXPIRED`, `EXPIRED_IN_MATCH`: [src/signal/common.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/signal/common.rs#L204)

## Query Conclusion

Current best answer:

- Yes, there is a real tracking gap in `uniform_orders.parquet` for `FILUSDT`.
- The problematic batch is not the last `open` batch at `2026-04-03 13:54:07 UTC`.
- The actual `NEW`-without-terminal batch is earlier, at `2026-04-03 13:46:05.348xxx UTC`.
- That batch contains `10` `client_order_id`s.
- All `10` affected rows are `hedge` orders.
- All `10` affected rows are `SELL @ 0.840`.
- In `uniform_orders.parquet`, each of these IDs has only one row with status `NEW`.

Important negative finding:

- The `FILUSDT open` batch around `2026-04-03 13:53:07 UTC` and `2026-04-03 13:54:07 UTC` is normal.
- Those `open` orders all show `NEW -> CANCELED` in `uniform_orders.parquet`.
- So the suspected issue should not be anchored on `13:54:07 UTC`; that is only the last `open` batch before `16:00:00 UTC`, not the missing-terminal batch.

## Affected Batch

Shared `from_key`:

```text
hedge|1775223964404371:ret_score=0.00018521:ret_thr=0:vol=0.00097360:env_score=0:env_thr=0:tlen=146193.00000000
```

Affected `client_order_id`s:

| client_order_id | UTC first seen | Beijing first seen | side | price | amount_init | statuses in `uniform_orders` |
| --- | --- | --- | --- | ---: | ---: | --- |
| 6581245784287610412 | 2026-04-03 13:46:05.348316 | 2026-04-03 21:46:05.348316 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610420 | 2026-04-03 13:46:05.348474 | 2026-04-03 21:46:05.348474 | SELL | 0.840 | 119.0 | `NEW` |
| 6581245784287610419 | 2026-04-03 13:46:05.348482 | 2026-04-03 21:46:05.348482 | SELL | 0.840 | 119.0 | `NEW` |
| 6581245784287610418 | 2026-04-03 13:46:05.348487 | 2026-04-03 21:46:05.348487 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610416 | 2026-04-03 13:46:05.348492 | 2026-04-03 21:46:05.348492 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610415 | 2026-04-03 13:46:05.348549 | 2026-04-03 21:46:05.348549 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610413 | 2026-04-03 13:46:05.348555 | 2026-04-03 21:46:05.348555 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610417 | 2026-04-03 13:46:05.348559 | 2026-04-03 21:46:05.348559 | SELL | 0.840 | 238.0 | `NEW` |
| 6581245784287610421 | 2026-04-03 13:46:05.348567 | 2026-04-03 21:46:05.348567 | SELL | 0.840 | 119.0 | `NEW` |
| 6581245784287610414 | 2026-04-03 13:46:05.348722 | 2026-04-03 21:46:05.348722 | SELL | 0.840 | 238.0 | `NEW` |

## Nearby Non-Issue Batch

The last `FILUSDT open` batch before `UTC 2026-04-03 16:00:00` starts at:

- `2026-04-03 13:54:07.707578 UTC`
- `2026-04-03 21:54:07.707578 Beijing`
- example `client_order_id`: `6581339075272245249`

That nearby `open` batch is not the broken one.

Observed behavior for the `13:53:07` and `13:54:07` `open` batches:

- `from_key` prefix is `open`
- statuses are consistently `NEW|CANCELED`
- terminal `CANCELED` arrives around `13:55:07 UTC` to `13:56:07 UTC`

So these `open` batches do not explain the tracking gap.

## Cross-Check Against Unmatched Exports

Additional cross-check performed:

- `07:03 UTC` had another `FILUSDT` `NEW`-only batch in `uniform_orders.parquet`
- those IDs do appear in `trade_updates_unmatched.parquet` with `order_status=FILLED`
- that earlier batch therefore looks like a `uniform_orders` terminal-publication gap rather than total exchange-state loss

For the `13:46 UTC` batch listed above:

- no matching follow-up rows were found in `order_updates_unmatched.parquet`
- no matching follow-up rows were found in `trade_updates_unmatched.parquet`

This makes the `13:46 UTC` batch the highest-priority candidate for the actual tracking / reconcile failure.

## Current Working Interpretation

At this point, the strongest current statement is:

- local uniform tracking for this `hedge` batch stopped at `NEW`
- no terminal state was exported into `uniform_orders.parquet`
- unlike the `07:03 UTC` case, there is also no fallback evidence in the unmatched order/trade parquet exports
- the concrete gap to explain is the `2026-04-03 13:46:05.348xxx UTC` `hedge` batch, not the later `13:54:07 UTC` `open` batch

## MM PT Log Findings

Searched file:

- `/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404`

All `10` affected orders are explicitly sent at `2026-04-03T13:46:04Z`:

- see lines `168675` to `168684`

Representative log slice:

- [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:168675)

The runtime then splits into two classes.

### Class A: 9 orders hit `ws open failed`, then lose local tracking

Affected IDs:

- `6581245784287610412`
- `6581245784287610413`
- `6581245784287610414`
- `6581245784287610415`
- `6581245784287610417`
- `6581245784287610418`
- `6581245784287610419`
- `6581245784287610420`
- `6581245784287610421`

Observed sequence from logs:

1. order sent
2. `ws open failed: req_type=4013 status=400 code=-2013`
3. `ws order update missing local order`
4. for a subset, `ws cancel failed ... local=missing`
5. later, `trade update order missing`

Representative lines:

- open failed: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:168689)
- missing local order after that: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:168707)
- later trade update still missing local order: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:173113)

### Class B: 1 order stays locally tracked, then is force-retired

Affected ID:

- `6581245784287610416`

Observed sequence from logs:

1. order sent
2. no `ws open failed` line was found for this ID
3. `ws cancel failed ... tracked=true ... local=present ... status=Create exch_ord_id=Some(36622207281)`
4. `force_terminal_start reason=cancel_reconcile query still create (max attempts)`
5. `mark hedge order terminal`
6. much later, `trade update order missing`

Representative lines:

- cancel failed while still locally present: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:168708)
- force terminal: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:169056)
- mark terminal: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:169058)
- later trade update missing: [mm_pt_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260404:173117)

Per-ID summary from the grep results:

- `6581245784287610412`: `send`, `open_failed`, `ws_order_missing`, `trade_missing`
- `6581245784287610420`: `send`, `open_failed`, `ws_order_missing`, `trade_missing`
- `6581245784287610419`: `send`, `open_failed`, `ws_order_missing`, `trade_missing`
- `6581245784287610418`: `send`, `open_failed`, `ws_order_missing`, `cancel_failed`, `trade_missing`
- `6581245784287610416`: `send`, `cancel_failed`, `force_terminal`, `mark_terminal`, `trade_missing`
- `6581245784287610415`: `send`, `open_failed`, `ws_order_missing`, `trade_missing`
- `6581245784287610413`: `send`, `open_failed`, `ws_order_missing`, `cancel_failed`, `trade_missing`
- `6581245784287610417`: `send`, `open_failed`, `ws_order_missing`, `cancel_failed`, `trade_missing`
- `6581245784287610421`: `send`, `open_failed`, `ws_order_missing`, `cancel_failed`, `trade_missing`
- `6581245784287610414`: `send`, `open_failed`, `ws_order_missing`, `trade_missing`

## Code-Level Interpretation

Key code paths:

- `ws open failed` immediately calls `retire_hedge_order(...)`, which removes the order from local tracking and from `OrderManager`: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L2191)
- `retire_hedge_order(...)` clears local state aggressively via `order_mgr.remove(client_order_id)`: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L318)
- once local state is gone, later WS order updates hit `ws order update missing local order`: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L370)
- later trade updates also hit `trade update order missing`: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1872)
- query reconcile can synthesize a `NEW` order update from `Create` query status: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1300)
- a `NEW` order update publishes a uniform `NEW` event: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1761)
- terminal order updates publish uniform terminal events only when a local order still exists at apply time: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1771)
- `mark_hedge_order_terminal_on_query_failure(...)` only calls `retire_hedge_order(...)`; it does not publish a uniform terminal event before deletion: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1003)

Current code-level read:

- For the 9 `open_failed` IDs, the strategy likely deletes local tracking too early on the `TradeRequestKind::Open` failure path.
- After that deletion, any later WS order update, cancel response, or trade update can no longer attach to local state.
- For `6581245784287610416`, local state survives long enough to enter `Create`, but the cancel-reconcile max-attempt path force-retires it without publishing a uniform terminal record.
- So at least one concrete bug is already visible in code: `mark_hedge_order_terminal_on_query_failure(...)` logs "mark hedge order terminal" but does not actually emit a uniform terminal event before removing the order.

## Account Monitor Findings

Searched file:

- `/home/ubuntu/.pmdaemon/logs/mm_am_binance_alpha-error.log-20260404`

This resolves the most confusing part of the incident:

- these `10` orders were real exchange orders
- they did receive Binance account-stream `NEW`
- they later all received Binance account-stream `FILLED`
- therefore the `ws open failed -2013` lines in `mm_pt` cannot be treated as proof that the order never existed

Representative `NEW` account updates:

- [mm_am_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_am_binance_alpha-error.log-20260404:57210)
- [mm_am_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_am_binance_alpha-error.log-20260404:57220)

Representative `FILLED` account updates:

- [mm_am_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_am_binance_alpha-error.log-20260404:59234)
- [mm_am_binance_alpha-error.log-20260404](/home/ubuntu/.pmdaemon/logs/mm_am_binance_alpha-error.log-20260404:59270)

All `10` affected IDs show the same pattern:

| client_order_id | exchange order_id | account-stream `NEW` | account-stream `FILLED` |
| --- | ---: | --- | --- |
| 6581245784287610412 | 36622207277 | yes | yes |
| 6581245784287610420 | 36622207278 | yes | yes |
| 6581245784287610419 | 36622207279 | yes | yes |
| 6581245784287610418 | 36622207280 | yes | yes |
| 6581245784287610416 | 36622207281 | yes | yes |
| 6581245784287610415 | 36622207282 | yes | yes |
| 6581245784287610413 | 36622207283 | yes | yes |
| 6581245784287610417 | 36622207284 | yes | yes |
| 6581245784287610421 | 36622207285 | yes | yes |
| 6581245784287610414 | 36622207286 | yes | yes |

Observed timing:

- `NEW` arrives around `2026-04-03T13:46:05Z`
- `FILLED` arrives around `2026-04-03T13:55:57Z`

Operational conclusion:

- the exchange accepted these orders
- the exchange later filled these orders
- the local `mm_pt` path dropped tracking state before the later account/trade updates could be applied

## Revised Interpretation Of The `NEW` + `ws open failed -2013` Conflict

Current best explanation:

1. Hedge orders are sent.
2. The strategy immediately schedules order-query watchdogs after send: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1495)
3. Query responses can come back as `Create`, and the strategy converts that into a synthetic `OrderStatus::New`: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L1300)
4. That synthetic or later account-driven `NEW` publishes a uniform `NEW` event.
5. Separately, the Binance WS `order.place` response is logged as `-2013 Order does not exist`.
6. The local strategy trusts that `open failed` result and immediately retires the hedge order: [mm_hedge_strategy.rs](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs#L2191)
7. Later real exchange/account updates still arrive, but local state is already gone.

This means the contradiction is real:

- local order-state evidence says the order existed
- account-stream evidence says the order existed and filled
- but the Binance WS `new_order` response path still reported `-2013`

So the immediate root problem for `mm_pt` is not "why was there a `NEW`".
The immediate root problem is:

- `ws open failed -2013` is not reliable enough here to justify immediate `retire_hedge_order(...)`

That is the decision that breaks downstream tracking.

## Next Step Placeholder

Next investigation step to append later:

1. Trace these `10` `client_order_id`s through pre-trade logs, account monitor logs, and strategy-local state transitions.
2. Determine whether the loss happened before exchange acknowledgement, after `NEW` publication, or during reconcile / terminal publication.
