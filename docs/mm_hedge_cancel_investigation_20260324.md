# MM Hedge Cancel Investigation (2026-03-24)

## Scope

This note summarizes the MM hedge cancel storm seen on 2026-03-24 for the `mm_pt_binance_alpha` / `mm_te_binance_alpha` deployment.

Primary questions:

1. Why did hedge-side cancels fail?
2. Why did the cancel-failed-to-query fallback appear not to stabilize the strategy?
3. Why did hedge-side order count grow far beyond the expected small steady-state set?

## Affected Hedge Client Order IDs

Observed in `~/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260324`:

- `strategy_id=1471842518`
  - `6321515479672293402`
  - `6321515479672293403`
  - `6321515479672293404`
- `strategy_id=1251599577`
  - `5375579250902437797`
  - `5375579250902437798`
  - `5375579250902437799`
  - `5375579250902437800`
  - `5375579250902437801`
- `strategy_id=358241179`
  - `1538634147885485783`

These same IDs repeat many times in the cancel-failure loop.

## What The Pre-Trade Log Shows

Source:

- `~/.pmdaemon/logs/mm_pt_binance_alpha-error.log-20260324`

In the window around `2026-03-24T00:13:07Z` to `00:13:10Z`, the same hedge orders repeatedly alternate between:

- `cancel hedge order sent`
- `ws cancel failed: req_type=4014 status=400 code=-2011`

Representative pattern:

- cancel sent for the same `client_order_id`
- Binance WS cancel response immediately fails with `-2011`
- the same `client_order_id` is canceled again
- this repeats multiple times within a few seconds

Important negative observation from the nearby tail window:

- no `query回补`
- no `mark hedge order terminal`
- no `publish order query failed`
- no `build order query failed`

That matters because the code is supposed to transition from cancel failure into query-based reconcile.

## What The Trade-Engine Log Shows

Source checked:

- `~/.pmdaemon/logs/mm_te_binance_alpha-error.log`

Recent live `trade_engine` errors show a large number of Binance WS cancel failures of the same class:

- `type=BinanceWsCancelUMOrder`
- `status=400`
- `code=-2013`
- `msg=Order does not exist.`

The same live log also shows repeated Binance trade websocket handshake failures:

- `trade ws client id=0 failed to connect (websocket handshake (wss))`

Interpretation:

- `-2011` on the pre-trade side and `-2013 / Order does not exist` on the trade-engine side are consistent with the same broad category:
  - the exchange no longer recognizes the order being canceled
  - or local state is stale relative to exchange state

This is not evidence that Binance refused to cancel a live order for business-rule reasons. It is stronger evidence that the cancel request targeted an order the exchange already considered absent / terminal / unknown.

## Expected Code Path

Relevant files:

- `src/strategy/mm_hedge_strategy.rs`
- `src/funding_rate/mm_decision/mod.rs`
- `src/strategy/trade_engine_response.rs`

Expected behavior:

1. `handle_query_timer()` first calls `request_cancel_for_hedge_orders()`.
2. If any hedge orders are still locally non-terminal, the strategy sends cancel requests and returns without sending the next hedge query.
3. If Binance cancel fails with `-2011`, `apply_trade_engine_response()` should classify it as `CancelRejected` / `is_cancel_not_cancellable()`.
4. The strategy should then enter cancel-reconcile by calling `send_order_query(...)`.
5. While the order is in reconcile, `is_cancel_reconciling(order_id)` should prevent repeated re-cancel of the same order.

So in steady-state, the strategy should not keep spamming cancel for the same order IDs after the first `-2011`.

## Observed Mismatch

The runtime behavior does not match the intended control flow.

Observed:

- same hedge order IDs repeatedly receive `cancel hedge order sent`
- same hedge order IDs repeatedly receive `ws cancel failed ... code=-2011`
- nearby logs do not show the expected reconcile evidence

Implication:

- either the cancel-failed-to-query transition is not being reached reliably
- or it is reached but the reconcile state is not sticking long enough to block the next cancel cycle

## Most Likely Current Explanation

Most likely chain:

1. Old hedge orders remain in local `hedge_order_ids`.
2. Cancel requests are sent.
3. Binance replies that the order does not exist / is unknown.
4. The strategy should move into query reconcile, but in practice the order remains eligible for `request_cancel_for_hedge_orders()` on the next timer pass.
5. This creates a cancel storm on the same IDs.

This is enough to explain hedge-side order growth:

- old hedge orders are not being retired cleanly from local tracking
- the cancel loop keeps the strategy in an unhealthy state
- once old state handling is broken, later hedge rounds can accumulate instead of staying bounded

## Why "Next Hedge" Is Still A Concern

By design, a new hedge query should not be sent while old hedge orders are still locally tracked as live or reconcile-pending.

So if hedge-side order count still explodes in production, one of the following is true:

1. old orders are eventually being dropped from local tracking without being cleanly reconciled
2. another path is generating new hedge work after local state has already drifted
3. multiple symbols / strategies are simultaneously accumulating stale hedge orders, so the global count still explodes even if each loop is local

The logs above strongly support item 1 as at least part of the problem.

## Current Best Answer To "Why Did Cancel Fail?"

Best current answer:

- the exchange side reported that the target hedge orders did not exist anymore
- therefore the cancel failure is not the root cause by itself
- the deeper bug is that local hedge order state did not retire those IDs after this response class

Short form:

- cancel failed because Binance considered the order absent / unknown
- order explosion happened because local strategy state did not converge after that failure

## What Is Still Missing

Still not proven from the current logs:

- whether `send_order_query(...)` was never called
- or was called but query responses were missing / misrouted / immediately cleared

The current log evidence only proves that the runtime kept re-canceling the same IDs and did not visibly converge.

## Important Clarification About "Order Explosion"

There is an important logical constraint in the current implementation:

- if a given `MarketMakerHedgeStrategy` keeps old hedge orders in `hedge_order_ids`, then `handle_query_timer()` will keep returning early from `request_cancel_for_hedge_orders()`
- in that state, the same strategy should be blocked from sending the next hedge query
- without the next hedge query, it should also be blocked from producing the next hedge order batch

So "old orders were never retired" cannot by itself explain sustained creation of new hedge batches for the same strategy.

If hedge-side order count still grows over time, then at least one of these must also be happening:

1. old hedge orders are later dropped from local tracking
2. local state says they are terminal / missing even while exchange reality is not clean
3. growth is aggregated across multiple symbols / hedge strategies, not just one blocked instance

That means the real question is narrower:

- not just why old hedge orders got stuck for a while
- but also which later path caused them to be released from local tracking

The most likely release paths are:

1. `status.is_finished()` update path
2. `Filled` trade update path
3. `mark_hedge_order_terminal_on_query_failure(...)`
4. `request_cancel_for_hedge_orders()` seeing `order_manager.get(order_id) == None`

At the moment, path 3 is still the strongest suspect.

## Log File Integrity Issue

Both large archived logs checked here are not normal text files at the byte level:

- `mm_pt_binance_alpha-error.log-20260324`
- `mm_te_binance_alpha-error.log-20260324`

Observed:

- `file` reports them as `data`
- the first `64 KiB` of both files are entirely `NUL (0x00)` bytes
- `grep -a` can still extract text later in the file, but tools like `vim` may treat them as binary or behave poorly

This likely explains why `vim` cannot open the log normally.

Operational implication:

- the logs are still partially usable with binary-safe tools such as `grep -a`, `strings`, or custom byte scanning
- but they are not trustworthy as clean plain-text append-only logs
- this also weakens confidence that all relevant reconcile lines are preserved or easy to surface interactively

## Recommended Next Instrumentation

If we want to finish root-cause analysis quickly, add temporary logs in `src/strategy/mm_hedge_strategy.rs` for:

- after `send_order_query(client_order_id, reason)` with `sent=true/false`
- current `pending_order_queries.contains_key(client_order_id)`
- current `order_query_watchdogs.contains_key(client_order_id)`
- when `request_cancel_for_hedge_orders()` skips an order because `is_cancel_reconciling(order_id)==true`
- when `apply_query_engine_response()` receives reconcile results for these hedge order IDs

That instrumentation should tell us exactly where the intended cancel->query->terminal chain is breaking.
