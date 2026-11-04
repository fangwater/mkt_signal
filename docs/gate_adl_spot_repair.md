# Gate FR ADL spot repair

Gate funding-rate environments with `GateMargin` as the opening venue and
`GateFutures` as the hedge venue repair new ADL fills on the spot leg. Other
deployment modes retain the factual fill without activating this repair.

`gate_account_monitor` subscribes to `futures.usertrades` with decimal-contract
support. A trade with the documented `text=auto_deleveraging` identity retains
its native order ID, trade ID, timestamp, price, side and contract quantity.
The aggregate `futures.orders` notification does not create another fill.
Pre-trade publishes both unmatched trade evidence and a `uniform_orders` fill
attributed to `exchange_forced_close:adl:order=...:trade=...`, without assigning
the exchange-owned fill to a strategy.

For Gate FR, pre-trade converts the factual contracts through the existing
contract multiplier. A separate spot-repair instance reuses the normal hedge
order lifecycle, without becoming the symbol's ordinary arbitrage terminal
recorder. An ADL buy that removes a futures short produces spot sells; an ADL
sell that removes a futures long produces spot buys to reduce short inventory.
Repair quantity is capped by the remaining ADL work and the same-direction
spot inventory, with venue quantity/minimum rules retained.

The first order is attempted from the account event. Subsequent children use
the existing per-symbol `arb_amount_u` size and a fresh Gate spot BBO, at most
two seconds old. Only one live or unresolved repair child is allowed. Spot
sells use market IOC. Reducing spot buys use a base-quantity limit IOC at the
fresh ask, because Gate market buys otherwise express quote notional. These
buys do not enable new borrowing. Neither direction reopens a futures hedge.

Actual partial fills reduce the repair work; confirmed unfilled quantities
are retried. Duplicate native `(order_id, trade_id)` identities do not create
more repair work. A query timeout retains the original child and its reserved
quantity, then queries that same order again. It is not treated as a zero-fill
terminal. Missing/stale quotes and quantities below venue minima leave work
pending instead of increasing the order size.

This path handles live user-stream events. It does not query old ADL history
or turn historical ADL records into new orders. Repair work and duplicate
tracking are held in the pre-trade process; a process restart during repair
requires reconciling the outstanding order and actual net exposure before any
manual replay.

Publish locally built `gate_account_monitor`, `pre_trade`, and `trade_engine`
artifacts together when deploying this change. The existing external-fill IPC
and persisted order formats are reused; no persistence schema migration is
required. Follow the repository's production `arbmm`, checksum and env-local
start/stop rules.
