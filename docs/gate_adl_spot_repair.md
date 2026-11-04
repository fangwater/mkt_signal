# Gate, Bybit and Binance FR ADL spot repair

Funding-rate environments (`ArbMode::FundingArb`) with the following opening
and hedge venues repair new ADL fills on the spot leg:

| Exchange | Opening venue | Hedge venue | Authoritative ADL event |
| --- | --- | --- | --- |
| Gate | `GateMargin` | `GateFutures` | `futures.usertrades`, `text=auto_deleveraging` |
| Bybit | `BybitMargin` | `BybitFutures` | Full `execution` stream, `execType=AdlTrade` |
| Binance | `BinanceMargin` | `BinanceFutures` | `ORDER_TRADE_UPDATE`, `c=adl_autoclose` or `adl_autoclose-...`, with an actual fill |

This repair requires the exact same-exchange pair and USDT linear futures.
Exec, intra-arbitrage, cross-exchange, COIN-M and other deployment modes retain
factual forced-close evidence without activating this repair. Liquidation,
delivery and settlement fills remain record-only.

`gate_account_monitor` subscribes to `futures.usertrades` with decimal-contract
support. A trade with the documented `text=auto_deleveraging` identity retains
its native order ID, trade ID, timestamp, price, side and contract quantity.
The aggregate `futures.orders` notification does not create another fill.
Bybit's account monitor already subscribes to the full `execution` channel
alongside `execution.fast`; ADL is parsed from the full channel with the native
order and execution identities. Binance retains its existing native order
stream and PM/standard USD-M account scope. Its actual ADL fills use a local
adapter for repair; no additional external-fill IPC event is emitted.

Pre-trade publishes both unmatched trade evidence and a `uniform_orders` fill
attributed to `exchange_forced_close:adl:order=...:trade=...`, without assigning
the exchange-owned fill to a strategy.

Pre-trade converts factual native quantities through the existing venue
multiplier (including Gate decimal contracts). A separate spot-repair instance
for each symbol and futures venue reuses the normal hedge order lifecycle,
without becoming the symbol's ordinary arbitrage terminal recorder. An ADL
buy that removes a futures short produces spot sells; an ADL
sell that removes a futures long produces spot buys to reduce short inventory.
Repair quantity is capped by the remaining ADL work and the same-direction
spot inventory, with venue quantity/minimum rules retained.

The first order is attempted from the account event. Subsequent children use
the existing per-symbol `arb_amount_u` size and a fresh matching spot BBO, at
most two seconds old. Only one live or unresolved repair child is allowed.
Bybit and Binance submit market orders in base quantity; Bybit market buys
explicitly set `marketUnit=baseCoin`. Gate spot sells use market IOC; reducing
Gate spot buys use a base-quantity limit IOC at the fresh ask, because Gate
market buys otherwise express quote notional. Repair orders do not enable new
borrowing. Neither direction reopens a futures hedge.

Actual partial fills reduce the repair work; confirmed unfilled quantities
are retried. Gate and Bybit deduplicate native `(order_id, trade_id)` pairs.
Binance requires a valid cumulative filled quantity and retains a per-native-
order cumulative watermark, adding only its positive increase. Duplicate or
older updates cannot add repair work, including when the native trade ID is
zero; a later cumulative update can cover an earlier missed partial fill.
Native fill IDs remain unchanged in the factual record. A query timeout retains
the original child and its reserved quantity, then queries that same order
again. It is not treated as a zero-fill
terminal. Missing/stale quotes and quantities below venue minima leave work
pending instead of increasing the order size.

This path handles live user-stream events. It does not query old ADL history
or turn historical ADL records into new orders. Repair work and duplicate
tracking are held in the pre-trade process; a process restart during repair
requires reconciling the outstanding order and actual net exposure before any
manual replay.

Publish locally built `pre_trade`, `trade_engine` and the matching venue's
account-monitor artifacts together when deploying this change. The existing
external-fill IPC and persisted order formats are reused; no persistence
schema migration is required. Follow the repository's production `arbmm`,
checksum and env-local start/stop rules.
