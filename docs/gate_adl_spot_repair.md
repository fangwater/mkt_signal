# FR ADL spot repair

Funding-rate environments (`ArbMode::FundingArb`) with the following opening
and hedge venues repair new ADL fills on the spot leg:

| Exchange | Opening venue | Hedge venue | Authoritative ADL event |
| --- | --- | --- | --- |
| Gate | `GateMargin` | `GateFutures` | `futures.usertrades`, `text=auto_deleveraging` |
| Bybit | `BybitMargin` | `BybitFutures` | Full `execution` stream, `execType=AdlTrade` |
| Binance | `BinanceMargin` | `BinanceFutures` | `ORDER_TRADE_UPDATE`, `c=adl_autoclose` or `adl_autoclose-...`, with an actual fill |
| Bitget | `BitgetMargin` | `BitgetFutures` | UTA `fill`, explicit ADL `tradeSide`; order-to-fill identity join when needed |
| OKX | `OkexMargin` | `OkexFutures` | `orders`, `category=adl`, actual `fillSz`/`fillPx`/`fillTime` and native `tradeId` |

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

Bitget's account monitor already subscribes to UTA `fill` and `order`. The
documented `tradeSide` values `dte_sys_adl_close_long`,
`dte_sys_adl_close_short`, `dte_sys_adl_buy_in_single_side_mode` and
`dte_sys_adl_sell_in_single_side_mode` identify ADL. An ADL marker on the
actual fill is sufficient, including with a numeric `clientOid`. A marker on
the order can classify cached external fills or later fills by native order
ID. Aggregate order quantities never create an extra repair fill. Known
exchange-owned order updates have no strategy client ID. `execType=reduce`
retains its separate `forced_reduction` attribution and does not trigger ADL
repair. The `adl-notification` channel lacks native order/trade IDs and is not
used to invent fills or additional repair work.

OKX's `orders` subscription already includes SWAP. Its VIP `fills` channel
explicitly excludes ADL, liquidation and other non-order-book events; the
repair therefore works without VIP fills access. Only actual USDT perpetual
fills (`instType=SWAP`, `*-USDT-SWAP`) enter this path. Native order and trade
IDs are retained, including negative forced-close trade IDs. Missing/zero
trade IDs, cumulative-only updates and ADL-related order cancellations do not
create repair fills. `fillSz` remains native contracts in the factual record
and converts through the existing multiplier for repair. `full_liquidation`,
`partial_liquidation` and `delivery` categories retain factual fill evidence
without activating ADL repair. A documented forced close bypasses strategy
attribution even with a numeric `clOrdId`. Its order update is not dispatched
again as an ordinary strategy fill. Negative/non-matching trade IDs and any
forced-close category on `fills` are ignored by that channel, so duplicate
streams cannot create a second repair task.

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
Bybit, Binance and OKX submit market orders in base quantity; Bybit market buys
explicitly set `marketUnit=baseCoin`. Gate and Bitget reducing spot buys use a
base-quantity limit IOC at the fresh ask, because their market buys otherwise
express quote notional. Gate spot sells use market IOC; Bitget spot sells use
market orders. Bitget repair children explicitly use UTA `category=spot`,
because `category=margin` automatically borrows funds. Gate, Bybit, Binance
and Bitget repair orders do not enable new borrowing. OKX retains the existing
cross SPOT route and the account's `autoLoan` setting, without changing account
configuration. Its reducing spot market children set `tgtCcy=base_ccy` and
`banAmend=true` to keep the submitted base size fixed. Neither direction
reopens a futures hedge.

Actual partial fills reduce the repair work; confirmed unfilled quantities
are retried. Gate, Bybit, Bitget and OKX deduplicate native `(order_id, trade_id)`
pairs within each symbol/venue repair instance. Repeated order updates with a
different update timestamp do not increase repair work. New external-fill
records use the canonical `FILLED` status code.
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

Bitget external contracts:

- [UTA tradeSide and execType enumeration](https://www.bitget.com/docs/uta/enum#tradeside)
- [UTA fill channel](https://www.bitget.com/docs/uta/websocket/private/Fill-Channel)
- [UTA order channel](https://www.bitget.com/docs/uta/websocket/private/Order-Channel)
- [UTA order placement and quantity units](https://www.bitget.com/docs/uta/websocket/private/Place-Order-Channel)
- [ADL notifications](https://www.bitget.com/docs/uta/websocket/private/ADL-Notification-Channel)

OKX external contracts:

- [Orders channel and actual fill fields](https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-order-channel)
- [Fills channel exclusions](https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-fills-channel)
- [Spot market quantity and banAmend](https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-place-order)
