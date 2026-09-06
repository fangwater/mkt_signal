# RapidX Integration

RapidX/LTP is an execution and market-data provider, not a new `TradingVenue`.
Binance and OKX retain their native symbols, order types and IPC message types.
This integration has not been deployed or validated with live account actions.

## Configuration

Use the existing execution-backend selector consistently in `trade_engine`,
`pre_trade`, `trade_signal` and the account monitor:

```bash
export TRADE_ENGINE_EXEC_BACKEND_MAP='binance=rapidx,okex=rapidx'
```

Provide `LTP_API_KEY`, `LTP_API_SECRET` and `LTP_PORTFOLIO_ID` securely in the
environment. The portfolio ID binds the credential's account identity; it is
not an account-mode switch. Unsupported venues and malformed backend maps fail
startup. There is no fallback to native execution on RapidX errors.

The account publisher is `rapidx_account_monitor --exchange binance` or
`rapidx_account_monitor --exchange okex`. Existing deployment/start wrappers do
not automatically select this new binary. REST and private WS source IPs use
the existing trade-engine configuration; the monitor also accepts `--local-ip`.
`LTP_REST_URL` and `LTP_WS_URL` override the private endpoints when needed.

Account IPC is isolated as `account_pubs/rapidx_<exchange>_<portfolioId>_pm`
inside the existing namespace. Its publisher and consumers require
`safe_overflow=false`; native account streams retain their existing policy.
Run only one execution backend for a given exchange within an IPC namespace.

## Implemented

- WS login must succeed before requests are sent. Login code `0` and action
  code `200000` are handled separately. Requests carry microsecond `ts`.
- Placement/cancellation ACKs correlate by transport ID and expected action;
  an ACK does not invent an order lifecycle or fill.
- Sent requests with a disconnect, failed send or timeout become an unknown
  action result and enter existing query reconciliation. They are not resent.
- REST order queries preserve the business query ID. Only documented business
  code `401018` means not found; HTTP 404 and malformed responses are errors.
  Duplicate-client-ID/already-completed action responses also query lifecycle.
- Orders, asset balances, signed/zero positions, unrealized PNL and reported
  account risk enter existing account message types with portfolio validation.
- Private messages are journaled before conversion, including original Trades,
  signed fees, rebates and financial fields. Journals are permission `0600`
  under `data/rapidx_account/<source>/` by default.
- Bounded REST snapshots refresh Assets, Positions and Accounts in rotation.
  Complete position snapshots clear previously observed missing positions;
  journal recovery restores position identities across monitor restarts.
  Disconnect/snapshot failure publishes an invalid account risk value.

## Market Data

`spread_pbs --market-data-provider rapidx` selects the public RapidX feed;
`native` remains the default. Existing `--venue` and `--test` routing applies.
Binance spot/perpetual and OKX spot are supported. `binance-both` combines its
spot and perpetual pipelines without introducing new venue IDs.

The adapter handles BBO, trades, snapshot/incremental depth, mark price, index
price and open interest. Funding rate uses the documented REST endpoint,
not a fictitious WS subscription. Its poller follows validated symbol refreshes
and spaces requests by four seconds to respect the three-per-ten-second limit.
Depth gaps are rejected and reconnect for a fresh snapshot; native adapters'
existing gap behavior is unchanged.

The current implementation is unauthenticated and accepts at most five selected
pairs per venue pipeline. It does not implement authenticated larger universes
or connection sharding. OKX perpetual RapidX market data fails startup pending
confirmation of BBO/depth/OI quantity units. Native OKX market data remains usable
with RapidX execution. Separate kline/ticker pipelines are not added here.

## Remaining Boundaries

- Raw Trades journaling is not historical transaction recovery, a durable
  consumer-acknowledged replay protocol, or full `persist_manager` fee/ledger
  normalization. Journal rotation/retention also needs operational policy.
- Borrow capacity, liabilities, interest, collateral discounts and all portfolio
  financial fields are not normalized to full OKX parity. Missing values are
  not fabricated from balance, equity or maximum transferable amounts.
- Complete asset snapshots do not yet clear omitted assets with durable asset
  identity recovery. There is no atomic all-account readiness snapshot protocol.
- Nonnumeric external client IDs remain outside the native numeric order
  lifecycle; raw evidence is retained, but unmatched/forced-close recovery is
  not complete. Malformed lifecycle messages invalidate the monitor session.
- Native Binance auto-repay/collection are disabled for RapidX. Exec startup is
  refused because cancel-all, leverage and rule initialization still use native
  account paths. FR/MM paths may remain gated by unavailable normalized account
  fields; this is not a claim of production-ready automatic trading.
- No private credential smoke test, live order, leverage/account change or
  deployment was performed for this implementation.

## Protocol References

- [Market data overview](https://apidocliquidity.readme.io/reference/market-data-overview)
- [Order book](https://apidocliquidity.readme.io/reference/market-data-order-book)
- [Funding rate](https://apidocliquidity.readme.io/reference/get-current-fundingfee)
- [Private WS](https://apidocliquidity.readme.io/reference/ws-user-data-overview)
- [Private channels](https://apidocliquidity.readme.io/reference/ws-user-data-orders-trades-assets-positions)
- [Portfolio account](https://apidocliquidity.readme.io/reference/get-portfolio-overview)
- [Portfolio assets](https://apidocliquidity.readme.io/reference/get-portfolio-assets-details)
- [Positions](https://apidocliquidity.readme.io/reference/query-portfolio-position)
- [Error codes](https://apidocliquidity.readme.io/docs/error-codes)
