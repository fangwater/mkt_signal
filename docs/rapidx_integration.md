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
  Complete asset/position snapshots clear previously observed missing identities;
  journal recovery restores both kinds of identity across monitor restarts.
  Older snapshots cannot rewind newer updates. Omission clearing uses the
  request-start boundary, not response-arrival time.
  Disconnect/snapshot failure publishes an invalid account risk value.
- Account REST refresh and execution-history recovery run separately from the
  private WS receive loop. Full account snapshots have a 20-second deadline;
  private heartbeats are not blocked by REST pagination.
- Reported account risk stays gated until fresh Assets, Positions, Accounts,
  LoanInfo, LoanCapacity and recent execution recovery have completed. Abnormal
  or unknown account/loan status cannot reopen the gate. This is a monitor-side
  recovery gate, not an atomic consumer-acknowledged account snapshot contract.

## Finance And Recovery

`latest_financial.json` in the source journal directory is atomically replaced
with permission `0600`. It exposes separate gross/net equity, margin values,
available margin, frozen/maintenance margin, asset debts, explicit loans and
per-currency `max_borrow_capacity`. Decimal values retain string precision;
unreported metrics stay `null`. Raw financial evidence and per-source observation
times are retained. `recovery_ready=false` marks incomplete/disconnected state.
These inspection fields do not silently become strategy borrowing permissions.

Loan status and portfolio borrowing capacity use the read-only
`rapidxLoan/loan/info` and `rapidxLoan/loan/maxLoan` endpoints. Explicit portfolio
loan capacity is not native spot automatic-borrow capacity, and no borrow or
repay request is sent. Unknown accrued interest is not fabricated as zero.

Execution recovery defaults to the preceding 24 hours on first startup;
`--history-lookback-hours` allows 1 through 2136 hours (89 days). Subsequent
sessions resume their durable checkpoint with a one-minute overlap. Recovery
uses one-hour windows, recent execution pages and the 7-to-90-day archive,
including the boundary in both queries with duplicate checking. The active tail
is polled every 30 seconds. A checkpoint older than the documented 90-day
retention is an error, not silently truncated history.

Every page is checked for scope, timestamps, duplicate IDs and stable pagination;
an incomplete window never advances the checkpoint. Each endpoint/window has a
100-page bound and 2.1-second inter-page spacing. Execution observations preserve
string order/client/transaction IDs. WS signed fees and REST split fee/rebate
currencies are retained separately, not added together or apportioned from
ambiguous cumulative fields. Repeated same-source evidence is idempotent;
conflicting records fail recovery. Journal writes precede progress checkpoints.
Only an unterminated final journal line is ignored on restart; complete corrupt
records fail recovery. Journal write/sync failure stops further processing.

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

- Historical executions are recovered into the local journal, not yet through a
  durable consumer-acknowledged replay protocol or full `persist_manager`
  order/fee/ledger reconciliation. Journal rotation/retention needs operational
  policy. Late venue corrections beyond the one-minute overlap require a wider
  recovery, and endpoint retention/completeness still needs authenticated testing.
- Financial/loan inspection is implemented, but accrued interest, collateral
  tier rules and all strategy-facing financial fields are not at full OKX parity.
  Neither native max-loan query semantics nor automatic borrowing are inferred
  from the explicit portfolio-loan API. There is no atomic all-account IPC
  readiness snapshot protocol.
- Nonnumeric external client IDs remain outside the native numeric order
  lifecycle; they no longer invalidate an otherwise valid order stream. Their
  execution evidence is retained, but central unmatched/forced-close attribution
  is not complete. Malformed lifecycle messages invalidate the monitor session.
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
- [Loan status](https://apidocliquidity.readme.io/reference/query-loan-info)
- [Portfolio borrowing capacity](https://apidocliquidity.readme.io/reference/query-max-loan-amount)
- [Recent execution pages](https://apidocliquidity.readme.io/reference/query-transactions-pageable)
- [Archived execution pages](https://apidocliquidity.readme.io/reference/query-archived-transactions-pageable)
- [Error codes](https://apidocliquidity.readme.io/docs/error-codes)
