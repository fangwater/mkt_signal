# RapidX Integration

RapidX/LTP is an execution and market-data provider, not a new `TradingVenue`.
Binance and OKX retain their existing strategy venue identities and internal symbols.
Authenticated account actions must be smoke-tested for each deployed portfolio.

## Configuration

Use the existing execution-backend selector consistently in `trade_engine`,
`pre_trade`, `trade_signal` and the account monitor:

```bash
export TRADE_ENGINE_EXEC_BACKEND_MAP='binance=rapidx,okex=rapidx'
```

For a Binance Intra cash leg, `SPOT` is the default. Select RapidX margin
orders explicitly when that deployment is intended to auto-borrow:

```bash
export RAPIDX_BINANCE_CASH_BUSINESS_TYPE=MARGIN
```

Only `SPOT` and `MARGIN` are accepted. This setting changes the cash-leg order
symbol between `BINANCE_SPOT_*` and `BINANCE_MARGIN_*`; futures remain `PERP`.

Provide `LTP_API_KEY`, `LTP_API_SECRET` and `LTP_PORTFOLIO_ID` securely in the
environment. The portfolio ID binds the credential's account identity; it is
not an account-mode switch. Unsupported venues and malformed backend maps fail
startup. There is no fallback to native execution on RapidX errors.

The account publisher is `rapidx_account_monitor --exchange binance` or
`rapidx_account_monitor --exchange okex`. Exec startup wrappers select this
named binary and pass the exchange when the backend is RapidX. Prepare
`<env>/rapidx_account_monitor` before using `start-exec.sh`; this change does not
automatically install it through deploy/publish wrappers. Other strategy wrappers
are unchanged. REST and private WS source IPs use
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
- Binance order and cancel routing distinguishes RapidX `SPOT`, `MARGIN` and
  `PERP`. Cash LIMIT maker orders use `GTX`; MARKET orders omit time in force.
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
- Settlement recovery accepts the observed `TRANSFER` statement type as ledger
  evidence; it is not converted into a fill.
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

## Execution Persistence

The monitor now forwards durable execution observations directly to
`persist_manager` in the same `IPC_NAMESPACE`, without passing incremental fills
through the cumulative order-update path. Run that consumer to drain the outbox;
its absence does not discard the journal or block the WS reader.

The shared `persist_common::rapidx_execution` contract retains string identities,
decimal strings and separate fee currencies. The RocksDB `rapidx_executions` CF
uses a stable key over portfolio, exchange, transaction ID and observation source.
WS and REST observations occupy separate records because their fee semantics
differ. They are evidence of the same execution, not two additive fills. Neither
observation increases `uniform_orders.amount_update` or assigns a strategy owner.
Conflicting same-source records are rejected without overwriting existing facts.

The receiver acknowledges the exact key and canonical content only after a
synchronous RocksDB write. With persist sync enabled, the fact and replication
outbox share that durable write. Lost ACKs and IPC sends retry with a bounded
128-record in-flight window; duplicate receipts do not append more outbox rows.
The journal remains intact, and monitor restart replays all retained executions.
Delivery also runs during connection attempts and reconnect backoff.
`latest_financial.json.execution_persistence_pending` reports journal backlog plus
in-flight records; `recovery_ready` still describes account/history recovery, not
central persistence completion. A persist ACK means local durable storage, not
that the remote sync collector has received the record.

The new CF participates in generic persist sync replication, but not the
timestamp-keyed order-export repair sweep or existing order parquet schema.
Replication must be enabled when facts are first inserted; enabling it later
does not backfill facts previously acknowledged with replication disabled.
Use the read-only JSONL exporter for inspection (timestamps are microseconds,
the requested interval is half-open):

```bash
cargo run -p persist_manager --features runtime --bin rapidx_execution_export -- \
  --db-path data/persist_manager --portfolio 123 --exchange OKX \
  --start-us 1788652800000000 --end-us 1788739200000000 --observation rest
```

Replace the example portfolio with the intended identity. `--observation ws`
selects signed WS fees; `all` retains both observations and must not be summed as
fills or fees. `--source-id` reads the corresponding source CF in a sync-center
database. JSONL is written to stdout, diagnostics to stderr. The exporter never
writes to the database; an error can leave partial stdout output and must not be
treated as a complete export. Export rows preserve the venue's millisecond
timestamp inside the execution evidence.

## Execution Reconciliation

Add `--reconcile` to `rapidx_execution_export` to produce one JSON report with
unique executions and per-order totals instead of raw observation JSONL. This
requires `--observation all`. WS/REST records join by portfolio, venue and
transaction ID. Identity, time, exact quantity, price, reported PNL and charged
fees must agree; decimal spellings such as `1` and `1.0` compare equal. Conflicts
fail before report output, including a counterpart outside the requested window.

Order totals cover only `[start-us, end-us)`, not necessarily the whole order.
Quantities retain venue units. Fees use exact decimal arithmetic and separate
currency buckets with charged/rebated amounts and an optional net charge. There
is no currency conversion or strategy net PNL calculation. The report declares
`coverage=persisted_executions_in_window` and `strategy_attribution=unassigned`;
it does not verify exchange history completeness. `realized_pnl_reported` is
only the venue's reported PNL summed for each order, not settlement cash.

WS signed fees supply per-fill rebates. REST-only nonzero cumulative rebates
remain unresolved and are never summed or apportioned. The report is written
but the command exits with an error; `--allow-incomplete-fees` permits inspection
with success status without overriding conflicts. An unresolved fill makes its
order's net fee totals unknown. `--max-executions` bounds retained identities for
the whole selected portfolio/venue, including out-of-window counterparts
(default 100000); exceeding the bound fails rather than dropping records.

Repeat `--account-journal /path/to/source.jsonl` to attach `LiquidationPosition`
or `LiquidationPositionByUser` evidence from account-monitor journals. Exact
portfolio/venue/order identity, symbol, direction and order lifetime are checked
before marking `exchange_forced_close:liquidation`. Cancellation snapshots and
arbitrary `tradeSource` strings are not proof of forced execution. Penalties and
order-level trading-fee/PNL snapshots stay separate from fill totals and are not
prorated for the requested window (`liquidation_totals_verified=false`). Matched
and other-scope liquidation observation counts are reported; other portfolios
on `LiquidationPositionByUser` never enter the selected portfolio's totals.
Whole liquidation order lifetimes inside the
window without any selected fill appear in `unmatched_liquidations` and cause
an error, never synthetic fills. Corrupt and unterminated journal lines fail in
this offline audit, unlike live journal recovery. Retry after the writer finishes
or use a closed snapshot. These are read-only inputs, not live actions.

## Settlement Ledger

The account monitor recovers `FUNDING_FEE`, `DEDUCT_INTEREST`,
`LIQUIDATION_FEE` and `LIQ_COMPENSATION` through the documented REST statement
endpoint. There is no invented statement WS subscription. Recovery uses
one-hour windows, a separate durable `statement_history_end_ms` checkpoint and
a 60-second overlap; initial lookback defaults to 24 hours and must fit within
the endpoint's 90-day retention. Tail refresh runs every 30 seconds. Requests
within pagination and catch-up windows are spaced by 1.5 seconds; multiple
deployments sharing credentials still need coordinated rate budgets.

Statement recovery is required for account readiness. The financial inspection
snapshot includes its checkpoint and `statement_persistence_pending`. Records
are journaled and synced before checkpoint advancement, then retried through
the existing durable ACK mechanism. Restart replays the journal; stable
portfolio/venue/statement identities deduplicate unchanged records and reject
conflicts. Pagination inconsistencies and cross-portfolio rows fail closed.

Statements use the independent `rapidx_statements` RocksDB column family and
sync outbox. They do not create trades, change positions, or reapply balance
deltas to account snapshots. Like execution facts, their hash keys are excluded
from timestamp-based order repair and order parquet schemas.

Read-only reporting from a closed database snapshot:

```bash
rapidx_statement_export --db-path /path/to/persist_manager \
  --portfolio 123 --exchange BINANCE \
  --start-us 1788220800000000 --end-us 1788307200000000
```

Use `--source-id` for a namespaced sync-center source. Reports cover the
half-open microsecond window and retain raw decimal strings. Exact totals are
grouped by portfolio, venue, currency, symbol, business and statement type.
Reported settlement, available-balance changes, overdraft changes and loan
changes remain separate: the official interest example has zero `deltaAmount`
but increased overdraft. Zero settlement therefore does not mean zero interest.
No sign reinterpretation, cross-currency sum, net-PNL formula or strategy
attribution is inferred. Coverage is `persisted_statements_in_window`, not proof
of complete venue history. `--max-statements` bounds selected records; corruption
and conflicting records fail before report output. No accrued-unpaid-interest
calculation, automatic borrowing or repayment is added.

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

### Exec Startup Work

Portfolio-scoped startup cancellation and BatchExec leverage initialization now
have RapidX adapters. Cancellation first validates a complete perpetual open-order
snapshot for the configured portfolio and logical venue, then sends individual
JSON-body DELETE requests and queries until the snapshot is empty. The user-wide
`cancelAll` endpoint is deliberately not used. Accepted cancellation is not final
confirmation. Malformed/incomplete snapshots, cross-portfolio rows, failed
requests and the existing `EXEC_STARTUP_CANCEL_TIMEOUT_SECS` deadline fail the
gate without falling back to native exchange credentials.

The `rapidx_open_orders` operator tool inspects and, only with `--execute`,
cancels portfolio-scoped `SPOT`, `MARGIN` and `PERP` orders individually. The
`rapidx_order_smoke` tool sends one order or cancel through the trade-engine IPC
path, defaults to dry-run, requires `--execute`, and enforces a maximum 10 USDT
notional cap for placements.

The leverage adapter sets the existing BatchExec default of 5 and reads the exact
perpetual symbol back before activation. Only Binance/OKX USDT perpetual symbols
are supported; this does not add inverse-futures support or change account mode.
The existing leverage marker is bound to venue/backend/portfolio. A scope change
invalidates its symbol set. RapidX confirmations are process-local and must be
renewed after restart; persisted markers alone cannot activate RapidX symbols.

The rule producer in the independent `crypto_cta_manager` repository reads the
source's existing `env_path` (default derived from its RocksDB path) and resolves
`TRADE_ENGINE_EXEC_BACKEND` / `TRADE_ENGINE_EXEC_BACKEND_MAP` with the same
precedence as Exec. RapidX sources use authenticated `sym/info`; native sources
retain their venue public APIs. Manager refreshes every 60 seconds, spaces RapidX
requests by four seconds, and publishes through the same source/venue Redis key.
It neither imports this repository nor opens an Exec database for writing.

The single cache contract now requires `execution_backend` (`native` or `ltp`)
and carries `portfolio_id` (null for native). Both repositories must be updated
together; old cache shapes are rejected, not supported through another format.
The normal Exec cache reloader validates this identity before applying rules.
RapidX startup additionally requires a complete validated cache no older than
180 seconds (at most 60 seconds future skew) before any cancellation or leverage
write. A missing, mismatched, invalid or stale startup cache fails closed. Exec
does not poll `sym/info` independently or silently fall back to native rules.
Hot reload retains the last good snapshot on refresh errors, matching the native
path; a complete snapshot omitting or suspending a symbol blocks new orders.

With these prerequisites met, Binance/OKX USDT perpetual RapidX Exec can pass the
former blanket startup refusal. Startup scripts select RapidX credentials and
skip native Python cancellation dependencies. Inverse futures remain unsupported.
This is implemented and fixture-tested startup wiring, not an authenticated live
trading certification. Manager's separate native account query/leverage controls
reject RapidX sources rather than touching native accounts.

- Durable execution-fact replay, read-only execution/order fee reconciliation
  and settlement-ledger recovery/reporting are implemented. Cross-checking ledger
  totals against account snapshots and a normalized strategy PNL view remain
  incomplete. In
  particular, REST's reported cumulative rebate is not apportioned to fills.
  Journal rotation/retention needs operational
  policy. Late venue corrections beyond the one-minute overlap require a wider
  recovery, and endpoint retention/completeness still needs authenticated testing.
- Financial/loan inspection is implemented, but accrued interest, collateral
  tier rules and all strategy-facing financial fields are not at full OKX parity.
  Neither native max-loan query semantics nor automatic borrowing are inferred
  from the explicit portfolio-loan API. There is no atomic all-account IPC
  readiness snapshot protocol.
- Nonnumeric external client IDs remain outside the native numeric order
  lifecycle; they no longer invalidate an otherwise valid order stream. Their
  execution evidence is centrally persisted. Read-only liquidation attribution
  uses documented journal evidence, but live unmatched/uniform-order forced-close
  plumbing remains incomplete. Malformed lifecycle messages invalidate the session.
- Native Binance auto-repay/collection are disabled for RapidX. Exec startup is
  conditional on Manager rule provenance, scoped cancellation and verified leverage.
  FR/MM paths may remain gated by unavailable normalized account
  fields; this is not a claim of production-ready automatic trading.
- No private credential smoke test, live order, leverage/account change or
  deployment was performed for this implementation.

## Protocol References

- [Market data overview](https://apidocliquidity.readme.io/reference/market-data-overview)
- [Order book](https://apidocliquidity.readme.io/reference/market-data-order-book)
- [Funding rate](https://apidocliquidity.readme.io/reference/get-current-fundingfee)
- [Private WS](https://apidocliquidity.readme.io/reference/ws-user-data-overview)
- [Private channels](https://apidocliquidity.readme.io/reference/ws-user-data-orders-trades-assets-positions)
- [Liquidation order evidence](https://apidocliquidity.readme.io/reference/ws-liquidation)
- [Portfolio account](https://apidocliquidity.readme.io/reference/get-portfolio-overview)
- [Portfolio assets](https://apidocliquidity.readme.io/reference/get-portfolio-assets-details)
- [Positions](https://apidocliquidity.readme.io/reference/query-portfolio-position)
- [Loan status](https://apidocliquidity.readme.io/reference/query-loan-info)
- [Portfolio borrowing capacity](https://apidocliquidity.readme.io/reference/query-max-loan-amount)
- [Recent execution pages](https://apidocliquidity.readme.io/reference/query-transactions-pageable)
- [Archived execution pages](https://apidocliquidity.readme.io/reference/query-archived-transactions-pageable)
- [Settlement statements](https://apidocliquidity.readme.io/reference/query-statement)
- [Open orders](https://apidocliquidity.readme.io/reference/current-open-orders)
- [Single-order cancellation](https://apidocliquidity.readme.io/reference/cancel-order)
- [User-wide cancellation scope](https://apidocliquidity.readme.io/reference/cancel-one-portfolio-orders)
- [Set leverage](https://apidocliquidity.readme.io/reference/set-leverage)
- [Read leverage](https://apidocliquidity.readme.io/reference/get-perp-leverage)
- [Symbol trading rules](https://apidocliquidity.readme.io/reference/sym-info)
- [Error codes](https://apidocliquidity.readme.io/docs/error-codes)
