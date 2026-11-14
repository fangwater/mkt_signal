# RapidX Integration

最后更新: 2026-11-14 12:20 UTC。

RapidX/LTP is an execution and market-data provider, not a new `TradingVenue`.
Binance and OKX retain their existing strategy venue identities and internal symbols.
Authenticated account actions must be smoke-tested for each deployed portfolio.

## Configuration

Intra environments require `PRE_TRADE_NOTIFICATION_URL`. Newly generated
`env.sh` files default it to the host-local notification service at
`http://127.0.0.1:18100/v1/notify` and allow an operator override.

Use the existing execution-backend selector consistently in `trade_engine`,
`pre_trade`, `trade_signal` and the account monitor:

```bash
export TRADE_ENGINE_EXEC_BACKEND_MAP='binance=rapidx,okex=rapidx'
```

For a Binance cash leg, `MARGIN` is the default (RapidX `BINANCE_MARGIN_*`
sells auto-borrow). Select RapidX spot orders explicitly when that deployment
must not borrow:

```bash
export RAPIDX_BINANCE_CASH_BUSINESS_TYPE=SPOT
```

Only `SPOT` and `MARGIN` are accepted. This setting changes the cash-leg order
symbol between `BINANCE_SPOT_*` and `BINANCE_MARGIN_*`; futures remain `PERP`.

Confirmed by live smoke on the rx01 portfolio (small DOGE orders, then cleaned
up): an unfilled post-only `BINANCE_MARGIN` sell opens and cancels with no loan
created; a filled `BINANCE_MARGIN` sell auto-borrows the base asset at fill
time even when the coin is absent from `rapidxLoan/loan/config`'s explicit
borrowable list and `loan/maxLoan` reports zero. The resulting liability does
**not** appear in `rapidxLoan/loan/info`; it surfaces in the portfolio assets
snapshot as per-coin `debt`/`borrow` with negative `equity`. Buying the coin
back auto-repays the debt. `rapidxLoan/loan/info` therefore only covers
explicit portfolio loans — margin-order debt must be enumerated from
`portfolio/assets`.

Provide `LTP_API_KEY`, `LTP_API_SECRET` and `LTP_PORTFOLIO_ID` securely in the
environment. The portfolio ID binds the credential's account identity; it is
not an account-mode switch. Unsupported venues and malformed backend maps fail
startup. There is no fallback to native execution on RapidX errors.

The account publisher is `rapidx_account_monitor --exchange binance` or
`rapidx_account_monitor --exchange okex`. Standard Exec deploy/publish wrappers
install it as `<env>/rapidx_account_monitor` for RapidX accounts; startup wrappers
select that binary and pass the exchange. Native accounts keep their monitor at
`<env>/account_monitor`. REST and private WS source IPs use
the existing trade-engine configuration; the monitor also accepts `--local-ip`.
`LTP_REST_URL` and `LTP_WS_URL` override the private endpoints when needed.

Account IPC is isolated as `account_pubs/rapidx_<exchange>_<portfolioId>_pm`
inside the existing namespace. Its publisher and consumers require
`safe_overflow=false`; native account streams retain their existing policy.
Run only one execution backend for a given exchange within an IPC namespace.

## Mixed spot and perpetual targets

RapidX Binance/OKX Exec owns both the configured linear perpetual market and
its USDT cash market. Publish both legs in the same Manager strategy:

```json
{"strategy_name":"basis","targets":{"BTC":1,"BTCUSDT":-1,"ETH":{"qty":"clear"}}}
```

`BTC` means the BTC/USDT cash pair, `BTCUSDT` and `BTCUSDC` mean perpetuals.
All quantities are base coins. Cash uses the existing Binance SPOT/MARGIN setting
above (MARGIN permits borrowing); RapidX OKX uses SPOT. Native OKX unified Exec
also owns USDT cross SPOT; see [OKX unified Exec](okx_unified_exec.md). Other native
deployments retain their single-market scope, and COIN-M is never added to a RapidX deployment.
Manager writes the two venue scopes in one Redis transaction, including empty
scopes when a leg is removed. Exec expands bare assets only inside the cash
reloader; order, quote, rule and ledger identities retain the native pair plus
venue. Viz and Manager expose cash positions as bare assets. Batch, POV, CTaker
and Chase all accept cash targets; Chase cancels and waits for terminal
confirmation before reposting cash makers. Cash does not initialize contract
leverage. Startup validates both Manager rule caches and cancels orders in each
selected business type within the configured portfolio.

Manager reconstructs factual FIFO independently by source, strategy, symbol and
venue, then sums realized PnL, floating PnL and estimated fees. Cash fills and
initial snapshots use the same bare-asset display identity as targets. Binance
cash theoretical pricing uses spot minute candles with a separate cache key from
perpetual candles; existing OKX theoretical pricing limitations remain.

Publish Manager and Exec together before enabling mixed targets. Manager must
publish both rule caches, and the account's market-data publisher must provide
both markets (for Binance, `spread_pbs --venue binance-both`). This code change
does not deploy binaries or submit live orders.

## Standard Exec Deployment

Each RX account uses one standard Exec environment. All factor strategies for
that account publish into the same Exec strategy tables; separate Exec
environments per factor are unnecessary. This account integration retains
BatchExec/ChaseExec's existing position ledgers and execution algorithms. Numeric
`qty` targets and the strategy-scoped `qty: "clear"` command use the same path.

Initialize a new Binance RX environment with:

```bash
scripts/deploy_exec.sh --env-name binance_exec_rx01 \
  --venue binance-futures --exec-backend rapidx
```

New environments default to native when the backend
option is omitted. For an existing environment, the wrapper reads the backend
from its `env.sh` and rejects a conflicting `--exec-backend`; it never rewrites
the operator's account settings. RX requires `LTP_API_KEY`, `LTP_API_SECRET` and
`LTP_PORTFOLIO_ID`, without native Binance/OKX credentials. Binance COIN-M remains
unsupported by RX.

Build release artifacts only in `/home/fanghaizhou/mkt_signal` on synchronized
`arbmm`. `--scripts-only` initializes scripts/config without installing binaries;
`--skip-build` also installs prebuilt artifacts. To initialize a production host,
use `--scripts-only` there, configure its `env.sh` securely, then publish locally
with `scripts/publish-exec.sh --host <host> --env-name binance_exec_rx01 --skip-build`.
Publish reads the target's `env.sh` before selecting artifacts,
requires its venue/namespace to match, checks that all target processes including
the RX monitor are stopped, uploads to staging, verifies SHA-256, rechecks the
backend and processes, and atomically replaces individual files. It preserves
`env.sh` and operator configs. Deploy/publish start no processes and migrate no
signals; the standard start/stop wrappers retain their environment-local script
flow.

Before starting execution, register the source in Manager so its market-rule
cache matches the RX backend and portfolio. An absent or mismatched cache blocks
Exec startup. `start-exec.sh --check-only` validates packaging and credentials
without starting processes; it does not certify the live cache or account
readiness. Starting Exec is a separate live action and does not start
`trade_signal` or move CTA signals.

## Implemented

- WS login must succeed before requests are sent. Login code `0` and action
  code `200000` are handled separately. Requests carry microsecond `ts`.
- Placement/cancellation ACKs correlate by transport ID and expected action;
  an ACK does not invent an order lifecycle or fill.
- Binance order and cancel routing distinguishes RapidX `SPOT`, `MARGIN` and
  `PERP`. Cash LIMIT maker orders use `GTX`; MARKET orders omit time in force.
- RapidX cash MARKET BUY orders use quote-currency `quoteOrderQty` exclusively;
  all other Binance order shapes continue to use base `orderQty`.
- Sent requests with a disconnect, failed send or timeout become an unknown
  action result and enter existing query reconciliation. They are not resent.
- Empty private websocket text/binary frames are ignored; malformed non-empty
  payloads still fail the session and trigger fail-closed snapshot recovery.
- REST order queries preserve the business query ID. Only documented business
  code `401018` means not found; HTTP 404 and malformed responses are errors.
  Duplicate-client-ID/already-completed action responses also query lifecycle.
- Orders, asset balances, signed/zero positions, unrealized PNL and reported
  account risk enter existing account message types with portfolio validation.
- A `NORMAL` account snapshot with `maintainMargin=0` and a reported `uniMMR=0`
  is normalized to the documented no-requirement sentinel `999999` for internal
  risk messages. A zero ratio with nonzero maintenance margin, liquidation, or
  monitor invalidation remains zero and therefore fail-closed.
- Live balances, positions and financial state stay in memory and the account
  IPC stream. There is no raw private-message journal or financial snapshot file.
  Only unacknowledged facts and small recovery metadata use the monitor outbox.
- Settlement recovery accepts the observed `TRANSFER` statement type as ledger
  evidence; it is not converted into a fill.
- Bounded REST snapshots refresh Assets, Positions and Accounts in rotation.
  Complete asset/position snapshots clear previously observed missing identities;
  compact recovery metadata restores both kinds of identity across restarts.
  Older snapshots cannot rewind newer updates. Omission clearing uses the
  request-start boundary, not response-arrival time.
  Disconnect/snapshot failure publishes an invalid account risk value.
- Account REST refresh and execution-history recovery run separately from the
  private WS receive loop. Full account snapshots have a 20-second deadline;
  private heartbeats are not blocked by REST pagination.
- Reported account risk stays gated until fresh Assets, Positions, Accounts,
  LoanInfo, LoanCapacity, Statements and recent execution recovery have completed. Abnormal
  or unknown account/loan status cannot reopen the gate. This is a monitor-side
  recovery gate, not an atomic consumer-acknowledged account snapshot contract.

## Finance And Recovery

Live account risk, balances and positions use the existing account IPC contract.
Event `RapidXFinancial` (4020), on the same source-scoped PM stream, carries
portfolio/exchange, `published_ms`, recovery readiness, the independent Accounts
observation timestamp, frozen margin (decimal string or null), execution/statement
pending counts and their recovery checkpoints. Manager consumes it in memory.
Periodic emission never refreshes an older Accounts observation. Disconnect
publishes invalid risk and `recovery_ready=false`. Other consumers ignore this
financial event. Loan status and portfolio borrowing capacity still use the
read-only bound `rapidxLoan/loan/info` and `rapidxLoan/loan/maxLoan` endpoints;
they do not grant strategy borrowing permissions.

Live Trades and recent REST execution observations feed the same durable owner.
Execution and statement recovery run independently of account REST refreshes and
WS reception, in bounded one-hour windows with a one-minute overlap. A new outbox
anchors execution recovery at its creation time, rather than pretending to own
older account history. New statement collection begins at the current time;
existing collection resumes its durable checkpoint. Execution REST recovery runs
on reconnect and every 30 seconds, covering monitor downtime and bounded-outbox
backpressure. The venue's existing 90-day history limit still applies; an expired
or incomplete recovery window fails closed without advancing its checkpoint.

Every page is checked for scope, time, duplicate IDs and stable pagination. Each
window has a 100-page bound; execution pages are spaced by 2.1 seconds and
statement pages by 1.5 seconds. Facts are saved
before their progress checkpoint; neither incomplete pagination nor a malformed
record can skip an unprocessed interval. WS signed fees and REST split fees and
rebates remain separate observations of the same fill.

## Durable Delivery And Fact Ownership

`data/rapidx_delivery/<source>/delivery.wal` is a bounded, ACK-pruned outbox,
not an account-history archive. Files are mode 0600 and an exclusive lock prevents
concurrent monitor/import writers. It stores at most 4096 pending facts, known
asset/position identities, recovery checkpoints and explicit legacy-import cursors.
It stores no account balances, margin snapshots or raw pushes. Known identities
are synchronized only when first seen, before publication. On restart they carry
zero timestamps and no recovered amounts; fresh complete snapshots clear omitted
identities and supply current values.

Outbox filesystem work runs on a blocking worker. Accepted facts are synchronized
before IPC delivery. The sender has independent 128-record retry windows and
exact-content ACKs, with a one-second retry delay. Only the matching durable ACK
retires a fact. ACK cleanup need not synchronously flush: losing cleanup on crash
only causes idempotent replay. Compaction after 128 retired records or 8 MiB of
new log entries removes acknowledged history. A full queue applies backpressure;
it never evicts facts. Write failures stop processing and require restart.
Only an unterminated terminal WAL append is recoverable; complete corrupt records
fail recovery. Graceful shutdown tries to drain for five seconds, leaving any
remaining facts durable for restart.

The sole durable fact owner is the source's Exec `persist_manager`:

- `rapidx_executions`: unchanged execution/fee contract and stable scoped key.
- `rapidx_execution_roles`: immutable `ExecutionRoleRecord { record, role }`,
  containing the validated execution observation and `MAKER`/`TAKER`. Unknown
  venue roles remain unknown and never prevent retaining a valid execution.
- `rapidx_statements`: the original settlement fact by scoped statement ID.
- `rapidx_statement_observations`: exact subsequent statement observations whose
  only difference is the already-supported equal before/after available-balance
  rounding shift of at most 1e-18. The original fact is never rewritten; the
  differing observation is durably saved before its exact-content ACK.

All participate in generic persistence sync, never the timestamp-keyed order
export repair sweep or order parquet schema. With replication enabled, each
insert and its replication outbox share one synchronized RocksDB batch. Duplicate
receipts do not append duplicate replication rows. A local ACK does not certify
remote replication; enabling sync later does not backfill earlier acknowledged
facts. WS/REST deduplication, strategy attribution and `uniform_orders` quantities
are unchanged. Manager only reads these CFs; it never writes the Exec database.

## Replacing The Legacy Monitor

Build the monitor and persistence owner locally on synchronized `arbmm`:

```bash
cargo build --release --bin rapidx_account_monitor
cargo build --release -p persist_manager --features runtime --bin persist_manager
```

Publish checksum-verified artifacts atomically. Before stopping a live process,
preserve its logs; use only its environment-local stop/start scripts. Upgrade
`persist_manager` first, without restarting trading, then source the intended
host-only env in a private shell and run the new monitor binary in import mode:

```bash
./rapidx_account_monitor --exchange binance --import-legacy data/rapidx_account/<source> --dry-run
./rapidx_account_monitor --exchange binance --import-legacy data/rapidx_account/<source>
```

Import mode makes no exchange requests or account-state publications. The first
pass may run beside the old monitor: it pins each file's observed length, validates
scoped facts, sends them through the owning persistence IPC, and checkpoints its
byte offset only after saving corresponding pending facts. It retains at most a
small batch in memory. After a successful first pass and full ACK drain, stop
only the old account monitor, rerun import to consume its final appended tail,
and start the new monitor using its existing environment-local wrapper. Retain
all old journals and snapshots as historical evidence; do not delete or overwrite
them. If import fails, keep the old monitor and Manager generation running and
resolve the explicit conflict before cutover. Deploy the updated Manager only
after required role evidence has been imported and verified.

The read-only execution/statement exporters remain available; select one fee
observation source or reconcile both before summing. Exporting does not alter
source facts, and a failed partial stdout export is not a complete report.

### jp-meta deployment receipt

The IPC/outbox replacement from production `arbmm` commits `bba62aa4` and
`585a1d7d` was deployed on 2026-11-14. Locally built binaries were uploaded,
SHA-256 verified and atomically installed; only the three persistence owners
and account monitors were replaced through their environment-local scripts.
Trading, Viz, Config and source-IP settings were preserved.

| Environment | Persistence PID | Monitor PID | Preserved CPU mask |
| --- | --- | --- | --- |
| binance_exec_rx01 | 376822 | 413434 | 0-5,48-95 |
| binance_exec_rx03 | 388029 | 415832 | 0-5,48-95 |
| binance_exec_rx04_5m05 | 327610 | 364231 | 0-5,48-95 |

Monitor SHA-256 is
`d1b23e3c666fef3a9939e00dd692bb8478bf8f8493290c590f04a646b14dd2a7`;
persistence owner SHA-256 is
`5ea37317b7db3f7e20f1e86e51ad70695a6014001cfdda17d5e5b0bbb0b50577`.
The absolute-path follow-up was required to create a previously absent delivery
directory durably. The isolated owner rehearsal verified readiness before SIGINT.
RX04's first stop wrapper exceeded its caller's 45-second deadline; repeating
that same wrapper detected and stopped the remaining old monitor before cutover.

First and stopped-tail imports validated 14,033,126 complete legacy rows and
drained exact persistence ACKs. All 13,109,928,015 bytes of legacy JSONL were
retained; frozen sizes and modification times stayed unchanged. A read-only
Manager IPC probe confirmed all three sources recovered with fresh account and
financial observations and finite frozen margin. Manager `08fad46` was deployed
separately as `20261114T105203Z`, PID438574, CPU24; all eight NAV sources loaded.

All 145 sampled configuration hashes, four immutable position snapshot tables
and 180 sampled long-lived protected process identities/CPU masks matched.
An additional baseline finite NAV `sync_history` PID289921 disappeared; its exit
status is unavailable and successful completion is not certified. The original
audit records this exception. Focused tests passed: trade_engine LTP88,
mkt_parsers193, persist_common18 and persist_manager runtime library79; no CTP
execution occurred. The outbox's four tests passed again after the path fix.

A 30-second sample counted two fsync calls per monitor, versus approximately
100/sec in the earlier RX03 sample. This reduces observed write frequency but
does not certify the Redis publication incident resolved: warm Manager timeouts
still occurred at 11:18 and 11:55 UTC. Redis error counters stayed ERR1/NOPERM1;
the exact residual cause remains unproven. Host-only receipts and rollback files:
`~/crypto_cta_manager/backups/rapidx-ipc-20261114/`. Preserve the old archives;
do not blindly restore a pre-cutover database over newly accepted facts.

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
an error, never synthetic fills. Corrupt and unterminated legacy journal lines
fail in this offline audit. Use a complete closed archive. The replacement
monitor does not append these journals. These are read-only inputs, not live actions.

## Settlement Ledger

The account monitor recovers `FUNDING_FEE`, `DEDUCT_INTEREST`,
`LIQUIDATION_FEE` and `LIQ_COMPENSATION` through the documented REST statement
endpoint. There is no invented statement WS subscription. Recovery uses
one-hour windows, a separate durable `statement_history_end_ms` checkpoint and
a 60-second overlap. A new collector begins at the current time; imported
collectors resume their durable checkpoint within the endpoint's 90-day
retention. Tail refresh runs every 30 seconds. Requests
within pagination and catch-up windows are spaced by 1.5 seconds; multiple
deployments sharing credentials still need coordinated rate budgets.

Statement recovery is required for account readiness. The live financial IPC
frame includes its checkpoint and `statement_persistence_pending`. Records
enter the synchronized bounded outbox before checkpoint advancement, then retry
through the durable ACK mechanism. Restart replays pending facts; stable
portfolio/venue/statement identities deduplicate unchanged records and reject
conflicts. Pagination inconsistencies and cross-portfolio rows fail closed.

An overlapping REST read may revise both available-balance context fields by
the same tiny amount (RX01 observed 1e-20 USDT on interest statements). The
durable persistence owner accepts an absolute shift up to 1e-18 only when exact
decimal arithmetic
confirms that the before/after shifts are identical and all other fields,
including identity, settlement amount and debt, remain unchanged. It preserves
the original statement bytes and saves the exact subsequent observation in
`rapidx_statement_observations` before ACK. Larger shifts, changed available deltas,
scientific-notation comparisons and actual settlement/debt conflicts still fail
closed. The IPC contract and persisted statement keys are unchanged.

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
perpetual symbol back before activation. Binance/OKX USDT and USDC perpetual
symbols with active Manager rules are supported. Cash targets skip contract
leverage entirely. This does not add inverse-futures support or change account mode.
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
  Legacy archive retention needs an operational policy; the current monitor
  outbox compacts acknowledged entries. Late venue corrections beyond the
  one-minute overlap require a wider
  recovery, and endpoint retention/completeness still needs authenticated testing.
- Financial/loan inspection is implemented, but accrued interest, collateral
  tier rules and all strategy-facing financial fields are not at full OKX parity.
  Margin-order auto-borrow is confirmed at fill time (see the smoke results
  above), but the explicit `rapidxLoan/loan/borrow` endpoint is not exercised
  and `loan/maxLoan` semantics do not describe margin-order borrow capacity.
  There is no atomic all-account IPC readiness snapshot protocol.
- Nonnumeric external client IDs remain outside the native numeric order
  lifecycle; they no longer invalidate an otherwise valid order stream. Their
  execution evidence is centrally persisted. Read-only liquidation attribution
  uses documented journal evidence, but live unmatched/uniform-order forced-close
  plumbing remains incomplete. Malformed lifecycle messages invalidate the session.
- For RapidX, Binance auto-repay enumerates liabilities from the portfolio
  assets snapshot (`debt` per coin — the only surface where margin-order
  auto-borrow debt appears), merges explicit `rapidxLoan/loan/info` loans per
  coin, and repays `min(debt, available)` via `rapidxLoan/loan/repay`
  (hourly at :55 UTC, amounts floored to 2 decimals, ambiguous transport
  failures are not retried blindly); collection remains disabled. Exec startup is
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
