# Binance COIN-M support

`binance-coin-futures` is an independent `TradingVenue`. Symbols retain the
Binance delivery API form at exchange boundaries, for example `BTCUSD_PERP`
and `ETHUSD_260925`.

## Account modes

Both Binance account modes are supported:

- `BINANCE_ACCOUNT_MODE=STANDARD`: orders, cancels, queries, leverage and
  snapshots use `/dapi/v1/*`; user data uses a DAPI listen key and
  `wss://dstream.binance.com/ws/<listenKey>`.
- `BINANCE_ACCOUNT_MODE=UNIFIED`: COIN-M execution uses `/papi/v1/cm/*`.
  The Portfolio Margin user stream is shared with UM, and events carrying
  `fs=CM` are routed to the COIN-M venue and account scope.

`BINANCE_API_KEY` and `BINANCE_API_SECRET` are required in both modes. Optional
endpoint overrides are `BINANCE_DAPI_URL` and `BINANCE_PAPI_URL`.

The account monitor enables COIN-M when any of `OPEN_VENUE`, `HEDGE_VENUE`,
`EXEC_VENUE`, `EXEC_START_VENUE`, or `VENUE` is
`binance-coin-futures`. For an Exec deployment, `EXEC_VENUE` / `EXEC_START_VENUE`
select exactly one market and take precedence over generic opening/hedging venue
settings. A U-margined Exec never enables COIN-M automatically.
`BINANCE_ENABLE_COIN_FUTURES=1` is available only for standalone monitor
deployments without an Exec market binding.

## Quantity semantics

Exchange order quantity and position amount are contract counts. The internal
base exposure is price-dependent:

```text
base_qty = contracts * contractSize / price
```

`contractSize` comes from DAPI `exchangeInfo`. General order sizing uses the
order price, fills use the execution price, and account positions use the current
COIN-M mark price. Exec target sizing uses that same mark as its position ledger;
changing a maker limit price preserves the native contract count. A missing contract size or non-positive price rejects the
conversion instead of falling back to a linear multiplier.

For standard-account intra trading, the account monitor polls DAPI balances
every five seconds. New risk is blocked when the collateral asset snapshot is
missing/stale or when `availableBalance / (crossWalletBalance + crossUnPnl)` is
below 10%. `BINANCE_CM_WALLET_POLL_INTERVAL_SECS` can override the interval.

## Runtime

Public market data uses DAPI `exchangeInfo` and DStream for depth, BBO, trades,
klines, mark prices, funding rates and liquidations. All Binance perpetual markets share these services:

```text
spread_pbs/binance-futures/ask_bid_spread
dat_pbs/binance-futures/trade
dat_pbs/binance-futures/incremental
dat_pbs/binance-futures/derivatives
```

`spread_pbs --venue binance-futures` covers active USDT, USDC and USD perpetuals
in one process, without requiring matching spot pairs. Internally it connects
USD-M through FAPI/FStream and COIN-M through DAPI/DStream, sharing the configured
source IPs and core. The full, market and bookticker roles apply to both markets,
so split market/bookticker deployments do not duplicate each other's IPC
publishers. `binance-both` also adds spot to these two futures markets.

USDT, USDC and USD contracts share the `binance-futures` venue component for
BBO, trade, incremental and derivatives IPC, including COIN-M-only runs.
Each data type still has its own service and unchanged payload format. Wire symbols
remain `BTCUSDT`, `BTCUSDC` and `BTCUSD_PERP`; COIN-M amounts remain contract counts.
USD-M/COIN-M tasks share one publisher per service with independent symbol-slot
caches. Exec consumes each combined service once and routes messages back to the
native market by symbol before applying contract-size conversions. Market-specific
consumers filter out the other market. Proxy/test roots use the same combined names.
Update publishers and consumers together when deploying; the former
`binance-coin-futures` market-data IPC services are no longer published by spread_pbs.
`SPREAD_PBS_SYMBOLS=BTCUSD` selects the wire subscription `BTCUSD_PERP`; a filter
containing only USDT/USDC or only USD starts the matching futures market. Stop
any standalone COIN-M publisher before starting a combined futures deployment.
The RapidX public provider covers USD-M only; COIN-M requires native feeds.

Examples:

```bash
dat_pbs --venue binance-coin-futures
spread_pbs --venue binance-futures --core "$SPREAD_PBS_CORE"
pre_trade --open-venue binance-margin --hedge-venue binance-coin-futures
exec-pre-trade --venue binance-coin-futures
```

The Exec startup gate queries and cancels all existing COIN-M orders before
starting. `scripts/binance_cancel_all_std_cm_orders.py` and the unified cancel
script are dry-run unless `--execute` is provided.

## Exec and CTA Manager

Each Exec deployment and Manager source owns exactly one market:
`venue = "binance-futures"` accepts USDT/USDC perpetuals, while
`venue = "binance-coin-futures"` accepts USD coin-margined perpetuals.
Use independent environment directories, source IDs, IPC namespaces, Redis
prefixes, persist_manager RocksDB stores, and Viz/Config listeners. One Manager
continues to manage both types as independent sources. Neither Config nor Manager
splits a strategy across markets; an opposite-market target or override is rejected
before runtime publication. Batch/POV and Chase also validate persisted target and
allocation symbols before leverage initialization or strategy registration.

Startup cancellation touches only the configured market. The trade engine rejects
another market's order, cancel, amend, leverage and position-query requests. The
account monitor starts only the selected standard futures stream and filters
Portfolio Margin order/position events before forwarding them into the environment.
Shared Portfolio Margin collateral, debt and risk remain visible for correct account
risk calculations; this separation does not create separate exchange margin pools.
STANDARD USD-M retains the Multi-Assets startup requirement. No exchange account
mode is changed automatically. Public market-data services may still be shared;
each Exec consumes only symbols from its own market.
Manager remains the sole owner of rule refresh; Exec consumes its complete
current cache, including `contractSize`, tradable status and quantity filters.

A Manager position-strategy request can contain:

```json
{
  "strategy_name": "coin_btc",
  "targets": {"BTCUSD": {"qty": 0.01, "signal": 0}}
}
```

Exec and Manager targets identify perpetual contracts by their quote suffix:
`BTCUSDT` is USDT-margined, `BTCUSDC` is USDC-margined, and `BTCUSD` is
coin-margined. USD-M sources accept USDT/USDC; COIN-M sources accept USD.
Quantity remains base coin: this means 0.01 BTC, before binding shares. The
current internal key is `BTCUSD`; only exchange requests restore `BTCUSD_PERP`.
Wire-name aliases normalize to the same key and duplicates are errors. Delivery
contracts are rejected by the target API. USD is the quote currency, while
collateral and settlement are in the underlying coin.

At a 50,000 USD mark and 100 USD BTC contract size, 0.01 BTC corresponds to five
contracts. Exec rounds in contract units using the venue step and minimum;
requests below the minimum are held. The allocated position and outstanding
orders conserve USD face value when the mark changes and are displayed as
face value / current mark. The target remains the requested coin quantity, so
price changes can change the executable gap. A zero target closes the allocated
contract count. Batch/POV-to-Chase switches transfer the conserved face value.

Native Batch, POV and Chase work in STANDARD and UNIFIED modes. Chase uses
signed PUT `/dapi/v1/order` or `/papi/v1/cm/order` for in-place amendments.
RapidX/LTP COIN-M execution remains unsupported. The existing `_usdt` parameter
names are retained; for COIN-M their notional budgets and tolerances are USD.
Missing valid mark/contract data blocks quantity conversion.

Manager restores wire symbols for leverage and commission APIs and computes
factual inverse trade PnL by matching USD face, rather than fill-time coin
quantity. Its reported USD PnL is coin PnL converted at the close/mark; it excludes
collateral revaluation and account-ledger flows. Minute-kline theoretical NAV
and the USDT/BFUSD live-equity widget remain USD-M only.

## Separate Viz frontends

Exec Viz requires `[servers.exec_pre_trade].venue` alongside its explicit namespace.
Set it to the deployment's `EXEC_VENUE`. Wrong-market state/risk samples are
discarded. `VIZ_CHECK_CONFIG_ONLY=1` validates the configuration without opening
IPC or HTTP services; the start wrapper runs this before stopping an existing Viz.

USD-M/OKX uses `docs/exec_pre_trade_dashboard.html`. COIN-M uses its independent
frontend `web/exec_coin/index.html`, with per-symbol coin quantities and USD
notional labels. The server selects the frontend from configuration, never from
a browser query parameter. Each deployment retains its own root, WebSocket,
snapshot and read-only Config proxy. COIN-M instance 01 defaults to Viz 10141 /
Config 18261; USD-M instance 01 retains 10041 / 18161. Explicit port overrides
remain supported and must be checked for availability before provisioning.

Existing Exec Viz TOMLs need the explicit venue added before publishing this
release. Provision a fresh environment/source for the other market; do not change
an existing deployment's market or relabel/copy its order history. Existing archives
remain immutable under their original source IDs. Historical reconstruction still
resolves their factual markets from their archived symbols; this grants no runtime
permission to publish or execute the other market.
