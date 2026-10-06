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
`binance-coin-futures`. A native Exec with `EXEC_VENUE=binance-futures` or
`EXEC_START_VENUE=binance-futures` also enables COIN-M. `BINANCE_ENABLE_COIN_FUTURES=1` is available for
standalone monitor deployments that do not expose a venue variable.

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
klines, mark prices, funding rates and liquidations. The normal venue-local
services are therefore:

```text
spread_pbs/binance-coin-futures/ask_bid_spread
dat_pbs/binance-coin-futures/derivatives
```

`spread_pbs --venue binance-futures` covers active USDT, USDC and USD perpetuals
in one process, without requiring matching spot pairs. Internally it connects
USD-M through FAPI/FStream and COIN-M through DAPI/DStream, sharing the configured
source IPs and core. The full, market and bookticker roles apply to both markets,
so split market/bookticker deployments do not duplicate each other's IPC
publishers. `binance-both` also adds spot to these two futures markets.

IPC services keep their native `binance-futures` and `binance-coin-futures`
names so Exec receives the correct market and contract quantity semantics.
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

A native Binance Exec and Manager source configured with
`venue = "binance-futures"` manage USDT, USDC and USD perpetuals in the same
account and process. Target suffixes route USDT/USDC to USD-M and USD to COIN-M.
STANDARD accounts retain the USD-M Multi-Assets startup requirement; UNIFIED
accounts use Portfolio Margin UM/CM endpoints. No exchange account mode is
changed automatically. `venue = "binance-coin-futures"` remains available for
COIN-M-only environments. The deploy/publish/start/stop wrappers accept both.

Manager publishes both market scopes in one Redis transaction with one receipt
timestamp. It retains the existing per-market rule caches and position ledgers;
Exec loads both, subscribes to both BBO/volume/mark feeds, and requires the
matching position snapshot before reconciling each market. The Config API shows
one strategy with the merged targets; parameter updates and strategy removal
apply to both scopes. Native combined Exec startup cancels open orders in both
markets before execution begins.
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
coin-margined. Native `binance-futures` sources accept all three suffixes;
COIN-M-only sources accept USD.
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
