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
`binance-coin-futures`. `BINANCE_ENABLE_COIN_FUTURES=1` is available for
standalone monitor deployments that do not expose a venue variable.

## Quantity semantics

Exchange order quantity and position amount are contract counts. The internal
base exposure is price-dependent:

```text
base_qty = contracts * contractSize / price
```

`contractSize` is loaded from DAPI `exchangeInfo`. Order sizing uses the order
price, fills use the execution price, and account positions use the current
COIN-M mark price. A missing contract size or non-positive price rejects the
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

Examples:

```bash
dat_pbs --venue binance-coin-futures
spread_pbs --venue binance-coin-futures
pre_trade --open-venue binance-margin --hedge-venue binance-coin-futures
exec-pre-trade --venue binance-coin-futures
```

The Exec startup gate queries and cancels all existing COIN-M orders before
starting. `scripts/binance_cancel_all_std_cm_orders.py` and the unified cancel
script are dry-run unless `--execute` is provided.
