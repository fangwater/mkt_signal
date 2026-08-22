# Bitget COIN-FUTURES

`bitget-coin-futures` is an independent `TradingVenue` for Bitget UTA inverse
contracts. It does not change the meaning of `bitget`, `bitget-futures`, or
`bitget-both`; coin futures must be selected explicitly.

## Account model

Bitget COIN-FUTURES uses the existing UTA v3 private WebSocket and REST
credentials. There is no standard-versus-unified account matrix. The
`trade_engine` startup precheck still requires UTA and `one_way_mode`.

Account balances and account-level risk remain in the shared
`BitgetUnified` scope. COIN-FUTURES orders, fills, positions, and unrealized
PnL use the independent `BitgetUnifiedCoinFutures` scope so USDT-FUTURES
positions cannot enter the coin-futures leg.

## Symbols and quantities

Bitget exchange symbols use the `_CM` suffix:

```text
exchange: BTCUSD_CM
internal: BTCUSDCM
```

When a strategy starts from the spot canonical symbol `BTCUSDT`, venue
normalization derives `BTCUSD_CM` for the coin-futures leg.

Bitget COIN-FUTURES `qty` is USD contract notional. The instrument metadata
table stores a face value of 1 USD per quantity unit. Internal exposure is
always base quantity:

```text
base_qty = venue_qty * 1 USD / mark_price
```

Order alignment performs the inverse calculation and rounds the resulting
venue quantity to `quantityMultiplier`. Persisted quantities and max-position
checks use base quantity.

## Runtime services

Explicit venue examples:

```bash
depth_pub --venue bitget-coin-futures
spread_pbs --venue bitget-coin-futures
pre_trade --open-venue bitget-margin --hedge-venue bitget-coin-futures
```

The relevant IPC services are:

```text
spread_pbs/bitget-coin-futures/ask_bid_spread
dat_pbs/bitget-coin-futures/derivatives
```

`bitget_account_monitor` detects `bitget-coin-futures` in `OPEN_VENUE`,
`HEDGE_VENUE`, or `EXEC_VENUE` and additionally polls:

```text
GET /api/v3/position/current-position?category=COIN-FUTURES
```

## Position-tier cache

Run a separate sidecar for COIN-FUTURES. The product type selects exchange
symbols such as `BTCUSD_CM`; the default cache key and process name are also
separate from USDT-FUTURES.

```bash
PRODUCT_TYPE=COIN-FUTURES scripts/start_bitget_position_tier_sidecar.sh
```

Default Redis key:

```text
bitget_position_tier_cache:COIN-FUTURES
```

## Read-only checks

Instrument metadata and funding history can be checked without credentials:

```bash
curl -sS 'https://api.bitget.com/api/v3/market/instruments?category=COIN-FUTURES'
curl -sS 'https://api.bitget.com/api/v3/market/history-fund-rate?category=COIN-FUTURES&symbol=BTCUSD_CM&limit=2'
```

Do not use private order, cancel, leverage, or flatten scripts for a smoke test.
