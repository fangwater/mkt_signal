# Tonglian domestic-futures baseline replay

`tonglian_baseline_replay` streams the single CSV entry from each Tonglian
`future_<exchange>l2_<YYYYMMDD>.zip`. It supports `ccfx`, `xdce`, `xgfe`,
`xsge`, `xsie`, and `xzce` through one header-driven snapshot adapter. It does
not read combinations, TQS, order queues, or EFP instruments.

The output keeps the existing independent 5-second and 60-second trade/depth
staging layout. Trade tables contain the 32 standard trade-flow columns plus
`quality_flags`, `volume_multiple`, and `volume_multiple_verified`. Domestic
depth tables store only the five observed levels in four columns:

```text
bid_prices  Array(Float64)
bid_amounts Array(Float64)
ask_prices  Array(Float64)
ask_amounts Array(Float64)
```

`cn_features_replay` reads these arrays as an exact five-level shape. It does
not call `TradeFlowFeatureMsg`, `BaselineReplayState`, or any crypto factor
formula, and it never expands the arrays to 20 levels. The crypto
`db_fusion_factor_replay` remains a separate wide-20 path.

## Volume-multiple snapshot

At process startup, the replay loads multiplier values and the `verified` state only from the fixed local PostgreSQL source:

```text
host     /mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run
port     5433
database market_metadata
table    public.domestic_future_product_multipliers
```

The table is queried once by exchange and held as a read-only in-memory catalog for all replay workers. It currently contains 88 unique products, all marked verified. `PD` and `PT` are stored as `1000`, and the DCE source alias `YY` is normalized to product `Y`. The table name and endpoint are fixed; the six templates contain no product map. The replay never calls DataGateway or an exchange API, never infers a missing value from Tonglian turnover, and never defaults a multiplier to `1`. Those upstream checks remain only as snapshot-construction provenance in [preprocess/database/domestic_future_product_multipliers.sql](../../preprocess/database/domestic_future_product_multipliers.sql).

For verified products, the price conversion fields are calculated as:

```text
vwap      = amount / volume / volume_multiple
buy_vwap  = buy_amount / buy_volume / volume_multiple
sell_vwap = sell_amount / sell_volume / volume_multiple
```

A PostgreSQL connection or query failure aborts startup. If a replayed instrument has no product row in the loaded catalog, resolution panics immediately with exchange, product, instrument, and `TradDay`; there is no skip or fallback path. Row-level `effective_from` is inclusive and `effective_to` is exclusive when present. The current rows have neither date populated, so each replay config must still declare a separately validated coverage interval. The supplied templates permit only `2025-11-03`; this database snapshot must not be interpreted as complete historical metadata.

## Snapshot rules

- `ActionDay + UpdateTime` is parsed as `Asia/Shanghai`, then converted to a
  real UTC millisecond timestamp. `TradDay` remains the trading-day partition.
- `Volume` and `Turnover` have independent cumulative states. A reset or gap in
  one does not reset the other.
- A positive `Volume` difference creates one inferred trade interval. Direction
  uses the previous valid best bid/ask, then the tick rule, then a 50/50 split.
- A cumulative high/low new extreme supplements interval high/low. A field
  reset does not create an extreme.
- Depth at bar timestamp `t` is the last valid snapshot strictly before `t`.
  The first bar without a causal prior book has no depth row. Those trade-only
  bars must not enter `cn_features_replay`: factor replay hard-rejects any row
  without a joined native five-level book and does not compute factors on a
  disconnected depth.
- Source gaps and configured auction minutes end forward-fill and direction
  continuity. Auction snapshots still update cumulative baselines but do not
  create auction bars.
- Only source-covered buckets are emitted. The replay does not synthesize long
  closed-market grids.

## Running

All templates default to a one-day dry run:

```bash
cargo run --bin tonglian_baseline_replay -- \
  --config config/tonglian_baseline_xzce.toml
```

For a bounded real-file parser check:

```bash
cargo run --bin tonglian_baseline_replay -- \
  --config config/tonglian_baseline_xzce.toml \
  --max-source-rows-per-file 100000
```

Set `dry_run = false` only after reviewing source, exclusion, ordering, and bar
counters. With `overwrite_existing = true`, the selected trading-day range is
deleted synchronously from all four exchange tables before insertion; a
non-empty `symbols` list also limits deletion to those normalized contract
symbols. Dry runs count and skip malformed, invalid-day, and out-of-order rows
for diagnosis. A formal replay fails on the first such row and rejects
`max_source_rows_per_file`, so a bounded diagnostic run cannot be written as a
partial day. RowBinary inserts use explicit column lists matching the schemas.
Do not widen `start_date` or `end_date` until the PostgreSQL metadata and its date applicability have been independently validated for the wider interval; configuration validation rejects ranges outside the explicitly declared coverage.
