# LSEG Futures 1-Minute Fusion Factors

`lseg_futures_fusion_factor_replay` computes every current implemented fusion
factor for fixed-expiry LSEG/CME contracts at one-minute frequency. It does not
make a dominant-contract series and it does not change the raw LSEG data
directories.

## Inputs

- `baseline_data_1m_drop_special/{exchange}/{product}/{YYYYMMDD}.parquet` is
  the trade input. It supplies the common 32 trade-flow fields. Printable TAS
  trades are included; `Special` remains excluded from price, amount, and
  direction fields according to its source contract.
- `level2_1s/{exchange}/{product}/{YYYYMMDD}.parquet` supplies the native
  Normalized LL2 book. It has exactly ten price/size levels per side. LSEG's
  `bid0*` and `ask0*` are level one.

For a trade bar whose `ts` is the UTC left edge of `[t, t+60s)`, the factor
input book has `depth_ts = t+59s`: the final complete one-second snapshot of
that closed bar. The output keeps `ts=t` and records `depth_ts` explicitly.
Missing `t+59s` depth means there is no factor row; the replay never carries a
book through an unobserved second.

## Semantics

The replay maintains one state per `contract_id` and resets it on every gap
larger than one minute. It therefore cannot carry rolling windows over the CME
maintenance/closed interval or between contracts. It does not forward-fill any
source price field. Bars before all required source price fields are initialized
are skipped. Factor values that are warming up, mathematically invalid, or
depend on unavailable source values are written as parquet null.

`replay_workers` distributes independent fixed-expiry contracts across stable
worker lanes. A single contract always stays on one lane, so its rolling state
is serialized across all trading days; output remains one parquet file per
input product/day.

The depth adapter preserves the native ten levels. Its internal cache uses NaN
sentinels after level ten; levels 11 through 20 are not invented with zeroes,
copied prices, or order-count fields. A formula requesting 15 or 20 levels uses
all available native levels instead. Sums become visible-book sums; means,
VWAPs, quantiles, and cross-sectional statistics use the actual number of
available levels. This preserves every implemented factor without silently
adding synthetic depth.

## Run

The checked-in config is a dry-run for one ES trading day:

```bash
cargo run --release --bin lseg_futures_fusion_factor_replay -- \
  --config config/lseg_futures_fusion_factor_replay.toml
```

First validate the configuration without reading source parquet:

```bash
cargo run --bin lseg_futures_fusion_factor_replay -- \
  --config config/lseg_futures_fusion_factor_replay.toml --validate-config-only
```

Set `dry_run = false`, choose the intended date range/product/contract filters,
and set `overwrite = true` only when replacing the corresponding output files.
