# LSEG Features 1-Minute Replay

`lseg_futures_fusion_factor_replay` computes the independent 632-factor
`lseg_features` registry for fixed-expiry LSEG/CME contracts at one-minute
frequency. It is parallel to `cn_features`, but consumes native ten-level CME
books and TAS-derived trades rather than domestic five-level snapshots.

The registry is generated from the same
`final_factor_pool_update20260123.py` reference as `cn_features`. Its static
`lseg_features_all` plan validates every one of the 632 computation paths at
startup; it does not probe or silently reduce to the legacy generic-fusion
subset.

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
book through an unobserved second. The reader projects only identity and
price/size columns, then retains only these `t+59s` snapshots, rather than
materializing a whole day of one-second books in replay state.
Depth parquet is consumed in bounded row-group batches, so the complete daily
one-second table is never held in a DataFrame at once.

## Semantics

The replay maintains one state per `contract_id` and resets it on every gap
larger than one minute. It therefore cannot carry rolling windows over the CME
maintenance/closed interval or between contracts. It does not forward-fill any
source price field. Special-only and one-sided minutes remain rows; only the
individual factors needing their unavailable source values become parquet null.

`replay_workers` distributes independent fixed-expiry contracts across stable
worker lanes. A single contract always stays on one lane, so its rolling state
is serialized across all trading days; output remains one parquet file per
input product/day.

The depth adapter preserves the native ten levels. No levels 11 through 20 are
invented with zeroes, copied prices, or order-count fields. Formula depth
windows are natively ten-level, with full-book sums, means, VWAPs, quantiles,
and cross-sectional statistics calculated from the observed LL2 levels.

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
