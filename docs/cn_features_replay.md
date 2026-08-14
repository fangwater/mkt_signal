# China futures `cn_features` replay

`cn_features_replay` is the offline factor path for domestic commodity and
financial futures. Input state, factor registry, formulas, and output are
separate from the Crypto fusion path.

| Contract | China futures | Crypto |
| --- | --- | --- |
| Module namespace | `factor_pub::cn_features` | `factor_pub::fusion_factor_pub` |
| Replay binary | `cn_features_replay` | `db_fusion_factor_replay` |
| Depth shape | Native five-level arrays | Native 20-level arrays |
| Factor namespace | `cn_features_*` | Crypto factor names |
| Date selection | Inclusive `trading_day` range | UTC timestamp range |

The registries do not accept each other's names. For example,
`cn_features_baseline_118` is a CN factor, while `baseline_118` is rejected by
the CN replay.

## Complete registry

`cn_features_all` expands to all 632 migrated factors. The canonical output
name is `cn_features_` followed by the lower-case legacy identifier.

| Family | Count |
| --- | ---: |
| `factor_001..177` | 177 |
| `factor_trades_001..050` | 50 |
| `baseline_001..200` | 200 |
| `TD_TI_001..045` | 45 |
| `TD_MT_001..044` | 44 |
| `TP_VPI_001..019` | 19 |
| `TD_VI_001..028` | 28 |
| `TD_PT_001..029` | 29 |
| `TD_CI_001..010` | 10 |
| `TD_SI_001..013` | 13 |
| `TD_PR_001..017` | 17 |

All formulas are dispatched inside `factor_pub::cn_features`. The replay does
not post-process selected factor IDs or overwrite their results in a
compatibility layer.

Complete registration means that every factor has a CN computation path. It
does not mean that every inherited formula has passed an economic-meaning
review. The material reinterpretations are listed below so they can be
reviewed one by one.

## Native depth contract

The replay reads these ClickHouse columns directly:

```text
bid_prices  Array(Float64)
bid_amounts Array(Float64)
ask_prices  Array(Float64)
ask_amounts Array(Float64)
```

The structural contract is strict:

- All four arrays empty means that the row has no book.
- Otherwise every array must contain exactly five elements.
- Four, six, or twenty levels are input errors; mixed empty/five-level arrays
  are also input errors.
- There is no 20-level representation, padding, or fabricated level 6-20.

Within a valid five-element array, `NaN` means that the individual field is
missing. It is not converted to zero and does not invalidate unrelated sides or
levels. Infinite values, finite non-positive prices, finite negative amounts,
and a crossed finite best book are input errors.

Missing values propagate according to each formula's actual dependencies:

- A missing whole book makes book factors NULL while trade-only factors still
  evaluate and advance their own history.
- A missing fifth level leaves BBO and first-three-level factors available, but
  makes five-level aggregates that include it NULL.
- A missing bid field does not by itself make an ask-only factor NULL.
- A rolling book factor is NULL while a required missing observation remains in
  its window. Rolling helpers do not skip the missing row.
- Cross-snapshot formulas do not bridge an unavailable required book state.

All output factor columns are `Nullable(Float64)`. Unavailable required input
and incomplete warm-up produce NULL. A genuine mathematical zero remains zero;
zero-denominator behavior follows the migrated formula's explicit definition
and is never used as a substitute for missing input.

## Five-level formula changes

Depth formulas were changed at their definitions rather than filtered by factor
number at replay time.

| Legacy dependency | CN definition |
| --- | --- |
| Full 10/15/20-level aggregate | All five native levels |
| Inner/full concentration | First three levels divided by all five |
| Legacy outer level 10 or 20 | Native level 5 |
| Random ten-level sample | Deterministic use of all five levels |
| Per-level cross-sectional statistic | Exactly the five observed levels |
| Missing level in an aggregate | NULL, never skip-and-reaverage |

Notable definitions are:

- `factor_118` uses the native five-level bid VWAP and requires all five input
  price/amount pairs.
- `factor_157`, `factor_158`, and `factor_159` use five-level cross-sectional
  inputs.
- `factor_160` computes the mean percentage change of the five per-level bid
  shares. It does not retain the legacy random ten-of-twenty selection.
- `factor_166` is the five-level book imbalance
  `(sum_bid_amount - sum_ask_amount) / (sum_bid_amount + sum_ask_amount)`.
  The legacy expression subtracted an amount from a price and was dimensionally
  invalid, so it was not preserved.

## Trade fields and meaning review

The 32 trade fields are passed to CN formulas without synthetic proxies. In
particular, missing `count`, directional counts, and large/medium/small order
fields remain missing; they are not reconstructed from volume or split into
fixed proportions. Zero volume or amount is not rewritten to an epsilon, and a
missing trade field does not cause the whole row to be dropped.

`vwap`, `buy_vwap`, and `sell_vwap` are hidden when the volume multiple is not
verified or when the row has no volume. Factors that require those fields then
return NULL. The factor engine does not infer a multiplier from depth.

Seven legacy baselines referenced feature columns that are not present in the
domestic 32-field input. They are registered, but their CN definitions are
material substitutions and therefore need explicit factor-by-factor approval:

| Factor | Missing legacy input | Current CN definition | Review concern |
| --- | --- | --- | --- |
| `baseline_048` | `active_buy_ratio_5m` | 30-row mean of `buy_volume / volume` | Horizon and source aggregation changed |
| `baseline_075` | `large_pct_30m` | `volume / mean(volume, 300)` | Large-order meaning is lost |
| `baseline_078` | `large_pct_120m` | `amount / mean(amount, 300)` | Large-order meaning and horizon are lost |
| `baseline_094` | `small_pct_30m` | `volume / mean(volume, 300)` | Small-order meaning is lost; duplicates `baseline_075` |
| `baseline_095` | `small_pct_120m` | `amount / mean(amount, 300)` | Small-order meaning is lost; duplicates `baseline_078` |
| `baseline_102` | `net_buy_small_pct_15m` | 120-row mean of `net_buy_pct` | Small-order and horizon meaning changed |
| `baseline_155` | `active_buy_ratio_240m` | 150-row mean of `buy_volume / volume` | Upstream 240-minute aggregation is unavailable |

Four additional source formulas were repaired rather than copied literally:

- `factor_054` uses `(bid_price_1 + ask_price_1) / 2` as its mid-price term.
  The source expression averaged `bid_price_1` with `bid_amount_1`, which mixed
  incompatible units.
- `baseline_157` uses close-price efficiency over 30 rows. The source code used
  volume while its own comment stated that this was the wrong input.
- `baseline_159` and `baseline_160` apply sine/cosine to one-row close log
  return. Applying them to the absolute quoted price was non-stationary and
  contract-scale dependent. These transforms remain unusual and should still
  be reviewed for research value.

All other factors retain their legacy field selection and operation structure,
subject to the explicit five-level changes above. They can still return NULL if
their source trade field is absent in the domestic baseline.

## Trading day and output

The selected range applies to `TradDay`-derived `trading_day`, not UTC calendar
dates, so a night session remains attached to its domestic trading day. Output
rows retain `trading_day`, source quality flags, and the source
volume-multiple verification bit.

The output database and table are separate from Crypto, for example:

```text
cn_features.cn_features_xzce_5s
```

All six templates default to `dry_run = true`. Validate a template without
contacting ClickHouse:

```bash
cargo run --bin cn_features_replay -- \
  --config config/futures_fusion_factor_replay_xzce.toml \
  --validate-config-only
```

Run input replay and factor calculation without writing output by omitting
`--validate-config-only` while keeping `dry_run = true`.
