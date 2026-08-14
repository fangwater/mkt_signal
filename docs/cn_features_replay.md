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
review. The generated
[factor-by-factor review](cn_features_factor_review.md) lists source inputs,
legacy depth, CN status, and the reason for all 632 factors. The review has no
runtime role and cannot filter or overwrite outputs.

The source classification is exhaustive: all `factor_001..177` functions read
book fields, while the other 455 factors are trade/bar-only. There is no
separate `DepthDerived` class. Of the 177 book factors, 122 used more than five
levels in the source and are materially redefined for the native five-level
book.

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

- `factor_118` uses the second-level bid/ask mid and the native five-level bid
  VWAP. It requires the two second-level prices and all five bid price/amount
  pairs.
- `factor_119` uses the second-level bid/ask mid and the native five-level ask
  VWAP. It requires the two second-level prices and all five ask price/amount
  pairs.
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

`vwap`, `buy_vwap`, and `sell_vwap` are hidden only when the volume multiple is
not verified. A legitimate no-trade row can carry the baseline contract's
same-segment compatibility-filled VWAP values; those inputs are preserved even
when `volume == 0`. An input VWAP that is itself `NaN` still propagates. The
factor engine does not infer a multiplier from depth.

Seven legacy baselines reference time-aggregate columns that are not stored in
the domestic 32-field row. The CN engine reconstructs them in state from the
same five-second trade fields and upstream preprocessing formulas before
applying each baseline formula:

| Factor | Reconstructed input | Five-second reconstruction | Baseline operation |
| --- | --- | --- | --- |
| `baseline_048` | `active_buy_ratio_5m` | `sum_60(buy_amount) / (sum_60(buy_amount) + sum_60(sell_amount))` | 30-row mean |
| `baseline_075` | `large_pct_30m` | `sum_360(large_order) / sum_360(large_order + medium_order + small_order)` | value / 300-row mean |
| `baseline_078` | `large_pct_120m` | `sum_1440(large_order) / sum_1440(large_order + medium_order + small_order)` | value / 300-row mean |
| `baseline_094` | `small_pct_30m` | `sum_360(small_order) / sum_360(large_order + medium_order + small_order)` | value / 300-row mean |
| `baseline_095` | `small_pct_120m` | `sum_1440(small_order) / sum_1440(large_order + medium_order + small_order)` | value / 300-row mean |
| `baseline_102` | `net_buy_small_pct_15m` | `sum_180(net_buy_small) / sum_180(small_order)` | 120-row mean |
| `baseline_155` | `active_buy_ratio_240m` | `sum_2880(buy_amount) / (sum_2880(buy_amount) + sum_2880(sell_amount))` | 150-row mean |

The aggregate windows use `min_periods=1` at segment start. A zero activity
denominator maps to `0.5` for active-buy ratios and to `0` for order-size
shares, matching the upstream preprocessing defaults. A missing required CN
field is not skipped: the reconstructed value stays missing until that row
leaves its aggregate window. Segment and trading-day resets clear these states.

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
subject to the explicit five-level changes, reconstructed upstream aggregates,
and formula repairs above. They can still return NULL if a field they actually
read is absent in the domestic baseline.

## Formula and missing-value verification

Corrections are made in the factor implementation or shared mathematical
operator. Replay does not contain factor-number filters, output overrides, or
fallback values. The implementation audit restored these source definitions:

- `factor_049` uses the five-level bid-price mean.
- `factor_050`, `factor_051`, `factor_052`, `factor_093`, and `factor_094` use
  the five-level ask-price mean.
- `factor_118` and `factor_119` use the source's second-level mid and full
  five-level side VWAP.
- `TD_TI_033` honors `rolling(300, min_periods=100)`; the shared rolling
  correlation operator now applies its `min_periods` argument.

The generated test-only manifest partitions all 632 source factors into 455
trade/bar-only factors and 177 book factors. A branch with no book from its
first row must keep every trade/bar-only factor finite and numerically equal to
a valid-book branch after deterministic warm-up. Removing the current whole
book after warm-up must make all 177 book factors NULL. Separate exhaustive
tests inject `NaN` into each native depth field and verify that any factor which
remains available does not skip or consume the missing value. Representative
side/level tests additionally verify that an unrelated ask side or inner level
remains available. `factor_052` explicitly checks all price inputs required by
its 300-difference ask-price window rather than converting `NaN < 0` to a zero
contribution.

The review generator also audits Rust dispatcher ownership. The current route
partition is 200 baseline, 205 OPV, 59 plain, and 168 direct implementations;
every source factor must appear in exactly one route.

For formulas whose source used at most five levels and that were not explicitly
redefined or repaired, run the reproducible Python/Rust audit in an environment
with NumPy and pandas:

```bash
python3 scripts/audit_cn_factor_parity.py
```

The current audit covers 506 factors at rows 199, 399, and 799. All 1,518
comparisons pass with a relative tolerance of `1e-8`. This includes the seven
reconstructed upstream aggregates. The 122 deep-book redefinitions and five
deliberate formula repairs are excluded from legacy-formula parity by
definition and are itemized in the review; these sets overlap by one factor.

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
