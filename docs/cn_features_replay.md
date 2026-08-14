# China futures cn_features replay

`cn_features_replay` is the offline factor path for domestic commodity and
financial futures. It is deliberately separate from the crypto
`db_fusion_factor_replay` path.

| Contract | China futures | Crypto |
| --- | --- | --- |
| Module namespace | `factor_pub::cn_features` | `factor_pub::fusion_factor_pub` |
| Replay binary | `cn_features_replay` | `db_fusion_factor_replay` |
| Depth shape | four native `[f64; 5]` arrays | 20 wide price/amount levels |
| Message/state | `FuturesFusionInput` / `FuturesFusionState` | `TradeFlowFeatureMsg` / `BaselineReplayState` |
| Factor names | `cn_features_*` only | crypto factor plan names |
| Date selection | inclusive `trading_day` range | UTC timestamp range |

The two registries do not accept each other's factor names. In particular,
`baseline_118` is not a valid cn_features factor.

## Native depth contract

The replay reads these ClickHouse columns directly:

```text
bid_prices  Array(Float64)
bid_amounts Array(Float64)
ask_prices  Array(Float64)
ask_amounts Array(Float64)
```

Every array must have length 5. Length 4, 6, or 20 is an input-contract error.
There is no 20-level intermediate representation and no NaN padding for
unobserved levels 6-20. A genuinely missing value inside one of the five native
levels remains missing; factors that require that level return NULL.

## Initial registry

The initial registry is intentionally small. These factors establish and test
the independent five-level path; they are not a port of the crypto factor pool.

| Factor | Formula |
| --- | --- |
| `cn_features_book_mid_price` | `(bid_price_1 + ask_price_1) / 2` |
| `cn_features_book_spread` | `ask_price_1 - bid_price_1` |
| `cn_features_book_imbalance_5` | `(sum(bid_amount_1..5) - sum(ask_amount_1..5)) / (sum(bid_amount_1..5) + sum(ask_amount_1..5))` |

All factor output columns are `Nullable(Float64)`. Adding a factor requires a
new cn_features identifier and a China-futures-specific formula review. A
crypto implementation with a similar name is not sufficient evidence.

The input also retains `quality_flags`, `volume_multiple`, and
`volume_multiple_verified`. Current book-only primitives do not depend on
turnover conversion. Future trade or VWAP factors must explicitly decide how
to handle unverified volume multiple metadata; they must not silently inherit a
crypto formula.

## Trading-day and output rules

The selected date range applies to `TradDay`-derived `trading_day`, not UTC
calendar dates, so a night session remains attached to its domestic trading
day. Output rows retain `trading_day`, source quality flags, and the source
volume-multiple verification bit.

The output database and table names are separate from crypto, for example:

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

Run the input replay and factor calculation without writing output by omitting
`--validate-config-only` while keeping `dry_run = true`.
