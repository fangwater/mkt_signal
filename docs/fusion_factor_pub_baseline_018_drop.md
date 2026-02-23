# Fusion Factor Pub: baseline_018 Drop Rule

## Background

`baseline_018` is defined as:

- `ratio = buy_volume / sell_volume`
- output = `pct_change(ratio, 60)`

When `sell_volume == 0` on a bar, the ratio is not finite and can force the factor to return `None`.
That can make runtime status flip from `all_ready` back to `warming_up`, which is not desired.

## Rule

For symbols that include `baseline_018` in factor plan, `fusion_factor_pub` now applies
an input gate before updating symbol state:

- if `sell_volume` is not finite, or
- if `sell_volume <= 1e-12`

then the whole sample is dropped.

## Scope

The drop rule is applied in both paths:

- RocksDB bootstrap replay
- Real-time trade_flow stream

## Runtime Effect

Dropped sample behavior:

- Do not push this sample into in-memory rolling state
- Do not evaluate factors for this sample
- Do not publish fusion output for this sample

Logging and metrics:

- Drop warning is throttled: first 3 events, then every 200 events
- Periodic stats include `baseline018_dropped`
- Bootstrap done log includes `baseline018_dropped`

## Note on Window Semantics

Because dropped samples are not inserted into rolling state,
`baseline_018`'s 60-lag effectively advances on valid samples,
not on every fixed 5-second bar.
