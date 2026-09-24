# Intra same-side spread factors

For the open venue v1 and hedge venue v2, `rolling_metrics` samples these raw ratios from paired BBO updates:

```text
bidbid_ho = (v2_bid - v1_bid) / v1_bid
askask_oh = (v1_ask - v2_ask) / v1_ask
```

The ratios are not bps. A positive `bidbid_ho` favors buying v1 as Maker and selling v2 as Taker; a positive `askask_oh` favors selling v1 as Maker and buying v2 as Taker. They describe prices conditional on the Maker order filling.

`rolling_metrics_thresholds_{open}_{hedge}` publishes `bidbid_ho`, `askask_oh`, and their `_quantiles` arrays. The margin/spot to futures rolling defaults include q85 and q90 for both factors. Existing Redis factor configurations must also include them before switching intra thresholds; the new series need enough samples to become ready.

The intra spread mapping keeps the old `bidask` / `askbid` factors by default. To switch MT thresholds, set the four mapping values in `intra_spread_thresholds_config_{open}_{hedge}` to:

```text
forward_open_mt     bidbid_ho_90   (>)
forward_cancel_mt   bidbid_ho_85   (<)
backward_open_mt    askask_oh_90   (>)
backward_cancel_mt  askask_oh_85   (<)
```

The intra config server accepts these values in Spread Threshold Mapping. `intra_scripts/sync_intra_spread_thresholds.py --same-side` also writes this mapping when explicitly run. MM thresholds continue to use `spread`. Forward close uses the backward open factor, and backward close uses the forward open factor. Rolling quantiles and live spread checks use the same raw-ratio units.
