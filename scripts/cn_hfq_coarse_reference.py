"""CN HFQ hourly/daily aggregation and independent continuously rolling formulas."""
from __future__ import annotations

import numpy as np
import pandas as pd

from build_cn_hfq_5min import aggregate_groups


def aggregate_coarse(frame: pd.DataFrame, minutes: int, day: int):
    if minutes not in (60, 1440):
        raise ValueError('only 1h and 1day supported')
    ts = frame.ts.to_numpy(dtype=np.int64)
    if not len(ts):
        raise ValueError('empty official trading day')
    source_starts = np.r_[True, np.diff(ts) != 60]
    index = np.arange(len(ts))
    if minutes == 1440:
        starts = np.array([0])
        segment_starts = ts[:1]
    else:
        anchor = np.maximum.accumulate(np.where(source_starts, index, 0))
        starts = np.flatnonzero(source_starts | ((index-anchor) % minutes == 0))
        segment_starts = ts[source_starts]
    ends = np.r_[starts[1:], len(ts)]
    result, metadata = aggregate_groups(frame, starts, ends)
    return result, dict(
        bar_minutes=minutes, trading_day=day,
        ts=result.ts.astype(int).tolist(),
        segment_start_ts=segment_starts.astype(int).tolist(),
        continuous_bars=True, **metadata,
    )


def representative_factors(frame: pd.DataFrame):
    """Seven native formulas, strict original bar windows and global shift(1)."""
    mid = (frame.bid0p+frame.ask0p)/2
    ask3 = frame[[f'ask{i}v' for i in range(3)]].sum(axis=1, skipna=False)
    bid5 = frame[[f'bid{i}v' for i in range(5)]].sum(axis=1, skipna=False)
    ask5 = frame[[f'ask{i}v' for i in range(5)]].sum(axis=1, skipna=False)
    mean30 = ask3.rolling(30, min_periods=30).mean()
    ratio = ask3/mean30
    ratio = ratio.mask(mean30.abs() <= 1e-12, 0.)
    # The five-level mean bid price / mid ratio ranked in an unshortened
    # 500-row window. Native formula uses an unnormalised one-based rank.
    mean_bid = frame[[f'bid{i}p' for i in range(5)]].mean(axis=1, skipna=False)
    r = (mean_bid/mid).where(mid.abs() > 1e-12)
    def rank_last(x):
        return (x < x[-1]).sum() + ((x == x[-1]).sum()+1)/2
    raw = dict(
        factor_005=mid.diff(), factor_007=bid5-ask5,
        factor_034=ratio,
        factor_081=(frame.bid4v+frame.ask4v).rolling(3, min_periods=3).mean(),
        factor_031=r.rolling(500, min_periods=500).apply(rank_last, raw=True),
        baseline_097=frame.close.rolling(45, min_periods=45).mean(),
        baseline_106=(frame.close.rolling(20, min_periods=20).mean()
                      +frame.high.rolling(20, min_periods=20).mean()
                      -frame.low.rolling(20, min_periods=20).mean()),
    )
    return {'cn_features_'+k: v.shift(1).to_numpy() for k,v in raw.items()}
