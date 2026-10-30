import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts'))
from build_cn_hfq_5min import EXPECTED_COLUMNS, KEYS
from cn_hfq_coarse_reference import aggregate_coarse, representative_factors


def source(ts):
    n=len(ts)
    d={k:np.ones(n) for k in EXPECTED_COLUMNS if k not in KEYS}
    d.update(contract_id=['RB']*n,ts=np.array(ts,dtype=np.int64))
    return pd.DataFrame(d)


def test_hourly_buckets_stay_within_source_segments_keep_tail_and_null_book():
    x=source(list(range(23*3600+58*60,25*3600+60,60))+list(range(26*3600,27*3600+60,60)))
    x.loc[len(x)-1,'bid4p']=np.nan
    b,m=aggregate_coarse(x,60,20210104)
    assert m['source_minutes']==[60,3,60,1]
    assert m['ts']==x.ts.iloc[[0,60,63,123]].tolist()
    assert m['segment_start_ts']==x.ts.iloc[[0,63]].tolist()
    assert np.isnan(b.bid4p.iloc[-1])
    assert m['continuous_bars'] is True


def test_official_daily_contains_night_and_day_without_filling_gap():
    x=source([0,60,120,3600,3660])
    x['count']=[0.,1.,2.,0.,0.]
    x['open']=[500.,100.,103.,500.,500.]
    x['close']=[500.,102.,105.,500.,500.]
    x['high']=[500.,104.,106.,500.,500.]
    x['low']=[-500.,99.,101.,-500.,-500.]
    x['volume']=[0.,1.,3.,0.,0.]
    x['vwap']=[np.nan,120.,130.,120.,120.]
    b,m=aggregate_coarse(x,1440,20210104)
    assert m['ts']==[0] and m['source_minutes']==[5] and m['end_ts']==[3720]
    assert (b.open[0],b.high[0],b.low[0],b.close[0])==(100.,106.,99.,105.)
    assert b.vwap[0]==127.5


def test_unshortened_windows_continue_over_all_clock_gaps_and_prefixes():
    x=source(np.arange(530)*86400)
    # Include both a weekend and an explicitly absent observation day.
    x['ts']=np.arange(530)*86400+np.where(np.arange(530)>250,4*86400,0)
    x['close']=np.arange(530)+100.
    x['high']=x.close+2
    x['low']=x.close-1
    for i in range(5):
        x[f'bid{i}p']=x.close-i
        x[f'ask{i}p']=x.close+i+1
    y=representative_factors(x)
    assert np.isnan(y['cn_features_factor_031'][:500]).all()
    assert np.isfinite(y['cn_features_factor_031'][500:]).all()
    assert y['cn_features_baseline_097'][45]==122.
    assert y['cn_features_factor_005'][252]==1.
    for k,v in representative_factors(x.iloc[:510]).items():
        np.testing.assert_array_equal(v,y[k][:510])
    x.loc[520,'ask1v']=np.nan
    z=representative_factors(x)
    assert np.isnan(z['cn_features_factor_034'][521])
    assert np.isfinite(z['cn_features_baseline_097'][521])
