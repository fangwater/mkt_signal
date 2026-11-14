import importlib.util
from pathlib import Path
import numpy as np
import pytest

spec=importlib.util.spec_from_file_location('cme_clock',Path(__file__).resolve().parents[2]/'scripts/cme_coarse_factor_reference.py')
ref=importlib.util.module_from_spec(spec);spec.loader.exec_module(ref)


@pytest.mark.parametrize('minutes',[60,1440])
def test_adjacent_sessions_carry_missing_sessions_and_hfq_changes_reset(minutes):
    assert ref.reset_flags(minutes,[True],42,41,1,1).tolist()==[False]
    assert ref.reset_flags(minutes,[True],43,41,1,1).tolist()==[True]
    assert ref.reset_flags(minutes,[True],42,41,2,1).tolist()==[True]
    assert ref.reset_flags(minutes,[True],42,None,1,None).tolist()==[True]


def test_intraday_breaks_and_existing_frequency_rules_are_preserved():
    assert ref.reset_flags(60,[True,False,True,False],42,41).tolist()==[False,False,True,False]
    for minutes in [5,15]:assert ref.reset_flags(minutes,[True,False],42,41).tolist()==[True,False]


def test_official_daily_bar_can_span_multiple_sessions_and_exceed_1440_minutes():
    m=dict(frequency='1day',bar_minutes=1440,ts=[0],end_ts=[169200],last_minute_ts=[169140],
           source_minutes=[2760],segment_start=[True],source_depth_ts_utc_ns=[169200*10**9-1],
           contract_id=['URO'],ric=['UROH22'])
    ref.validate_clock(m,1440)
    m['source_depth_ts_utc_ns'][0]+=1
    with pytest.raises(ValueError):ref.validate_clock(m,1440)


def test_daily_rolling_uses_bars_shift_and_closed_history_only():
    n=100;close=np.arange(n,dtype=float)+100
    kwargs=dict(close=close,high=close+1,low=close-1,bid=close-.1,ask=close+.1,
                bids=np.ones((n,10)),asks=np.ones((n,10)),reset=np.r_[True,np.zeros(n-1,bool)])
    a=ref.representative_factors(**kwargs)
    assert np.isnan(a['lseg_features_baseline_097'][:45]).all()
    assert a['lseg_features_baseline_097'][45]==122.
    assert a['lseg_features_baseline_106'][20]==111.5
    assert a['lseg_features_factor_013'][1]==.5
    b=ref.representative_factors(**{k:v[:70] for k,v in kwargs.items()})
    for name in a:np.testing.assert_array_equal(a[name][:70],b[name])
    kwargs['reset'][60]=True;c=ref.representative_factors(**kwargs)
    assert np.isnan(c['lseg_features_baseline_097'][60:]).all()
