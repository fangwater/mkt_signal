import numpy as np
import pytest
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('ll2_quality',Path(__file__).resolve().parents[2]/'scripts/cme_reject_unordered_ll2.py')
quality=importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)
unordered_known_prices=quality.unordered_known_prices


@pytest.mark.parametrize('bids,asks,expected', [
    ([100,99,98],[101,102,103],False),
    ([100,np.nan,99],[101,np.nan,102],False),
    ([100,np.nan,101],[102,103,104],True),
    ([100,99,98],[101,np.nan,100.5],True),
    ([3072.3,3072.2,3072.6],[3074.3,3074.5,3074.7],True),
    ([30.2,30.185,30.18],[30.21,30.2,30.205],True),
    ([-5,-6,-7],[-4,-3,-2],False),
    ([np.nan,np.nan,np.nan],[np.nan,np.nan,np.nan],False),
])
def test_rank_order_respects_gaps_and_signed_hfq_prices(bids,asks,expected):
    assert unordered_known_prices([bids],[asks]).tolist()==[expected]
